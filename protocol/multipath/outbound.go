package multipath

import (
	"context"
	"net"
	"slices"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

func RegisterOutbound(registry *outbound.Registry) {
	outbound.Register[option.MultipathOutboundOptions](registry, C.TypeMultipath, NewOutbound)
}

type Outbound struct {
	outbound.Adapter
	ctx              context.Context
	manager          adapter.OutboundManager
	logger           log.ContextLogger
	tags             []string
	children         []adapter.Outbound
	udpTag           string
	udpOutbound      adapter.Outbound
	aggregation      M.Socksaddr
	cfg              coreConfig
	handshakeTimeout time.Duration
}

func NewOutbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.MultipathOutboundOptions) (adapter.Outbound, error) {
	if len(options.Outbounds) != 2 {
		return nil, E.New("multipath PoC requires exactly 2 outbounds")
	}
	if options.Server == "" || options.ServerPort == 0 {
		return nil, E.New("missing multipath server/server_port")
	}
	preferred := options.Preferred
	if preferred == "" {
		preferred = options.Outbounds[0]
	}
	preferredIndex := slices.Index(options.Outbounds, preferred)
	if preferredIndex < 0 {
		return nil, E.New("preferred outbound is not in outbounds: ", preferred)
	}
	tags := append([]string(nil), options.Outbounds...)
	weights := append([]uint32(nil), options.BandwidthMbps...)
	if len(weights) != 0 && len(weights) != len(tags) {
		return nil, E.New("bandwidth_mbps must be empty or match outbounds length")
	}
	if preferredIndex != 0 {
		tags[0], tags[preferredIndex] = tags[preferredIndex], tags[0]
		if len(weights) > 0 {
			weights[0], weights[preferredIndex] = weights[preferredIndex], weights[0]
		}
	}
	udpTag := options.UDPOutbound
	if udpTag == "" {
		udpTag = preferred
	}
	threshold := options.ActivationThresholdMbps
	if threshold == 0 && options.ActivationAfterBytes == 0 {
		threshold = 150
	}
	window := time.Duration(options.ActivationWindow)
	if window <= 0 {
		window = time.Second
	}
	chunkSize := int(options.ChunkSize)
	if chunkSize == 0 {
		chunkSize = 64 * 1024
	}
	if chunkSize < 1024 || chunkSize > maxFramePayload {
		return nil, E.New("invalid chunk_size")
	}
	queueFrames := int(options.QueueFrames)
	if queueFrames == 0 {
		queueFrames = 256
	}
	if queueFrames < 8 || queueFrames > 4096 {
		return nil, E.New("invalid queue_frames")
	}
	queueBytes := int64(chunkSize) * int64(queueFrames)
	if queueBytes > maxQueueBytes {
		return nil, E.New("chunk_size * queue_frames exceeds 64 MiB")
	}
	maxReorderBufferBytes := int64(options.MaxReorderBytes)
	if maxReorderBufferBytes == 0 {
		maxReorderBufferBytes = 64 << 20
	}
	if maxReorderBufferBytes < int64(chunkSize) || maxReorderBufferBytes > maxReorderBytes {
		return nil, E.New("invalid max_reorder_bytes")
	}
	replayBytes := int64(options.Leg1ReplayBytes)
	if replayBytes == 0 {
		replayBytes = 64 << 20
	}
	if replayBytes < int64(chunkSize) || replayBytes > maxReplayBytes {
		return nil, E.New("invalid leg1_replay_bytes")
	}
	replayTimeout := time.Duration(options.Leg1ReplayTimeout)
	if replayTimeout <= 0 {
		replayTimeout = 5 * time.Second
	}
	if replayTimeout < 100*time.Millisecond || replayTimeout > 5*time.Minute {
		return nil, E.New("invalid leg1_replay_timeout")
	}
	handshakeTimeout := time.Duration(options.HandshakeTimeout)
	if handshakeTimeout <= 0 {
		handshakeTimeout = 10 * time.Second
	}
	if handshakeTimeout < time.Second || handshakeTimeout > time.Minute {
		return nil, E.New("invalid handshake_timeout")
	}
	dependencies := append([]string(nil), options.Outbounds...)
	if !slices.Contains(dependencies, udpTag) {
		dependencies = append(dependencies, udpTag)
	}
	return &Outbound{
		Adapter:          outbound.NewAdapter(C.TypeMultipath, tag, []string{N.NetworkTCP, N.NetworkUDP}, dependencies),
		ctx:              ctx,
		manager:          service.FromContext[adapter.OutboundManager](ctx),
		logger:           logger,
		tags:             tags,
		udpTag:           udpTag,
		aggregation:      M.ParseSocksaddrHostPort(options.Server, options.ServerPort),
		handshakeTimeout: handshakeTimeout,
		cfg: coreConfig{
			ChunkSize:            chunkSize,
			QueueFrames:          queueFrames,
			QueueBytes:           queueBytes,
			ThresholdBytesPS:     uint64(threshold) * 1000 * 1000 / 8,
			ActivationAfterBytes: options.ActivationAfterBytes,
			ActivationWindow:     window,
			BandwidthMbps:        weights,
			MaxReorderFrames:     2048,
			MaxReorderBytes:      maxReorderBufferBytes,
			ReplayBytes:          replayBytes,
			ReplayTimeout:        replayTimeout,
		},
	}, nil
}

func (o *Outbound) Start() error {
	o.children = make([]adapter.Outbound, len(o.tags))
	for index, tag := range o.tags {
		child, loaded := o.manager.Outbound(tag)
		if !loaded {
			return E.New("multipath child outbound not found: ", tag)
		}
		if !slices.Contains(child.Network(), N.NetworkTCP) {
			return E.New("multipath child does not support TCP: ", tag)
		}
		o.children[index] = child
	}
	udpOutbound, loaded := o.manager.Outbound(o.udpTag)
	if !loaded {
		return E.New("multipath UDP outbound not found: ", o.udpTag)
	}
	if !slices.Contains(udpOutbound.Network(), N.NetworkUDP) {
		return E.New("multipath UDP outbound does not support UDP: ", o.udpTag)
	}
	o.udpOutbound = udpOutbound
	return nil
}

func (o *Outbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	switch N.NetworkName(network) {
	case N.NetworkUDP:
		return o.udpOutbound.DialContext(ctx, network, destination)
	case N.NetworkTCP:
	default:
		return nil, E.Extend(N.ErrUnknownNetwork, network)
	}
	if len(o.children) != 2 {
		return nil, E.New("multipath outbound is not started")
	}
	sessionID, err := newSessionID()
	if err != nil {
		return nil, E.Cause(err, "generate multipath session id")
	}
	destinationString := destination.String()
	primaryConn, err := o.children[0].DialContext(ctx, N.NetworkTCP, o.aggregation)
	if err != nil {
		return nil, E.Cause(err, "dial multipath preferred leg ", o.tags[0])
	}
	response, err := o.clientHandshake(ctx, primaryConn, helloMessage{
		Session:     sessionID,
		LegID:       0,
		ChunkSize:   uint32(o.cfg.ChunkSize),
		Destination: destinationString,
	})
	if err != nil {
		primaryConn.Close()
		return nil, E.Cause(err, "multipath preferred handshake")
	}
	cfg := o.cfg
	cfg.ChunkSize = int(response.ChunkSize)
	cfg.QueueBytes = int64(cfg.ChunkSize) * int64(cfg.QueueFrames)
	cfg.OnActivate = func() {
		o.logger.InfoContext(ctx, "multipath booster activated for ", destination)
	}
	core, appConn := newCore(ctx, cfg)
	if _, err = core.addLeg(0, primaryConn, nil); err != nil {
		appConn.Close()
		primaryConn.Close()
		return nil, err
	}
	o.logger.InfoContext(ctx, "multipath connection to ", destination, " via preferred ", o.tags[0])
	go o.joinSecondary(core, sessionID, uint32(cfg.ChunkSize), destinationString)
	return appConn, nil
}

func (o *Outbound) clientHandshake(ctx context.Context, conn net.Conn, message helloMessage) (helloResponse, error) {
	deadline := time.Now().Add(o.handshakeTimeout)
	if contextDeadline, loaded := ctx.Deadline(); loaded && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return helloResponse{}, err
	}
	defer conn.SetDeadline(time.Time{})
	if err := writeHello(conn, message); err != nil {
		return helloResponse{}, err
	}
	return readHelloResponse(conn)
}

func (o *Outbound) joinSecondary(core *mpCore, sessionID [16]byte, chunkSize uint32, destination string) {
	ctx := core.Context()
	for {
		select {
		case <-core.Done():
			return
		default:
		}
		attemptCtx, cancel := context.WithTimeout(ctx, o.handshakeTimeout)
		conn, err := o.children[1].DialContext(attemptCtx, N.NetworkTCP, o.aggregation)
		if err == nil {
			_, err = o.clientHandshake(attemptCtx, conn, helloMessage{
				Session:     sessionID,
				LegID:       1,
				ChunkSize:   chunkSize,
				Destination: destination,
			})
		}
		var leg *mpLeg
		if err == nil {
			leg, err = core.addLeg(1, conn, nil)
		}
		cancel()
		if err == nil {
			o.logger.InfoContext(ctx, "multipath secondary leg ready via ", o.tags[1])
			select {
			case <-core.Done():
				return
			case <-leg.Done():
				o.logger.WarnContext(ctx, "multipath secondary leg lost; retrying via ", o.tags[1])
			}
			continue
		}
		if conn != nil {
			conn.Close()
		}
		o.logger.WarnContext(ctx, "multipath secondary leg failed: ", err)
		retryTimer := time.NewTimer(2 * time.Second)
		select {
		case <-core.Done():
			retryTimer.Stop()
			return
		case <-retryTimer.C:
		}
	}
}

func (o *Outbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	if o.udpOutbound == nil {
		return nil, E.New("multipath outbound is not started")
	}
	return o.udpOutbound.ListenPacket(ctx, destination)
}
