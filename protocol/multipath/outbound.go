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
	ctx         context.Context
	manager     adapter.OutboundManager
	logger      log.ContextLogger
	tags        []string
	children    []adapter.Outbound
	udpTag      string
	udpOutbound adapter.Outbound
	aggregation M.Socksaddr
	cfg         coreConfig
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
	if threshold == 0 {
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
	dependencies := append([]string(nil), options.Outbounds...)
	if !slices.Contains(dependencies, udpTag) {
		dependencies = append(dependencies, udpTag)
	}
	return &Outbound{
		Adapter:     outbound.NewAdapter(C.TypeMultipath, tag, []string{N.NetworkTCP, N.NetworkUDP}, dependencies),
		ctx:         ctx,
		manager:     service.FromContext[adapter.OutboundManager](ctx),
		logger:      logger,
		tags:        tags,
		udpTag:      udpTag,
		aggregation: M.ParseSocksaddrHostPort(options.Server, options.ServerPort),
		cfg: coreConfig{
			ChunkSize:        chunkSize,
			QueueFrames:      queueFrames,
			ThresholdBytesPS: uint64(threshold) * 1000 * 1000 / 8,
			ActivationWindow: window,
			BandwidthMbps:    weights,
			MaxReorderFrames: 2048,
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
	if err = writeHello(primaryConn, helloMessage{Session: sessionID, LegID: 0, Destination: destinationString}); err != nil {
		primaryConn.Close()
		return nil, E.Cause(err, "write multipath preferred hello")
	}
	cfg := o.cfg
	cfg.OnActivate = func() {
		o.logger.InfoContext(ctx, "multipath booster activated for ", destination)
	}
	core, appConn := newCore(cfg)
	if err = core.addLeg(0, primaryConn, nil); err != nil {
		appConn.Close()
		return nil, err
	}
	o.logger.InfoContext(ctx, "multipath connection to ", destination, " via preferred ", o.tags[0])
	secondaryCtx := context.WithoutCancel(ctx)
	go o.joinSecondary(secondaryCtx, core, sessionID, destinationString)
	return appConn, nil
}

func (o *Outbound) joinSecondary(ctx context.Context, core *mpCore, sessionID [16]byte, destination string) {
	for {
		select {
		case <-core.Done():
			return
		default:
		}
		conn, err := o.children[1].DialContext(ctx, N.NetworkTCP, o.aggregation)
		if err == nil {
			err = writeHello(conn, helloMessage{Session: sessionID, LegID: 1, Destination: destination})
		}
		if err == nil {
			err = core.addLeg(1, conn, nil)
		}
		if err == nil {
			o.logger.InfoContext(ctx, "multipath secondary leg ready via ", o.tags[1])
			return
		}
		if conn != nil {
			conn.Close()
		}
		o.logger.WarnContext(ctx, "multipath secondary leg failed: ", err)
		select {
		case <-core.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (o *Outbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	if o.udpOutbound == nil {
		return nil, E.New("multipath outbound is not started")
	}
	return o.udpOutbound.ListenPacket(ctx, destination)
}
