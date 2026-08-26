package multipath

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/common/listener"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func RegisterInbound(registry *inbound.Registry) {
	inbound.Register[option.MultipathInboundOptions](registry, C.TypeMultipath, NewInbound)
}

type serverSession struct {
	id          [16]byte
	destination M.Socksaddr
	chunkSize   uint32
	core        *mpCore
	appConn     net.Conn
}

type Inbound struct {
	inbound.Adapter
	ctx              context.Context
	router           adapter.ConnectionRouterEx
	logger           log.ContextLogger
	listener         *listener.Listener
	cfg              coreConfig
	handshakeTimeout time.Duration

	access   sync.Mutex
	sessions map[[16]byte]*serverSession
}

func NewInbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.MultipathInboundOptions) (adapter.Inbound, error) {
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
	maxReorderFrames := int(options.MaxReorderFrames)
	if maxReorderFrames == 0 {
		maxReorderFrames = 2048
	}
	if maxReorderFrames < 64 || maxReorderFrames > 65536 {
		return nil, E.New("invalid max_reorder_frames")
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
	if len(options.BandwidthMbps) != 0 && len(options.BandwidthMbps) != 2 {
		return nil, E.New("multipath PoC bandwidth_mbps must contain exactly 2 entries")
	}
	i := &Inbound{
		Adapter:          inbound.NewAdapter(C.TypeMultipath, tag),
		ctx:              ctx,
		router:           router,
		logger:           logger,
		sessions:         make(map[[16]byte]*serverSession),
		handshakeTimeout: handshakeTimeout,
		cfg: coreConfig{
			ChunkSize:            chunkSize,
			QueueFrames:          queueFrames,
			QueueBytes:           queueBytes,
			ThresholdBytesPS:     uint64(threshold) * 1000 * 1000 / 8,
			ActivationAfterBytes: options.ActivationAfterBytes,
			ActivationWindow:     window,
			BandwidthMbps:        append([]uint32(nil), options.BandwidthMbps...),
			MaxReorderFrames:     maxReorderFrames,
			MaxReorderBytes:      maxReorderBufferBytes,
			ReplayBytes:          replayBytes,
			ReplayTimeout:        replayTimeout,
		},
	}
	i.listener = listener.New(listener.Options{
		Context:           ctx,
		Logger:            logger,
		Network:           []string{N.NetworkTCP},
		Listen:            options.ListenOptions,
		ConnectionHandler: i,
	})
	return i, nil
}

func (i *Inbound) Start(stage adapter.StartStage) error {
	if stage != adapter.StartStateStart {
		return nil
	}
	return i.listener.Start()
}

func (i *Inbound) Close() error {
	listenerErr := i.listener.Close()
	i.access.Lock()
	sessions := make([]*serverSession, 0, len(i.sessions))
	for _, session := range i.sessions {
		sessions = append(sessions, session)
	}
	i.sessions = make(map[[16]byte]*serverSession)
	i.access.Unlock()
	for _, session := range sessions {
		session.core.Close()
	}
	return listenerErr
}

func (i *Inbound) NewConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	_ = conn.SetDeadline(time.Now().Add(i.handshakeTimeout))
	hello, err := readHello(conn)
	if err != nil {
		N.CloseOnHandshakeFailure(conn, onClose, E.Cause(err, "read multipath hello"))
		return
	}
	if hello.LegID > 1 {
		i.rejectHello(conn, onClose, E.New("invalid multipath leg id: ", hello.LegID))
		return
	}
	destination := M.ParseSocksaddr(hello.Destination)
	if !destination.IsValid() || destination.Port == 0 {
		i.rejectHello(conn, onClose, E.New("invalid multipath destination: ", hello.Destination))
		return
	}

	i.access.Lock()
	session := i.sessions[hello.Session]
	if session != nil {
		if session.destination.String() != destination.String() || session.chunkSize != hello.ChunkSize {
			i.access.Unlock()
			i.rejectHello(conn, onClose, E.New("multipath session parameters mismatch"))
			return
		}
		if hello.LegID != 1 {
			i.access.Unlock()
			i.rejectHello(conn, onClose, E.New("multipath session already has a control leg"))
			return
		}
		err = session.core.reserveLeg(hello.LegID)
		i.access.Unlock()
		if err != nil {
			i.rejectHello(conn, onClose, err)
			return
		}
		if err = writeHelloResponse(conn, helloResponse{Status: helloStatusOK, ChunkSize: session.chunkSize}); err != nil {
			session.core.cancelLegReservation(hello.LegID)
			N.CloseOnHandshakeFailure(conn, onClose, E.Cause(err, "write multipath hello response"))
			return
		}
		_ = conn.SetDeadline(time.Time{})
		if _, err = session.core.commitLeg(hello.LegID, conn, onClose); err != nil {
			N.CloseOnHandshakeFailure(conn, onClose, err)
			return
		}
		i.logger.InfoContext(ctx, "multipath leg ", hello.LegID, " joined session for ", destination)
		return
	}
	if hello.LegID != 0 {
		i.access.Unlock()
		i.rejectHello(conn, onClose, E.New("multipath control leg must create the session"))
		return
	}
	if int(hello.ChunkSize) > i.cfg.ChunkSize {
		i.access.Unlock()
		i.rejectHello(conn, onClose, E.New("multipath requested chunk size exceeds server limit: ", hello.ChunkSize, " > ", i.cfg.ChunkSize))
		return
	}

	cfg := i.cfg
	cfg.ChunkSize = int(hello.ChunkSize)
	cfg.QueueBytes = int64(cfg.ChunkSize) * int64(cfg.QueueFrames)
	cfg.OnActivate = func() {
		i.logger.InfoContext(ctx, "multipath server booster activated for ", destination)
	}
	core, appConn := newCore(i.ctx, cfg)
	session = &serverSession{
		id:          hello.Session,
		destination: destination,
		chunkSize:   uint32(cfg.ChunkSize),
		core:        core,
		appConn:     appConn,
	}
	if err = core.reserveLeg(hello.LegID); err != nil {
		i.access.Unlock()
		appConn.Close()
		i.rejectHello(conn, onClose, err)
		return
	}
	i.sessions[hello.Session] = session
	i.access.Unlock()
	if err = writeHelloResponse(conn, helloResponse{Status: helloStatusOK, ChunkSize: session.chunkSize}); err != nil {
		core.cancelLegReservation(hello.LegID)
		core.fail(err)
		i.removeSession(hello.Session, session)
		N.CloseOnHandshakeFailure(conn, onClose, E.Cause(err, "write multipath hello response"))
		return
	}
	_ = conn.SetDeadline(time.Time{})
	if _, err = core.commitLeg(hello.LegID, conn, onClose); err != nil {
		core.fail(err)
		i.removeSession(hello.Session, session)
		N.CloseOnHandshakeFailure(conn, onClose, E.Cause(err, "commit multipath control leg"))
		return
	}

	metadata.Inbound = i.Tag()
	metadata.InboundType = i.Type()
	metadata.Destination = destination
	i.logger.InfoContext(ctx, "multipath session established to ", destination, " on leg ", hello.LegID)
	logicalOnClose := N.OnceClose(func(closeErr error) {
		core.fail(closeErr)
		i.removeSession(hello.Session, session)
	})
	go func() {
		<-core.Done()
		i.removeSession(hello.Session, session)
	}()
	i.router.RouteConnectionEx(ctx, appConn, metadata, logicalOnClose)
}

func (i *Inbound) rejectHello(conn net.Conn, onClose N.CloseHandlerFunc, err error) {
	_ = writeHelloResponse(conn, helloResponse{Status: helloStatusRejected})
	N.CloseOnHandshakeFailure(conn, onClose, err)
}

func (i *Inbound) removeSession(id [16]byte, session *serverSession) {
	i.access.Lock()
	if current := i.sessions[id]; current == session {
		delete(i.sessions, id)
	}
	i.access.Unlock()
}
