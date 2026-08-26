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
	core        *mpCore
	appConn     net.Conn
}

type Inbound struct {
	inbound.Adapter
	ctx      context.Context
	router   adapter.ConnectionRouterEx
	logger   log.ContextLogger
	listener *listener.Listener
	cfg      coreConfig

	access   sync.Mutex
	sessions map[[16]byte]*serverSession
}

func NewInbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.MultipathInboundOptions) (adapter.Inbound, error) {
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
	maxReorderFrames := int(options.MaxReorderFrames)
	if maxReorderFrames == 0 {
		maxReorderFrames = 2048
	}
	if maxReorderFrames < 64 || maxReorderFrames > 65536 {
		return nil, E.New("invalid max_reorder_frames")
	}
	if len(options.BandwidthMbps) != 0 && len(options.BandwidthMbps) != 2 {
		return nil, E.New("multipath PoC bandwidth_mbps must contain exactly 2 entries")
	}
	i := &Inbound{
		Adapter:  inbound.NewAdapter(C.TypeMultipath, tag),
		ctx:      ctx,
		router:   router,
		logger:   logger,
		sessions: make(map[[16]byte]*serverSession),
		cfg: coreConfig{
			ChunkSize:        chunkSize,
			QueueFrames:      queueFrames,
			ThresholdBytesPS: uint64(threshold) * 1000 * 1000 / 8,
			ActivationWindow: window,
			BandwidthMbps:    append([]uint32(nil), options.BandwidthMbps...),
			MaxReorderFrames: maxReorderFrames,
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
	return i.listener.Close()
}

func (i *Inbound) NewConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	hello, err := readHello(conn)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		N.CloseOnHandshakeFailure(conn, onClose, E.Cause(err, "read multipath hello"))
		return
	}
	if hello.LegID > 1 {
		N.CloseOnHandshakeFailure(conn, onClose, E.New("invalid multipath leg id: ", hello.LegID))
		return
	}
	destination := M.ParseSocksaddr(hello.Destination)
	if !destination.IsValid() || destination.Port == 0 {
		N.CloseOnHandshakeFailure(conn, onClose, E.New("invalid multipath destination: ", hello.Destination))
		return
	}

	i.access.Lock()
	session := i.sessions[hello.Session]
	if session != nil {
		if session.destination.String() != destination.String() {
			i.access.Unlock()
			N.CloseOnHandshakeFailure(conn, onClose, E.New("multipath session destination mismatch"))
			return
		}
		err = session.core.addLeg(hello.LegID, conn, onClose)
		i.access.Unlock()
		if err != nil {
			N.CloseOnHandshakeFailure(conn, onClose, err)
			return
		}
		i.logger.InfoContext(ctx, "multipath leg ", hello.LegID, " joined session for ", destination)
		return
	}

	cfg := i.cfg
	cfg.OnActivate = func() {
		i.logger.InfoContext(ctx, "multipath server booster activated for ", destination)
	}
	core, appConn := newCore(cfg)
	session = &serverSession{
		id:          hello.Session,
		destination: destination,
		core:        core,
		appConn:     appConn,
	}
	if err = core.addLeg(hello.LegID, conn, onClose); err != nil {
		i.access.Unlock()
		appConn.Close()
		N.CloseOnHandshakeFailure(conn, onClose, err)
		return
	}
	i.sessions[hello.Session] = session
	i.access.Unlock()

	metadata.Inbound = i.Tag()
	metadata.InboundType = i.Type()
	metadata.Destination = destination
	i.logger.InfoContext(ctx, "multipath session established to ", destination, " on leg ", hello.LegID)
	logicalOnClose := N.OnceClose(func(closeErr error) {
		core.fail(closeErr)
		i.removeSession(hello.Session, session)
	})
	i.router.RouteConnectionEx(ctx, appConn, metadata, logicalOnClose)
}

func (i *Inbound) removeSession(id [16]byte, session *serverSession) {
	i.access.Lock()
	if current := i.sessions[id]; current == session {
		delete(i.sessions, id)
	}
	i.access.Unlock()
}
