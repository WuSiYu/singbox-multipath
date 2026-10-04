package multipath

import (
	"container/list"
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
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
	group         *recoveryServerGroup
	recoveryEntry *list.Element
	id            [16]byte
	destination   M.Socksaddr
	frameSize     uint32
	policy        sessionPolicy
	requestStatus bool
	core          *mpCore
	appConn       net.Conn
}

type Inbound struct {
	inbound.Adapter
	ctx              context.Context
	router           adapter.ConnectionRouterEx
	logger           log.ContextLogger
	listener         *listener.Listener
	cfg              coreConfig
	handshakeTimeout time.Duration

	access         sync.Mutex
	sessions       map[[16]byte]*serverSession
	statusWake     chan struct{}
	recoveryMu     sync.Mutex
	recoveryGroups map[[16]byte]*recoveryServerGroup
	recoveryClosed bool
	recoveryCancel context.CancelFunc

	psk        string
	allowedIPs []netip.Prefix
	// Closed session IDs (access) reject delayed creates and joins; seen
	// nonces (nonceMu) reject replayed authenticated hellos.
	closedSessions map[[16]byte]time.Time
	nonceMu        sync.Mutex
	nonces         map[[16]byte]time.Time
}

const (
	closedSessionRetention = 2 * time.Minute
	closedSessionCharge    = 128
	maxHelloNonces         = 1 << 16
)

func NewInbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.MultipathInboundOptions) (adapter.Inbound, error) {
	warnDeprecated(logger, options.MultipathDeprecatedFlatOptions, "")
	memoryLimit, automaticMemoryLimit, memoryErr := resolveMemoryLimit(options.MemoryLimit.Value())
	if memoryErr != nil {
		logger.Warn("detect available memory for multipath: ", memoryErr, "; using 256 MiB fallback")
	}
	memory := newMemoryBudget(memoryLimit, automaticMemoryLimit)
	if memoryLimit < minimumSessionMemory(coreConfig{FrameSize: 1024}) {
		return nil, E.New("memory_limit is too small for one multipath session")
	}
	handshakeTimeout := time.Duration(options.HandshakeTimeout)
	if handshakeTimeout <= 0 {
		handshakeTimeout = 10 * time.Second
	}
	if handshakeTimeout < time.Second || handshakeTimeout > time.Minute {
		return nil, E.New("invalid handshake_timeout")
	}
	i := &Inbound{
		Adapter:          inbound.NewAdapter(C.TypeMultipath, tag),
		ctx:              ctx,
		router:           router,
		logger:           logger,
		sessions:         make(map[[16]byte]*serverSession),
		statusWake:       make(chan struct{}, 1),
		handshakeTimeout: handshakeTimeout,
		recoveryGroups:   make(map[[16]byte]*recoveryServerGroup),
		psk:              options.PSK,
		allowedIPs:       options.AllowedIPs,
		closedSessions:   make(map[[16]byte]time.Time),
		nonces:           make(map[[16]byte]time.Time),
		cfg:              coreConfig{Memory: memory, HandshakeTimeout: handshakeTimeout}}
	if i.psk == "" && len(i.allowedIPs) == 0 && !listenIsPrivate(options.ListenOptions) {
		logger.Warn("multipath listener has neither psk nor allowed_ips and is not bound to a private address; anyone who can reach it can relay through this server")
	}
	i.listener = listener.New(listener.Options{
		Context:           ctx,
		Logger:            logger,
		Network:           []string{N.NetworkTCP, N.NetworkUDP},
		Listen:            options.ListenOptions,
		ConnectionHandler: i,
		PacketHandler:     i,
	})
	return i, nil
}

func (i *Inbound) Start(stage adapter.StartStage) error {
	if stage != adapter.StartStateStart {
		return nil
	}
	if err := i.listener.Start(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(i.ctx)
	i.recoveryCancel = cancel
	i.cfg.Memory.startLogging(ctx, i.logger, "server")
	go i.senderStatusLoop(ctx)
	go i.recoveryMaintenance(ctx)
	return nil
}

func (i *Inbound) Close() error {
	if i.recoveryCancel != nil {
		i.recoveryCancel()
	}
	i.cfg.Memory.stopLogging()
	i.recoveryMu.Lock()
	i.recoveryClosed = true
	var groups []*recoveryServerGroup
	for _, g := range i.recoveryGroups {
		groups = append(groups, g)
	}
	clear(i.recoveryGroups)
	i.recoveryMu.Unlock()
	for _, g := range groups {
		g.close()
	}
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
	if !i.sourceAllowed(metadata.Source) {
		N.CloseOnHandshakeFailure(conn, onClose, E.New("multipath client ", metadata.Source, " is not in allowed_ips"))
		return
	}
	_ = conn.SetDeadline(time.Now().Add(i.handshakeTimeout))
	hello, auth, err := readHelloWithAuth(conn)
	if err != nil {
		var versionErr *helloVersionError
		if errors.As(err, &versionErr) {
			i.rejectHello(conn, onClose, helloRejectVersion, err)
			return
		}
		N.CloseOnHandshakeFailure(conn, onClose, E.Cause(err, "read multipath hello"))
		return
	}
	if i.psk != "" {
		if err = verifyHelloAuth(auth, i.psk, time.Now(), i.rememberNonce); err != nil {
			i.rejectHello(conn, onClose, helloRejectAuthentication, err)
			return
		}
	}
	if hello.LegID > 1 {
		i.rejectHello(conn, onClose, helloRejectInvalidLegID, E.New("invalid multipath leg id: ", hello.LegID))
		return
	}
	if hello.Control {
		i.serveRecoveryControl(conn, hello, onClose)
		return
	}
	var group *recoveryServerGroup
	if hello.Recovery {
		group = i.recoveryGroup(hello.Group)
		if group == nil {
			i.rejectHello(conn, onClose, helloRejectSessionMismatch, errRecoveryNotReady)
			return
		}
		group.policy.update(hello.RecoveryEpoch, hello.RecoveryMask, hello.RecoveryUDP)
	}
	destination := M.ParseSocksaddr(hello.Destination)
	if !destination.IsValid() || destination.Port == 0 {
		i.rejectHello(conn, onClose, helloRejectInvalidDestination, E.New("invalid multipath destination: ", hello.Destination))
		return
	}

	i.access.Lock()
	session := i.sessions[hello.Session]
	if session != nil {
		if session.destination.String() != destination.String() || session.frameSize != hello.FrameSize || session.policy != hello.Policy || session.requestStatus != hello.RequestStatus || session.group != group {
			i.access.Unlock()
			i.rejectHello(conn, onClose, helloRejectSessionMismatch, E.New("multipath session parameters mismatch"))
			return
		}
		// Any leg may (re)join while its slot is free.
		err = session.core.reserveLeg(hello.LegID)
		i.access.Unlock()
		if err != nil {
			i.rejectHello(conn, onClose, helloRejectLegUnavailable, err)
			return
		}
		if err = writeHelloResponse(conn, helloResponse{Status: helloStatusOK, FrameSize: session.frameSize, PolicyDigest: session.policy.digest()}); err != nil {
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
	if _, closed := i.closedSessions[hello.Session]; !hello.Create || closed {
		i.access.Unlock()
		i.rejectHello(conn, onClose, helloRejectSessionUnavailable, E.New("multipath session is not established or already closed"))
		return
	}
	if group != nil {
		group.mu.Lock()
		_, closed := group.closedTCP[hello.Session]
		unavailable := group.closed || closed
		group.mu.Unlock()
		if unavailable {
			i.access.Unlock()
			i.rejectHello(conn, onClose, helloRejectSessionUnavailable, errCoreClosed)
			return
		}
	}
	if err = hello.Policy.validate(int(hello.FrameSize)); err != nil {
		i.access.Unlock()
		i.rejectHello(conn, onClose, helloRejectPolicy, err)
		return
	}
	cfg, err := configForPolicy(i.cfg.Memory, int(hello.FrameSize), hello.Policy.Download, hello.Policy.Upload)
	if err != nil {
		i.access.Unlock()
		i.rejectHello(conn, onClose, helloRejectPolicy, err)
		return
	}
	cfg.HandshakeTimeout = i.handshakeTimeout
	cfg.LegAbsentTimeout = legAbsentTimeout(i.handshakeTimeout)
	if group != nil {
		cfg.Recovery = &group.policy
	}
	cfg.OnProtocolError = func(err error) {
		i.logger.ErrorContext(ctx, "multipath protocol error for ", destination, ": ", err)
	}
	if hello.RequestStatus {
		cfg.OnStatusEvent = i.wakeSenderStatus
		cfg.SendStatus = true
	}
	cfg.OnLeg1Active = func(info activationInfo, reconnect bool) {
		i.logger.InfoContext(
			ctx,
			"multipath leg1 joined data path: side=server destination=", destination,
			" reconnect=", reconnect,
			" ", info.String(),
		)
	}
	if group != nil {
		group.mu.Lock()
		if group.closed || !i.cfg.Memory.reservePage(128, true) {
			group.mu.Unlock()
			i.access.Unlock()
			i.rejectHello(conn, onClose, helloRejectLegUnavailable, errMemoryLimit)
			return
		}
		group.closedTCP[hello.Session] = time.Time{}
		group.mu.Unlock()
	}
	core, appConn, err := newCoreWithError(i.ctx, cfg)
	if err != nil {
		if group != nil {
			group.mu.Lock()
			if !group.closed {
				group.closedTCP[hello.Session] = time.Now()
			}
			group.mu.Unlock()
		}
		i.access.Unlock()
		i.rejectHello(conn, onClose, helloRejectLegUnavailable, E.Cause(err, "create multipath session"))
		return
	}
	session = &serverSession{
		group:         group,
		id:            hello.Session,
		destination:   destination,
		frameSize:     uint32(cfg.FrameSize),
		policy:        hello.Policy,
		requestStatus: hello.RequestStatus,
		core:          core,
		appConn:       appConn,
	}
	if err = core.reserveLeg(hello.LegID); err != nil {
		i.access.Unlock()
		appConn.Close()
		i.rejectHello(conn, onClose, helloRejectLegUnavailable, err)
		return
	}
	i.sessions[hello.Session] = session
	if group != nil {
		group.mu.Lock()
		if !group.closed {
			session.recoveryEntry = group.tcpSessions.PushBack(hello.Session)
		}
		group.mu.Unlock()
	}
	i.access.Unlock()
	if err = writeHelloResponse(conn, helloResponse{Status: helloStatusOK, FrameSize: session.frameSize, PolicyDigest: session.policy.digest()}); err != nil {
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
		// The router can report a target dial/early-write failure before it
		// calls Close on appConn. A prior MP failure is already frozen and
		// cannot be reclassified by this application-side cleanup callback.
		core.noteCloseSource(closeSourceLocalEndpoint)
		if closeErr == nil || errors.Is(closeErr, io.EOF) {
			_ = appConn.Close()
		} else {
			core.fail(closeErr)
		}
	})
	go func() {
		<-core.released
		i.removeSession(hello.Session, session)
	}()
	i.router.RouteConnectionEx(ctx, appConn, metadata, logicalOnClose)
}

func (i *Inbound) rejectHello(conn net.Conn, onClose N.CloseHandlerFunc, reason helloRejectReason, err error) {
	_ = writeHelloResponse(conn, helloResponse{Status: helloStatusRejected, RejectReason: reason})
	N.CloseOnHandshakeFailure(conn, onClose, err)
}

func (i *Inbound) removeSession(id [16]byte, session *serverSession) {
	i.access.Lock()
	if current := i.sessions[id]; current == session {
		delete(i.sessions, id)
		if i.closedSessions == nil {
			i.closedSessions = make(map[[16]byte]time.Time)
		}
		if _, exists := i.closedSessions[id]; !exists && i.cfg.Memory.reservePage(closedSessionCharge, true) {
			i.closedSessions[id] = time.Now()
		}
		if session.group != nil {
			session.group.mu.Lock()
			if session.recoveryEntry != nil {
				session.group.tcpSessions.Remove(session.recoveryEntry)
				session.recoveryEntry = nil
			}
			if !session.group.closed {
				session.group.closedTCP[id] = time.Now()
			}
			session.group.mu.Unlock()
		}
	}
	i.access.Unlock()
}

func (i *Inbound) sourceAllowed(source M.Socksaddr) bool {
	if len(i.allowedIPs) == 0 {
		return true
	}
	address := source.Addr.Unmap()
	for _, prefix := range i.allowedIPs {
		if prefix.Contains(address) {
			return true
		}
	}
	return false
}

// rememberNonce records a hello nonce; it reports false for a replay.
func (i *Inbound) rememberNonce(nonce [16]byte, now time.Time) bool {
	i.nonceMu.Lock()
	defer i.nonceMu.Unlock()
	if i.nonces == nil {
		i.nonces = make(map[[16]byte]time.Time)
	}
	if _, seen := i.nonces[nonce]; seen {
		return false
	}
	if len(i.nonces) >= maxHelloNonces {
		i.pruneNoncesLocked(now)
		if len(i.nonces) >= maxHelloNonces {
			return false
		}
	}
	i.nonces[nonce] = now
	return true
}

func (i *Inbound) pruneNoncesLocked(now time.Time) {
	for nonce, at := range i.nonces {
		if now.Sub(at) >= 2*helloAuthSkew {
			delete(i.nonces, nonce)
		}
	}
}

// expireClosedSessions forgets tombstones and nonces after their retention.
func (i *Inbound) expireClosedSessions(now time.Time) {
	i.access.Lock()
	for id, at := range i.closedSessions {
		if now.Sub(at) >= closedSessionRetention {
			delete(i.closedSessions, id)
			i.cfg.Memory.releaseSession(closedSessionCharge)
		}
	}
	i.access.Unlock()
	i.nonceMu.Lock()
	i.pruneNoncesLocked(now)
	i.nonceMu.Unlock()
}

func listenIsPrivate(options option.ListenOptions) bool {
	if options.Listen == nil {
		return false
	}
	address := netip.Addr(*options.Listen)
	return address.IsLoopback() || address.IsPrivate() || address.IsLinkLocalUnicast()
}

func (i *Inbound) wakeSenderStatus() {
	select {
	case i.statusWake <- struct{}{}:
	default:
	}
}

func (i *Inbound) sendSenderStatus(now time.Time, force bool) {
	i.access.Lock()
	sessions := make([]*serverSession, 0, len(i.sessions))
	for _, session := range i.sessions {
		sessions = append(sessions, session)
	}
	i.access.Unlock()
	for _, session := range sessions {
		session.core.queueSenderStatus(now, force)
	}
}

func (i *Inbound) senderStatusLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			i.sendSenderStatus(now, false)
		case <-i.statusWake:
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
				i.sendSenderStatus(time.Now(), true)
			}
		}
	}
}
