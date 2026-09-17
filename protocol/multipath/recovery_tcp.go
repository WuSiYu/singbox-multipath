package multipath

import (
	"context"
	"net"
	"sync/atomic"
	"time"

	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func (o *Outbound) recoveryHello(session [16]byte, id byte, destination string, create bool) helloMessage {
	h := helloMessage{Session: session, LegID: id, RequestStatus: o.statusFile != "", FrameSize: uint32(o.cfg.FrameSize), Destination: destination, Policy: o.policy, Recovery: true, Group: o.recovery.id, Create: create}
	h.RecoveryEpoch, h.RecoveryMask, h.RecoveryUDP = o.recovery.policy.snapshot()
	return h
}

func (o *Outbound) dialRecovery(ctx context.Context, destination M.Socksaddr) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, o.handshakeTimeout)
	retainDialContext := false
	defer func() {
		if !retainDialContext {
			cancel()
		}
	}()
	id, err := o.recovery.waitReady(ctx)
	if err != nil {
		return nil, err
	}
	session, err := newSessionID()
	if err != nil {
		return nil, err
	}
	var conn net.Conn
	var message helloMessage
	var lazyCancel context.CancelFunc
	for attempt := 0; attempt < 2; attempt++ {
		attemptCtx, attemptCancel := context.WithTimeout(ctx, min(o.failoverTimeout, o.handshakeTimeout/2))
		message = o.recoveryHello(session, id, destination.String(), true)
		o.recovery.attempts[id].Add(1)
		conn, err = o.children[id].DialContext(attemptCtx, N.NetworkTCP, o.aggregation)
		if err == nil && !o.tcpFastOpen {
			err = o.clientHandshake(attemptCtx, conn, message)
		}
		if err == nil && o.tcpFastOpen {
			lazyCancel = attemptCancel
		} else {
			attemptCancel()
		}
		if err == nil {
			break
		}
		if conn != nil {
			conn.Close()
		}
		if attempt == 1 || !o.recovery.policy.allows(1-id) {
			return nil, err
		}
		id = 1 - id
	}
	var confirmed atomic.Bool
	var early *clientFastOpenConn
	var preamble func(net.Conn) error
	if o.tcpFastOpen {
		early, err = newClientFastOpenConn(conn, message, time.Now().Add(min(o.handshakeTimeout, o.failoverTimeout)))
		if err != nil {
			conn.Close()
			lazyCancel()
			return nil, err
		}
		conn = early
		preamble = func(conn net.Conn) error {
			if err := early.waitStarted(); err != nil {
				return err
			}
			response, err := readHelloResponse(conn)
			if err != nil {
				return err
			}
			if response.FrameSize != message.FrameSize || response.PolicyDigest != message.Policy.digest() {
				return errRecoveryNotReady
			}
			confirmed.Store(true)
			return conn.SetDeadline(time.Time{})
		}
	} else {
		confirmed.Store(true)
	}
	core, app, err := newCoreWithError(ctx, o.connectionCoreConfig(ctx, destination, session))
	if err != nil {
		conn.Close()
		if lazyCancel != nil {
			lazyCancel()
		}
		return nil, err
	}
	if _, err = core.addLegWithReadPreamble(id, conn, nil, preamble); err != nil {
		core.Close()
		conn.Close()
		if lazyCancel != nil {
			lazyCancel()
		}
		return nil, err
	}
	status := o.registerStatusSession(session, destination.String(), core, leg1PhaseWaiting)
	stop := context.AfterFunc(o.recovery.ctx, func() { core.Close() })
	go func() { <-core.Done(); stop() }()
	// Joining before the first write would defeat lazy TFO. After it starts,
	// either path may complete session creation; a server tombstone prevents a
	// late retry from creating a second target connection for the same ID.
	if early != nil {
		retainDialContext = true
	}
	go func() {
		if early != nil {
			_ = early.waitStarted()
			lazyCancel()
			cancel()
		}
		createDeadline := time.Now().Add(o.handshakeTimeout)
		for id := byte(0); id < 2; id++ {
			go o.rejoinRecovery(core, session, id, destination.String(), &confirmed, createDeadline, status)
		}
	}()
	if early != nil {
		return &earlyLogicalConn{Conn: app, core: core, primary: early}, nil
	}
	return app, nil
}

func (o *Outbound) rejoinRecovery(core *mpCore, session [16]byte, id byte, destination string, confirmed *atomic.Bool, createDeadline time.Time, status *statusSession) {
	for !core.isDone() && o.recovery.ctx.Err() == nil {
		if leg := core.getLeg(id); leg != nil {
			select {
			case <-leg.Done():
			case <-core.Done():
				return
			case <-o.recovery.ctx.Done():
				core.Close()
				return
			}
			continue
		}
		if !confirmed.Load() && time.Now().After(createDeadline) {
			core.fail(errRecoveryNotReady)
			return
		}
		if !o.recovery.policy.allows(id) {
			if !recoveryWait(core.ctx, 100*time.Millisecond) {
				return
			}
			continue
		}
		if id == 1 {
			status.beginLeg1Attempt()
		}
		ctx, cancel := context.WithTimeout(core.ctx, min(o.handshakeTimeout, o.failoverTimeout))
		o.recovery.attempts[id].Add(1)
		conn, err := o.children[id].DialContext(ctx, N.NetworkTCP, o.aggregation)
		if err == nil {
			err = o.clientHandshake(ctx, conn, o.recoveryHello(session, id, destination, !confirmed.Load()))
			if err == nil {
				confirmed.Store(true)
			}
		}
		if err == nil {
			_, err = core.addLeg(id, conn, nil)
		}
		cancel()
		if err == nil {
			if id == 1 {
				status.setLeg1Phase(leg1PhaseReady)
			}
			continue
		}
		if conn != nil {
			conn.Close()
		}
		if reason, rejected := helloRejectReasonFromError(err); rejected && reason == helloRejectSessionUnavailable && confirmed.Load() {
			core.peerSessionClosed(err)
			return
		}
		if id == 1 {
			status.setLeg1Phase(leg1PhaseRetrying)
		}
		if !recoveryWait(core.ctx, 250*time.Millisecond) {
			return
		}
	}
}
