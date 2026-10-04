package multipath

import (
	"context"
	"net"
	"sync/atomic"
	"time"

	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// sessionHello describes one leg of a logical session. Every session may be
// created by whichever leg reaches the server first; later legs join it.
func (o *Outbound) sessionHello(session [16]byte, id byte, destination string, create bool) helloMessage {
	h := helloMessage{
		Session: session, LegID: id, RequestStatus: o.statusFile != "", FrameSize: uint32(o.cfg.FrameSize),
		Destination: destination, Policy: o.policy, Create: create, PSK: o.psk,
	}
	if o.recovery != nil {
		h.Recovery, h.Group = true, o.recovery.id
		h.RecoveryEpoch, h.RecoveryMask, h.RecoveryUDP = o.recovery.policy.snapshot()
	}
	return h
}

// dialSession creates a logical session on the preferred leg, or on the other
// leg if the preferred one cannot reach the server, and then keeps both legs
// attached for the session's lifetime. Losing any single leg is not fatal.
func (o *Outbound) dialSession(ctx context.Context, destination M.Socksaddr) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, o.handshakeTimeout)
	retainDialContext := false
	defer func() {
		if !retainDialContext {
			cancel()
		}
	}()
	id := byte(0)
	if o.recovery != nil {
		var err error
		if id, err = o.recovery.waitReady(ctx); err != nil {
			return nil, err
		}
	}
	session, err := newSessionID()
	if err != nil {
		return nil, err
	}
	if o.recovery != nil {
		// Publish ownership before any transport can create the server session,
		// including an early-write hello. Keep it until the logical core terminates.
		if err = o.recovery.registerTCP(session); err != nil {
			return nil, err
		}
	}
	retainedSession := false
	defer func() {
		if !retainedSession && o.recovery != nil {
			o.recovery.unregisterTCP(session)
		}
	}()
	attemptTimeout := o.handshakeTimeout / 2
	if o.recovery != nil {
		attemptTimeout = min(o.failoverTimeout, attemptTimeout)
	}
	var conn net.Conn
	var message helloMessage
	var lazyCancel context.CancelFunc
	for attempt := 0; attempt < 2; attempt++ {
		attemptCtx, attemptCancel := context.WithTimeout(ctx, attemptTimeout)
		message = o.sessionHello(session, id, destination.String(), true)
		o.legAttempts[id].Add(1)
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
		if o.status != nil {
			o.status.recordLegError(id, "dial", destination.String(), statusSessionID(session), 0, err, time.Now())
		}
		if attempt == 1 || o.recovery != nil && !o.recovery.policy.allows(1-id) || ctx.Err() != nil {
			return nil, E.Cause(err, "dial multipath leg", id)
		}
		id = 1 - id
	}
	var confirmed atomic.Bool
	var early *clientFastOpenConn
	var preamble func(net.Conn) error
	if o.tcpFastOpen {
		early, err = newClientFastOpenConn(conn, message, o.clientHandshakeDeadline(ctx))
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
				return errPolicyNotConfirmed
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
	phase := leg1PhaseConnecting
	if early != nil {
		phase = leg1PhaseWaiting
	}
	status := o.registerStatusSession(session, destination.String(), core, phase)
	o.logger.InfoContext(ctx, "multipath connection to ", destination, " created via leg", id, " ", o.tags[id])
	if o.recovery != nil {
		stop := context.AfterFunc(o.recovery.ctx, func() { core.Close() })
		retainedSession = true
		go func() {
			<-core.Done()
			stop()
			o.recovery.unregisterTCP(session)
		}()
	}
	// Joining before the first write would defeat lazy TFO. After it starts,
	// either path may complete session creation; a server tombstone prevents a
	// late retry from creating a second target connection for the same ID.
	if early != nil {
		retainDialContext = true
	}
	if !core.startWorkers(func() {
		if early != nil {
			_ = early.waitStarted()
			lazyCancel()
			cancel()
		}
		createDeadline := time.Now().Add(o.handshakeTimeout)
		for id := byte(0); id < 2; id++ {
			core.startWorkers(func() { o.maintainLeg(core, session, id, destination.String(), &confirmed, createDeadline, status) })
		}
	}) && early != nil {
		lazyCancel()
		cancel()
	}
	if early != nil {
		return &earlyLogicalConn{Conn: app, core: core, primary: early}, nil
	}
	return app, nil
}

// maintainLeg keeps one leg attached: it redials after any loss, with
// exponential backoff, until the logical session ends.
func (o *Outbound) maintainLeg(core *mpCore, session [16]byte, id byte, destination string, confirmed *atomic.Bool, createDeadline time.Time, status *statusSession) {
	backoff := time.Duration(0)
	for !core.isDone() && o.ctx.Err() == nil {
		if leg := core.getLeg(id); leg != nil {
			attached := time.Now()
			select {
			case <-leg.Done():
			case <-core.Done():
				return
			case <-o.ctx.Done():
				core.Close()
				return
			}
			if time.Since(attached) >= legBackoffMax {
				backoff = 0
			}
			if id == 1 && !core.isDone() {
				status.setLeg1Phase(leg1PhaseRetrying)
				o.logger.WarnContext(core.ctx, "multipath leg", id, " lost; reconnecting via ", o.tags[id])
			}
			continue
		}
		if !confirmed.Load() && time.Now().After(createDeadline) {
			core.fail(errRecoveryNotReady)
			return
		}
		if o.recovery != nil && !o.recovery.policy.allows(id) {
			if !recoveryWait(core.ctx, 100*time.Millisecond) {
				return
			}
			continue
		}
		if backoff > 0 && !recoveryWait(core.ctx, backoff) {
			return
		}
		if id == 1 {
			status.beginLeg1Attempt()
		}
		timeout := o.handshakeTimeout
		if o.recovery != nil {
			timeout = min(timeout, o.failoverTimeout)
		}
		ctx, cancel := context.WithTimeout(core.ctx, timeout)
		o.legAttempts[id].Add(1)
		stage := "dial"
		conn, err := o.children[id].DialContext(ctx, N.NetworkTCP, o.aggregation)
		if err == nil {
			stage = "handshake"
			err = o.clientHandshake(ctx, conn, o.sessionHello(session, id, destination, !confirmed.Load()))
			if err == nil {
				confirmed.Store(true)
			}
		}
		if err == nil {
			stage = "attach"
			_, err = core.addLeg(id, conn, nil)
		}
		cancel()
		if err == nil {
			if id == 1 {
				status.setLeg1Phase(leg1PhaseReady)
				o.logger.InfoContext(core.ctx, "multipath secondary leg ready via ", o.tags[1])
			}
			continue
		}
		if conn != nil {
			conn.Close()
		}
		reason, rejected := helloRejectReasonFromError(err)
		if rejected && reason == helloRejectSessionUnavailable && confirmed.Load() {
			core.peerSessionClosed(err)
			return
		}
		if rejected && reason.fatal(confirmed.Load()) {
			core.fail(err)
			return
		}
		if id == 1 {
			status.setLeg1Phase(leg1PhaseRetrying)
		}
		if !rejected || reason != helloRejectLegUnavailable {
			status.recordLegError(id, "secondary_"+stage, err)
			o.logger.DebugContext(core.ctx, "multipath leg", id, " attach failed: ", err)
		}
		backoff = nextLegBackoff(backoff, o.recovery != nil)
	}
}

const (
	legBackoffMin = time.Second
	legBackoffMax = 30 * time.Second
)

// nextLegBackoff doubles the redial interval up to legBackoffMax. Failover
// sessions retry quickly because shared health checks gate the attempts.
func nextLegBackoff(current time.Duration, recovery bool) time.Duration {
	if recovery {
		return 250 * time.Millisecond
	}
	if current < legBackoffMin {
		return legBackoffMin
	}
	return min(legBackoffMax, current*2)
}

var errPolicyNotConfirmed = E.New("multipath server did not confirm the requested frame size and directional policy")

// legAbsentTimeout bounds how long a session without failover waits with no
// attached leg for the client to redial one.
func legAbsentTimeout(handshakeTimeout time.Duration) time.Duration {
	return max(30*time.Second, 3*handshakeTimeout)
}
