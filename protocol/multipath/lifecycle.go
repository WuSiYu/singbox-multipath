package multipath

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"time"
)

const applicationCloseIdleTimeout = 2 * time.Minute

func (c *mpCore) startWorkers(workers ...func()) bool {
	c.workerMu.Lock()
	defer c.workerMu.Unlock()
	if c.workerClosed {
		return false
	}
	for _, worker := range workers {
		c.workerGroup.Add(1)
		go func(run func()) {
			defer c.workerGroup.Done()
			run()
		}(worker)
	}
	return true
}

func (c *mpCore) stopWorkerAdmission() {
	c.workerMu.Lock()
	c.workerClosed = true
	c.workerMu.Unlock()
}

func (c *mpCore) Context() context.Context {
	return c.ctx
}

func (c *mpCore) AppConn() net.Conn {
	return c.appConn
}

func (c *mpCore) Done() <-chan struct{} {
	return c.done
}

func (c *mpCore) Close() error {
	c.noteCloseSource(closeSourceShutdown)
	c.fail(net.ErrClosed)
	// Service shutdown and explicit core disposal must also interrupt a peer
	// close that is waiting for the local application to drain buffered data.
	_, _ = c.appConn.closeInternal()
	return nil
}

func (c *mpCore) closeApplication() error {
	// MP failures mark their source before exposing an application I/O error.
	// A Close on a still-healthy logical connection comes from its caller.
	c.noteCloseSource(closeSourceLocalEndpoint)
	c.localClosing.Store(true)
	c.localReadClosed.Store(true)
	err, _ := c.appConn.closeInternal()
	if c.getLeg(0) == nil && c.cfg.Recovery == nil {
		c.fail(io.EOF)
	} else {
		c.finishApplicationClose()
	}
	return err
}

func (c *mpCore) finishApplicationClose() {
	if !c.localClosing.Load() {
		return
	}
	if c.ackedFIN.Load() {
		c.terminate(io.EOF, frameTypeSessionClose)
		return
	}
	// Full Close has relinquished the application handle. Permit buffered TX
	// to drain while Data ACKs advance, but do not keep abandoned sessions
	// forever after a lost FIN/ACK or both data paths disappearing. CloseWrite
	// and an open idle connection never enter this timer.
	now := time.Now()
	c.stateMu.Lock()
	if c.closeProgressAt.IsZero() || c.tx.Una > c.closeProgressACK {
		c.closeProgressAt, c.closeProgressACK = now, c.tx.Una
	}
	expired := now.Sub(c.closeProgressAt) >= applicationCloseIdleTimeout
	c.stateMu.Unlock()
	if expired {
		c.fail(errors.New("multipath application close drain timed out"))
	}
}

func (c *mpCore) isDone() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func (c *mpCore) fail(err error) {
	c.noteCloseSource(closeSourceTransport)
	c.terminate(err, frameTypeSessionClose)
}

func (c *mpCore) peerSessionClosed(err error) {
	// A peer close without provenance must not become a local endpoint close
	// when the relay reacts to the resulting logical I/O error.
	c.noteCloseSource(closeSourcePeerUnknown)
	c.terminateWithReceiveDrain(err, 0, errors.Is(err, io.EOF) && c.receiveComplete())
}

func (c *mpCore) terminate(err error, terminalFrameType byte) {
	c.terminateWithReceiveDrain(err, terminalFrameType, false)
}

func (c *mpCore) terminateWithReceiveDrain(err error, terminalFrameType byte, drainReceive bool) {
	if err == nil {
		err = errCoreClosed
	}
	c.closeOne.Do(func() {
		c.stopWorkerAdmission()
		c.appConn.setTerminalError(err, drainReceive)
		c.failureMu.Lock()
		c.failure = err.Error()
		c.failureAt = time.Now()
		c.failureMu.Unlock()
		if terminalFrameType == frameTypeReset && c.cfg.OnProtocolError != nil {
			c.cfg.OnProtocolError(err)
		}
		var finalStatus *senderStatus
		if c.cfg.SendStatus {
			status := c.nextSenderStatus(time.Now())
			finalStatus = &status
		}
		c.legsMu.RLock()
		legs := make([]*mpLeg, 0, len(c.legs))
		for _, leg := range c.legs {
			legs = append(legs, leg)
		}
		c.legsMu.RUnlock()
		for _, leg := range legs {
			var legStatus *senderStatus
			if leg.id == 0 || c.cfg.Recovery != nil {
				legStatus = finalStatus
			}
			leg.requestShutdown(err, terminalFrameType, legStatus)
		}
		c.cancel()
		_ = c.txPipe.Close()
		if drainReceive {
			// Receipt ACKs promise ownership, not application consumption. Keep
			// the read half alive until rxLoop delivers all bytes preceding FIN.
			_ = c.appConn.closeWriteInternal()
		} else {
			_ = c.rxPipe.Close()
			_, _ = c.appConn.closeInternal()
		}
		// Done promises that subsequent application writes are rejected.
		// Publish it only after closing TX; complete RX may continue draining.
		close(c.done)
		go c.releaseAfterShutdown(legs, err)
	})
}

func (c *mpCore) protocolFail(err error) {
	c.noteCloseSource(closeSourceTransport)
	c.terminate(err, frameTypeReset)
}

func closeLegsAfterDrain(legs []*mpLeg, err error) {
	timer := time.NewTimer(sessionCloseDrainTimeout)
	defer timer.Stop()
	for _, leg := range legs {
		select {
		case <-leg.writerDone:
		case <-timer.C:
			for _, pendingLeg := range legs {
				pendingLeg.close(err)
			}
			return
		}
	}
	for _, leg := range legs {
		leg.close(err)
	}
}

func (c *mpCore) reserveLeg(id uint8) error {
	if id > 1 {
		return errors.New("invalid multipath leg id")
	}
	c.legsMu.Lock()
	defer c.legsMu.Unlock()
	if c.isDone() {
		return errCoreClosed
	}
	if previous := c.retiring[id]; previous != nil {
		select {
		case <-previous.readerDone:
		default:
			return errors.New("previous multipath reader is draining")
		}
		select {
		case <-previous.writerDone:
		default:
			return errors.New("previous multipath writer is draining")
		}
		delete(c.retiring, id)
	}
	if c.legs[id] != nil || c.reserved[id] {
		return errors.New("duplicate multipath leg")
	}
	c.reserved[id] = true
	return nil
}

func (c *mpCore) cancelLegReservation(id uint8) {
	c.legsMu.Lock()
	delete(c.reserved, id)
	c.legsMu.Unlock()
}

// joiningLegs reports a leg reservation awaiting its handshake to complete.
func (c *mpCore) joiningLegs() bool {
	c.legsMu.RLock()
	defer c.legsMu.RUnlock()
	return len(c.reserved) > 0
}

func (c *mpCore) getLeg(id uint8) *mpLeg {
	c.legsMu.RLock()
	leg := c.legs[id]
	c.legsMu.RUnlock()
	return leg
}

func (c *mpCore) availableLegs() []*mpLeg {
	c.legsMu.RLock()
	legs := make([]*mpLeg, 0, 2)
	if leg := c.legs[0]; leg != nil {
		legs = append(legs, leg)
	}
	if leg := c.legs[1]; leg != nil {
		legs = append(legs, leg)
	}
	c.legsMu.RUnlock()
	return legs
}

func (c *mpCore) writeControlFrame(leg *mpLeg, frame wireFrame) error {
	if frame.typ == frameTypePing {
		c.markProbeSent(leg.id, frame.seq, time.Now())
	}
	leg.transportWriteStarted.Store(time.Now().UnixNano())
	err := writeWireFrame(leg.conn, frame)
	leg.transportWriteStarted.Store(0)
	if err != nil && frame.typ == frameTypePing {
		c.cancelProbe(leg.id, frame.seq)
	}
	return err
}

func (c *mpCore) finishLegShutdown(leg *mpLeg, request legShutdownRequest) {
	if request.status != nil {
		_ = writeWireFrame(leg.conn, wireFrame{typ: frameTypeSenderStatus, status: *request.status})
	}
	if request.frameType != 0 {
		_ = writeWireFrame(leg.conn, wireFrame{typ: request.frameType, closeReason: c.wireCloseReason()})
	}
	leg.close(request.err)
}

func updateAtomicPeak(peak *atomic.Int64, value int64) {
	for current := peak.Load(); value > current; current = peak.Load() {
		if peak.CompareAndSwap(current, value) {
			return
		}
	}
}
