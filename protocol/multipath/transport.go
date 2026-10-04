package multipath

import (
	"errors"
	"io"
	"math"
	"time"
)

func (c *mpCore) legWriteLoop(leg *mpLeg) {
	defer close(leg.writerDone)
	defer func() {
		c.stateMu.Lock()
		defer c.stateMu.Unlock()
		select {
		case frame := <-leg.send:
			leg.queuedBytes.Add(-int64(len(frame.data)))
			frame.buffer.Release()
		default:
		}
		leg.busy = false
	}()
	if leg.startupFeedback != nil {
		if err := c.writeControlFrame(leg, *leg.startupFeedback); err != nil {
			c.legFailed(leg, legFailureWriteControl, err)
			return
		}
	}
	for {
		select {
		case request := <-leg.shutdown:
			c.finishLegShutdown(leg, request)
			return
		default:
		}
		// These controls have priority only over data not yet submitted to the
		// child. They cannot overtake a Write already blocked inside TCP/QUIC.
		select {
		case control := <-leg.control:
			if err := c.writeControlFrame(leg, control); err != nil {
				c.legFailed(leg, legFailureWriteControl, err)
				return
			}
			continue
		default:
		}
		select {
		case feedback := <-leg.feedback:
			if err := c.writeControlFrame(leg, feedback); err != nil {
				c.legFailed(leg, legFailureWriteControl, err)
				return
			}
			continue
		default:
		}
		select {
		case request := <-leg.shutdown:
			c.finishLegShutdown(leg, request)
			return
		case <-c.done:
			select {
			case request := <-leg.shutdown:
				c.finishLegShutdown(leg, request)
			default:
				leg.close(errCoreClosed)
			}
			return
		case <-leg.done:
			return
		case control := <-leg.control:
			if err := c.writeControlFrame(leg, control); err != nil {
				c.legFailed(leg, legFailureWriteControl, err)
				return
			}
		case feedback := <-leg.feedback:
			if err := c.writeControlFrame(leg, feedback); err != nil {
				c.legFailed(leg, legFailureWriteControl, err)
				return
			}
		case <-leg.telemetry:
			if frame, pending := leg.takeTelemetry(); pending {
				if err := c.writeControlFrame(leg, frame); err != nil {
					c.legFailed(leg, legFailureWriteControl, err)
					return
				}
			}
		case frame := <-leg.send:
			length := int64(len(frame.data))
			leg.queuedBytes.Add(-length)
			leg.writingBytes.Store(length)
			leg.writeStarted.Store(time.Now().UnixNano())
			leg.transportWriteStarted.Store(time.Now().UnixNano())
			err := writeWireFrame(leg.conn, frame)
			leg.transportWriteStarted.Store(0)
			leg.writeStarted.Store(0)
			leg.writingBytes.Store(0)
			c.stateMu.Lock()
			frame.buffer.Release()
			leg.busy = false
			c.stateMu.Unlock()
			if err != nil {
				c.legFailed(leg, legFailureWriteData, err)
				return
			}
			c.legCounters[leg.id].txBytes.Add(uint64(length))
			c.legCounters[leg.id].txFrames.Add(1)
			wakeFlow(c.pumpWake)
		}
	}
}

func (c *mpCore) legReadLoop(leg *mpLeg) {
	defer close(leg.readerDone)
	scratch := c.memory.takeReservedBuffer(c.cfg.FrameSize)
	defer c.memory.putReservedBuffer(scratch)
	if leg.readPreamble != nil {
		if err := leg.readPreamble(leg.conn); err != nil {
			if !c.isDone() {
				c.legFailed(leg, legFailureHandshake, err)
				// An explicit rejection (policy, authentication, version)
				// would repeat on every leg: end the session.
				if reason, rejected := helloRejectReasonFromError(err); rejected && reason.fatal(false) || errors.Is(err, errPolicyNotConfirmed) {
					c.fail(err)
				}
			}
			return
		}
	}
	leg.ready.Store(true)
	wakeFlow(c.pumpWake)
	for {
		var mappingErr error
		frame, err := readFrameData(leg.conn, scratch, func(piece wireFrame) error { mappingErr = c.receiveMapping(leg, piece); return mappingErr })
		if err != nil {
			if !c.isDone() {
				if mappingErr != nil && leg.id == 0 {
					c.protocolFail(err)
				} else {
					c.legFailed(leg, legFailureReadData, err)
				}
			}
			return
		}
		if frame.typ == frameTypeData {
			continue
		}
		switch frame.typ {
		case frameTypeWindow:
			err = c.handleWindow(frame.flow)
		case frameTypePing:
			leg.tryQueueControl(wireFrame{typ: frameTypePong, seq: frame.seq})
		case frameTypePong:
			c.handlePong(leg.id, frame.seq, time.Now())
		case frameTypeSenderStatus:
			c.handlePeerSenderStatus(frame.status, time.Now())
		case frameTypeReset:
			c.peerSessionClosed(errors.New("multipath peer reset"))
			return
		case frameTypeSessionClose:
			// Session close follows the acknowledged DATA_FIN, so it is
			// ordered by the byte stream and valid on any leg.
			c.notePeerCloseReason(frame.closeReason)
			c.peerSessionClosed(io.EOF)
			return
		case frameTypeFIN:
			err = c.receiveMapping(leg, frame)
		}
		if err != nil {
			// A malformed or failed secondary is isolated. Its unacknowledged
			// logical bytes remain in the sender's connection queue.
			if leg.id == 1 {
				c.legFailed(leg, legFailureReadData, err)
			} else {
				c.protocolFail(err)
			}
			return
		}
	}
}

func (c *mpCore) receiveMapping(leg *mpLeg, frame wireFrame) error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.getLeg(leg.id) != leg {
		return errCoreClosed
	}
	var err error
	if frame.typ == frameTypeFIN {
		err = c.rx.SetFIN(frame.seq)
	} else {
		if frame.pathSeq != leg.received.Next || uint64(len(frame.data)) > math.MaxUint64-frame.pathSeq {
			return errors.New("invalid multipath path sequence")
		}
		if leg.received.Generation != 0 && frame.generation != leg.received.Generation {
			return errors.New("multipath path incarnation changed in place")
		}
		_, err = c.rx.Insert(frame.seq, frame.data)
		if err == nil {
			leg.received.Generation = frame.generation
			leg.received.Next += uint64(len(frame.data))
			leg.received.ReceivedAt = uint64(max(1, time.Since(c.startedAt).Microseconds()))
			if leg.received.FirstNext == 0 {
				leg.received.FirstNext = leg.received.Next
				leg.received.FirstReceivedAt = leg.received.ReceivedAt
			}
			c.legCounters[leg.id].rxBytes.Add(uint64(len(frame.data)))
			if !frame.more {
				c.legCounters[leg.id].rxFrames.Add(1)
			}
		}
	}
	if err != nil {
		return err
	}
	c.feedbackDirty = true
	c.updateStateCountersLocked()
	wakeFlow(c.rxWake)
	wakeFlow(c.pumpWake)
	return nil
}

func (c *mpCore) legFailed(leg *mpLeg, stage legFailureStage, err error) {
	c.legsMu.Lock()
	if c.legs[leg.id] != leg {
		c.legsMu.Unlock()
		leg.close(err)
		return
	}
	delete(c.legs, leg.id)
	c.retiring[leg.id] = leg
	c.legsMu.Unlock()
	c.stateMu.Lock()
	leg.path.Close()
	c.stateMu.Unlock()
	leg.close(err)
	c.cancelLegProbe(leg.id)
	if leg.peerTerminal.Load() {
		// The secondary's terminal copy may arrive before the primary's FIN
		// and terminal event. It retires this path without pretending that the
		// ordered logical shutdown has arrived, and without pointless retries.
		wakeFlow(c.pumpWake)
		return
	}
	eventErr := c.sourcedLegError(err)
	if !isEndpointLegError(eventErr) {
		c.legFailureMu.Lock()
		c.legFailures[leg.id]++
		c.lastFailLeg = leg.id
		c.lastFailStage = stage
		c.legFailureMu.Unlock()
	}
	if c.cfg.OnStatusEvent != nil {
		c.cfg.OnStatusEvent()
	}
	if c.cfg.OnLegFailure != nil {
		c.cfg.OnLegFailure(leg.id, stage, eventErr)
	}
	// No single leg is essential: the session continues on the other leg
	// and the client redials this one (see maintainLeg).
	wakeFlow(c.pumpWake)
}
