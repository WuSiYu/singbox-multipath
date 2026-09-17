package multipath

import (
	"errors"
	"math"
	"time"

	"github.com/sagernet/sing-box/protocol/multipath/stream"
)

// pumpLoop is the only assignment owner. Transport workers may block, but they
// never hold stateMu and cannot block feedback, application reads or reinjection.
func (c *mpCore) pumpLoop() {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	var lastFeedback time.Time
	for {
		select {
		case <-c.done:
			return
		case <-c.pumpWake:
		case <-timer.C:
		}
		now := time.Now()
		if c.cfg.Recovery != nil {
			for _, leg := range c.availableLegs() {
				if !c.cfg.Recovery.allows(leg.id) {
					c.legFailed(leg, legFailureReadData, errors.New("multipath path declared unavailable by shared health check"))
				}
			}
		}
		c.stateMu.Lock()
		if c.isDone() {
			c.stateMu.Unlock()
			return
		}
		wait := time.Second
		primary := c.controlLeg()
		if c.cfg.Recovery != nil && now.Sub(lastFeedback) >= time.Second {
			c.feedbackDirty = true
		}
		if primary != nil && primary.ready.Load() && c.feedbackDirty {
			if now.Sub(lastFeedback) >= 5*time.Millisecond || c.rx.Complete() {
				c.legsMu.RLock()
				feedback := c.feedbackLockedWithoutLegs()
				c.legsMu.RUnlock()
				frame := wireFrame{typ: frameTypeWindow, flow: feedback}
				select {
				case <-primary.feedback:
				default:
				}
				primary.feedback <- frame
				c.feedbackDirty = false
				lastFeedback = now
			} else {
				wait = 5*time.Millisecond - now.Sub(lastFeedback)
			}
		}
		for _, leg := range c.availableLegs() {
			if leg.path.Stalled(now, c.cfg.PathStallTimeoutMin) && !leg.path.Stale {
				leg.path.Stale = true
				c.replayTO.Add(1)
			}
			if leg.path.Outstanding() != 0 {
				wait = min(wait, 50*time.Millisecond)
			}
		}
		var pumpErr error
		for attempts := 0; attempts < 2; attempts++ {
			if sent, err := c.reinjectLocked(now); err != nil {
				pumpErr = err
				break
			} else if sent {
				continue
			}
			segment, ok := c.tx.NextRange(c.cfg.FrameSize)
			if !ok {
				break
			}
			leg := c.choosePathLocked(segment.Length)
			if leg == nil {
				break
			}
			if pumpErr = c.submitLocked(leg, segment, false, now); pumpErr != nil {
				break
			}
		}
		if primary != nil && c.tx.SendFIN() {
			if !primary.tryQueueControl(wireFrame{typ: frameTypeFIN, seq: c.tx.FIN}) {
				// A full control channel is transient. Do not lose the FIN.
				c.tx.FINSent = false
				c.tx.Next--
			} else {
				c.finPath = primary
			}
		}
		if c.cfg.Recovery != nil && primary != nil && c.finPath != primary && c.tx.FINSent && !c.tx.FINAcked {
			if primary.tryQueueControl(wireFrame{typ: frameTypeFIN, seq: c.tx.FIN}) {
				c.finPath = primary
			}
		}
		c.updateStateCountersLocked()
		c.stateMu.Unlock()
		if errors.Is(pumpErr, errMemoryLimit) {
			pumpErr = nil
			wait = min(wait, 50*time.Millisecond)
		}
		if pumpErr != nil {
			c.protocolFail(pumpErr)
			return
		}
		c.finishApplicationClose()
		// Do not let a probe initiate an otherwise lazy TFO connection.
		if primary != nil && primary.ready.Load() && (c.legCounters[0].txBytes.Load() > 0 || c.legCounters[0].rxBytes.Load() > 0) {
			c.scheduleProbes(now)
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(max(time.Millisecond, wait))
	}
}

// Caller holds both stateMu and legsMu. Feedback contains whole-path receipt
// frontiers, not local transport send completions and not application reads.
func (c *mpCore) feedbackLockedWithoutLegs() flowMessage {
	message := flowMessage{Next: c.rx.Ack(), Limit: c.rx.Advertise(c.memory.boosterAllowed())}
	if !c.memory.boosterAllowed() {
		message.Flags |= flowFlagPressure
	}
	for id, leg := range c.legs {
		message.Paths[id] = leg.received
	}
	return message
}

func (c *mpCore) handleWindow(message flowMessage) error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if err := c.tx.Acknowledge(message.Next, message.Limit); err != nil {
		return err
	}
	c.peerPressure = message.Flags&flowFlagPressure != 0
	now := time.Now()
	for _, leg := range c.availableLegs() {
		if err := leg.path.Feedback(message.Paths[leg.id], now); err != nil {
			return err
		}
		leg.inflight.Store(int64(leg.path.Outstanding()))
	}
	for c.mappingHead < len(c.mappings) && c.mappings[c.mappingHead].end <= c.tx.Una {
		c.mappings[c.mappingHead] = dataMapping{}
		c.mappingHead++
	}
	if c.mappingHead < len(c.mappings) {
		c.mappings[c.mappingHead].seq = max(c.mappings[c.mappingHead].seq, c.tx.Una)
	}
	if c.mappingHead == len(c.mappings) {
		c.mappings = c.mappings[:0]
		c.mappingHead = 0
	} else if c.mappingHead >= 1024 && c.mappingHead*2 >= len(c.mappings) {
		n := copy(c.mappings, c.mappings[c.mappingHead:])
		clear(c.mappings[n:])
		c.mappings = c.mappings[:n]
		c.mappingHead = 0
	}
	c.updateStateCountersLocked()
	wakeFlow(c.txWake)
	wakeFlow(c.pumpWake)
	return nil
}

func (c *mpCore) choosePathLocked(length int) *mpLeg {
	legs := c.availableLegs()
	provisionalRate := float64(0)
	for _, leg := range legs {
		provisionalRate = max(provisionalRate, leg.path.Rate)
	}
	// Selection policy and transient readiness are distinct: a healthy secondary
	// remains the sole new-data path while busy or discovery/window constrained.
	// Only unavailable/stale paths or allocator protection permit primary fallback.
	exclusiveSecondary := c.trafficSavingSecondaryLocked()
	var chosen *mpLeg
	score := math.Inf(1)
	for _, leg := range legs {
		if exclusiveSecondary != nil && leg != exclusiveSecondary {
			continue
		}
		if c.cfg.Recovery != nil && !c.cfg.Recovery.allows(leg.id) {
			continue
		}
		if leg.path.Stale || leg.busy {
			continue
		}
		emergency := c.cfg.Recovery != nil && c.controlLeg() == leg && leg.id == 1
		if (!c.active.Load() && leg.id == 0) || emergency {
			return leg
		}
		if leg.id == 1 && (!c.active.Load() || !leg.ready.Load() || c.peerPressure || !c.memory.boosterAllowed()) {
			continue
		}
		// Startup sampling is bounded. Thereafter the connection-level byte
		// window and memory, not an extra per-path cwnd, bound lookahead.
		initial := min(uint64(c.cfg.QueueBytes), uint64(c.cfg.FrameSize)*4)
		pipeline := leg.path.Pipeline(initial, uint64(c.cfg.SendBufferBytes))
		if leg.path.Outstanding()+uint64(length) > pipeline {
			continue
		}
		next := leg.path.DrainTime(provisionalRate)
		if next < score {
			chosen, score = leg, next
		}
	}
	return chosen
}

func (c *mpCore) submitLocked(leg *mpLeg, segment stream.Segment, repair bool, now time.Time) error {
	prepaid := false
	essential := leg.id == 0 || (c.cfg.Recovery != nil && c.controlLeg() == leg)
	if !c.memory.reservePage(128, essential) {
		if !essential || leg.prepaidFlights >= 16 {
			return errMemoryLimit
		}
		leg.prepaidFlights++
		prepaid = true
	}
	pathSeq, err := leg.path.Submitted(segment.Length, now, prepaid)
	if err != nil {
		leg.path.ReleaseFlight(prepaid)
		return err
	}
	if !repair {
		if err = c.tx.Sent(segment); err != nil {
			return err
		}
		wakeFlow(c.txWake)
		c.mappings = append(c.mappings, dataMapping{
			seq: segment.Seq, end: segment.End(), path: leg.id, generation: leg.path.Generation,
			pathEnd: pathSeq + uint64(segment.Length), sentAt: now,
		})
	}
	segment.Buffer.Retain()
	frame := wireFrame{typ: frameTypeData, seq: segment.Seq, pathSeq: pathSeq, generation: leg.path.Generation, data: segment.Data(), buffer: segment.Buffer, replay: repair}
	leg.busy = true
	leg.queuedBytes.Add(int64(segment.Length))
	leg.inflight.Store(int64(leg.path.Outstanding()))
	updateAtomicPeak(&c.legPeak[leg.id], int64(leg.path.Outstanding()))
	select {
	case leg.send <- frame:
		return nil
	default:
		segment.Buffer.Release()
		leg.queuedBytes.Add(-int64(segment.Length))
		leg.busy = false
		return errors.New("multipath assignment invariant violated")
	}
}

// Reinjection borrows data from the same queue used by first transmission. It
// neither consumes new window space nor needs a new payload allocation. There
// is no separate leg1 replay map, idle-credit epoch, or reconnect-on-RTO state.
func (c *mpCore) reinjectLocked(now time.Time) (bool, error) {
	for i := c.mappingHead; i < len(c.mappings); i++ {
		mapping := &c.mappings[i]
		owner := c.getLeg(mapping.path)
		lost := owner == nil || owner.path.Generation != mapping.generation
		stale := !lost && owner.path.Stale
		// A receipt for data ABOVE snd_una is ordinary reordering, not loss.
		// Only the missing connection head can have been pruned after receipt.
		pruned := i == c.mappingHead && mapping.seq == c.tx.Una && !lost && owner.path.Received >= mapping.pathEnd
		if !lost && !stale && !pruned {
			continue
		}
		// A path receipt with no corresponding Data ACK may indicate receive
		// pruning. Allow normal coalescing/reordering before repairing it.
		if pruned && !stale && now.Sub(mapping.sentAt) < owner.path.RTO(c.cfg.PathStallTimeoutMin) {
			continue
		}
		target := c.controlLeg()
		if mapping.path == 0 && stale && c.active.Load() {
			target = c.getLeg(1)
		}
		if c.cfg.Recovery != nil && target != nil && !c.cfg.Recovery.allows(target.id) {
			continue
		}
		if target == nil || target.busy || !target.ready.Load() || target.path.Stale {
			continue
		}
		if !mapping.repairedAt.IsZero() && now.Sub(mapping.repairedAt) < target.path.RTO(c.cfg.PathStallTimeoutMin) {
			continue
		}
		segment, ok := c.tx.Range(max(mapping.seq, c.tx.Una), int(mapping.end-max(mapping.seq, c.tx.Una)))
		if !ok {
			continue
		}
		if err := c.submitLocked(target, segment, true, now); err != nil {
			return false, err
		}
		mapping.path, mapping.generation = target.id, target.path.Generation
		mapping.pathEnd = target.path.Sent
		mapping.sentAt, mapping.repairedAt = now, now
		c.fallbackB.Add(uint64(segment.Length))
		c.fallbackF.Add(1)
		c.fallbackE.Add(1)
		return true, nil
	}
	return false, nil
}

// Caller holds stateMu. Busy writers are healthy, and must not cause spillover.
func (c *mpCore) trafficSavingSecondaryLocked() *mpLeg {
	if !c.active.Load() || !c.cfg.Leg0TrafficSaving || c.peerPressure || !c.memory.boosterAllowed() {
		return nil
	}
	leg := c.getLeg(1)
	if leg == nil || !leg.ready.Load() || leg.path.Stale || (c.cfg.Recovery != nil && !c.cfg.Recovery.allows(1)) {
		return nil
	}
	return leg
}

func (c *mpCore) dataModeLocked() uint64 {
	if c.cfg.Recovery != nil {
		if leg := c.controlLeg(); leg != nil && leg.id == 1 {
			return 5
		}
	}
	if !c.active.Load() {
		return 1
	}
	if !c.cfg.Leg0TrafficSaving {
		return 2
	}
	if c.trafficSavingSecondaryLocked() != nil {
		return 3
	}
	return 4
}

func dataModeName(mode uint64) string {
	switch mode {
	case 1:
		return "leg0"
	case 2:
		return "aggregate"
	case 3:
		return "leg1"
	case 4:
		return "leg0_fallback"
	case 5:
		return "failover"
	default:
		return "unknown"
	}
}
