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
		primary := c.pickControlLeg(now)
		if c.cfg.Recovery != nil && now.Sub(lastFeedback) >= time.Second {
			c.feedbackDirty = true
		}
		if primary != nil && c.feedbackDirty {
			if now.Sub(lastFeedback) >= 5*time.Millisecond || c.rx.Complete() {
				c.legsMu.RLock()
				feedback := c.feedbackLockedWithoutLegs()
				c.legsMu.RUnlock()
				frame := wireFrame{typ: frameTypeWindow, flow: feedback}
				blocked := primary.blockedFor(now)
				for _, leg := range c.availableLegs() {
					// Feedback goes on the least blocked leg. The other leg
					// carries a copy at least every 20 ms, or immediately when
					// the chosen leg's writer is stuck, so ACKs, windows and
					// path receipts never depend on a single path.
					if leg == primary || leg.ready.Load() && (now.Sub(leg.feedbackAt) >= feedbackCopyInterval || blocked >= max(10*time.Millisecond, leg.path.SRTT/2)) {
						leg.queueFeedback(frame, now)
					}
				}
				c.feedbackDirty = false
				lastFeedback = now
			} else {
				wait = 5*time.Millisecond - now.Sub(lastFeedback)
			}
		}
		legs := c.availableLegs()
		// Failover sessions are owned through the shared control channel and
		// its lease instead; others end after a bounded leg-less period.
		if c.cfg.Recovery == nil && len(legs) == 0 && !c.joiningLegs() {
			if c.legsAbsentSince.IsZero() {
				c.legsAbsentSince = now
			} else if now.Sub(c.legsAbsentSince) >= c.cfg.LegAbsentTimeout {
				c.stateMu.Unlock()
				c.fail(errNoLegs)
				return
			}
		} else {
			c.legsAbsentSince = time.Time{}
		}
		var dead []*mpLeg
		for _, leg := range legs {
			if leg.path.Stalled(now, c.cfg.PathStallTimeoutMin) && !leg.path.Stale {
				leg.path.Stale = true
				c.replayTO.Add(1)
			}
			// A leg without any progress for this long is closed so its
			// manager can redial; the session keeps running elsewhere.
			if leg.path.Stale && leg.path.Stalled(now, deadLegTimeout) {
				dead = append(dead, leg)
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
				c.finPath, c.finSentAt = primary, now
			}
		}
		// DATA_FIN carries its sequence number, so any leg may repeat it. Resend
		// when its leg disappeared or it stayed unacknowledged for an RTO.
		if primary != nil && c.tx.FINSent && !c.tx.FINAcked && c.finPath != nil {
			gone := c.getLeg(c.finPath.id) != c.finPath
			if gone || now.Sub(c.finSentAt) >= c.finPath.path.RTO(c.cfg.PathStallTimeoutMin) {
				target := primary
				if !gone && primary == c.finPath {
					for _, leg := range c.availableLegs() {
						if leg != c.finPath && leg.ready.Load() {
							target = leg
						}
					}
				}
				if target.tryQueueControl(wireFrame{typ: frameTypeFIN, seq: c.tx.FIN}) {
					c.finPath, c.finSentAt = target, now
				}
			}
			wait = min(wait, 50*time.Millisecond)
		}
		c.updateStateCountersLocked()
		c.stateMu.Unlock()
		for _, leg := range dead {
			c.legFailed(leg, legFailureReplay, errLegStalled)
		}
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

const (
	feedbackCopyInterval = 20 * time.Millisecond
	deadLegTimeout       = 15 * time.Second
)

var (
	errNoLegs     = errors.New("multipath session lost every leg")
	errLegStalled = errors.New("multipath leg made no progress")
)

// pickControlLeg chooses the ready leg whose writer is least blocked for
// connection-level control (feedback, FIN, status). Ties prefer leg0.
func (c *mpCore) pickControlLeg(now time.Time) *mpLeg {
	var best *mpLeg
	var bestBlocked time.Duration
	for _, leg := range c.availableLegs() {
		if !leg.ready.Load() || c.cfg.Recovery != nil && !c.cfg.Recovery.allows(leg.id) {
			continue
		}
		if blocked := leg.blockedFor(now); best == nil || blocked < bestBlocked {
			best, bestBlocked = leg, blocked
		}
	}
	return best
}

// Caller holds both stateMu and legsMu. Feedback contains whole-path receipt
// frontiers, not local transport send completions and not application reads.
func (c *mpCore) feedbackLockedWithoutLegs() flowMessage {
	target := c.receiveTargetLocked(time.Now())
	c.feedbackSeq++
	message := flowMessage{Next: c.rx.Ack(), Limit: c.rx.Advertise(target), Seq: c.feedbackSeq}
	// Informational only: the receiver's node is using its emergency margin.
	if c.memory.underPressure() {
		message.Flags |= flowFlagPressure
	}
	for id, leg := range c.legs {
		message.Paths[id] = leg.received
	}
	return message
}

// receiveTargetLocked returns the window to advertise: the configured ceiling
// capped by this session's fair share of the node's receive region. Storage is
// allocated only as data arrive and the sender's own history limit keeps
// in-flight data near twice the path BDP, so a large window costs nothing
// until used. The share keeps the sum of windows of active sessions within
// the region, which is what prevents over-commitment and receive drops.
func (c *mpCore) receiveTargetLocked(now time.Time) uint64 {
	return min(uint64(c.cfg.ReceiveWindowBytes), uint64(c.memory.receiveShare(c, now)))
}

func (c *mpCore) handleWindow(message flowMessage) error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if err := c.tx.Acknowledge(message.Next, message.Limit); err != nil {
		return err
	}
	// Copies of feedback travel on both legs and may arrive out of order.
	// Monotonic fields merge above; the rest only applies when newer.
	if message.Seq > c.peerFeedbackSeq {
		c.peerFeedbackSeq = message.Seq
		c.peerPressure = message.Flags&flowFlagPressure != 0
	}
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
		if cap(c.mappings) > 1024 {
			c.mappings = nil
		}
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

// ecfSlack bounds how much later than the best path a segment may complete
// when it is handed to a slower path that happens to be idle.
const ecfSlack = 1.5

// choosePathLocked implements earliest-completion-first scheduling (ECF, as in
// BLEST and Linux MPTCP's linger-time rule): each path's completion time is
// its queued work over its delivery rate plus its one-way delay. The best
// available path is used, but a slower path only while its completion time
// stays within ecfSlack of the best path overall; otherwise new data waits for
// the faster path instead of queueing behind a slow one and blocking in-order
// delivery.
func (c *mpCore) choosePathLocked(length int) *mpLeg {
	legs := c.availableLegs()
	provisionalRate := float64(0)
	for _, leg := range legs {
		provisionalRate = max(provisionalRate, leg.path.Rate)
	}
	// Selection policy and transient readiness are distinct: a healthy secondary
	// remains the sole new-data path while busy or discovery/window constrained.
	// Only unavailable/stale paths permit primary fallback.
	exclusiveSecondary := c.trafficSavingSecondaryLocked()
	var chosen *mpLeg
	chosenTime, bestTime := math.Inf(1), math.Inf(1)
	// Startup sampling is bounded. Thereafter the connection-level byte
	// window and memory, not an extra per-path cwnd, bound lookahead.
	initial := min(uint64(c.cfg.QueueBytes), uint64(c.cfg.FrameSize)*4)
	for _, leg := range legs {
		if exclusiveSecondary != nil && leg != exclusiveSecondary {
			continue
		}
		if c.cfg.Recovery != nil && !c.cfg.Recovery.allows(leg.id) {
			continue
		}
		if leg.path.Stale {
			continue
		}
		if !c.active.Load() {
			// Before activation only the preferred data leg carries data.
			if preferred := c.preferredDataLeg(); preferred != nil && !preferred.busy {
				return preferred
			}
			return nil
		}
		// Data are already retained in the send history and receive storage
		// is accounted by the receiver, so memory never gates a path.
		if leg.id == 1 && (!c.active.Load() || !leg.ready.Load()) {
			continue
		}
		completion := leg.path.CompletionTime(length, provisionalRate)
		bestTime = min(bestTime, completion)
		if leg.busy {
			continue
		}
		if leg.path.Outstanding()+uint64(length) > leg.path.Pipeline(initial, uint64(c.cfg.SendBufferBytes)) {
			continue
		}
		if completion < chosenTime {
			chosen, chosenTime = leg, completion
		}
	}
	if chosen != nil && exclusiveSecondary == nil && chosenTime > max(bestTime*ecfSlack, bestTime+0.002) {
		return nil
	}
	return chosen
}

func (c *mpCore) submitLocked(leg *mpLeg, segment stream.Segment, repair bool, now time.Time) error {
	prepaid := false
	essential := leg.id == 0 || leg == c.preferredDataLeg()
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
		target := c.preferredDataLeg()
		if lost || stale {
			// Repair on a different, healthy path whenever one exists.
			if other := c.otherUsableLeg(mapping.path); other != nil {
				target = other
			}
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
	if !c.active.Load() || !c.cfg.Leg0TrafficSaving {
		return nil
	}
	leg := c.getLeg(1)
	if leg == nil || !leg.ready.Load() || leg.path.Stale || (c.cfg.Recovery != nil && !c.cfg.Recovery.allows(1)) {
		return nil
	}
	return leg
}

func (c *mpCore) dataModeLocked() uint64 {
	if leg := c.preferredDataLeg(); leg != nil && leg.id == 1 && (!c.active.Load() || c.getLeg(0) == nil || !c.usableLeg(c.getLeg(0))) {
		return 5
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
