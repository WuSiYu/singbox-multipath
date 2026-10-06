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
		// A sender paused by our "full" report must learn promptly when room
		// returns, even if nothing arrives or is read meanwhile.
		if c.reportedFull {
			if now.Sub(lastFeedback) >= 20*time.Millisecond {
				c.feedbackDirty = true
			}
			wait = min(wait, 20*time.Millisecond)
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
				c.repairScan = true
				c.replayTO.Add(1)
			}
			// A leg without any progress for this long is closed so its
			// manager can redial, but only while the session keeps running
			// on the other leg; a sole leg is left to the session timeouts.
			if leg.path.Stale && leg.path.Stalled(now, deadLegTimeout) && c.otherUsableLeg(leg.id) != nil {
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
	// The receiver is full: it refused data since the last feedback or has
	// room for less than two frames. The sender then pauses new data and only
	// repairs holes, so stored out-of-order data drain instead of growing.
	full := c.rx.Dropped > c.droppedReported || c.memory.receiveRoom() < 2*int64(c.cfg.FrameSize)
	c.droppedReported = c.rx.Dropped
	if full {
		message.Flags |= flowFlagPressure
	}
	c.reportedFull = full
	// Ask only for dropped bytes that can be stored: while full, those near
	// the next expected byte, whose repair advances in-order delivery and
	// releases memory; a repair sent beyond them would just be dropped again.
	limit := uint64(math.MaxUint64)
	if full {
		limit = c.rx.HeadPageEnd() + (stream.NearHeadPages-1)*stream.PageSize
	}
	message.NACKs = c.rx.DroppedRanges(maxFlowNACKs, limit)
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
// the region, which is what prevents over-commitment and receive drops. It
// never falls below one frame: the peer may send a whole frame before any
// feedback, and a smaller window would turn that first frame into an error.
func (c *mpCore) receiveTargetLocked(now time.Time) uint64 {
	return max(uint64(c.cfg.FrameSize), min(uint64(c.cfg.ReceiveWindowBytes), uint64(c.memory.receiveShare(c, now))))
}

// repairBudgetLocked limits receiver-requested repairs to a quarter of the
// carrying paths' delivery rate per RTT, so repairs never crowd out new data.
func (c *mpCore) repairBudgetLocked(now time.Time) bool {
	rate, rtt := c.carryingRateLocked()
	interval := max(rtt, 10*time.Millisecond)
	if now.Sub(c.repairWindowAt) >= interval {
		c.repairWindowAt, c.repairWindowBytes = now, 0
	}
	return c.repairWindowBytes < max(uint64(rate*interval.Seconds()/4), 2*uint64(c.cfg.FrameSize))
}

func (c *mpCore) handleWindow(message flowMessage) error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if err := c.tx.Acknowledge(message.Next, message.Limit); err != nil {
		return err
	}
	now := time.Now()
	// Copies of feedback travel on both legs and may arrive out of order.
	// Monotonic fields merge above; the rest only applies when newer.
	if message.Seq > c.peerFeedbackSeq {
		c.peerFeedbackSeq = message.Seq
		c.peerPressure = message.Flags&flowFlagPressure != 0
		c.queueRepairsLocked(message.NACKs, now)
	}
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

// provisionalLocked returns what an unmeasured path borrows: the best measured
// delivery rate and the longest measured round-trip time.
func (c *mpCore) provisionalLocked() (float64, time.Duration) {
	rate, delay := float64(0), time.Duration(0)
	for _, leg := range c.availableLegs() {
		rate = max(rate, leg.path.Rate)
		if leg.path.MinimumRTT > 0 {
			delay = max(delay, leg.path.MinimumRTT)
		} else {
			delay = max(delay, leg.path.SRTT)
		}
	}
	return rate, delay
}

// initialPipeline bounds a path until its first delivery-rate sample.
// Thereafter the connection-level byte window and memory, not an extra
// per-path cwnd, bound lookahead.
func (c *mpCore) initialPipeline() uint64 {
	return min(uint64(c.cfg.QueueBytes), uint64(c.cfg.FrameSize)*4)
}

// ecfSlack bounds how much later than the best path a segment may complete
// when it is handed to a slower path that happens to be idle, while the
// application keeps the connection backlogged.
const ecfSlack = 1.5

// choosePathLocked implements earliest-completion-first scheduling (ECF, as in
// BLEST and Linux MPTCP's linger-time rule): each path's completion time is
// its queued work over its delivery rate plus its one-way delay. The best
// available path is used, but a slower path only while its completion time
// stays within ecfSlack of the best path overall; otherwise new data waits for
// the faster path instead of queueing behind a slow one and blocking in-order
// delivery. Once the application has finished (DATA_FIN queued) or handed
// over everything it has for now, the rule is strict ECF: a slower path only
// takes a segment it delivers before the best path could deliver all pending
// data, so the tail of a transfer is not left on a slow or starting path.
func (c *mpCore) choosePathLocked(length int) *mpLeg {
	legs := c.availableLegs()
	provisionalRate, provisionalDelay := c.provisionalLocked()
	// Selection policy and transient readiness are distinct: a healthy secondary
	// remains the sole new-data path while busy or discovery/window constrained.
	// Only unavailable/stale paths permit primary fallback.
	// A full receiver asked for a pause: keep at most two frames of new data
	// in flight until it reports room again; repairs continue meanwhile.
	if c.peerPressure && c.tx.Next-c.tx.Una >= 2*uint64(c.cfg.FrameSize) {
		return nil
	}
	exclusiveSecondary := c.trafficSavingSecondaryLocked()
	now := time.Now()
	var chosen, best *mpLeg
	chosenTime, bestTime := math.Inf(1), math.Inf(1)
	initial := c.initialPipeline()
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
		completion := leg.path.CompletionTime(length, provisionalRate, provisionalDelay, now)
		if completion < bestTime {
			best, bestTime = leg, completion
		}
		// A penalized path recently held the connection head back; it takes
		// no new data until its penalty (one of its RTTs) expires.
		if leg.busy || leg.penaltyUntil.After(now) {
			continue
		}
		if leg.path.Outstanding()+uint64(length) > leg.path.Pipeline(initial, uint64(c.cfg.SendBufferBytes)) {
			continue
		}
		if completion < chosenTime {
			chosen, chosenTime = leg, completion
		}
	}
	if chosen == nil || exclusiveSecondary != nil || chosen == best {
		return chosen
	}
	bestRate := best.path.Rate
	if bestRate <= 0 {
		bestRate = provisionalRate
	}
	// BLEST: the receiver cannot deliver past a segment still on the slower
	// path, so the window must hold what the best path sends meanwhile, or
	// the best path stalls behind it. This also bounds probing.
	if room := c.tx.WindowEnd - min(c.tx.WindowEnd, c.tx.Next+uint64(length)); bestRate*(chosenTime-bestTime) > float64(room) {
		return nil
	}
	pending := c.tx.WriteNext - min(c.tx.Next, c.tx.WriteNext)
	backlogged := c.writerBacklogged(now) || pending+uint64(c.cfg.FrameSize) > c.unsentLimitLocked()
	// While the application is backlogged, a slower path keeps up to twice
	// what it delivered in its last round trip (at least two frames) in
	// flight regardless of its estimate. Its delivery-rate estimate would
	// otherwise be capped by the scheduler's own restraint (an application-
	// limited sample): a path that joins late, or recovers from a degradation,
	// would never show its capacity. This grows its in-flight data
	// geometrically, like the child's own slow start, up to twice its
	// bandwidth-delay product, which keeps it busy.
	probing := backlogged && chosen.path.Outstanding() < max(2*uint64(c.cfg.FrameSize), 2*chosen.path.RecentDelivery(now))
	if c.tx.HasFIN || !backlogged {
		drain := bestTime + float64(pending-min(pending, uint64(length)))/max(bestRate, 1)
		if chosenTime > drain+0.002 {
			return nil
		}
	}
	if !probing && chosenTime > max(bestTime*ecfSlack, bestTime+0.002) {
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
	if sent, err := c.repairDroppedLocked(now); sent || err != nil {
		return sent, err
	}
	if sent, err := c.repairMappingsLocked(now); sent || err != nil {
		return sent, err
	}
	if sent, err := c.opportunisticLocked(now); sent || err != nil {
		return sent, err
	}
	return c.tailReinjectLocked(now)
}

// tailReinjectLocked resends, on an idle path, data near the connection head
// that a slower path would deliver later than the idle path can, once there
// is no new data left to send. At the end of a transfer the receiver waits for
// the last byte; frames still queued on a slower or still-starting path would
// otherwise set the completion time. Whichever copy arrives first is used.
func (c *mpCore) tailReinjectLocked(now time.Time) (bool, error) {
	if c.mappingHead >= len(c.mappings) || !c.active.Load() || c.trafficSavingSecondaryLocked() != nil {
		return false, nil
	}
	if c.tx.Next < c.tx.WriteNext || !c.tx.HasFIN && c.writerBacklogged(now) {
		return false, nil
	}
	provisionalRate, provisionalDelay := c.provisionalLocked()
	for i := c.mappingHead; i < len(c.mappings) && i < c.mappingHead+64; i++ {
		mapping := &c.mappings[i]
		if !mapping.repairedAt.IsZero() || mapping.end <= c.tx.Una {
			continue
		}
		slow := c.getLeg(mapping.path)
		if slow == nil || slow.path.Generation != mapping.generation || mapping.pathEnd <= slow.path.Received {
			continue
		}
		length := mapping.end - max(mapping.seq, c.tx.Una)
		fast := c.bestIdleLegLocked(slow, length)
		if fast == nil {
			return false, nil
		}
		fastArrival := fast.path.CompletionTime(int(length), provisionalRate, provisionalDelay, now)
		if fastArrival+0.002 >= slow.path.RemainingDelivery(mapping.pathEnd, mapping.sentAt, provisionalDelay, now) {
			continue
		}
		segment, ok := c.tx.Range(max(mapping.seq, c.tx.Una), int(length))
		if !ok {
			continue
		}
		if err := c.submitLocked(fast, segment, true, now); err != nil {
			return false, err
		}
		mapping.repairedAt = now
		c.tailE.Add(1)
		c.tailB.Add(uint64(segment.Length))
		return true, nil
	}
	return false, nil
}

// repairDroppedLocked resends byte ranges the receiver reported as dropped
// for lack of memory. Several holes are repaired per round trip.
func (c *mpCore) repairDroppedLocked(now time.Time) (bool, error) {
	for len(c.repairQueue) > 0 && c.repairBudgetLocked(now) {
		r := &c.repairQueue[0]
		start, end := max(r.Start, c.tx.Una), min(r.End, c.tx.Next)
		if start >= end {
			c.repairQueue = c.repairQueue[1:]
			continue
		}
		target := c.bestIdleLegLocked(nil, end-start)
		if target == nil {
			return false, nil
		}
		segment, ok := c.tx.Range(start, int(min(end-start, uint64(c.cfg.FrameSize))))
		if !ok {
			c.repairQueue = c.repairQueue[1:]
			continue
		}
		if err := c.submitLocked(target, segment, true, now); err != nil {
			return false, err
		}
		r.Start = segment.End()
		c.repairWindowBytes += uint64(segment.Length)
		c.fallbackB.Add(uint64(segment.Length))
		c.fallbackF.Add(1)
		c.fallbackE.Add(1)
		return true, nil
	}
	return false, nil
}

// queueRepairsLocked accepts the receiver's dropped ranges, at most once per
// range start within a retransmission timeout.
func (c *mpCore) queueRepairsLocked(nacks []stream.Range, now time.Time) {
	for start := range c.repairAt {
		if start < c.tx.Una {
			delete(c.repairAt, start)
		}
	}
	for _, r := range nacks {
		r.Start, r.End = max(r.Start, c.tx.Una), min(r.End, c.tx.Next)
		if r.Start >= r.End {
			continue
		}
		if at, ok := c.repairAt[r.Start]; ok && now.Sub(at) < c.repairTimeoutLocked() {
			continue
		}
		if c.repairAt == nil {
			c.repairAt = make(map[uint64]time.Time)
		}
		c.repairAt[r.Start] = now
		c.repairQueue = append(c.repairQueue, r)
	}
	if len(c.repairQueue) > 0 {
		wakeFlow(c.pumpWake)
	}
}

func (c *mpCore) repairTimeoutLocked() time.Duration {
	timeout := time.Duration(0)
	for _, leg := range c.availableLegs() {
		timeout = max(timeout, leg.path.RTO(c.cfg.PathStallTimeoutMin))
	}
	return max(timeout, 200*time.Millisecond)
}

// bestIdleLegLocked returns the usable, idle leg with the earliest completion
// time, excluding avoid.
func (c *mpCore) bestIdleLegLocked(avoid *mpLeg, length uint64) *mpLeg {
	now := time.Now()
	provisional, provisionalDelay := c.provisionalLocked()
	var best *mpLeg
	bestTime := math.Inf(1)
	for _, leg := range c.availableLegs() {
		if leg == avoid || leg.busy || !c.usableLeg(leg) || !leg.ready.Load() {
			continue
		}
		if t := leg.path.CompletionTime(int(length), provisional, provisionalDelay, now); t < bestTime {
			best, bestTime = leg, t
		}
	}
	return best
}

// repairMappingsLocked reinjects mappings whose leg failed (lost generation)
// or stalled, and the connection head when a path receipt shows it arrived
// but the Data ACK did not move (receiver eviction). The full scan only runs
// after a leg failed or stalled; otherwise only the head is examined.
func (c *mpCore) repairMappingsLocked(now time.Time) (bool, error) {
	last := len(c.mappings)
	if !c.repairScan {
		last = min(last, c.mappingHead+1)
	}
	candidates := false
	for i := c.mappingHead; i < last; i++ {
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
		candidates = true
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
		if target == nil || target.busy || !target.ready.Load() || !c.usableLeg(target) {
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
	if c.repairScan && !candidates {
		c.repairScan = false
	}
	return false, nil
}

// opportunisticLocked implements opportunistic reinjection with penalization
// (as in MPTCP): when new data are blocked by the receive window or the send
// history while data near the connection head wait on a path that has
// delivered nothing for a whole RTT, or that would deliver them later than an
// idle path can now, the idle path resends them in order, and the slow path
// takes no new data for one of its RTTs.
func (c *mpCore) opportunisticLocked(now time.Time) (bool, error) {
	if c.mappingHead >= len(c.mappings) || !c.active.Load() {
		return false, nil
	}
	frame := uint64(c.cfg.FrameSize)
	windowBlocked := c.tx.WriteNext > c.tx.Next && c.tx.Next >= c.tx.WindowEnd
	historyBlocked := c.tx.Buffered()+frame > c.historyLimitLocked(now)
	if !windowBlocked && !historyBlocked {
		return false, nil
	}
	head := &c.mappings[c.mappingHead]
	slow := c.getLeg(head.path)
	if slow == nil || slow.path.Generation != head.generation {
		return false, nil
	}
	fast := c.bestIdleLegLocked(slow, frame)
	if fast == nil || fast.path.SRTT == 0 || slow.path.SRTT == 0 {
		return false, nil
	}
	// A path that delivered nothing for a whole RTT of its own holds the head
	// back; so does one whose remaining data arrive later than a resend on
	// the idle path. Ordinary RTT differences are left to ECF scheduling.
	stuck := now.Sub(slow.path.LastProgress) >= max(slow.path.SRTT, 2*fast.path.SRTT)
	_, provisionalDelay := c.provisionalLocked()
	limit := c.tx.Una + uint64(fast.path.Rate*slow.path.SRTT.Seconds())
	for i := c.mappingHead; i < len(c.mappings) && i < c.mappingHead+64; i++ {
		mapping := &c.mappings[i]
		if mapping.seq >= limit && i > c.mappingHead {
			break
		}
		if mapping.path != slow.id || mapping.generation != slow.path.Generation || mapping.end <= c.tx.Una {
			continue
		}
		if !mapping.repairedAt.IsZero() && now.Sub(mapping.repairedAt) < fast.path.SRTT {
			continue
		}
		length := mapping.end - max(mapping.seq, c.tx.Una)
		late := fast.path.CompletionTime(int(length), 0, provisionalDelay, now)+0.002 < slow.path.RemainingDelivery(mapping.pathEnd, mapping.sentAt, provisionalDelay, now)
		if !late && (!stuck || now.Sub(mapping.sentAt) < fast.path.SRTT) {
			continue
		}
		segment, ok := c.tx.Range(max(mapping.seq, c.tx.Una), int(mapping.end-max(mapping.seq, c.tx.Una)))
		if !ok {
			continue
		}
		if err := c.submitLocked(fast, segment, true, now); err != nil {
			return false, err
		}
		// The original copy stays in flight on the slow path; whichever
		// arrives first is used. Only the repair time is recorded.
		mapping.repairedAt = now
		slow.penaltyUntil = now.Add(slow.path.SRTT)
		c.opportunisticE.Add(1)
		c.fallbackB.Add(uint64(segment.Length))
		c.fallbackF.Add(1)
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
