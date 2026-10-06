package stream

import (
	"math"
	"time"
)

// Receipt describes bytes which traversed the complete child path, including
// duplicate logical data. ReceivedAt is the receiver's monotonic microsecond
// timestamp at the last DATA receipt, not the time the feedback was transmitted.
// Generation scopes all path sequence numbers to one transport incarnation.
type Receipt struct {
	Generation      uint64
	Next            uint64
	ReceivedAt      uint64
	FirstNext       uint64
	FirstReceivedAt uint64
}

type Flight struct {
	End     uint64
	SentAt  time.Time
	Prepaid bool
}

// Path has a delivery pipeline, not another congestion window. Packet-level
// pacing and loss recovery remain entirely in the child transport.
type Path struct {
	Generation    uint64
	Sent          uint64
	Received      uint64
	Rate          float64
	SRTT          time.Duration
	RTTVar        time.Duration
	MinimumRTT    time.Duration
	minimumAt     time.Time     // when MinimumRTT was last confirmed
	recentRTT     time.Duration // fast-moving average of the latest samples
	LastProgress  time.Time
	Stale         bool
	ReleaseFlight func(prepaid bool)
	flights       []Flight
	head          int
	sampleBytes   uint64
	sampleTime    uint64
	// restart marks that the path went idle: the next receipt starts a new
	// rate sample instead of averaging delivery over the idle gap.
	restart bool
	// Bytes delivered per round trip: the previous complete window and the
	// start of the current one.
	windowStart time.Time
	windowBase  uint64
	recentBytes uint64
}

func (p *Path) Outstanding() uint64 { return p.Sent - p.Received }
func (p *Path) Flights() int        { return len(p.flights) - p.head }

func (p *Path) Submitted(length int, now time.Time, prepaid ...bool) (uint64, error) {
	if length <= 0 || uint64(length) > math.MaxUint64-p.Sent {
		return 0, ErrSequence
	}
	seq := p.Sent
	if p.Outstanding() == 0 && p.sampleTime != 0 {
		p.restart = true
	}
	p.Sent += uint64(length)
	p.flights = append(p.flights, Flight{End: p.Sent, SentAt: now, Prepaid: len(prepaid) > 0 && prepaid[0]})
	return seq, nil
}

// Feedback from an old generation is ignored before examining its counters.
// Receipt never modifies the connection send queue or its Data ACK.
func (p *Path) Feedback(receipt Receipt, now time.Time) error {
	if receipt.Generation != p.Generation {
		return nil
	}
	if receipt.Next > p.Sent {
		return ErrSequence
	}
	if receipt.Next <= p.Received {
		return nil
	}
	p.rollWindow(now)
	// After an outage (nothing delivered for an RTO, and at least a second,
	// although data were outstanding) the first receipt spans the outage.
	// Delivery averaged over it says nothing about capacity and would
	// collapse the estimate, so it only re-anchors the sample, as after an
	// idle period. Shorter stalls, such as loss recovery on a lossy path,
	// are part of its delivery rate.
	if p.sampleTime != 0 && receipt.ReceivedAt > p.sampleTime &&
		time.Duration(receipt.ReceivedAt-p.sampleTime)*time.Microsecond >= max(p.RTO(0), time.Second) {
		p.restart = true
	}
	restarted := p.restart
	previous := p.Received
	p.Received = receipt.Next
	p.LastProgress = now
	p.Stale = false
	if p.sampleTime == 0 {
		// The first coalesced receipt may cover the whole discovery burst.
		// Preserve its receive-side span so learning does not require a second
		// otherwise idle round trip. This initializes an estimate, not a cap.
		if receipt.FirstNext > 0 && receipt.Next > receipt.FirstNext && receipt.ReceivedAt > receipt.FirstReceivedAt && receipt.FirstReceivedAt > 0 {
			p.Rate = float64(receipt.Next-receipt.FirstNext) * 1e6 / float64(receipt.ReceivedAt-receipt.FirstReceivedAt)
		}
		p.sampleTime, p.sampleBytes = receipt.ReceivedAt, receipt.Next
	} else if p.restart {
		// The first receipt after an idle period only re-anchors the sample.
		p.restart = false
		p.sampleTime, p.sampleBytes = receipt.ReceivedAt, receipt.Next
	} else if receipt.ReceivedAt > p.sampleTime {
		elapsed := float64(receipt.ReceivedAt-p.sampleTime) / 1e6
		rate := float64(receipt.Next-p.sampleBytes) / elapsed
		if p.Rate == 0 {
			p.Rate = rate
		} else {
			// Reliable streams release bursts after filling a packet gap.
			// Equal weight per receipt would treat those microsecond bursts as
			// sustained capacity. Weight by elapsed delivery time instead.
			horizon := max(p.SRTT, 5*time.Millisecond).Seconds()
			weight := -math.Expm1(-elapsed / horizon)
			p.Rate += weight * (rate - p.Rate)
		}
		p.sampleTime, p.sampleBytes = receipt.ReceivedAt, receipt.Next
	}
	// Use the last fully received mapping, avoiding the oldest mapping's extra
	// wait inside a coalesced acknowledgement. Queueing is still part of this
	// end-to-end delivery sample; a local Write is never used as an RTT sample.
	var sent, firstSent time.Time
	for p.head < len(p.flights) && p.flights[p.head].End <= receipt.Next {
		sent = p.flights[p.head].SentAt
		if firstSent.IsZero() {
			firstSent = sent
		}
		if p.ReleaseFlight != nil {
			p.ReleaseFlight(p.flights[p.head].Prepaid)
		}
		p.flights[p.head] = Flight{}
		p.head++
	}
	// A re-anchored receipt gives no rate sample, but the bytes it covers
	// arrived within their send-to-receipt time, which bounds the rate from
	// below. Without this, a path probed one frame at a time after its
	// estimate collapsed would never be measured again: each frame starts
	// from idle, and a frame delivered in one receipt yields no sample.
	if restarted && !firstSent.IsZero() && now.After(firstSent) {
		p.Rate = max(p.Rate, float64(receipt.Next-previous)/now.Sub(firstSent).Seconds())
	}
	if !sent.IsZero() && now.After(sent) {
		rtt := now.Sub(sent)
		if p.SRTT == 0 {
			p.SRTT, p.RTTVar, p.MinimumRTT, p.minimumAt, p.recentRTT = rtt, rtt/2, rtt, now, rtt
		} else {
			p.recentRTT = (p.recentRTT + rtt) / 2
			delta := rtt - p.SRTT
			if delta < 0 {
				delta = -delta
			}
			p.RTTVar = (3*p.RTTVar + delta) / 4
			p.SRTT = (7*p.SRTT + rtt) / 8
			// A minimum not seen again within the window expires, as in BBR:
			// a path whose propagation delay grew must not keep its old one.
			if rtt <= p.MinimumRTT || now.Sub(p.minimumAt) >= MinimumRTTWindow {
				p.MinimumRTT, p.minimumAt = rtt, now
			}
		}
	}
	if p.head == len(p.flights) {
		p.flights = p.flights[:0]
		if cap(p.flights) > 128 {
			p.flights = nil
		}
		p.head = 0
	} else if p.head >= 256 && p.head*2 >= len(p.flights) {
		n := copy(p.flights, p.flights[p.head:])
		clear(p.flights[n:])
		p.flights = p.flights[:n]
		p.head = 0
	}
	return nil
}

func (p *Path) Close() {
	if p.ReleaseFlight != nil {
		for i := p.head; i < len(p.flights); i++ {
			p.ReleaseFlight(p.flights[i].Prepaid)
		}
	}
	p.flights = nil
	p.head = 0
}

func (p *Path) RTO(floor time.Duration) time.Duration {
	rto := time.Second
	if p.SRTT > 0 {
		rto = p.SRTT + 4*p.RTTVar
	}
	return max(200*time.Millisecond, floor, rto)
}

func (p *Path) Stalled(now time.Time, floor time.Duration) bool {
	if p.head == len(p.flights) {
		return false
	}
	base := p.flights[p.head].SentAt
	if p.LastProgress.After(base) {
		base = p.LastProgress
	}
	return now.Sub(base) >= p.RTO(floor)
}

func (p *Path) Pipeline(initial, maximum uint64) uint64 {
	if p.Rate == 0 || p.SRTT == 0 {
		return min(initial, maximum)
	}
	// Once sampled, eligibility is governed by the shared byte window,
	// retained-memory limit and child Write backpressure. A second BDP/cwnd
	// derived from reliable-stream delivery would throttle QUIC twice and
	// mistake in-order loss-recovery bursts for a physical congestion window.
	return maximum
}

// RecentDelivery returns the bytes this path delivered in about its last
// round trip: the larger of the previous window and the current one so far.
func (p *Path) RecentDelivery(now time.Time) uint64 {
	p.rollWindow(now)
	return max(p.recentBytes, p.Received-p.windowBase)
}

// rollWindow closes the delivery window after one propagation round trip.
// Smoothed RTT would include queueing that this window itself causes, so
// windows (and the in-flight data sized from them) would grow without bound.
// A window that saw no delivery for a whole extra round trip leaves nothing.
func (p *Path) rollWindow(now time.Time) {
	length := p.MinimumRTT
	if length == 0 {
		length = p.SRTT
	}
	length = max(length, 10*time.Millisecond)
	if p.windowStart.IsZero() {
		p.windowStart, p.windowBase = now, p.Received
		return
	}
	elapsed := now.Sub(p.windowStart)
	if elapsed < length {
		return
	}
	p.recentBytes = p.Received - p.windowBase
	if elapsed >= 2*length {
		p.recentBytes = 0
	}
	p.windowStart, p.windowBase = now, p.Received
}

// RemainingDelivery estimates the seconds until this path delivers its bytes
// up to pathEnd: what is still ahead over the delivery rate plus the one-way
// delay. A path without a rate sample is still in its first slow-start
// rounds: one initial window per round trip of at least provisionalDelay,
// doubling each round.
func (p *Path) RemainingDelivery(pathEnd uint64, sentAt time.Time, provisionalDelay time.Duration, now time.Time) float64 {
	if pathEnd <= p.Received {
		return 0
	}
	remaining := float64(pathEnd - p.Received)
	delay := p.MinimumRTT
	if delay == 0 {
		delay = max(p.SRTT, provisionalDelay)
	}
	if p.Rate <= 0 {
		return math.Ceil(math.Log2(1+remaining/initialWindowBytes))*delay.Seconds() + delay.Seconds()/2
	}
	return max(remaining/p.Rate+delay.Seconds()/2, p.observedDelay()-now.Sub(sentAt).Seconds())
}

// initialWindowBytes approximates a child transport's initial window (ten
// segments).
const initialWindowBytes = 12 << 10

// MinimumRTTWindow is how long a minimum RTT sample stays valid.
const MinimumRTTWindow = 10 * time.Second

// observedDelay is the send-to-arrival latency recent frames actually saw:
// their send-to-receipt time less the receipt's return trip. Delivery-rate
// samples come from bursts and miss what a window-limited or lossy child does
// to queued data (whole extra round trips); this does not. It follows the
// latest samples rather than SRTT, so a path that recovers from an outage is
// not judged by the frames that sat out the outage.
func (p *Path) observedDelay() float64 {
	if p.recentRTT == 0 {
		return 0
	}
	return max(0, (p.recentRTT - p.MinimumRTT/2).Seconds())
}

// CompletionTime estimates when a new segment of length bytes would be fully
// delivered on this path: queued work over the measured delivery rate plus the
// one-way propagation delay. An unmeasured path borrows provisionalRate and
// provisionalDelay: the best measured rate and the longest measured delay.
func (p *Path) CompletionTime(length int, provisionalRate float64, provisionalDelay time.Duration, now time.Time) float64 {
	rate := p.Rate
	if rate <= 0 {
		rate = provisionalRate
	}
	delay := p.MinimumRTT
	if delay == 0 {
		delay = p.SRTT
	}
	if delay == 0 {
		delay = provisionalDelay
	}
	// An idle path's estimates are stale: probe it optimistically so a path
	// that recovered from a degradation is measured again instead of starved.
	if p.Outstanding() == 0 && !p.LastProgress.IsZero() && now.Sub(p.LastProgress) >= max(2*p.SRTT, 100*time.Millisecond) {
		rate = max(rate, provisionalRate)
	}
	if rate <= 0 {
		rate = 1
	}
	estimate := float64(p.Outstanding()+uint64(length))/rate + delay.Seconds()/2
	if p.Outstanding() > 0 {
		// Data already queued on this path wait as long as recent frames did.
		estimate = max(estimate, p.observedDelay())
	}
	return estimate
}
