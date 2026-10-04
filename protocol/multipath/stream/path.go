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
	LastProgress  time.Time
	Stale         bool
	ReleaseFlight func(prepaid bool)
	flights       []Flight
	head          int
	sampleBytes   uint64
	sampleTime    uint64
}

func (p *Path) Outstanding() uint64 { return p.Sent - p.Received }
func (p *Path) Flights() int        { return len(p.flights) - p.head }

func (p *Path) Submitted(length int, now time.Time, prepaid ...bool) (uint64, error) {
	if length <= 0 || uint64(length) > math.MaxUint64-p.Sent {
		return 0, ErrSequence
	}
	seq := p.Sent
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
	var sent time.Time
	for p.head < len(p.flights) && p.flights[p.head].End <= receipt.Next {
		sent = p.flights[p.head].SentAt
		if p.ReleaseFlight != nil {
			p.ReleaseFlight(p.flights[p.head].Prepaid)
		}
		p.flights[p.head] = Flight{}
		p.head++
	}
	if !sent.IsZero() && now.After(sent) {
		rtt := now.Sub(sent)
		if p.SRTT == 0 {
			p.SRTT, p.RTTVar, p.MinimumRTT = rtt, rtt/2, rtt
		} else {
			delta := rtt - p.SRTT
			if delta < 0 {
				delta = -delta
			}
			p.RTTVar = (3*p.RTTVar + delta) / 4
			p.SRTT = (7*p.SRTT + rtt) / 8
			p.MinimumRTT = min(p.MinimumRTT, rtt)
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

// CompletionTime estimates when a new segment of length bytes would be fully
// delivered on this path: queued work over the measured delivery rate plus the
// one-way propagation delay. An unmeasured path borrows provisionalRate.
func (p *Path) CompletionTime(length int, provisionalRate float64) float64 {
	rate := p.Rate
	if rate <= 0 {
		rate = provisionalRate
	}
	if rate <= 0 {
		rate = 1
	}
	delay := p.MinimumRTT
	if delay == 0 {
		delay = p.SRTT
	}
	return float64(p.Outstanding()+uint64(length))/rate + delay.Seconds()/2
}
