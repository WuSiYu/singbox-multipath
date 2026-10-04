package multipath

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// A single policy belongs to an outbound (or its server-side recovery group),
// not to a TCP flow. The client is the sole authority for path selection.
type recoveryPolicy struct {
	mu    sync.Mutex
	epoch uint64
	mask  atomic.Uint32
	udp   atomic.Uint32
}

func (p *recoveryPolicy) allows(id uint8) bool { return p.mask.Load()&(1<<id) != 0 }
func (p *recoveryPolicy) snapshot() (uint64, byte, byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.epoch, byte(p.mask.Load()), byte(p.udp.Load())
}
func (p *recoveryPolicy) update(epoch uint64, mask, udp byte) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if epoch <= p.epoch || mask > 3 || udp > 1 {
		return false
	}
	p.epoch = epoch
	p.mask.Store(uint32(mask))
	p.udp.Store(uint32(udp))
	return true
}

// usableLeg reports whether leg can carry new data now. Leg0 may carry the
// first bytes of a lazy fast-open session before its hello is answered.
func (c *mpCore) usableLeg(leg *mpLeg) bool {
	return leg != nil && (leg.id == 0 || leg.ready.Load()) && !leg.path.Stale &&
		(c.cfg.Recovery == nil || c.cfg.Recovery.allows(leg.id))
}

// preferredDataLeg carries new data before activation (and all data when
// aggregation is off): leg0 while usable, otherwise leg1. Losing or stalling
// leg0 therefore moves the session to leg1 without the failover option.
func (c *mpCore) preferredDataLeg() *mpLeg {
	for id := uint8(0); id < 2; id++ {
		if leg := c.getLeg(id); c.usableLeg(leg) {
			return leg
		}
	}
	return nil
}

// otherUsableLeg returns a usable leg other than id, if any.
func (c *mpCore) otherUsableLeg(id uint8) *mpLeg {
	if leg := c.getLeg(1 - id); c.usableLeg(leg) {
		return leg
	}
	return nil
}

type recoveryHealth struct {
	lastTCP, lastUDP time.Time
	since            time.Time
	healthy          bool
	hadFailure       bool
	// failures counts recent losses of a healthy path; the return hold
	// doubles with each one inside the failback window.
	failures    int
	lastFailure time.Time
}

const failbackHoldMin = 3 * time.Second

// hold is the stability period before returning to this path: three seconds
// after an isolated failure, doubling for repeated failures, up to delay.
func (h *recoveryHealth) hold(delay time.Duration) time.Duration {
	if h.failures <= 1 {
		return min(delay, failbackHoldMin)
	}
	return min(delay, failbackHoldMin<<min(h.failures-1, 16))
}

// Each reply proves a recently issued challenge. Time spent behind a stalled
// reliable stream does not become fresh evidence when that stream unblocks.
func (h *recoveryHealth) refresh(now time.Time, timeout, delay time.Duration) {
	good := !h.lastTCP.IsZero() && !h.lastUDP.IsZero() && now.Sub(h.lastTCP) < timeout && now.Sub(h.lastUDP) < timeout
	if good && !h.healthy {
		h.since = now
	}
	if !good {
		// An unanswered startup probe is not a recovered-path failure. Once
		// a confirmed healthy path fails, all later returns use the hold.
		if h.healthy {
			h.hadFailure = true
			if h.lastFailure.IsZero() || now.Sub(h.lastFailure) >= delay+timeout {
				h.failures = 0
			}
			h.failures++
			h.lastFailure = now
		}
		h.since = time.Time{}
	}
	h.healthy = good
}

func recoveryChoice(current, preferred byte, health [2]recoveryHealth, now time.Time, delay time.Duration) byte {
	if current == preferred || !health[current].healthy {
		if health[preferred].healthy {
			return preferred
		}
		if health[1-preferred].healthy {
			return 1 - preferred
		}
		return current
	}
	if health[preferred].healthy && (!health[preferred].hadFailure || now.Sub(health[preferred].since) >= health[preferred].hold(delay)) {
		return preferred
	}
	return current
}

func recoveryDurations(timeout, delay time.Duration) (time.Duration, time.Duration, error) {
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	if delay == 0 {
		delay = 30 * time.Second
	}
	if timeout < time.Second || timeout > 5*time.Minute {
		return 0, 0, errors.New("failover_timeout must be between 1s and 5m")
	}
	if delay < time.Second || delay > time.Hour {
		return 0, 0, errors.New("failback_delay must be between 1s and 1h")
	}
	return timeout, delay, nil
}
