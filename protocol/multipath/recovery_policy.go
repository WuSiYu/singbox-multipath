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

func (c *mpCore) controlLeg() *mpLeg {
	if c.cfg.Recovery == nil {
		return c.getLeg(0)
	}
	for id := uint8(0); id < 2; id++ {
		if leg := c.getLeg(id); leg != nil && c.cfg.Recovery.allows(id) {
			return leg
		}
	}
	return nil
}

type recoveryHealth struct {
	lastTCP, lastUDP time.Time
	since            time.Time
	healthy          bool
	hadFailure       bool
}

// Each reply proves a recently issued challenge. Time spent behind a stalled
// reliable stream does not become fresh evidence when that stream unblocks.
func (h *recoveryHealth) refresh(now time.Time, timeout time.Duration) {
	good := !h.lastTCP.IsZero() && !h.lastUDP.IsZero() && now.Sub(h.lastTCP) < timeout && now.Sub(h.lastUDP) < timeout
	if good && !h.healthy {
		h.since = now
	}
	if !good {
		// An unanswered startup probe is not a recovered-path failure. Once
		// a confirmed healthy path fails, all later returns use the hold.
		if h.healthy {
			h.hadFailure = true
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
	if health[preferred].healthy && (!health[preferred].hadFailure || now.Sub(health[preferred].since) >= delay) {
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
