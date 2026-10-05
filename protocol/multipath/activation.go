package multipath

import "time"

// The rate trigger is opt-in: by default leg1 joins when leg0 is the
// bottleneck (the queue trigger), not merely because a flow is fast.
func resolveActivationThreshold(threshold *uint32, _ uint64) uint64 {
	if threshold != nil {
		return uint64(*threshold) * 1_000_000 / 8
	}
	return 0
}

const (
	defaultActivationWindow = 200 * time.Millisecond
	minActivationWindow     = 20 * time.Millisecond
	maxActivationWindow     = 10 * time.Second
)

func (c *mpCore) activationLoop() {
	if !c.cfg.AggregationEnabled || c.active.Load() ||
		(!c.cfg.ActivationOnQueue && c.cfg.ThresholdBytesPS == 0 && c.cfg.ActivationAfterBytes == 0) {
		return
	}
	interval := c.cfg.ActivationWindow / 10
	if interval < 20*time.Millisecond {
		interval = 20 * time.Millisecond
	}
	if interval > 200*time.Millisecond {
		interval = 200 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	windowStart := time.Now()
	windowBase := c.ingressBytes.Load()
	var queueHighSince time.Time
	var plateau ratePlateau
	for {
		select {
		case <-c.done:
			return
		case now := <-ticker.C:
			if c.active.Load() {
				return
			}
			bytesNow := c.ingressBytes.Load()
			if info, ok := activationAfterBytes(c.cfg, bytesNow, windowBase, now.Sub(windowStart)); ok {
				c.activate(info)
				return
			}
			if (c.cfg.ThresholdBytesPS > 0 || (c.cfg.ActivationAfterBytes > 0 && c.cfg.ActivationAfterBytesMinBytesPS > 0)) && now.Sub(windowStart) >= c.cfg.ActivationWindow {
				delta := bytesNow - windowBase
				elapsed := now.Sub(windowStart)
				rate := uint64(0)
				if elapsed > 0 {
					rate = uint64(float64(delta) / elapsed.Seconds())
				}
				if c.cfg.ThresholdBytesPS > 0 && rate >= c.cfg.ThresholdBytesPS {
					c.activate(activationInfo{
						Reason:           activationReasonThroughput,
						WindowBytes:      delta,
						RateBytesPS:      rate,
						ThresholdBytesPS: c.cfg.ThresholdBytesPS,
						Elapsed:          elapsed,
					})
					return
				}
				windowStart = now
				windowBase = bytesNow
			}
			if !c.cfg.ActivationOnQueue {
				continue
			}
			primary := c.getLeg(0)
			if primary == nil {
				continue
			}
			// Leg0 is the bottleneck when the application keeps the small,
			// rate-sized unsent queue at least half full and leg0's delivery
			// rate stopped growing. A backlog during leg0's slow start only
			// shows its window opening; a path added then would carry the
			// connection head while leg0 overtakes it.
			c.stateMu.Lock()
			backlogBytes := int64(c.tx.WriteNext - min(c.tx.Next, c.tx.WriteNext))
			queueBytes := int64(c.unsentLimitLocked())
			full := plateau.update(primary.path.Rate, primary.path.SRTT, now)
			c.stateMu.Unlock()
			if backlogBytes*2 >= queueBytes {
				if queueHighSince.IsZero() {
					queueHighSince = now
				} else if now.Sub(queueHighSince) >= c.cfg.ActivationWindow && full {
					c.activate(activationInfo{
						Reason:           activationReasonLeg0Queue,
						BacklogBytes:     backlogBytes,
						QueueBytes:       queueBytes,
						Elapsed:          now.Sub(queueHighSince),
						RequiredDuration: c.cfg.ActivationWindow,
					})
					return
				}
			} else {
				queueHighSince = time.Time{}
			}
		}
	}
}

func activationAfterBytes(cfg coreConfig, bytesNow, windowBase uint64, elapsed time.Duration) (activationInfo, bool) {
	if cfg.ActivationAfterBytes == 0 || bytesNow < cfg.ActivationAfterBytes {
		return activationInfo{}, false
	}
	info := activationInfo{
		Reason:         activationReasonBytes,
		CurrentBytes:   bytesNow,
		ThresholdBytes: cfg.ActivationAfterBytes,
	}
	if cfg.ActivationAfterBytesMinBytesPS == 0 {
		return info, true
	}
	if elapsed < cfg.ActivationWindow || elapsed <= 0 {
		return activationInfo{}, false
	}
	delta := bytesNow - windowBase
	rate := uint64(float64(delta) / elapsed.Seconds())
	if rate < cfg.ActivationAfterBytesMinBytesPS {
		return activationInfo{}, false
	}
	info.RateBytesPS = rate
	info.MinRateBytesPS = cfg.ActivationAfterBytesMinBytesPS
	info.Elapsed = elapsed
	return info, true
}

func (c *mpCore) activate(info activationInfo) {
	if !c.cfg.AggregationEnabled {
		return
	}
	c.activateOnce.Do(func() {
		c.activationMu.Lock()
		c.activation = info
		c.activationAt = time.Now()
		c.activationMu.Unlock()
		c.active.Store(true)
		close(c.activeCh)
		c.notifyLeg1Active()
	})
}

func (c *mpCore) notifyLeg1Active() {
	if !c.active.Load() || c.cfg.OnLeg1Active == nil {
		return
	}
	leg := c.getLeg(1)
	if leg == nil {
		return
	}
	c.activationMu.Lock()
	if c.notifiedLeg1 == leg {
		c.activationMu.Unlock()
		return
	}
	info := c.activation
	reconnect := c.leg1Joins > 0
	c.notifiedLeg1 = leg
	c.leg1Joins++
	callback := c.cfg.OnLeg1Active
	c.activationMu.Unlock()
	callback(info, reconnect)
}

// ratePlateau detects that a path's delivery rate stopped growing, as BBR
// detects a full pipe: no 25% gain over two round trips. Growth restarts the
// count at once; a path that delivers nothing measurable does not grow.
type ratePlateau struct {
	roundStart time.Time
	best       float64
	rounds     int
}

func (p *ratePlateau) update(rate float64, rtt time.Duration, now time.Time) bool {
	switch {
	case p.roundStart.IsZero():
		p.roundStart, p.best = now, rate
	case rate > 0 && rate >= p.best*1.25:
		p.roundStart, p.best, p.rounds = now, rate, 0
	case now.Sub(p.roundStart) >= max(rtt, 10*time.Millisecond):
		p.roundStart, p.best = now, max(p.best, rate)
		p.rounds++
	}
	return p.rounds >= 2
}
