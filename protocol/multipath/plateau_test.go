package multipath

import (
	"testing"
	"time"
)

func TestRatePlateau(t *testing.T) {
	var p ratePlateau
	now := time.Unix(0, 0)
	step := func(rate float64, rtt time.Duration, elapsed time.Duration) bool {
		now = now.Add(elapsed)
		return p.update(rate, rtt, now)
	}
	// No sample yet: rounds of 10 ms pass without growth.
	step(0, 0, 0)
	step(0, 0, 10*time.Millisecond)
	if !step(0, 0, 10*time.Millisecond) {
		t.Fatal("a path delivering nothing must count as not growing")
	}
	// The first sample is growth and restarts the count immediately.
	if step(5e6, 100*time.Millisecond, time.Millisecond) {
		t.Fatal("first delivery sample did not restart the plateau count")
	}
	// Slow start: doubling every round trip never plateaus.
	rate := 5e6
	for range 6 {
		rate *= 2
		if step(rate, 100*time.Millisecond, 100*time.Millisecond) {
			t.Fatal("doubling rate reported as a plateau")
		}
	}
	// Under 25% growth for two round trips is a plateau.
	step(rate*1.1, 100*time.Millisecond, 100*time.Millisecond)
	if !step(rate*1.15, 100*time.Millisecond, 100*time.Millisecond) {
		t.Fatal("flat rate not reported as a plateau")
	}
}
