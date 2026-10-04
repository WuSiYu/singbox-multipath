package multipath

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
)

func TestRecoveryDurations(t *testing.T) {
	timeout, delay, err := recoveryDurations(0, 0)
	if err != nil || timeout != 5*time.Second || delay != 30*time.Second {
		t.Fatalf("defaults: %s %s %v", timeout, delay, err)
	}
	timeout, delay, err = recoveryDurations(7*time.Second, 45*time.Second)
	if err != nil || timeout != 7*time.Second || delay != 45*time.Second {
		t.Fatalf("overrides: %s %s %v", timeout, delay, err)
	}
	for _, pair := range [][2]time.Duration{{-1, 0}, {time.Millisecond, 0}, {6 * time.Minute, 0}, {0, -1}, {0, time.Millisecond}, {0, 2 * time.Hour}} {
		if _, _, err := recoveryDurations(pair[0], pair[1]); err == nil {
			t.Fatalf("accepted invalid durations: %v", pair)
		}
	}
}

func TestRecoveryHealthAndHold(t *testing.T) {
	now := time.Now()
	var health [2]recoveryHealth
	for id := range health {
		health[id] = recoveryHealth{lastTCP: now, lastUDP: now}
		health[id].refresh(now, 5*time.Second, 30*time.Second)
	}
	if got := recoveryChoice(0, 0, health, now, 30*time.Second); got != 0 {
		t.Fatal(got)
	}
	health[0].refresh(now.Add(5*time.Second), 5*time.Second, 30*time.Second)
	health[1].lastTCP = now.Add(5 * time.Second)
	health[1].lastUDP = health[1].lastTCP
	health[1].refresh(health[1].lastTCP, 5*time.Second, 30*time.Second)
	if got := recoveryChoice(0, 0, health, now.Add(5*time.Second), 30*time.Second); got != 1 {
		t.Fatal(got)
	}
	recovered := now.Add(6 * time.Second)
	health[0].lastTCP = recovered
	health[0].lastUDP = recovered
	health[0].refresh(recovered, 5*time.Second, 30*time.Second)
	// An isolated failure holds the return for three seconds, not the full
	// failback_delay, so a short blip does not cost half a minute.
	if got := recoveryChoice(1, 0, health, recovered.Add(2*time.Second), 30*time.Second); got != 1 {
		t.Fatal("early failback", got)
	}
	if got := recoveryChoice(1, 0, health, recovered.Add(3*time.Second), 30*time.Second); got != 0 {
		t.Fatal("missing failback", got)
	}
	// One missed probe does not restart the hold period.
	health[0].refresh(recovered.Add(3*time.Second), 5*time.Second, 30*time.Second)
	if health[0].since != recovered {
		t.Fatal("hold restarted after a missed probe")
	}
	health[1].healthy = false
	if got := recoveryChoice(1, 0, health, recovered.Add(3*time.Second), 30*time.Second); got != 0 {
		t.Fatal("held back the only available path")
	}
	// UDP can prefer leg1 independently of TCP's leg0 preference.
	health[1].healthy = true
	if got := recoveryChoice(1, 1, health, recovered, 30*time.Second); got != 1 {
		t.Fatal("UDP preference changed")
	}
}

func TestRecoveryFailbackHoldBacksOff(t *testing.T) {
	now := time.Now()
	h := recoveryHealth{lastTCP: now, lastUDP: now}
	h.refresh(now, 5*time.Second, 30*time.Second)
	var holds []time.Duration
	for flap := 0; flap < 5; flap++ {
		down := now.Add(time.Duration(flap*10+6) * time.Second)
		h.refresh(down, 5*time.Second, 30*time.Second)
		up := down.Add(time.Second)
		h.lastTCP, h.lastUDP = up, up
		h.refresh(up, 5*time.Second, 30*time.Second)
		holds = append(holds, h.hold(30*time.Second))
	}
	want := []time.Duration{3 * time.Second, 6 * time.Second, 12 * time.Second, 24 * time.Second, 30 * time.Second}
	for i := range want {
		if holds[i] != want[i] {
			t.Fatalf("holds %v, want %v", holds, want)
		}
	}
	// A path that stayed healthy for the whole failback window starts over.
	later := now.Add(10 * time.Minute)
	h.refresh(later, 5*time.Second, 30*time.Second)
	if h.hold(30*time.Second) != 3*time.Second {
		t.Fatal("hold did not reset after a quiet window")
	}
}

func TestRecoveryUDPFragments(t *testing.T) {
	for _, size := range []int{0, 1, 900, 901, 8192, 65507} {
		budget := newMemoryBudget(8<<20, false)
		var packets []recoveryDatagram
		c, err := newRecoveryPacketConn([16]byte{1}, budget, func(d recoveryDatagram) error {
			wire, e := d.encode()
			if e != nil {
				return e
			}
			decoded, e := decodeRecoveryDatagram(wire)
			packets = append(packets, decoded)
			return e
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		p := bytes.Repeat([]byte{byte(size)}, size)
		if n, e := c.WriteTo(p, M.ParseSocksaddr("127.0.0.1:53")); e != nil || n != size {
			t.Fatalf("%d: %d %v", size, n, e)
		}
		for j := len(packets) - 1; j >= 0; j-- {
			c.deliver(packets[j], time.Now())
			c.deliver(packets[j], time.Now())
		}
		c.SetReadDeadline(time.Now().Add(time.Second))
		got := make([]byte, 65535)
		n, _, err := c.ReadFrom(got)
		if err != nil || !bytes.Equal(got[:n], p) {
			t.Fatalf("%d: payload corrupted n=%d %v", size, n, err)
		}
		c.Close()
		if s := budget.snapshot(); s.UsedBytes != s.CachedBytes {
			t.Fatalf("budget leak %+v", s)
		}
	}
}

func TestRecoveryPrimaryLossAndRejoin(t *testing.T) {
	for _, aggregate := range []bool{false, true} {
		cfg := testCoreConfig()
		cfg.AggregationEnabled = aggregate
		policy := &recoveryPolicy{}
		policy.update(1, 3, 0)
		cfg.Recovery = policy
		cfg.Memory = newMemoryBudget(32<<20, false)
		a, appA := newCore(context.Background(), cfg)
		cfg.Memory = newMemoryBudget(32<<20, false)
		b, appB := newCore(context.Background(), cfg)
		t.Cleanup(func() { a.Close(); b.Close() })
		var paths [2]net.Conn
		for id := byte(0); id < 2; id++ {
			left, right := net.Pipe()
			paths[id] = left
			connectTestLeg(t, a, b, id, left, right)
		}
		if aggregate {
			a.activate(activationInfo{Reason: activationReasonBytes})
			b.activate(activationInfo{Reason: activationReasonBytes})
		}
		payload := bytes.Repeat([]byte("bidirectional-recovery-data-"), 128*1024)
		appA.SetDeadline(time.Now().Add(10 * time.Second))
		appB.SetDeadline(time.Now().Add(10 * time.Second))
		results := make(chan error, 4)
		for _, pair := range [][2]net.Conn{{appA, appB}, {appB, appA}} {
			go func(c net.Conn) {
				_, err := c.Write(payload)
				if err == nil {
					err = c.(interface{ CloseWrite() error }).CloseWrite()
				}
				results <- err
			}(pair[0])
			go func(c net.Conn) {
				got, err := io.ReadAll(c)
				if err == nil && !bytes.Equal(got, payload) {
					err = io.ErrUnexpectedEOF
				}
				results <- err
			}(pair[1])
		}
		paths[0].Close()
		for range 4 {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		if a.isDone() || b.isDone() {
			t.Fatal("leg failure closed logical stream")
		}
		left, right := net.Pipe()
		deadline := time.Now().Add(time.Second)
		for a.reserveLeg(0) != nil {
			if time.Now().After(deadline) {
				t.Fatal("leg0 cannot rejoin")
			}
			time.Sleep(time.Millisecond)
		}
		if _, err := a.commitLeg(0, left, nil); err != nil {
			t.Fatal(err)
		}
		for b.reserveLeg(0) != nil {
			if time.Now().After(deadline) {
				t.Fatal("peer leg0 cannot rejoin")
			}
			time.Sleep(time.Millisecond)
		}
		if _, err := b.commitLeg(0, right, nil); err != nil {
			t.Fatal(err)
		}
		a.Close()
		b.Close()
	}
}
