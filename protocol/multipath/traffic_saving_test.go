package multipath

import (
	"bytes"
	"context"
	"github.com/sagernet/sing-box/protocol/multipath/stream"
	"io"
	"net"
	"testing"
	"time"
)

func TestTrafficSavingSelection(t *testing.T) {
	cfg := testCoreConfig()
	cfg.Leg0TrafficSaving = true
	c := &mpCore{cfg: cfg, memory: newMemoryBudget(64<<20, false), legs: make(map[uint8]*mpLeg), tx: stream.NewSender(uint64(cfg.FrameSize))}
	p, s := &mpLeg{id: 0}, &mpLeg{id: 1}
	p.ready.Store(true)
	s.ready.Store(true)
	c.legs[0], c.legs[1] = p, s
	check := func(want *mpLeg, mode uint64) {
		t.Helper()
		if got := c.choosePathLocked(cfg.FrameSize); got != want || c.dataModeLocked() != mode {
			t.Fatalf("path=%p want=%p mode=%s", got, want, dataModeName(c.dataModeLocked()))
		}
	}
	check(p, 1)
	c.active.Store(true)
	check(s, 3)
	s.busy = true
	check(nil, 3) // Healthy backpressure must not spill new DATA onto leg0.
	s.busy = false
	s.path.Sent = uint64(cfg.FrameSize) * 4
	check(nil, 3) // Discovery credit exhausted is not a failure.
	s.path.Stale = true
	check(p, 4)
	s.path.Stale = false
	s.path.Sent = 0
	check(s, 3)
	// Neither receiver nor local memory pressure moves new data back to leg0;
	// a full receiver only limits how much new data are in flight.
	c.peerPressure = true
	check(s, 3)
	c.peerPressure = false
	c.memory.other = c.memory.limit
	check(s, 3)
	c.memory.other = 0
	s.ready.Store(false)
	check(p, 4)
	s.ready.Store(true)
	check(s, 3)
	cfg.Leg0TrafficSaving = false
	c.cfg = cfg
	s.busy = true
	check(p, 2)
}

func TestTrafficSavingExclusiveDataAndHalfClose(t *testing.T) {
	cfg := testCoreConfig()
	cfg.Leg0TrafficSaving = true
	left, a := newCore(context.Background(), cfg)
	right, b := newCore(context.Background(), cfg)
	defer left.Close()
	defer right.Close()
	p0, p1 := net.Pipe()
	connectTestLeg(t, left, right, 0, p0, p1)
	s0, s1 := net.Pipe()
	connectTestLeg(t, left, right, 1, s0, s1)
	left.activate(activationInfo{Reason: activationReasonBytes})
	right.activate(activationInfo{Reason: activationReasonBytes})
	payload := bytes.Repeat([]byte("exclusive-traffic-saving-"), 65536)
	result := make(chan error, 1)
	go func() {
		_, err := a.Write(payload)
		if err == nil {
			err = a.(closeWriter).CloseWrite()
		}
		result <- err
	}()
	_ = b.SetReadDeadline(time.Now().Add(10 * time.Second))
	got, err := io.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if err = <-result; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, got) {
		t.Fatal("DATA mismatch")
	}
	// A leg counts its bytes after the child write returns, which may be
	// after the peer has already read them.
	waitForStatus(t, func() bool { return left.legCounters[1].txBytes.Load() == uint64(len(payload)) })
	if left.legCounters[0].txBytes.Load() != 0 || left.legCounters[1].txBytes.Load() != uint64(len(payload)) {
		t.Fatalf("DATA leaked onto leg0: %d/%d", left.legCounters[0].txBytes.Load(), left.legCounters[1].txBytes.Load())
	}
	go func() {
		_, err := b.Write([]byte("reply-after-fin"))
		if err == nil {
			err = b.(closeWriter).CloseWrite()
		}
		result <- err
	}()
	_ = a.SetReadDeadline(time.Now().Add(10 * time.Second))
	reply, err := io.ReadAll(a)
	if err != nil {
		t.Fatal(err)
	}
	if err = <-result; err != nil {
		t.Fatal(err)
	}
	if string(reply) != "reply-after-fin" {
		t.Fatal("reverse stream lost after FIN")
	}
	if right.legCounters[0].txBytes.Load() != 0 {
		t.Fatal("reverse DATA leaked onto leg0")
	}
}
