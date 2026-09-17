package multipath

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestAutomaticMemoryLimit(t *testing.T) {
	if limit := automaticMemoryLimit(256 << 20); limit != 128<<20 {
		t.Fatalf("unexpected limit for small host: %d", limit)
	}
	if limit := automaticMemoryLimit(4 << 30); limit != 512<<20 {
		t.Fatalf("automatic limit was not capped: %d", limit)
	}
}

func TestIdleSessionsDoNotAllocateAdvertisedWindows(t *testing.T) {
	cfg := testCoreConfig()
	cfg.FrameSize, cfg.QueueFrames, cfg.QueueBytes = 65536, 256, 16<<20
	budget := newMemoryBudget(512<<20, false)
	cfg.Memory = budget
	var cores []*mpCore
	defer func() {
		for _, core := range cores {
			core.Close()
		}
		for _, core := range cores {
			<-core.released
		}
		if budget.sessions.Load() != 0 {
			t.Error("session share leaked")
		}
	}()
	for range 32 {
		core, _, err := newCoreWithError(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		cores = append(cores, core)
	}
	if budget.sessions.Load() != 32 {
		t.Fatal("incorrect live session count")
	}
	if budget.snapshot().Pressure {
		t.Fatal("idle session reservations exhausted the budget")
	}
	if budget.snapshot().UsedBytes > budget.limit/4 {
		t.Fatal("idle connections allocated their entire advertised windows")
	}
}

func acquireTestMemory(t *testing.T, budget *memoryBudget, size int) []byte {
	t.Helper()
	buffer, _ := budget.tryAcquirePrimary(size)
	if buffer == nil {
		t.Fatalf("test allocation failed: %d", size)
	}
	return buffer
}

func TestMemoryBudgetBoosterBackpressurePreservesPrimaryReserve(t *testing.T) {
	budget := newMemoryBudget(1024, false)
	first := acquireTestMemory(t, budget, 800)
	second := acquireTestMemory(t, budget, 100)
	if snapshot := budget.snapshot(); !snapshot.Pressure || snapshot.BoosterLimitBytes != 896 || snapshot.BoosterResumeBytes != 768 {
		t.Fatalf("unexpected pressure snapshot: %+v", snapshot)
	}
	if budget.boosterAllowed() || budget.reservePage(64, false) {
		t.Fatal("booster passed high watermark")
	}
	primary := acquireTestMemory(t, budget, 100)
	budget.release(primary)
	budget.release(second)
	if budget.boosterAllowed() {
		t.Fatal("booster resumed above low watermark")
	}
	budget.release(first)
	if !budget.boosterAllowed() || !budget.reservePage(64, false) {
		t.Fatal("booster did not resume")
	}
	budget.releaseSession(64)
	cached := acquireTestMemory(t, budget, 64)
	budget.release(cached)
	snapshot := budget.snapshot()
	if snapshot.Pressure || snapshot.PressureEvents != 1 {
		t.Fatalf("unexpected final memory snapshot: %+v", snapshot)
	}
	if snapshot.PeakUsedBytes < 1000 || snapshot.PeakCachedBytes < 64 {
		t.Fatalf("memory peaks not retained: %+v", snapshot)
	}
}

func TestMemoryBudgetSessionAdmissionIsReleased(t *testing.T) {
	cfg := testCoreConfig()
	reservation := minimumSessionMemory(cfg)
	budget := newMemoryBudget(reservation+1, false)
	cfg.Memory = budget
	first, _, err := newCoreWithError(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = newCoreWithError(context.Background(), cfg); !errors.Is(err, errMemoryLimit) {
		t.Fatalf("expected session admission failure, got %v", err)
	}
	first.Close()
	deadline := time.Now().Add(time.Second)
	for budget.snapshot().UsedBytes != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	second, _, err := newCoreWithError(context.Background(), cfg)
	if err != nil {
		t.Fatalf("released session budget was not reusable: %v", err)
	}
	second.Close()
}

func TestCoreMemoryPressureKeepsLeg0Available(t *testing.T) {
	cfg := testCoreConfig()
	budget := newMemoryBudget(4<<20, false)
	cfg.Memory = budget
	core, _, err := newCoreWithError(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer core.Close()
	core.activate(activationInfo{Reason: activationReasonBytes})

	leg0Core, leg0Peer := net.Pipe()
	defer leg0Peer.Close()
	leg1Core, leg1Peer := net.Pipe()
	defer leg1Peer.Close()
	if _, err = core.addLeg(0, leg0Core, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = core.addLeg(1, leg1Core, nil); err != nil {
		t.Fatal(err)
	}

	snapshot := budget.snapshot()
	pressureBytes := int(snapshot.BoosterLimitBytes - snapshot.UsedBytes)
	pressureBuffer := acquireTestMemory(t, budget, pressureBytes)
	defer budget.release(pressureBuffer)
	core.stateMu.Lock()
	selected := core.choosePathLocked(cfg.FrameSize)
	core.stateMu.Unlock()
	if selected == nil || selected.id != 0 {
		t.Fatalf("memory pressure selected leg %v instead of leg0", selected)
	}
}
