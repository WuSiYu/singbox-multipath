package multipath

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/protocol/multipath/stream"
)

func TestAutomaticMemoryLimit(t *testing.T) {
	if limit := automaticMemoryLimit(256 << 20); limit != 128<<20 {
		t.Fatalf("unexpected limit for small host: %d", limit)
	}
	if limit := automaticMemoryLimit(4 << 30); limit != 512<<20 {
		t.Fatalf("automatic limit was not capped: %d", limit)
	}
}

func TestCacheCheckoutReleasesReferencesAndSparseIndex(t *testing.T) {
	for _, reserved := range []bool{false, true} {
		budget := newMemoryBudget(32<<20, false)
		var buffers [][]byte
		for range 1024 {
			buffers = append(buffers, acquireTestMemory(t, budget, 1200))
		}
		for _, p := range buffers {
			budget.releaseTX(p)
		}
		original := budget.cache[1200]
		for range len(original) - 1 {
			if reserved {
				_ = budget.takeReservedBuffer(1200)
			} else {
				_ = acquireTestMemory(t, budget, 1200)
			}
		}
		cached := budget.cache[1200]
		if len(cached) != 1 || cap(cached) > 16 {
			t.Fatalf("sparse cache index retained: reserved=%v len=%d cap=%d", reserved, len(cached), cap(cached))
		}
		for _, list := range [][][]byte{original, cached} {
			// Original indexes may retain the entries copied during compaction,
			// but every slot beyond their old logical end must have been cleared.
			for _, p := range list[len(list):cap(list)] {
				if p != nil {
					t.Fatal("hidden payload reference")
				}
			}
		}
		if original[len(original)-1] != nil {
			t.Fatal("popped slot still owns payload")
		}
		if budget.snapshot().CachedBytes != 1200 {
			t.Fatal("cache accounting")
		}
	}
}

func TestCacheChangingSizesRemainBounded(t *testing.T) {
	budget := newMemoryBudget(2<<20, false)
	var held [][]byte
	for round := range 128 {
		size := 1200 + round*16
		count := int((budget.cacheLimit - budget.snapshot().CachedBytes) / int64(size))
		var fresh [][]byte
		for range count {
			fresh = append(fresh, acquireTestMemory(t, budget, size))
		}
		for _, p := range fresh {
			budget.releaseTX(p)
		}
		for _, p := range held {
			budget.releaseTX(p)
		}
		held = nil
		var reachable int64
		for _, list := range budget.cache {
			for index, p := range list[:cap(list)] {
				if index >= len(list) && p != nil {
					t.Fatal("cache hidden tail reference")
				}
				reachable += int64(cap(p))
			}
			if cap(list) > 16 && cap(list) >= len(list)*4 {
				t.Fatal("unbounded sparse cache index")
			}
		}
		if reachable != budget.snapshot().CachedBytes {
			t.Fatal("reachable payload is not accounted")
		}
		for range count - 1 {
			held = append(held, acquireTestMemory(t, budget, size))
		}
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
	buffer, _ := budget.tryAcquireTX(size)
	if buffer == nil {
		t.Fatalf("test allocation failed: %d", size)
	}
	return buffer
}

// Transmit payload and receive pages borrow from one pool, but each always
// leaves the other an eighth of it and neither enters the emergency margin.
func TestMemoryRegionsCannotStarveEachOther(t *testing.T) {
	budget := newMemoryBudget(16<<20, false)
	pool := budget.limit - budget.margin
	fillTX := func() [][]byte {
		var held [][]byte
		for {
			buffer, wait := budget.tryAcquireTX(64 << 10)
			if buffer == nil {
				if wait == nil {
					t.Fatal("blocked transmit allocation returned no wakeup")
				}
				return held
			}
			held = append(held, buffer)
		}
	}
	fillRX := func() int64 {
		var n int64
		for budget.rxAcquire(stream.PageCharge, false) {
			n++
		}
		return n * stream.PageCharge
	}
	held := fillTX()
	if tx := budget.snapshot().TXBytes; tx > pool-pool/8 || tx < pool-pool/8-(64<<10) {
		t.Fatalf("transmit took %d, want about %d", tx, pool-pool/8)
	}
	if got := fillRX(); got < pool/8-stream.PageCharge || got > pool/8 {
		t.Fatalf("receive kept %d with transmit full, want about %d", got, pool/8)
	}
	if budget.snapshot().Pressure {
		t.Fatal("regions alone must not enter the emergency margin")
	}
	if !budget.rxAcquire(stream.PageCharge, true) {
		t.Fatal("head page must use the margin")
	}
	budget.rxRelease(stream.PageCharge)
	for _, buffer := range held {
		budget.releaseTX(buffer)
	}
	budget.rxRelease(budget.snapshot().RXBytes)
	if got := fillRX(); got > pool-pool/8 {
		t.Fatalf("receive exceeded its region: %d", got)
	}
	if held = fillTX(); int64(len(held))*(64<<10) < pool/8-(64<<10) {
		t.Fatal("transmit starved by receive storage")
	}
}

func TestMemoryPressureIsObservationalAndRecovers(t *testing.T) {
	budget := newMemoryBudget(1<<20, false)
	if !budget.reserveSession(budget.limit - budget.margin/4) {
		t.Fatal("session reservation within the limit failed")
	}
	if !budget.snapshot().Pressure || budget.reservePage(4096, false) {
		t.Fatal("margin use must be reported and non-essential metadata refused")
	}
	if !budget.reservePage(128, true) {
		t.Fatal("essential metadata must use the margin")
	}
	budget.releaseSession(budget.limit - budget.margin/4 + 128)
	snapshot := budget.snapshot()
	if snapshot.Pressure || snapshot.PressureEvents != 1 || snapshot.UsedBytes != 0 {
		t.Fatalf("pressure did not clear: %+v", snapshot)
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

// Memory pressure is observational: a busy primary still lets new data use an
// eligible secondary, because sending retained history allocates nothing.
func TestCoreMemoryPressureKeepsBothLegs(t *testing.T) {
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
	held := snapshot.LimitBytes - snapshot.UsedBytes - 1024
	if !budget.reserveSession(held) {
		t.Fatal("pressure reservation failed")
	}
	defer budget.releaseSession(held)
	if !budget.snapshot().Pressure {
		t.Fatal("test did not create pressure")
	}
	core.stateMu.Lock()
	defer core.stateMu.Unlock()
	if selected := core.choosePathLocked(cfg.FrameSize); selected == nil || selected.id != 0 {
		t.Fatalf("pressure selected %v instead of the idle primary", selected)
	}
	core.getLeg(0).busy = true
	selected := core.choosePathLocked(cfg.FrameSize)
	core.getLeg(0).busy = false
	if selected == nil || selected.id != 1 {
		t.Fatalf("pressure disabled the secondary: %v", selected)
	}
}
