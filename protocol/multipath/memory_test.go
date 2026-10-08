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
	// Transmit leaves the other direction an eighth and arrivals a sixteenth.
	if tx, want := budget.snapshot().TXBytes, pool-pool/8-pool/16; tx > want || tx < want-(64<<10) {
		t.Fatalf("transmit took %d, want about %d", tx, want)
	}
	if got := fillRX(); got < pool/8-stream.PageCharge || got > pool/8+pool/16 {
		t.Fatalf("receive kept %d with transmit full, want at least %d", got, pool/8)
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
	if got := fillRX(); got > pool-pool/8-pool/16 {
		t.Fatalf("receive exceeded its region: %d", got)
	}
	if held = fillTX(); int64(len(held))*(64<<10) < pool/8-(64<<10) {
		t.Fatal("transmit starved by receive storage")
	}
	for _, buffer := range held {
		budget.releaseTX(buffer)
	}
	budget.rxRelease(budget.snapshot().RXBytes)
	// A session that only sends feedback for an idle receive direction does
	// not reserve receive memory: transmit keeps the whole region.
	now := time.Now()
	budget.transmitShare("sender", now, 0, true)
	budget.receiveShare("receiver", now, false, 0, false, false)
	held = fillTX()
	if tx := budget.snapshot().TXBytes; tx < pool-pool/8-pool/16-(128<<10) {
		t.Fatalf("idle receiver halved transmit: %d of %d", tx, pool)
	}
	for _, buffer := range held {
		budget.releaseTX(buffer)
	}
	// With active sessions in both directions each side keeps half.
	budget.receiveShare("receiver", now, true, 0, true, false)
	held = fillTX()
	if tx := budget.snapshot().TXBytes; tx > pool/2 || tx < pool/2-pool/16-(128<<10) {
		t.Fatalf("transmit took %d of %d with both directions active", tx, pool)
	}
	if got := fillRX(); got < pool/2-pool/16-stream.PageCharge {
		t.Fatalf("receive kept only %d of %d with both directions active", got, pool)
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

// Transmit memory is shared max-min fairly: keep-alive connections keep what
// they hold, a single bulk transfer gets the rest, and bulk transfers that
// all want more split it evenly.
func TestTransmitShareFollowsDemand(t *testing.T) {
	budget := newMemoryBudget(512<<20, false)
	now := time.Now()
	budget.access.Lock()
	region := budget.txRegionLocked()
	budget.access.Unlock()
	for i := 0; i < 52; i++ {
		budget.transmitShare(i, now, 4096, false)
	}
	if share := budget.transmitShare("bulk", now, 8<<20, true); share < region-52*4096 {
		t.Fatalf("bulk transfer beside 52 idle connections got %d of %d", share, region)
	}
	later := now.Add(levelTTL)
	budget.transmitShare("second", later, 0, true)
	if share := budget.transmitShare("bulk", later, 200<<20, true); share > (region-52*4096)/2 || share < (region-52*4096)/2-1 {
		t.Fatalf("two bulk transfers: first got %d of %d", share, region)
	}
	// A steady flow that holds less than an equal share keeps it.
	later = later.Add(levelTTL)
	budget.transmitShare("second", later, region/5, false)
	if share := budget.transmitShare("bulk", later, 200<<20, true); share < region-region/5-52*4096 {
		t.Fatalf("bulk beside a steady flow got %d of %d", share, region)
	}
}

// A window-limited download beside keep-alive connections that receive a few
// bytes now and then gets nearly the whole receive share.
func TestReceiveShareFollowsDemand(t *testing.T) {
	budget := newMemoryBudget(512<<20, false)
	now := time.Now()
	budget.access.Lock()
	whole := budget.rxRegionLocked() / 4 * 3
	budget.access.Unlock()
	for i := 0; i < 50; i++ {
		budget.receiveShare(i, now, true, 0, false, false)
	}
	if share := budget.receiveShare("bulk", now, true, 4<<20, true, false).share; share < (whole-stream.PageCharge)/stream.PageCharge*stream.PageSize {
		t.Fatalf("download beside 50 keep-alive connections got a %d byte window of %d", share, whole)
	}
	later := now.Add(levelTTL)
	budget.receiveShare("second", later, true, 0, true, false)
	if share := budget.receiveShare("bulk", later, true, 4<<20, true, false).share; share > whole/2 {
		t.Fatalf("two window-limited downloads: first got %d of %d", share, whole)
	}
}

// An open window is a promise: a sender may fill all of it, out of order
// across paths. Transfers whose application reads at once hold no pages, yet
// the windows of all sessions together must stay within the receive region,
// or out-of-order data from a few path skews fill it and every hole waits for
// a repair round trip (a 128 MB client with eight duplex transfers fell to
// tens of Mbps). Keep-alive connections never grow their windows.
func TestReceiveWindowsStayWithinRegion(t *testing.T) {
	budget := newMemoryBudget(16<<20, false)
	cfg := testCoreConfig()
	cfg.FrameSize = 64 << 10
	cfg.ReceiveWindowBytes = 512 << 20
	cfg.Memory = budget
	const sessions, transfers = 8, 6
	frame := uint64(cfg.FrameSize)
	cores := make([]*mpCore, sessions)
	now := time.Now()
	// Each session's reservation shrinks the region a little.
	regionNow := func() uint64 {
		budget.access.Lock()
		defer budget.access.Unlock()
		return uint64(budget.rxRegionLocked() / 4 * 3 / stream.PageCharge * stream.PageSize)
	}
	region := regionNow()
	firstWindows := make([]uint64, sessions)
	for i := range cores {
		cores[i], _ = newCore(context.Background(), cfg)
		defer cores[i].Close()
		cores[i].stateMu.Lock()
		cores[i].rx.Advertise(cores[i].receiveTargetLocked(now))
		firstWindows[i] = cores[i].rx.Limit - cores[i].rx.ReadNext
		cores[i].stateMu.Unlock()
	}
	promised := func() uint64 {
		var sum uint64
		for _, c := range cores {
			c.stateMu.Lock()
			window := c.rx.Limit - c.rx.ReadNext
			c.stateMu.Unlock()
			sum += window - min(window, frame)
		}
		return sum
	}
	if sum := promised(); sum > region {
		t.Fatalf("first windows total %d bytes, receive region %d", sum, region)
	}
	// fill delivers data in order up to end and lets the application read
	// them at once, so the session holds no pages afterwards.
	fill := func(c *mpCore, end uint64) {
		chunk := make([]byte, cfg.FrameSize)
		for c.rx.Next < end {
			n := min(uint64(len(chunk)), end-c.rx.Next)
			if _, err := c.rx.Insert(c.rx.Next, chunk[:n]); err != nil {
				t.Fatal(err)
			}
			for data := c.rx.Readable(); data != nil; data = c.rx.Readable() {
				c.rx.Consume(len(data))
			}
		}
	}
	for round := 0; round < 60; round++ {
		for i, c := range cores {
			c.stateMu.Lock()
			// The transfers take turns delivering most of their window
			// between two feedback frames; the others are keep-alive
			// connections.
			if i < transfers && (round+i)%3 == 0 {
				fill(c, c.rx.Limit-(c.rx.Limit-c.rx.ReadNext)/8)
			} else if i >= transfers {
				fill(c, c.rx.Next+100)
			}
			c.rx.Advertise(c.receiveTargetLocked(now))
			c.stateMu.Unlock()
			now = now.Add(levelTTL)
		}
		if sum := promised(); sum > region+transfers*frame {
			t.Fatalf("round %d: open windows total %d bytes, receive region %d", round, sum, region)
		}
		region = regionNow()
	}
	for i := transfers; i < sessions; i++ {
		c := cores[i]
		c.stateMu.Lock()
		window := c.rx.Limit - c.rx.ReadNext
		c.stateMu.Unlock()
		if window > firstWindows[i] {
			t.Fatalf("keep-alive session %d grew its window from %d to %d", i, firstWindows[i], window)
		}
	}
}

// Keep-alive connections keep their first windows while memory is to spare.
// A transfer that waits for memory asks them to recompute, and they give back
// what their senders do not use, so it gets nearly all of the region.
func TestBulkReceiverBesideKeepAliveSessions(t *testing.T) {
	budget := newMemoryBudget(64<<20, false)
	cfg := testCoreConfig()
	cfg.FrameSize = 64 << 10
	cfg.ReceiveWindowBytes = 512 << 20
	cfg.Memory = budget
	now := time.Now()
	keepAlive := make([]*mpCore, 20)
	for i := range keepAlive {
		c, _ := newCore(context.Background(), cfg)
		defer c.Close()
		keepAlive[i] = c
		c.stateMu.Lock()
		c.rx.Advertise(c.receiveTargetLocked(now))
		c.stateMu.Unlock()
	}
	now = now.Add(2 * activityWindow)
	bulk, _ := newCore(context.Background(), cfg)
	defer bulk.Close()
	grow := func() uint64 {
		bulk.stateMu.Lock()
		defer bulk.stateMu.Unlock()
		bulk.rx.Advertise(bulk.receiveTargetLocked(now))
		// The sender fills the window.
		if _, err := bulk.rx.Insert(bulk.rx.Next, make([]byte, bulk.rx.Limit-bulk.rx.Next)); err != nil {
			t.Fatal(err)
		}
		for data := bulk.rx.Readable(); data != nil; data = bulk.rx.Readable() {
			bulk.rx.Consume(len(data))
		}
		now = now.Add(levelTTL)
		return bulk.receiveTargetLocked(now)
	}
	grow()
	// The keep-alive sessions were asked; each recomputes as its pump would.
	for i, c := range keepAlive {
		c.stateMu.Lock()
		if c.windowReview.Swap(false) {
			c.receiveReviewed = true
		}
		if !c.receiveReviewed {
			c.stateMu.Unlock()
			t.Fatalf("keep-alive session %d was not asked to give its window back", i)
		}
		if _, err := c.rx.Insert(c.rx.Next, make([]byte, 100)); err != nil {
			t.Fatal(err)
		}
		c.rx.Consume(100)
		c.rx.Advertise(c.receiveTargetLocked(now))
		c.stateMu.Unlock()
	}
	now = now.Add(levelTTL)
	target := grow()
	budget.access.Lock()
	region := uint64(budget.rxRegionLocked() / 4 * 3 / stream.PageCharge * stream.PageSize)
	budget.access.Unlock()
	if target+uint64(cfg.FrameSize) < region {
		t.Fatalf("bulk transfer beside 20 keep-alive sessions got %d of a %d byte region", target, region)
	}
}

// A window-limited transfer whose application reads at once never shows its
// sender at the window edge when feedback is sent: each round trip delivers
// one window, spread over several feedback intervals. Its window must still
// grow.
func TestPromptReaderWindowGrows(t *testing.T) {
	budget := newMemoryBudget(64<<20, false)
	cfg := testCoreConfig()
	cfg.FrameSize = 64 << 10
	cfg.ReceiveWindowBytes = 512 << 20
	cfg.Memory = budget
	core, _ := newCore(context.Background(), cfg)
	defer core.Close()
	core.stateMu.Lock()
	defer core.stateMu.Unlock()
	now := time.Now()
	// Another session holds most of the region, so the first window is small.
	budget.access.Lock()
	budget.windows.report("other", now.UnixNano(), budget.rxRegionLocked()/4*3-(1<<20), false)
	budget.access.Unlock()
	core.rx.Advertise(core.receiveTargetLocked(now))
	first := core.rx.Limit - core.rx.ReadNext
	chunk := make([]byte, cfg.FrameSize)
	for i := 0; i < 8; i++ {
		// Half of the window arrives between two feedback frames.
		end := core.rx.ReadNext + (core.rx.Limit-core.rx.ReadNext)/2
		for core.rx.Next < end {
			n := min(uint64(len(chunk)), end-core.rx.Next)
			if _, err := core.rx.Insert(core.rx.Next, chunk[:n]); err != nil {
				t.Fatal(err)
			}
			for data := core.rx.Readable(); data != nil; data = core.rx.Readable() {
				core.rx.Consume(len(data))
			}
		}
		now = now.Add(5 * time.Millisecond)
		core.rx.Advertise(core.receiveTargetLocked(now))
	}
	budget.access.Lock()
	budget.windows.remove("other")
	budget.access.Unlock()
	now = now.Add(levelTTL)
	core.rx.Advertise(core.receiveTargetLocked(now))
	if window := core.rx.Limit - core.rx.ReadNext; window <= 4*first {
		t.Fatalf("window-limited transfer stayed at a %d byte window (first %d)", window, first)
	}
}

// A session that wanted more and then fell silent (its transfer paused, the
// connection kept open) no longer counts as growing after a second: it
// neither takes an equal share from others nor holds new connections to the
// small first window.
func TestStaleWantingExpires(t *testing.T) {
	budget := newMemoryBudget(64<<20, false)
	now := time.Now()
	budget.receiveShare("paused", now, true, 1<<20, true, false)
	if room := budget.receiveShare("new", now, false, 0, false, false); !room.busy {
		t.Fatal("a growing transfer was not seen")
	}
	later := now.Add(activityWindow + levelTTL)
	whole := budget.receiveShare("new", later, false, 0, false, false)
	if whole.busy {
		t.Fatal("a paused transfer still counts as growing")
	}
	budget.access.Lock()
	region := budget.rxRegionLocked() / 4 * 3
	budget.access.Unlock()
	if whole.share < (region-(1<<20)-stream.PageCharge)/stream.PageCharge*stream.PageSize {
		t.Fatalf("share %d beside a paused 1 MiB window in a %d region", whole.share, region)
	}
}

// Status reports and scheduling checks ask every session for its send history
// limit. A download-only node whose sessions hold no transmit data must not
// count them as senders: that reserved half of its pool for sending and
// halved every receive window.
func TestIdleSendersDoNotReserve(t *testing.T) {
	budget := newMemoryBudget(512<<20, false)
	now := time.Now()
	for i := 0; i < 64; i++ {
		budget.transmitShare(i, now, 0, false)
		budget.receiveShare(i, now, true, 0, true, false)
	}
	snapshot := budget.snapshot()
	if snapshot.ActiveSenders != 0 {
		t.Fatalf("%d idle directions counted as senders", snapshot.ActiveSenders)
	}
	budget.access.Lock()
	region, pool := budget.rxRegionLocked(), budget.poolLocked()
	budget.access.Unlock()
	if region < pool-pool/8-pool/16-budget.snapshot().ReservedBytes {
		t.Fatalf("download-only receive region %d of a %d pool", region, pool)
	}
}
