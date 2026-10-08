package multipath

import (
	"context"
	"errors"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/protocol/multipath/stream"
	"github.com/sagernet/sing/common/byteformats"
)

const (
	automaticMemoryLimitCap      = 512 << 20
	automaticMemoryLimitFallback = 256 << 20
	memoryCacheLimitCap          = 16 << 20
	sessionMemoryBase            = 128 << 10
	wireFrameMemoryEstimate      = int64(unsafe.Sizeof(wireFrame{}))

	// No active session's fair receive window drops below this.
	receiveWindowFloor = 256 << 10
	// A session counts towards a fair share while it moved data recently.
	activityWindow = time.Second
)

var errMemoryLimit = errors.New("multipath memory limit reached")

type memorySnapshot struct {
	LimitBytes         int64
	UsedBytes          int64
	CachedBytes        int64
	TXBytes            int64
	RXBytes            int64
	ReservedBytes      int64
	PressureThreshold  int64
	ActiveSenders      int
	ActiveReceivers    int
	ReceiveShareBytes  int64
	Automatic          bool
	Pressure           bool
	PressureSince      time.Time
	PressureEvents     uint64
	BackpressureEvents uint64
	PeakUsedBytes      int64
	PeakCachedBytes    int64
	PeakTXBytes        int64
	PeakRXBytes        int64
}

type memoryPressureEvent struct {
	entered  bool
	snapshot memorySnapshot
	duration time.Duration
}

// activitySet counts sessions which used one direction within activityWindow,
// with what each holds and whether it wants more. Callers hold
// memoryBudget.access.
type activitySet struct {
	seen      map[any]activity
	lastPrune int64
	// level caches maxMinLevel for levelTTL; it changes with demand, not
	// with every write.
	level, levelRegion, levelAt int64
}

type activity struct {
	at      int64
	held    int64
	wanting bool
}

func (s *activitySet) mark(key any, now int64) {
	s.report(key, now, 0, false)
}

func (s *activitySet) report(key any, now, held int64, wanting bool) {
	if s.seen == nil {
		s.seen = make(map[any]activity)
	}
	s.seen[key] = activity{at: now, held: held, wanting: wanting}
}

func (s *activitySet) remove(key any) {
	delete(s.seen, key)
}

func (s *activitySet) count(now int64) int {
	if now-s.lastPrune >= int64(activityWindow/10) {
		s.lastPrune = now
		for key, a := range s.seen {
			if now-a.at >= int64(activityWindow) {
				delete(s.seen, key)
			}
		}
	}
	return len(s.seen)
}

const levelTTL = 5 * time.Millisecond

// maxMinLevel divides region max-min fairly among the active sessions: one
// that wants no more and holds less than an equal share keeps what it holds,
// and the rest is split equally among the others. A bulk transfer beside
// dozens of keep-alive connections thus gets nearly the whole region, two
// bulk transfers get half each, and a transfer that starts wanting more
// makes the others give way. Without contention the whole region is open.
func (s *activitySet) maxMinLevel(region, now int64) int64 {
	if now-s.levelAt < int64(levelTTL) && s.levelRegion == region {
		return s.level
	}
	n := s.count(now)
	light := make([]int64, 0, n)
	for _, a := range s.seen {
		if !a.wanting {
			light = append(light, a.held)
		}
	}
	slices.Sort(light)
	remaining, k := region, int64(n)
	for _, held := range light {
		if held*k > remaining {
			break
		}
		remaining -= held
		k--
	}
	level := region
	if k > 0 {
		level = remaining / k
	}
	s.level, s.levelRegion, s.levelAt = level, region, now
	return level
}

// memoryBudget is one node's accounting region. It separates unsent/unacked
// transmit payload (tx), allocated receive pages (rx), and everything else
// (session reservations, flight records, UDP reassembly, cache). Each
// direction always keeps an eighth of the pool available to the other.
// Pressure is an observation only: it never changes path selection.
type memoryBudget struct {
	access   sync.Mutex
	sessions atomic.Int64

	limit      int64
	margin     int64
	cacheLimit int64
	automatic  bool

	tx, rx, other, cached int64
	cache                 map[int][][]byte

	senders, receivers activitySet

	pressure      bool
	pressureSince time.Time
	pressureCount uint64
	waitCount     uint64
	peakUsed      int64
	peakCached    int64
	peakTX        int64
	peakRX        int64

	waiting bool
	changed chan struct{}
	events  chan memoryPressureEvent

	logOnce   sync.Once
	logMu     sync.Mutex
	logCancel context.CancelFunc
	logDone   chan struct{}
}

func resolveMemoryLimit(configured uint64) (int64, bool, error) {
	if configured > math.MaxInt64 {
		return 0, false, errors.New("memory_limit exceeds the supported range")
	}
	if configured > 0 {
		return int64(configured), false, nil
	}
	available, err := availableMemory()
	if err != nil {
		return automaticMemoryLimitFallback, true, err
	}
	return automaticMemoryLimit(available), true, nil
}

func automaticMemoryLimit(available uint64) int64 {
	limit := available / 2
	if limit > automaticMemoryLimitCap {
		limit = automaticMemoryLimitCap
	}
	return int64(limit)
}

func newMemoryBudget(limit int64, automatic bool) *memoryBudget {
	cacheLimit := limit / 16
	if cacheLimit > memoryCacheLimitCap {
		cacheLimit = memoryCacheLimitCap
	}
	return &memoryBudget{
		limit:      limit,
		margin:     limit / 16,
		cacheLimit: cacheLimit,
		automatic:  automatic,
		cache:      make(map[int][][]byte),
		changed:    make(chan struct{}),
		events:     make(chan memoryPressureEvent, 4),
	}
}

func sessionMemoryReservation(cfg coreConfig) int64 {
	// Fixed channels: two assignments, 32 controls and one coalesced feedback
	// frame per path. The base also covers worker stacks and 16 prepaid primary
	// flight records. Two reader scratch buffers and one TX reserve keep every
	// session moving independently of shared allocations; the receive floor
	// is a window promise that never competes for the shared region.
	return sessionMemoryBase + 2*35*wireFrameMemoryEstimate + int64(cfg.FrameSize)*3 + stream.PageCharge
}

func (b *memoryBudget) usedLocked() int64 { return b.tx + b.rx + b.other + b.cached }
func (b *memoryBudget) poolLocked() int64 { return b.limit - b.margin }

// Transmit payload (tx) and receive pages (rx) borrow from the same pool. Each
// direction may use whatever the other is not using, but always leaves it a
// reserve: half of the pool while the other direction has active sessions,
// otherwise an eighth. Saturated uploads and downloads therefore split the
// node evenly instead of the transmit side starving every receive window.
// rx counts receive pages actually allocated (out-of-order and unread data).

func (b *memoryBudget) reserveLocked(set *activitySet) int64 {
	pool := b.poolLocked()
	if set.count(time.Now().UnixNano()) > 0 {
		return max(pool/8, (pool-b.other)/2)
	}
	return pool / 8
}

// txRegionLocked is the most unsent/unacked payload the node may hold now. It
// also leaves a sixteenth of the pool free, so arriving data never have to
// wait for acknowledgements of the opposite direction to be stored.
func (b *memoryBudget) txRegionLocked() int64 {
	pool := b.poolLocked()
	return max(0, pool-b.other-max(b.rx, b.reserveLocked(&b.receivers))-pool/16)
}

// rxRegionLocked is the most receive storage the node may hold for
// speculative (non-head) pages now. Like transmit, it stops a sixteenth short
// of the other direction's reserve so that reserve stays fully usable.
func (b *memoryBudget) rxRegionLocked() int64 {
	pool := b.poolLocked()
	return max(0, pool-b.other-max(b.tx, b.reserveLocked(&b.senders))-pool/16)
}

func (b *memoryBudget) makeRoomLocked(size int64) {
	if b.cached > 0 && b.usedLocked()+size > b.limit {
		b.dropCacheLocked()
	}
}

// tryAcquireTX returns an application payload buffer, or a channel closed when
// transmit memory may have become available.
func (b *memoryBudget) tryAcquireTX(size int) ([]byte, <-chan struct{}) {
	b.access.Lock()
	defer b.access.Unlock()
	if b.tx+int64(size) <= b.txRegionLocked() {
		if buffer := b.popCacheLocked(size); buffer != nil {
			b.tx += int64(size)
			b.updatePressureLocked(time.Now())
			return buffer[:size], nil
		}
		b.makeRoomLocked(int64(size))
		if b.usedLocked()+int64(size) <= b.limit {
			b.tx += int64(size)
			b.updatePressureLocked(time.Now())
			return make([]byte, size), nil
		}
	}
	b.waitCount++
	b.waiting = true
	return nil, b.changed
}

func (b *memoryBudget) releaseTX(buffer []byte) {
	if b == nil || cap(buffer) == 0 {
		return
	}
	size := int64(cap(buffer))
	b.access.Lock()
	b.tx -= size
	if b.tx < 0 {
		b.tx = 0
	}
	b.cacheLocked(buffer)
	b.updatePressureLocked(time.Now())
	b.signalLocked()
	b.access.Unlock()
}

// tryAcquireOther charges auxiliary buffers such as UDP reassembly. They may
// use the pool but not the emergency margin.
func (b *memoryBudget) tryAcquireOther(size int) []byte {
	b.access.Lock()
	defer b.access.Unlock()
	if buffer := b.popCacheLocked(size); buffer != nil {
		b.other += int64(size)
		return buffer[:size]
	}
	b.makeRoomLocked(int64(size))
	if b.usedLocked()+int64(size) > b.poolLocked() {
		b.enterPressureLocked(time.Now())
		return nil
	}
	b.other += int64(size)
	b.updatePressureLocked(time.Now())
	return make([]byte, size)
}

func (b *memoryBudget) releaseOther(buffer []byte) {
	if b == nil || cap(buffer) == 0 {
		return
	}
	size := int64(cap(buffer))
	b.access.Lock()
	b.other -= size
	if b.other < 0 {
		b.other = 0
	}
	b.cacheLocked(buffer)
	b.updatePressureLocked(time.Now())
	b.signalLocked()
	b.access.Unlock()
}

func (b *memoryBudget) cacheLocked(buffer []byte) {
	size := cap(buffer)
	if !b.pressure && b.cached+int64(size) <= b.cacheLimit && b.usedLocked()+int64(size) <= b.poolLocked() {
		b.cache[size] = append(b.cache[size], buffer[:size])
		b.cached += int64(size)
	}
}

// The caller has already charged a reusable session scratch/TX reservation.
func (b *memoryBudget) takeReservedBuffer(size int) []byte {
	b.access.Lock()
	if buffer := b.popCacheLocked(size); buffer != nil {
		b.updatePressureLocked(time.Now())
		b.access.Unlock()
		return buffer[:size]
	}
	b.access.Unlock()
	return make([]byte, size)
}

// A slice's unused capacity is still scanned by GC. Clear checked-out entries
// and shrink sparse indexes so neither payloads nor historical size-class peaks
// can remain reachable outside the cache's byte accounting.
func (b *memoryBudget) popCacheLocked(size int) []byte {
	buffers := b.cache[size]
	if len(buffers) == 0 {
		return nil
	}
	last := len(buffers) - 1
	buffer := buffers[last]
	buffers[last] = nil
	if last == 0 {
		delete(b.cache, size)
	} else if cap(buffers) > 16 && last <= cap(buffers)/4 {
		b.cache[size] = append([][]byte(nil), buffers[:last]...)
	} else {
		b.cache[size] = buffers[:last]
	}
	b.cached -= int64(size)
	return buffer
}

// Returning a reserved buffer does not release its session reservation. Cache
// storage is charged separately until reused or dropped.
func (b *memoryBudget) putReservedBuffer(buffer []byte) {
	b.access.Lock()
	b.cacheLocked(buffer)
	b.updatePressureLocked(time.Now())
	b.signalLocked()
	b.access.Unlock()
}

// reserveSession charges fixed per-session or per-record state. It may use the
// emergency margin; admission failure rejects the new session or record.
func (b *memoryBudget) reserveSession(bytes int64) bool {
	if b == nil || bytes <= 0 {
		return true
	}
	b.access.Lock()
	defer b.access.Unlock()
	b.makeRoomLocked(bytes)
	if b.usedLocked()+bytes > b.limit {
		return false
	}
	b.other += bytes
	b.updatePressureLocked(time.Now())
	return true
}

// reservePage charges metadata. Essential progress (the primary path, control
// records) may use the margin; anything else stays inside the pool.
func (b *memoryBudget) reservePage(size int64, essential bool) bool {
	b.access.Lock()
	defer b.access.Unlock()
	limit := b.poolLocked()
	if essential {
		limit = b.limit
	}
	b.makeRoomLocked(size)
	if b.usedLocked()+size > limit {
		b.enterPressureLocked(time.Now())
		return false
	}
	b.other += size
	b.updatePressureLocked(time.Now())
	return true
}

func (b *memoryBudget) releaseSession(bytes int64) {
	if b == nil || bytes <= 0 {
		return
	}
	b.access.Lock()
	b.other -= bytes
	if b.other < 0 {
		b.other = 0
	}
	b.updatePressureLocked(time.Now())
	b.signalLocked()
	b.access.Unlock()
}

// rxAcquire charges one receive page. The page holding the connection's next
// expected byte (head) may use the emergency margin so in-order progress never
// waits for speculative data elsewhere; other pages stay inside the receive
// region and are refused (to be repaired by the sender) beyond it.
func (b *memoryBudget) rxAcquire(size int64, head bool) bool {
	b.access.Lock()
	defer b.access.Unlock()
	b.makeRoomLocked(size)
	if head {
		if b.usedLocked()+size > b.limit {
			b.enterPressureLocked(time.Now())
			return false
		}
	} else if b.rx+size > b.rxRegionLocked() || b.usedLocked()+size > b.poolLocked() {
		// A full receive region is ordinary backpressure, not margin use.
		return false
	}
	b.rx += size
	b.updatePressureLocked(time.Now())
	return true
}

// receiveRoom is the receive storage still available for speculative pages.
func (b *memoryBudget) receiveRoom() int64 {
	b.access.Lock()
	defer b.access.Unlock()
	return min(b.rxRegionLocked()-b.rx, b.poolLocked()-b.usedLocked())
}

func (b *memoryBudget) rxRelease(size int64) {
	if b == nil || size <= 0 {
		return
	}
	b.access.Lock()
	b.rx -= size
	if b.rx < 0 {
		b.rx = 0
	}
	b.updatePressureLocked(time.Now())
	b.signalLocked()
	b.access.Unlock()
}

// receiveShare returns key's fair window share in payload bytes, and marks key
// as an active receiver when it is receiving data. A session that only sends
// feedback for an idle direction is not active: counting it would reserve
// half of the node's pool for receiving and halve the transmit region.
func (b *memoryBudget) receiveShare(key any, now time.Time, active bool, held int64, wanting bool) int64 {
	b.access.Lock()
	defer b.access.Unlock()
	nanos := now.UnixNano()
	if active {
		b.receivers.report(key, nanos, held, wanting)
	}
	if b.pressure {
		// Stored data already exceed the pool: grow no window beyond the
		// floor until applications drain it.
		return receiveWindowFloor
	}
	return b.receiveShareLocked(nanos)
}

// receiveShareLocked divides three quarters of the receive region max-min
// fairly among the active receivers: one whose sender is not limited by its
// window keeps what it holds (stored pages, or the window its sender keeps
// filling), and window-limited transfers share the rest. The remaining
// quarter absorbs page-granularity overhead and arrivals beyond a window
// edge, so a slow application can fill its own window without pushing
// out-of-order data elsewhere into the drop path.
func (b *memoryBudget) receiveShareLocked(nanos int64) int64 {
	share := b.receivers.maxMinLevel(b.rxRegionLocked()/4*3, nanos)
	return max(receiveWindowFloor, share/stream.PageCharge*stream.PageSize)
}

// transmitShare records key as an active sender holding held bytes of
// transmit payload, and whether it wants more, and returns its max-min fair
// share of the transmit region.
func (b *memoryBudget) transmitShare(key any, now time.Time, held int64, wanting bool) int64 {
	b.access.Lock()
	defer b.access.Unlock()
	nanos := now.UnixNano()
	b.senders.report(key, nanos, held, wanting)
	return b.senders.maxMinLevel(b.txRegionLocked(), nanos)
}

func (b *memoryBudget) forget(key any) {
	b.access.Lock()
	b.senders.remove(key)
	b.receivers.remove(key)
	b.access.Unlock()
}

func (b *memoryBudget) underPressure() bool {
	if b == nil {
		return false
	}
	b.access.Lock()
	b.updatePressureLocked(time.Now())
	pressure := b.pressure
	b.access.Unlock()
	return pressure
}

func (b *memoryBudget) snapshot() memorySnapshot {
	if b == nil {
		return memorySnapshot{}
	}
	b.access.Lock()
	now := time.Now()
	b.updatePressureLocked(now)
	snapshot := b.snapshotLocked(now)
	b.access.Unlock()
	return snapshot
}

func (b *memoryBudget) snapshotLocked(now time.Time) memorySnapshot {
	nanos := now.UnixNano()
	receivers := b.receivers.count(nanos)
	return memorySnapshot{
		LimitBytes:         b.limit,
		UsedBytes:          b.usedLocked(),
		CachedBytes:        b.cached,
		TXBytes:            b.tx,
		RXBytes:            b.rx,
		ReservedBytes:      b.other,
		PressureThreshold:  b.poolLocked(),
		ActiveSenders:      b.senders.count(nanos),
		ActiveReceivers:    receivers,
		ReceiveShareBytes:  b.receiveShareLocked(nanos),
		Automatic:          b.automatic,
		Pressure:           b.pressure,
		PressureSince:      b.pressureSince,
		PressureEvents:     b.pressureCount,
		BackpressureEvents: b.waitCount,
		PeakUsedBytes:      b.peakUsed,
		PeakCachedBytes:    b.peakCached,
		PeakTXBytes:        b.peakTX,
		PeakRXBytes:        b.peakRX,
	}
}

// Pressure means the emergency margin is in use. It is reported and logged,
// but sessions keep using every healthy path.
func (b *memoryBudget) updatePressureLocked(now time.Time) {
	b.updatePeaksLocked()
	used := b.usedLocked()
	if b.pressure {
		if used+b.margin/2 <= b.poolLocked() {
			duration := now.Sub(b.pressureSince)
			b.pressure = false
			b.pressureSince = time.Time{}
			b.emitEventLocked(false, duration, now)
		}
		return
	}
	if used > b.poolLocked() {
		b.enterPressureLocked(now)
	}
}

func (b *memoryBudget) enterPressureLocked(now time.Time) {
	if b.pressure {
		return
	}
	b.pressure = true
	b.pressureSince = now
	b.pressureCount++
	b.emitEventLocked(true, 0, now)
}

func (b *memoryBudget) updatePeaksLocked() {
	b.peakUsed = max(b.peakUsed, b.usedLocked())
	b.peakCached = max(b.peakCached, b.cached)
	b.peakTX = max(b.peakTX, b.tx)
	b.peakRX = max(b.peakRX, b.rx)
}

func (b *memoryBudget) emitEventLocked(entered bool, duration time.Duration, now time.Time) {
	event := memoryPressureEvent{entered: entered, duration: duration, snapshot: b.snapshotLocked(now)}
	select {
	case b.events <- event:
	default:
	}
}

func (b *memoryBudget) startLogging(ctx context.Context, logger log.ContextLogger, side string) {
	if b == nil {
		return
	}
	b.logOnce.Do(func() {
		ctx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		b.logMu.Lock()
		b.logCancel, b.logDone = cancel, done
		b.logMu.Unlock()
		runtimeLimit, runtimeAutomatic, releaseGuard, runtimeErr := processMemoryGuard.acquire()
		if runtimeErr != nil {
			logger.WarnContext(ctx, "detect available memory for Go runtime guard: ", runtimeErr)
		} else {
			source := "configured"
			if runtimeAutomatic {
				source = "automatic"
			}
			logger.InfoContext(ctx, "Go runtime soft memory limit: limit=", byteformats.FormatMemoryBytes(uint64(runtimeLimit)), " source=", source, " scope=process (not RSS)")
		}
		snapshot := b.snapshot()
		source := "configured"
		if snapshot.Automatic {
			source = "automatic"
		}
		logger.InfoContext(
			ctx,
			"multipath memory budget: side=", side,
			" limit=", byteformats.FormatMemoryBytes(uint64(snapshot.LimitBytes)),
			" source=", source,
			" pressure_threshold=", byteformats.FormatMemoryBytes(uint64(snapshot.PressureThreshold)),
			" cache_limit=", byteformats.FormatMemoryBytes(uint64(b.cacheLimit)),
		)
		go func() {
			defer close(done)
			defer releaseGuard()
			for {
				select {
				case <-ctx.Done():
					return
				case event := <-b.events:
					if event.entered {
						logger.InfoContext(
							ctx,
							"multipath memory pressure entered: side=", side,
							" used=", byteformats.FormatMemoryBytes(uint64(event.snapshot.UsedBytes)),
							" tx=", byteformats.FormatMemoryBytes(uint64(event.snapshot.TXBytes)),
							" rx=", byteformats.FormatMemoryBytes(uint64(event.snapshot.RXBytes)),
							" limit=", byteformats.FormatMemoryBytes(uint64(event.snapshot.LimitBytes)),
						)
					} else {
						logger.InfoContext(
							ctx,
							"multipath memory pressure cleared: side=", side,
							" used=", byteformats.FormatMemoryBytes(uint64(event.snapshot.UsedBytes)),
							" duration=", event.duration.Round(time.Millisecond),
						)
					}
				}
			}
		}()
	})
}

func (b *memoryBudget) stopLogging() {
	if b == nil {
		return
	}
	b.logMu.Lock()
	cancel, done := b.logCancel, b.logDone
	b.logMu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
}

func (b *memoryBudget) dropCacheLocked() {
	if b.cached == 0 {
		return
	}
	b.cached = 0
	b.cache = make(map[int][][]byte)
}

// signalLocked wakes transmit waiters only when someone is waiting, instead of
// recreating a channel for every released buffer.
func (b *memoryBudget) signalLocked() {
	if !b.waiting {
		return
	}
	b.waiting = false
	close(b.changed)
	b.changed = make(chan struct{})
}
