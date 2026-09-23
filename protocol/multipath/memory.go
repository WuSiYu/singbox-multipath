package multipath

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common/byteformats"
)

const (
	automaticMemoryLimitCap      = 512 << 20
	automaticMemoryLimitFallback = 256 << 20
	memoryCacheLimitCap          = 16 << 20
	sessionMemoryBase            = 128 << 10
	wireFrameMemoryEstimate      = int64(unsafe.Sizeof(wireFrame{}))
)

var errMemoryLimit = errors.New("multipath memory limit reached")

type memorySnapshot struct {
	LimitBytes         int64
	UsedBytes          int64
	CachedBytes        int64
	BoosterLimitBytes  int64
	BoosterResumeBytes int64
	Automatic          bool
	Pressure           bool
	PressureSince      time.Time
	PressureEvents     uint64
	BackpressureEvents uint64
	PeakUsedBytes      int64
	PeakCachedBytes    int64
}

type memoryPressureEvent struct {
	entered  bool
	snapshot memorySnapshot
	duration time.Duration
}

type memoryBudget struct {
	access   sync.Mutex
	sessions atomic.Int64

	limit         int64
	boosterLimit  int64
	boosterResume int64
	cacheLimit    int64
	automatic     bool
	used          int64
	cached        int64
	cache         map[int][][]byte
	pressure      bool
	pressureSince time.Time
	pressureCount uint64
	waitCount     uint64
	peakUsed      int64
	peakCached    int64
	changed       chan struct{}
	events        chan memoryPressureEvent
	logOnce       sync.Once
	logMu         sync.Mutex
	logCancel     context.CancelFunc
	logDone       chan struct{}
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
	boosterLimit := limit/8*7 + limit%8*7/8
	boosterResume := limit/4*3 + limit%4*3/4
	cacheLimit := limit / 16
	if cacheLimit > memoryCacheLimitCap {
		cacheLimit = memoryCacheLimitCap
	}
	return &memoryBudget{
		limit:         limit,
		boosterLimit:  boosterLimit,
		boosterResume: boosterResume,
		cacheLimit:    cacheLimit,
		automatic:     automatic,
		cache:         make(map[int][][]byte),
		changed:       make(chan struct{}),
		events:        make(chan memoryPressureEvent, 4),
	}
}

func sessionMemoryReservation(cfg coreConfig) int64 {
	// Fixed channels: one assignment, 32 controls and one coalesced feedback
	// frame per path. The base also covers worker stacks and 16 prepaid primary
	// flight records. Two reader scratch buffers and one primary TX reserve
	// keep head recovery independent of speculative allocations.
	return sessionMemoryBase + 2*34*wireFrameMemoryEstimate + int64(cfg.FrameSize)*3
}

func (b *memoryBudget) tryAcquirePrimary(size int) ([]byte, <-chan struct{}) {
	b.access.Lock()
	defer b.access.Unlock()
	if buffer := b.popCacheLocked(size); buffer != nil {
		return buffer[:size], nil
	}
	if b.used+int64(size) > b.limit && b.cached > 0 {
		b.dropCacheLocked()
	}
	if b.used+int64(size) <= b.limit {
		b.used += int64(size)
		b.updatePressureLocked(time.Now())
		return make([]byte, size), nil
	}
	return nil, b.changed
}

// The caller has already charged a reusable session scratch/TX reservation.
func (b *memoryBudget) takeReservedBuffer(size int) []byte {
	b.access.Lock()
	if buffer := b.popCacheLocked(size); buffer != nil {
		b.used -= int64(size)
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
	size := cap(buffer)
	b.access.Lock()
	if !b.pressure && b.cached+int64(size) <= b.cacheLimit && b.used+int64(size) <= b.limit {
		b.cache[size] = append(b.cache[size], buffer[:size])
		b.cached += int64(size)
		b.used += int64(size)
	}
	b.updatePressureLocked(time.Now())
	b.signalLocked()
	b.access.Unlock()
}

func (b *memoryBudget) reserveSession(bytes int64) bool {
	if b == nil || bytes <= 0 {
		return true
	}
	b.access.Lock()
	defer b.access.Unlock()
	if b.used+bytes > b.limit {
		b.dropCacheLocked()
	}
	if b.used+bytes > b.limit {
		return false
	}
	b.used += bytes
	b.updatePressureLocked(time.Now())
	return true
}

func (b *memoryBudget) releaseSession(bytes int64) {
	if b == nil || bytes <= 0 {
		return
	}
	b.access.Lock()
	b.used -= bytes
	if b.used < 0 {
		b.used = 0
	}
	b.updatePressureLocked(time.Now())
	b.signalLocked()
	b.access.Unlock()
}

func (b *memoryBudget) release(buffer []byte) {
	if b == nil || cap(buffer) == 0 {
		return
	}
	size := cap(buffer)
	b.access.Lock()
	b.updatePressureLocked(time.Now())
	if !b.pressure && b.cached+int64(size) <= b.cacheLimit {
		b.cache[size] = append(b.cache[size], buffer[:size])
		b.cached += int64(size)
	} else {
		b.used -= int64(size)
		if b.used < 0 {
			b.used = 0
		}
	}
	b.updatePressureLocked(time.Now())
	b.signalLocked()
	b.access.Unlock()
}

func (b *memoryBudget) boosterAllowed() bool {
	if b == nil {
		return true
	}
	b.access.Lock()
	b.updatePressureLocked(time.Now())
	allowed := !b.pressure && b.used < b.boosterLimit
	b.access.Unlock()
	return allowed
}

func (b *memoryBudget) snapshot() memorySnapshot {
	if b == nil {
		return memorySnapshot{}
	}
	b.access.Lock()
	b.updatePressureLocked(time.Now())
	snapshot := memorySnapshot{
		LimitBytes:         b.limit,
		UsedBytes:          b.used,
		CachedBytes:        b.cached,
		BoosterLimitBytes:  b.boosterLimit,
		BoosterResumeBytes: b.boosterResume,
		Automatic:          b.automatic,
		Pressure:           b.pressure,
		PressureSince:      b.pressureSince,
		PressureEvents:     b.pressureCount,
		BackpressureEvents: b.waitCount,
		PeakUsedBytes:      b.peakUsed,
		PeakCachedBytes:    b.peakCached,
	}
	b.access.Unlock()
	return snapshot
}

func (b *memoryBudget) updatePressureLocked(now time.Time) {
	b.updatePeaksLocked()
	if b.pressure {
		if b.used <= b.boosterResume {
			duration := now.Sub(b.pressureSince)
			b.pressure = false
			b.pressureSince = time.Time{}
			b.signalLocked()
			b.emitEventLocked(false, duration)
		}
		return
	}
	if b.used >= b.boosterLimit {
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
	b.signalLocked()
	b.emitEventLocked(true, 0)
}

func (b *memoryBudget) updatePeaksLocked() {
	if b.used > b.peakUsed {
		b.peakUsed = b.used
	}
	if b.cached > b.peakCached {
		b.peakCached = b.cached
	}
}

func (b *memoryBudget) emitEventLocked(entered bool, duration time.Duration) {
	event := memoryPressureEvent{
		entered:  entered,
		duration: duration,
		snapshot: memorySnapshot{
			LimitBytes:         b.limit,
			UsedBytes:          b.used,
			CachedBytes:        b.cached,
			BoosterLimitBytes:  b.boosterLimit,
			BoosterResumeBytes: b.boosterResume,
			Automatic:          b.automatic,
			Pressure:           b.pressure,
			PressureSince:      b.pressureSince,
			PressureEvents:     b.pressureCount,
			BackpressureEvents: b.waitCount,
			PeakUsedBytes:      b.peakUsed,
			PeakCachedBytes:    b.peakCached,
		},
	}
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
			" high=", byteformats.FormatMemoryBytes(uint64(snapshot.BoosterLimitBytes)),
			" resume=", byteformats.FormatMemoryBytes(uint64(snapshot.BoosterResumeBytes)),
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
							" high=", byteformats.FormatMemoryBytes(uint64(event.snapshot.BoosterLimitBytes)),
							" limit=", byteformats.FormatMemoryBytes(uint64(event.snapshot.LimitBytes)),
						)
					} else {
						logger.InfoContext(
							ctx,
							"multipath memory pressure cleared: side=", side,
							" used=", byteformats.FormatMemoryBytes(uint64(event.snapshot.UsedBytes)),
							" resume=", byteformats.FormatMemoryBytes(uint64(event.snapshot.BoosterResumeBytes)),
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
	b.used -= b.cached
	b.cached = 0
	b.cache = make(map[int][][]byte)
}

func (b *memoryBudget) signalLocked() {
	close(b.changed)
	b.changed = make(chan struct{})
}
