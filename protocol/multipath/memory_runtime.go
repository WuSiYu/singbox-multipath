package multipath

import (
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
)

// The MP allocator bounds live reservations, not GC headroom or child-protocol
// heaps. Share one runtime guard across instances; never multiply the soft limit
// by the number of configured MP inbounds/outbounds.
var processMemoryGuard runtimeMemoryGuard

type runtimeMemoryGuard struct {
	mu                sync.Mutex
	users             int
	previous, applied int64
}

func automaticRuntimeMemoryLimit(available, runtimeBytes uint64) int64 {
	// available already excludes this process's resident working set. Add its
	// Go footprint back, leaving 20% of remaining capacity for non-Go/OS use.
	headroom := available/5*4 + available%5*4/5
	runtimeBytes = min(runtimeBytes, uint64(math.MaxInt64))
	return int64(runtimeBytes + min(headroom, uint64(math.MaxInt64)-runtimeBytes))
}

func (g *runtimeMemoryGuard) acquire() (int64, bool, func(), error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	current := debug.SetMemoryLimit(-1)
	_, explicit := os.LookupEnv("GOMEMLIMIT")
	if g.users == 0 {
		if explicit || current != math.MaxInt64 {
			return current, false, func() {}, nil
		}
		available, err := availableMemory()
		if err != nil {
			return current, false, func() {}, err
		}
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		g.applied = automaticRuntimeMemoryLimit(available, stats.Sys-stats.HeapReleased)
		g.previous = debug.SetMemoryLimit(g.applied)
		current = g.applied
	}
	g.users++
	var once sync.Once
	return current, current == g.applied, func() { once.Do(g.release) }, nil
}

func (g *runtimeMemoryGuard) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.users--
	if g.users == 0 && debug.SetMemoryLimit(-1) == g.applied {
		// A caller may have changed the setting during service operation. Only
		// restore a value that is still ours, including failed startup cleanup.
		debug.SetMemoryLimit(g.previous)
	}
}
