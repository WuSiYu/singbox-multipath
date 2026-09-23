package multipath

import (
	"context"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"testing"

	"github.com/sagernet/sing-box/log"
)

func TestAutomaticRuntimeMemoryLimit(t *testing.T) {
	if got := automaticRuntimeMemoryLimit(1000, 100); got != 900 {
		t.Fatal(got)
	}
	if got := automaticRuntimeMemoryLimit(^uint64(0), ^uint64(0)); got != math.MaxInt64 {
		t.Fatal("overflow", got)
	}
	if got := automaticRuntimeMemoryLimit(0, 100); got != 100 {
		t.Fatal(got)
	}
}

func TestRuntimeMemoryGuardOwnership(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux automatic memory detection")
	}
	t.Setenv("GOMEMLIMIT", "")
	if err := os.Unsetenv("GOMEMLIMIT"); err != nil {
		t.Fatal(err)
	}
	previous := debug.SetMemoryLimit(math.MaxInt64)
	defer debug.SetMemoryLimit(previous)
	var g runtimeMemoryGuard
	limit, automatic, release1, err := g.acquire()
	if err != nil || !automatic || limit <= 0 || limit == math.MaxInt64 {
		t.Fatalf("guard: %d %v %v", limit, automatic, err)
	}
	defer release1()
	limit2, automatic2, release2, err := g.acquire()
	defer release2()
	if err != nil || !automatic2 || limit2 != limit {
		t.Fatal("instances did not share one limit")
	}
	release1()
	release1()
	if debug.SetMemoryLimit(-1) != limit {
		t.Fatal("first close restored a live instance's guard")
	}
	release2()
	if debug.SetMemoryLimit(-1) != math.MaxInt64 {
		t.Fatal("last close did not restore previous setting")
	}

	_, _, release3, err := g.acquire()
	if err != nil {
		t.Fatal(err)
	}
	debug.SetMemoryLimit(2 << 30)
	release3()
	if debug.SetMemoryLimit(-1) != 2<<30 {
		t.Fatal("overwrote a runtime override")
	}
	got, auto, release4, err := g.acquire()
	release4()
	if err != nil || auto || got != 2<<30 {
		t.Fatal("overwrote configured runtime limit")
	}
	debug.SetMemoryLimit(math.MaxInt64)
	t.Setenv("GOMEMLIMIT", "off")
	got, auto, release5, err := g.acquire()
	release5()
	if err != nil || auto || got != math.MaxInt64 {
		t.Fatal("ignored explicit GOMEMLIMIT=off")
	}
}

func TestMemoryLoggingStopsWithoutParentCancellation(t *testing.T) {
	previous := debug.SetMemoryLimit(-1)
	for range 10 {
		b := newMemoryBudget(16<<20, false)
		b.startLogging(context.Background(), log.NewNOPFactory().Logger(), "test")
		b.stopLogging()
		b.stopLogging()
		select {
		case <-b.logDone:
		default:
			t.Fatal("budget remains owned by a logging worker")
		}
	}
	if debug.SetMemoryLimit(-1) != previous {
		t.Fatal("service close leaked a process memory guard")
	}
}
