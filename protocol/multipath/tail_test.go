package multipath

import (
	"context"
	"testing"
	"time"

	"github.com/sagernet/sing-box/protocol/multipath/stream"
)

// At the end of a transfer, a frame still queued on a path that has not
// delivered yet is resent once on an idle faster path.
func TestTailReinjectionResendsStuckTail(t *testing.T) {
	core, _ := newCore(context.Background(), testCoreConfig())
	defer core.Close()
	core.active.Store(true)
	fast := &mpLeg{id: 0, send: make(chan wireFrame, 1), path: stream.Path{Generation: 1, Rate: 100 << 20, SRTT: 20 * time.Millisecond, MinimumRTT: 20 * time.Millisecond}}
	slow := &mpLeg{id: 1, send: make(chan wireFrame, 1), path: stream.Path{Generation: 2}}
	fast.ready.Store(true)
	slow.ready.Store(true)
	core.stateMu.Lock()
	defer core.stateMu.Unlock()
	core.legsMu.Lock()
	core.legs[0], core.legs[1] = fast, slow
	core.legsMu.Unlock()
	defer func() {
		core.legsMu.Lock()
		delete(core.legs, 0)
		delete(core.legs, 1)
		core.legsMu.Unlock()
	}()
	core.tx.WindowEnd = 1 << 20
	if err := core.tx.Append(stream.NewBuffer(make([]byte, 4096), nil)); err != nil {
		t.Fatal(err)
	}
	segment, _ := core.tx.NextRange(4096)
	now := time.Now()
	if err := core.submitLocked(slow, segment, false, now); err != nil {
		t.Fatal(err)
	}
	<-slow.send
	slow.busy = false
	// More data may still come: no tail yet.
	core.writerWaiting.Store(true)
	if sent, _ := core.tailReinjectLocked(now); sent {
		t.Fatal("reinjected while the application was still backlogged")
	}
	core.writerWaiting.Store(false)
	core.tx.CloseWrite()
	sent, err := core.tailReinjectLocked(now)
	if err != nil || !sent {
		t.Fatalf("tail not reinjected: %v", err)
	}
	frame := <-fast.send
	if !frame.replay || frame.seq != 0 || len(frame.data) != 4096 || core.tailE.Load() != 1 {
		t.Fatalf("unexpected tail frame: seq=%d len=%d replay=%v", frame.seq, len(frame.data), frame.replay)
	}
	frame.buffer.Release()
	fast.busy = false
	if sent, _ = core.tailReinjectLocked(now); sent {
		t.Fatal("tail frame reinjected twice")
	}
}
