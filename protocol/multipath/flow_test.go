package multipath

import (
	"bytes"
	"context"
	"io"
	"net"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sagernet/sing-box/protocol/multipath/stream"
)

// A complete DATA header and part of its payload arrive, then the original
// writer retains the caller's slice while leg0 is free to deliver a duplicate.
type partialBoosterConn struct {
	net.Conn
	started, release, stopped chan struct{}
	closeOnce                 sync.Once
	payload, blocked          bool
}

func newPartialBooster(conn net.Conn) *partialBoosterConn {
	return &partialBoosterConn{Conn: conn, started: make(chan struct{}), release: make(chan struct{}), stopped: make(chan struct{})}
}

func (c *partialBoosterConn) Write(data []byte) (int, error) {
	if len(data) == dataFrameHeaderSize && data[0] == frameTypeData {
		c.payload = true
		return c.Conn.Write(data)
	}
	// A frame header may also arrive contiguously with its payload.
	header := 0
	if !c.payload && len(data) > dataFrameHeaderSize && data[0] == frameTypeData {
		c.payload, header = true, dataFrameHeaderSize
	}
	if c.payload {
		c.payload = false
		if !c.blocked {
			c.blocked = true
			first := min(header+3, len(data))
			n, err := c.Conn.Write(data[:first])
			if err != nil {
				return n, err
			}
			close(c.started)
			select {
			case <-c.stopped:
				return n, net.ErrClosed
			case <-c.release:
			}
			more, err := c.Conn.Write(data[first:])
			return n + more, err
		}
	}
	return c.Conn.Write(data)
}

func (c *partialBoosterConn) Close() error {
	c.closeOnce.Do(func() { close(c.stopped) })
	return c.Conn.Close()
}

func flowTestConfig() coreConfig {
	cfg := testCoreConfig()
	cfg.FrameSize, cfg.QueueFrames, cfg.QueueBytes = 1024, 64, 64<<10
	cfg.ReceiveWindowBytes = 8 << 10
	cfg.PathStallTimeoutMin = 200 * time.Millisecond
	cfg.Memory = newMemoryBudget(1<<20, false)
	return cfg
}

func flowPayload(size int) []byte {
	data := make([]byte, size)
	for index := range data {
		data[index] = byte(index % 251)
	}
	return data
}

func flowSend(conn net.Conn, data []byte, closeWrite bool) <-chan error {
	result := make(chan error, 1)
	go func() {
		_, err := conn.Write(data)
		if err == nil && closeWrite {
			err = conn.(closeWriter).CloseWrite()
		}
		result <- err
	}()
	return result
}

func assertFlowAlive(t *testing.T, cores ...*mpCore) {
	t.Helper()
	for _, core := range cores {
		if core.isDone() {
			core.failureMu.Lock()
			reason := core.failure
			core.failureMu.Unlock()
			t.Fatalf("healthy leg0 session closed: %s", reason)
		}
		if core.memory.snapshot().PeakUsedBytes > core.memory.snapshot().LimitBytes {
			t.Fatal("memory budget exceeded")
		}
		if core.reorderPeak.Load() > core.cfg.ReceiveWindowBytes {
			t.Fatal("reorder limits exceeded")
		}
	}
}

func closeFlowCores(cores ...*mpCore) {
	for _, core := range cores {
		core.Close()
	}
	// Keep the synctest bubble alive while graceful-close timers drain a
	// deliberately stalled writer. Production timers advance independently.
	for _, core := range cores {
		core.workerGroup.Wait()
	}
}

func TestFlowTinyWindowSurvivesPartialBooster(t *testing.T) {
	for _, late := range []bool{false, true} {
		t.Run(map[bool]string{false: "permanent_stall", true: "late_duplicate"}[late], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				left, app := newCore(context.Background(), flowTestConfig())
				right, peerApp := newCore(context.Background(), flowTestConfig())
				defer closeFlowCores(left, right)
				left.activate(activationInfo{Reason: activationReasonBytes})
				a, b := net.Pipe()
				connectTestLeg(t, left, right, 0, a, b)
				a, b = net.Pipe()
				fault := newPartialBooster(a)
				connectTestLeg(t, left, right, 1, fault, b)
				if late {
					go func() { <-fault.started; time.Sleep(1350 * time.Millisecond); close(fault.release) }()
				}
				payload := flowPayload(2 << 20)
				result := flowSend(app, payload, true)
				_ = peerApp.SetReadDeadline(time.Now().Add(5 * time.Second))
				got, err := io.ReadAll(peerApp)
				if err != nil {
					t.Fatal(err)
				}
				if err = <-result; err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, payload) {
					t.Fatal("recovered stream was corrupted")
				}
				if left.fallbackF.Load() == 0 {
					t.Fatal("test did not exercise replay")
				}
				assertFlowAlive(t, left, right)
				// Recovery does not wait for the stalled writer or detach the leg.
				if left.getLeg(1) == nil {
					t.Fatal("single recovery detached leg1")
				}
				if late {
					time.Sleep(400 * time.Millisecond)
					synctest.Wait()
					assertFlowAlive(t, left, right)
				}
			})
		})
	}
}

func TestFlowBidirectionalStallAndMemoryPressure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		leftCfg, rightCfg := flowTestConfig(), flowTestConfig()
		leftCfg.ReceiveWindowBytes, rightCfg.ReceiveWindowBytes = 64<<10, 128<<10
		leftCfg.Memory, rightCfg.Memory = newMemoryBudget(512<<10, false), newMemoryBudget(512<<10, false)
		left, app := newCore(context.Background(), leftCfg)
		right, peerApp := newCore(context.Background(), rightCfg)
		defer closeFlowCores(left, right)
		left.activate(activationInfo{Reason: activationReasonBytes})
		right.activate(activationInfo{Reason: activationReasonBytes})
		a, b := net.Pipe()
		connectTestLeg(t, left, right, 0, a, b)
		a, b = net.Pipe()
		connectTestLeg(t, left, right, 1, newPartialBooster(a), newPartialBooster(b))
		payload := flowPayload(1 << 20)
		leftDone, rightDone := flowSend(app, payload, true), flowSend(peerApp, payload, true)
		readDone := make(chan error, 2)
		for _, conn := range []net.Conn{app, peerApp} {
			go func(conn net.Conn) {
				_ = conn.SetReadDeadline(time.Now().Add(8 * time.Second))
				got, err := io.ReadAll(conn)
				if err == nil && !bytes.Equal(got, payload) {
					err = io.ErrUnexpectedEOF
				}
				readDone <- err
			}(conn)
		}
		for range 2 {
			if err := <-readDone; err != nil {
				t.Fatal(err)
			}
		}
		if err := <-leftDone; err != nil {
			t.Fatal(err)
		}
		if err := <-rightDone; err != nil {
			t.Fatal(err)
		}
		if left.fallbackF.Load()+right.fallbackF.Load() == 0 {
			t.Fatal("no fallback exercised")
		}
		assertFlowAlive(t, left, right)
	})
}

func TestFlowSlowReaderDoesNotBlockControlOrReverseData(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		left, app := newCore(context.Background(), flowTestConfig())
		right, peerApp := newCore(context.Background(), flowTestConfig())
		defer closeFlowCores(left, right)
		left.activate(activationInfo{Reason: activationReasonBytes})
		a, b := net.Pipe()
		connectTestLeg(t, left, right, 0, a, b)
		a, b = net.Pipe()
		connectTestLeg(t, left, right, 1, a, b)
		payload := flowPayload(128 << 10)
		written := flowSend(app, payload, false)
		time.Sleep(time.Second)
		synctest.Wait()
		if left.ackedNext.Load() == 0 {
			t.Fatal("receipt ACK blocked behind application delivery")
		}
		if left.fallbackF.Load() != 0 {
			t.Fatal("slow application was treated as network loss")
		}
		if left.rttSnapshot()[0].Samples == 0 {
			t.Fatal("control probe blocked behind full receive window")
		}
		reply := []byte("reverse direction still works")
		replied := flowSend(peerApp, reply, false)
		_ = app.SetReadDeadline(time.Now().Add(time.Second))
		gotReply := make([]byte, len(reply))
		if _, err := io.ReadFull(app, gotReply); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(gotReply, reply) {
			t.Fatal("reverse payload corrupted")
		}
		if err := <-replied; err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(payload))
		_ = peerApp.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.ReadFull(peerApp, got); err != nil {
			t.Fatal(err)
		}
		if err := <-written; err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatal("slow-reader payload corrupted")
		}
		assertFlowAlive(t, left, right)
	})
}

func TestFlowIdleReleasesStorageWithoutRetractingWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := flowTestConfig()
		cfg.ReceiveWindowBytes = 128 << 10
		left, app := newCore(context.Background(), cfg)
		cfg.Memory = newMemoryBudget(1<<20, false)
		right, peerApp := newCore(context.Background(), cfg)
		defer closeFlowCores(left, right)
		a, b := net.Pipe()
		connectTestLeg(t, left, right, 0, a, b)
		for range 3 {
			payload := flowPayload(32 << 10)
			written := flowSend(app, payload, false)
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(peerApp, got); err != nil {
				t.Fatal(err)
			}
			if err := <-written; err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatal("payload mismatch after idle interval")
			}
			time.Sleep(750 * time.Millisecond)
			synctest.Wait()
			right.stateMu.Lock()
			held, _, _ := right.rx.Buffered()
			right.stateMu.Unlock()
			if held != 0 {
				t.Fatalf("idle connection retained %d unread bytes", held)
			}
			assertFlowAlive(t, left, right)
		}
	})
}

func TestFlowMessageCodec(t *testing.T) {
	for _, message := range []flowMessage{
		{Next: 5, Limit: 64, Seq: 9, Paths: [2]stream.Receipt{{Generation: 1, Next: 1024, ReceivedAt: 12345}, {Generation: 2, Next: 4096, ReceivedAt: 78900}}, Flags: flowFlagPressure},
		{Next: 1 << 40, Limit: 1<<40 + 1<<29, Seq: 1, NACKs: []stream.Range{{Start: 1<<40 + 100, End: 1<<40 + 200}, {Start: 1<<40 + 1<<28, End: 1<<40 + 1<<28 + 65536}}},
	} {
		a, b := net.Pipe()
		go func() { defer a.Close(); _ = writeWireFrame(a, wireFrame{typ: frameTypeWindow, flow: message}) }()
		core, _ := newCore(context.Background(), testCoreConfig())
		frame, err := readWireFrame(b, core)
		b.Close()
		core.Close()
		if err != nil || frame.typ != frameTypeWindow || !reflect.DeepEqual(frame.flow, message) {
			t.Fatalf("flow codec: %+v %v", frame, err)
		}
	}
}

func TestRecoveryTimeoutDefaultsAndBounds(t *testing.T) {
	path := stream.Path{}
	if got := path.RTO(0); got != time.Second {
		t.Fatalf("initial timeout %s", got)
	}
	path.SRTT, path.RTTVar = 80*time.Millisecond, 10*time.Millisecond
	if got := path.RTO(0); got != 200*time.Millisecond {
		t.Fatalf("adaptive timeout %s", got)
	}
	if got := path.RTO(2 * time.Second); got != 2*time.Second {
		t.Fatalf("configured floor ignored: %s", got)
	}
}

func TestMemoryLargeLimitRegions(t *testing.T) {
	budget := newMemoryBudget(1<<62, false)
	if budget.txRegionLocked() <= 0 || budget.rxRegionLocked() <= 0 || budget.underPressure() {
		t.Fatal("large memory region overflow")
	}
}

func TestFlowSharedBudgetPreservesEveryLeg0(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		budget := newMemoryBudget(4<<20, false)
		var cores []*mpCore
		var apps, peers []net.Conn
		for range 4 {
			cfg := flowTestConfig()
			cfg.Memory = budget
			left, app := newCore(context.Background(), cfg)
			right, peer := newCore(context.Background(), cfg)
			left.activate(activationInfo{Reason: activationReasonBytes})
			cores = append(cores, left, right)
			apps = append(apps, app)
			peers = append(peers, peer)
			a, b := net.Pipe()
			connectTestLeg(t, left, right, 0, a, b)
		}
		defer closeFlowCores(cores...)
		synctest.Wait()
		// Exhaust the shared regions and the margin: every session must still
		// progress through its own TX reserve and prepaid receive floor.
		snapshot := budget.snapshot()
		held := snapshot.LimitBytes - snapshot.UsedBytes - 1024
		if !budget.reserveSession(held) {
			t.Fatal("pressure reservation failed")
		}
		defer budget.releaseSession(held)
		payload := flowPayload(64 << 10)
		results := make(chan error, len(apps))
		for index, app := range apps {
			written := flowSend(app, payload, false)
			go func(peer net.Conn, written <-chan error) {
				_ = peer.SetReadDeadline(time.Now().Add(5 * time.Second))
				got := make([]byte, len(payload))
				_, readErr := io.ReadFull(peer, got)
				if readErr == nil {
					readErr = <-written
				}
				if readErr == nil && !bytes.Equal(got, payload) {
					readErr = io.ErrUnexpectedEOF
				}
				results <- readErr
			}(peers[index], written)
		}
		for range apps {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		if !budget.snapshot().Pressure {
			t.Fatal("test did not maintain memory pressure")
		}
		assertFlowAlive(t, cores...)
	})
}

func TestFlowFeedbackValidation(t *testing.T) {
	core, _ := newCore(context.Background(), flowTestConfig())
	defer core.Close()
	for _, message := range []flowMessage{{Next: 1, Limit: 2}, {Next: 1, Limit: 0}} {
		if core.handleWindow(message) == nil {
			t.Fatalf("accepted invalid feedback: %+v", message)
		}
	}
}

func TestFlowByteACKAndReorderedWindow(t *testing.T) {
	core, _ := newCore(context.Background(), flowTestConfig())
	defer core.Close()
	core.stateMu.Lock()
	core.tx.WindowEnd = 4096
	_ = core.tx.Append(stream.NewBuffer(make([]byte, 2048), nil))
	segment, _ := core.tx.NextRange(2048)
	_ = core.tx.Sent(segment)
	core.tx.CloseWrite()
	core.tx.SendFIN()
	core.stateMu.Unlock()
	if err := core.handleWindow(flowMessage{Next: 2049, Limit: 4096}); err != nil {
		t.Fatal(err)
	}
	if err := core.handleWindow(flowMessage{Next: 1024, Limit: 2048}); err != nil {
		t.Fatal(err)
	}
	core.stateMu.Lock()
	defer core.stateMu.Unlock()
	if core.tx.Una != 2049 || !core.tx.FINAcked || core.tx.WindowEnd != 4096 || core.tx.Buffered() != 0 {
		t.Fatal("stale feedback changed ACK or window")
	}
}

func TestFlowMinimumBudgetStillTransmits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := flowTestConfig()
		minimum := minimumSessionMemory(cfg)
		cfg.Memory = newMemoryBudget(minimum, false)
		left, app := newCore(context.Background(), cfg)
		cfg.Memory = newMemoryBudget(minimum, false)
		right, peer := newCore(context.Background(), cfg)
		defer closeFlowCores(left, right)
		left.activate(activationInfo{Reason: activationReasonBytes})
		a, b := net.Pipe()
		connectTestLeg(t, left, right, 0, a, b)
		payload := flowPayload(32 << 10)
		written := flowSend(app, payload, false)
		_ = peer.SetReadDeadline(time.Now().Add(5 * time.Second))
		got := make([]byte, len(payload))
		if _, err := io.ReadFull(peer, got); err != nil {
			t.Fatal(err)
		}
		if err := <-written; err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatal("minimum-budget payload corrupted")
		}
		assertFlowAlive(t, left, right)
	})
}
