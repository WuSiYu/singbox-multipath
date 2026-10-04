package multipath

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// Faults preserve ordered reliable bytes: only delivery timing is changed.
type auditDelayConn struct {
	net.Conn
	delay   time.Duration
	mode    string
	every   int
	count   int
	payload bool
	stopped chan struct{}
	once    sync.Once
	hits    int
}

func (c *auditDelayConn) Close() error {
	c.once.Do(func() { close(c.stopped) })
	return c.Conn.Close()
}

func (c *auditDelayConn) wait() error {
	c.hits++
	select {
	case <-time.After(c.delay):
		return nil
	case <-c.stopped:
		return net.ErrClosed
	}
}

func (c *auditDelayConn) Write(p []byte) (int, error) {
	isData := len(p) == dataFrameHeaderSize && p[0] == frameTypeData
	isPayload := c.payload
	c.payload = isData
	match := c.mode == "all" || c.mode == "header" && isData || c.mode == "payload" && isPayload || c.mode == "window" && len(p) >= flowFixedSize && p[0] == frameTypeWindow
	if match {
		c.count++
		if c.count%c.every == 0 {
			if c.mode == "payload" && len(p) > 3 {
				n, err := c.Conn.Write(p[:3])
				if err != nil {
					return n, err
				}
				if err = c.wait(); err != nil {
					return n, err
				}
				m, err := c.Conn.Write(p[3:])
				return n + m, err
			}
			if err := c.wait(); err != nil {
				return 0, err
			}
		}
	}
	return c.Conn.Write(p)
}

func auditFault(conn net.Conn, mode string, delay time.Duration, every int) *auditDelayConn {
	return &auditDelayConn{Conn: conn, mode: mode, delay: delay, every: every, stopped: make(chan struct{})}
}

func auditStream(seed int, n int) []byte {
	p := make([]byte, n)
	x := uint64(seed + 1)
	for i := range p {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		p[i] = byte(x)
	}
	return p
}

func TestFlowRegressionTransientStallMatrix(t *testing.T) {
	for _, which := range []string{"leg0_upload", "leg0_download", "leg1_upload", "leg1_download", "both_upload", "both_bidi"} {
		for _, mode := range []string{"header", "payload", "window"} {
			for _, delay := range []time.Duration{80 * time.Millisecond, 350 * time.Millisecond, 1200 * time.Millisecond} {
				if mode == "window" && (which == "leg1_upload" || which == "leg1_download") {
					continue
				}
				t.Run(fmt.Sprintf("%s/%s/%s", which, mode, delay), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						cfg := flowTestConfig()
						cfg.PathStallTimeoutMin = time.Second
						left, app := newCore(context.Background(), cfg)
						cfg.Memory = newMemoryBudget(1<<20, false)
						right, peer := newCore(context.Background(), cfg)
						defer closeFlowCores(left, right)
						left.activate(activationInfo{Reason: activationReasonBytes})
						right.activate(activationInfo{Reason: activationReasonBytes})
						var faults []*auditDelayConn
						for id := uint8(0); id < 2; id++ {
							a, b := net.Pipe()
							var ca, cb net.Conn = a, b
							if which == fmt.Sprintf("leg%d_upload", id) || which == "both_upload" || which == "both_bidi" {
								f := auditFault(a, mode, delay, 3)
								ca = f
								faults = append(faults, f)
							}
							if which == fmt.Sprintf("leg%d_download", id) || which == "both_bidi" {
								f := auditFault(b, mode, delay, 4)
								cb = f
								faults = append(faults, f)
							}
							connectTestLeg(t, left, right, id, ca, cb)
						}
						up, down := auditStream(101, 96<<10), auditStream(202, 81<<10)
						w1, w2 := flowSend(app, up, true), flowSend(peer, down, true)
						readDone := make(chan error, 2)
						for i, conn := range []net.Conn{peer, app} {
							want := [][]byte{up, down}[i]
							go func() {
								_ = conn.SetReadDeadline(time.Now().Add(120 * time.Second))
								got, err := io.ReadAll(conn)
								if err == nil && !bytes.Equal(got, want) {
									err = fmt.Errorf("corrupt data: got %d expected %d", len(got), len(want))
								}
								readDone <- err
							}()
						}
						for range 2 {
							if err := <-readDone; err != nil {
								t.Fatal(err)
							}
						}
						if err := <-w1; err != nil {
							t.Fatal(err)
						}
						if err := <-w2; err != nil {
							t.Fatal(err)
						}
						time.Sleep(1500 * time.Millisecond)
						synctest.Wait()
						assertFlowAlive(t, left, right)
						hits := 0
						for _, f := range faults {
							hits += f.hits
						}
						if hits == 0 {
							t.Fatal("no injected stalls")
						}
					})
				})
			}
		}
	}
}

func TestFlowRegressionIdleReturnUnderDelayedLeg0(t *testing.T) {
	for _, delay := range []time.Duration{200 * time.Millisecond, 350 * time.Millisecond, 1200 * time.Millisecond} {
		t.Run(delay.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := flowTestConfig()
				cfg.ReceiveWindowBytes = 32 << 10
				cfg.PathStallTimeoutMin = time.Second
				left, app := newCore(context.Background(), cfg)
				cfg.Memory = newMemoryBudget(1<<20, false)
				right, peer := newCore(context.Background(), cfg)
				defer closeFlowCores(left, right)
				left.activate(activationInfo{Reason: activationReasonBytes})
				right.activate(activationInfo{Reason: activationReasonBytes})
				a, b := net.Pipe()
				connectTestLeg(t, left, right, 0, auditFault(a, "window", delay, 3), auditFault(b, "window", delay, 5))
				a, b = net.Pipe()
				connectTestLeg(t, left, right, 1, auditFault(a, "payload", 350*time.Millisecond, 4), b)
				for round := 0; round < 20; round++ {
					payload := auditStream(round, 4<<10)
					sent := flowSend(app, payload, false)
					_ = peer.SetReadDeadline(time.Now().Add(20 * time.Second))
					got := make([]byte, len(payload))
					_, err := io.ReadFull(peer, got)
					if err != nil {
						t.Fatalf("round %d: %v", round, err)
					}
					if !bytes.Equal(got, payload) {
						t.Fatal("corruption")
					}
					if err = <-sent; err != nil {
						t.Fatal(err)
					}
					time.Sleep(time.Duration(240+round%5*20) * time.Millisecond)
					assertFlowAlive(t, left, right)
				}
			})
		})
	}
}

func TestFlowRegressionExplicitOutOfOrderAndDuplicate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := flowTestConfig()
		cfg.ReceiveWindowBytes = 16 << 10
		core, app := newCore(context.Background(), cfg)
		defer closeFlowCores(core)
		a, b := net.Pipe()
		defer b.Close()
		_, err := core.addLeg(0, a, nil)
		if err != nil {
			t.Fatal(err)
		}
		// Drain receiver control to keep a full independent leg0 write path.
		go io.Copy(io.Discard, b)
		seqs := []uint64{3, 2, 3, 0, 2, 1, 7, 5, 4, 7, 6, 0, 1}
		want := make([]byte, 0, 8*1024)
		for i := 0; i < 8; i++ {
			want = append(want, auditStream(i, 1024)...)
		}
		go func() {
			for index, seq := range seqs {
				_ = writeWireFrame(b, wireFrame{typ: frameTypeData, seq: seq * 1024, generation: 1, pathSeq: uint64(index) * 1024, data: auditStream(int(seq), 1024)})
			}
			_ = writeWireFrame(b, wireFrame{typ: frameTypeFIN, seq: 8 * 1024})
		}()
		_ = app.SetReadDeadline(time.Now().Add(3 * time.Second))
		got, err := io.ReadAll(app)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("data mismatch: %d %v", len(got), err)
		}
		assertFlowAlive(t, core)
	})
}

type auditControlGate struct {
	net.Conn
	started chan struct{}
	release chan struct{}
	stopped chan struct{}
	once    sync.Once
	blocked bool
}

func (c *auditControlGate) Close() error {
	c.once.Do(func() { close(c.stopped) })
	return c.Conn.Close()
}
func (c *auditControlGate) Write(p []byte) (int, error) {
	if !c.blocked && len(p) == controlFrameHeaderSize && p[0] == frameTypePing {
		c.blocked = true
		close(c.started)
		select {
		case <-c.release:
		case <-c.stopped:
			return 0, net.ErrClosed
		}
	}
	return c.Conn.Write(p)
}

func TestFlowRegressionTransientControlStallThenIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := flowTestConfig()
		cfg.PathStallTimeoutMin = time.Second
		left, app := newCore(context.Background(), cfg)
		cfg.Memory = newMemoryBudget(1<<20, false)
		right, peer := newCore(context.Background(), cfg)
		defer closeFlowCores(left, right)
		left.activate(activationInfo{Reason: activationReasonBytes})
		a, b := net.Pipe()
		connectTestLeg(t, left, right, 0, a, b)
		a, b = net.Pipe()
		fault := &auditControlGate{Conn: a, started: make(chan struct{}), release: make(chan struct{}), stopped: make(chan struct{})}
		leg, _ := connectTestLeg(t, left, right, 1, fault, b)
		leg.tryQueueControl(wireFrame{typ: frameTypePing, seq: 987})
		<-fault.started
		go func() { time.Sleep(1500 * time.Millisecond); close(fault.release) }()
		payload := auditStream(77, 7<<10)
		sent := flowSend(app, payload, false)
		_ = peer.SetReadDeadline(time.Now().Add(5 * time.Second))
		got := make([]byte, len(payload))
		_, err := io.ReadFull(peer, got)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("transfer: %v", err)
		}
		if err = <-sent; err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		left.replayMu.Lock()
		pending := left.replayBytes
		left.replayMu.Unlock()
		t.Logf("after recovered: replay=%d ack=%d next=%d fallback=%d", pending, left.ackedNext.Load(), left.txSeq.Load(), left.fallbackF.Load())
		time.Sleep(6 * time.Second)
		synctest.Wait()
		assertFlowAlive(t, left, right)
		if left.getLeg(1) == nil {
			t.Fatal("healthy idle leg1 detached after all data already ACKed")
		}
	})
}

func TestFlowRegressionHalfCloseBoundaries(t *testing.T) {
	for _, size := range []int{0, 1, 1024, 1025, 8192, 8193, 65536} {
		for _, pause := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/%t", size, pause), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					cfg := flowTestConfig()
					left, app := newCore(context.Background(), cfg)
					cfg.Memory = newMemoryBudget(1<<20, false)
					right, peer := newCore(context.Background(), cfg)
					defer closeFlowCores(left, right)
					left.activate(activationInfo{Reason: activationReasonBytes})
					right.activate(activationInfo{Reason: activationReasonBytes})
					a, b := net.Pipe()
					connectTestLeg(t, left, right, 0, a, b)
					a, b = net.Pipe()
					var cb net.Conn = b
					if pause {
						cb = auditFault(b, "payload", 350*time.Millisecond, 2)
					}
					connectTestLeg(t, left, right, 1, a, cb)
					// One direction ends without a request. Peer subsequently sends reply.
					closeTestWrite(t, app)
					_ = peer.SetReadDeadline(time.Now().Add(10 * time.Second))
					if p, err := io.ReadAll(peer); err != nil || len(p) != 0 {
						t.Fatalf("empty FIN %v", err)
					}
					payload := auditStream(44, size)
					sent := flowSend(peer, payload, true)
					_ = app.SetReadDeadline(time.Now().Add(10 * time.Second))
					got, err := io.ReadAll(app)
					if err != nil || !bytes.Equal(got, payload) {
						t.Fatalf("reply data %v len=%d", err, len(got))
					}
					if err = <-sent; err != nil {
						t.Fatal(err)
					}
					assertFlowAlive(t, left, right)
				})
			})
		}
	}
}

func TestFlowRegressionGracefulEOFWithBufferedResponse(t *testing.T) {
	for _, slow := range []bool{false, true} {
		t.Run(fmt.Sprint(slow), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := flowTestConfig()
				cfg.ReceiveWindowBytes = 64 << 10
				left, app := newCore(context.Background(), cfg)
				cfg.Memory = newMemoryBudget(1<<20, false)
				right, peer := newCore(context.Background(), cfg)
				defer closeFlowCores(left, right)
				a, b := net.Pipe()
				connectTestLeg(t, left, right, 0, a, b)
				a, b = net.Pipe()
				connectTestLeg(t, left, right, 1, a, b)
				request := []byte("request finished")
				response := auditStream(92, 32<<10)
				serverDone := make(chan error, 1)
				go func() {
					got, err := io.ReadAll(peer)
					if err == nil && !bytes.Equal(got, request) {
						err = fmt.Errorf("request mismatch")
					}
					if err == nil {
						_, err = peer.Write(response)
					}
					if err == nil {
						err = peer.(closeWriter).CloseWrite()
					}
					// Both application directions are finished, as in a completed relay.
					if err == nil {
						err = peer.Close()
					}
					serverDone <- err
				}()
				sent := flowSend(app, request, true)
				if slow {
					time.Sleep(500 * time.Millisecond)
				}
				_ = app.SetReadDeadline(time.Now().Add(5 * time.Second))
				got, err := io.ReadAll(app)
				if err != nil || !bytes.Equal(got, response) {
					t.Fatalf("normal close truncated response: got=%d want=%d err=%v", len(got), len(response), err)
				}
				if err = <-sent; err != nil {
					t.Fatal(err)
				}
				if err = <-serverDone; err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func TestFlowRegressionCloseDrainsAlreadyACKedReceiveData(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := flowTestConfig()
		cfg.ReceiveWindowBytes = 64 << 10
		left, app := newCore(context.Background(), cfg)
		cfg.Memory = newMemoryBudget(1<<20, false)
		right, peer := newCore(context.Background(), cfg)
		defer closeFlowCores(left, right)
		a, b := net.Pipe()
		connectTestLeg(t, left, right, 0, a, b)
		payload := auditStream(45, 4<<10)
		sent := flowSend(peer, payload, true)
		time.Sleep(500 * time.Millisecond)
		synctest.Wait()
		if right.ackedNext.Load() != uint64(len(payload))+1 {
			t.Fatalf("not fully receipt ACKed: %d", right.ackedNext.Load())
		}
		if err := <-sent; err != nil {
			t.Fatal(err)
		}
		right.Close()
		time.Sleep(50 * time.Millisecond)
		_ = app.SetReadDeadline(time.Now().Add(time.Second))
		got, err := io.ReadAll(app)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("receipt ACKed data discarded by close: got=%d want=%d err=%v", len(got), len(payload), err)
		}
	})
}

func TestFlowRegressionRepeatedBoosterDisconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := flowTestConfig()
		cfg.Memory = newMemoryBudget(512<<10, false)
		left, app := newCore(context.Background(), cfg)
		cfg.Memory = newMemoryBudget(512<<10, false)
		right, peer := newCore(context.Background(), cfg)
		defer closeFlowCores(left, right)
		left.activate(activationInfo{Reason: activationReasonBytes})
		a, b := net.Pipe()
		connectTestLeg(t, left, right, 0, auditFault(a, "window", 80*time.Millisecond, 3), b)
		for round := 0; round < 6; round++ {
			a, b := net.Pipe()
			fault := newPartialBooster(a)
			ll, rr := connectTestLeg(t, left, right, 1, fault, b)
			go func() { <-fault.started; time.Sleep(100 * time.Millisecond); _ = fault.Close() }()
			payload := auditStream(round+200, 64<<10)
			sent := flowSend(app, payload, false)
			_ = peer.SetReadDeadline(time.Now().Add(10 * time.Second))
			got := make([]byte, len(payload))
			_, err := io.ReadFull(peer, got)
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("reconnect %d payload: %v", round, err)
			}
			if err = <-sent; err != nil {
				t.Fatal(err)
			}
			<-ll.readerDone
			<-ll.writerDone
			<-rr.readerDone
			<-rr.writerDone
			time.Sleep(350 * time.Millisecond)
			synctest.Wait()
			assertFlowAlive(t, left, right)
		}
	})
}
