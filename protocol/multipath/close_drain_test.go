package multipath

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

type finAckGate struct {
	net.Conn
	started, release, stopped chan struct{}
	once                      sync.Once
	blocked                   bool
}

func (g *finAckGate) Close() error { g.once.Do(func() { close(g.stopped) }); return g.Conn.Close() }
func (g *finAckGate) Write(p []byte) (int, error) {
	if !g.blocked && len(p) == 1+flowPayloadSize && p[0] == frameTypeWindow && binary.BigEndian.Uint64(p[2:10]) == (32<<10)+1 {
		g.blocked = true
		close(g.started)
		select {
		case <-g.release:
		case <-g.stopped:
			return 0, net.ErrClosed
		}
	}
	return g.Conn.Write(p)
}

func waitCoreRelease(t *testing.T, cores ...*mpCore) {
	t.Helper()
	for _, c := range cores {
		select {
		case <-c.released:
		case <-time.After(3 * time.Second):
			t.Fatal("closed core retained workers or memory")
		}
		s := c.memory.snapshot()
		if s.UsedBytes != s.CachedBytes {
			t.Fatalf("budget leaked: used=%d cached=%d", s.UsedBytes, s.CachedBytes)
		}
	}
}

func TestGracefulCloseWaitsForFINReceipt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := flowTestConfig()
		left, app := newCore(context.Background(), cfg)
		cfg.Memory = newMemoryBudget(1<<20, false)
		right, peer := newCore(context.Background(), cfg)
		defer closeFlowCores(left, right)
		a, b := net.Pipe()
		gate := &finAckGate{Conn: b, started: make(chan struct{}), release: make(chan struct{}), stopped: make(chan struct{})}
		connectTestLeg(t, left, right, 0, a, gate)
		payload := auditStream(711, 32<<10)
		written := make(chan error, 1)
		go func() {
			_, err := app.Write(payload)
			if err == nil {
				err = app.Close()
			}
			written <- err
		}()
		got, err := io.ReadAll(peer)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("payload: %v", err)
		}
		if err = <-written; err != nil {
			t.Fatal(err)
		}
		<-gate.started
		time.Sleep(350 * time.Millisecond)
		if left.isDone() {
			t.Fatal("session-close preceded FIN receipt ACK")
		}
		close(gate.release)
		waitCoreRelease(t, left, right)
	})
}

func TestSimultaneousEmptyApplicationClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		left, app := newCore(context.Background(), flowTestConfig())
		right, peer := newCore(context.Background(), flowTestConfig())
		defer closeFlowCores(left, right)
		a, b := net.Pipe()
		connectTestLeg(t, left, right, 0, a, b)
		app.Close()
		peer.Close()
		waitCoreRelease(t, left, right)
	})
}

func TestApplicationCloseDrainsAcrossTransientLegs(t *testing.T) {
	for _, legID := range []uint8{0, 1} {
		for _, delay := range []time.Duration{80 * time.Millisecond, 350 * time.Millisecond, 1200 * time.Millisecond} {
			t.Run(fmt.Sprintf("leg%d/%s", legID, delay), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					cfg := flowTestConfig()
					cfg.PathStallTimeoutMin = time.Second
					left, app := newCore(context.Background(), cfg)
					cfg.Memory = newMemoryBudget(1<<20, false)
					right, peer := newCore(context.Background(), cfg)
					defer closeFlowCores(left, right)
					left.activate(activationInfo{Reason: activationReasonBytes})
					for id := uint8(0); id < 2; id++ {
						a, b := net.Pipe()
						var out net.Conn = a
						if id == legID {
							out = auditFault(a, "payload", delay, 3)
						}
						connectTestLeg(t, left, right, id, out, b)
					}
					payload := auditStream(313, 128<<10)
					written := make(chan error, 1)
					go func() {
						_, err := app.Write(payload)
						if err == nil {
							err = app.Close()
						}
						written <- err
					}()
					time.Sleep(500 * time.Millisecond)
					_ = peer.SetReadDeadline(time.Now().Add(90 * time.Second))
					got, err := io.ReadAll(peer)
					if err != nil || !bytes.Equal(got, payload) {
						t.Fatalf("close with transient retransmission: got=%d want=%d err=%v", len(got), len(payload), err)
					}
					if err = <-written; err != nil {
						t.Fatal(err)
					}
					waitCoreRelease(t, left, right)
				})
			})
		}
	}
}

func TestPhysicalEOFAfterFINDrainsReceive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		left, app := newCore(context.Background(), flowTestConfig())
		right, peer := newCore(context.Background(), flowTestConfig())
		defer closeFlowCores(left, right)
		a, b := net.Pipe()
		connectTestLeg(t, left, right, 0, a, b)
		payload := auditStream(36, 4096)
		written := flowSend(peer, payload, true)
		time.Sleep(500 * time.Millisecond)
		synctest.Wait()
		if !left.receiveComplete() {
			t.Fatal("FIN not received")
		}
		b.Close()
		time.Sleep(50 * time.Millisecond)
		got, err := io.ReadAll(app)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("EOF lost data: got=%d err=%v", len(got), err)
		}
		if err = <-written; err != nil {
			t.Fatal(err)
		}
	})
}

func TestForceCloseInterruptsReceiveDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		left, _ := newCore(context.Background(), flowTestConfig())
		right, peer := newCore(context.Background(), flowTestConfig())
		defer closeFlowCores(left, right)
		a, b := net.Pipe()
		connectTestLeg(t, left, right, 0, a, b)
		written := flowSend(peer, auditStream(33, 4096), true)
		time.Sleep(500 * time.Millisecond)
		synctest.Wait()
		if !left.receiveComplete() {
			t.Fatal("receive data incomplete")
		}
		if err := <-written; err != nil {
			t.Fatal(err)
		}
		peer.Close()
		<-left.Done()
		select {
		case <-left.released:
			t.Fatal("slow reader was not preserved")
		default:
		}
		left.Close()
		waitCoreRelease(t, left, right)
	})
}

func TestRejectPrematureFINAcknowledgement(t *testing.T) {
	c, _ := newCore(context.Background(), flowTestConfig())
	defer c.Close()
	if c.handleWindow(flowMessage{Next: 1, Limit: 1}) == nil {
		t.Fatal("accepted ACK without FIN")
	}
}

func TestControlWriteFailureAfterFINDrainsReceive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		left, app := newCore(context.Background(), flowTestConfig())
		right, peer := newCore(context.Background(), flowTestConfig())
		defer closeFlowCores(left, right)
		a, b := net.Pipe()
		leg, _ := connectTestLeg(t, left, right, 0, a, b)
		payload := auditStream(34, 4096)
		written := flowSend(peer, payload, true)
		time.Sleep(500 * time.Millisecond)
		synctest.Wait()
		if !left.receiveComplete() {
			t.Fatal("receive data incomplete")
		}
		left.legFailed(leg, legFailureWriteControl, errors.New("broken pipe"))
		got, err := io.ReadAll(app)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("write error lost complete RX: %d %v", len(got), err)
		}
		if err = <-written; err != nil {
			t.Fatal(err)
		}
	})
}

func TestCloseReadPreservesSendingDirection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		left, app := newCore(context.Background(), flowTestConfig())
		right, peer := newCore(context.Background(), flowTestConfig())
		defer closeFlowCores(left, right)
		a, b := net.Pipe()
		connectTestLeg(t, left, right, 0, a, b)
		if err := app.(interface{ CloseRead() error }).CloseRead(); err != nil {
			t.Fatal(err)
		}
		// Discarded input exceeds the receive window, requiring credit recycling.
		reverse := flowSend(peer, auditStream(303, 32<<10), false)
		payload := auditStream(304, 32<<10)
		sent := flowSend(app, payload, true)
		_ = peer.SetReadDeadline(time.Now().Add(3 * time.Second))
		got, err := io.ReadAll(peer)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("CloseRead interrupted TX: got=%d err=%v", len(got), err)
		}
		if err = <-sent; err != nil {
			t.Fatal(err)
		}
		if err = <-reverse; err != nil {
			t.Fatal(err)
		}
		assertFlowAlive(t, left, right)
	})
}
