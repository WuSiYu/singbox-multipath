package multipath

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/listener"
	L "github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// A sender may put one whole frame in flight before any feedback. Frames
// larger than the receive window floor must fit even when the node's fair
// share is at that floor, and when the session starts on leg1, which sends no
// startup window.
func TestFirstFrameFitsReceiveWindow(t *testing.T) {
	for _, tc := range []struct {
		name   string
		frame  int
		memory int64
		leg    uint8
	}{
		{"leg0-small-share", 1 << 20, 8 << 20, 0},
		{"leg1-created", 512 << 10, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testCoreConfig()
			cfg.FrameSize = tc.frame
			cfg.QueueFrames = 16
			cfg.QueueBytes = int64(16 * tc.frame)
			if tc.memory != 0 {
				cfg.Memory = newMemoryBudget(tc.memory, false)
			}
			left, leftApp, err := newCoreWithError(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer left.Close()
			right, rightApp, err := newCoreWithError(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer right.Close()
			// Data are queued before the receiver has sent any feedback.
			payload := flowPayload(4 * tc.frame)
			done := flowSend(leftApp, payload, true)
			a, b := net.Pipe()
			connectTestLeg(t, left, right, tc.leg, a, b)
			_ = rightApp.SetReadDeadline(time.Now().Add(10 * time.Second))
			received, err := io.ReadAll(rightApp)
			if err != nil || !bytes.Equal(received, payload) {
				right.failureMu.Lock()
				reason := right.failure
				right.failureMu.Unlock()
				t.Fatalf("received %d of %d bytes: %v (session failure: %v)", len(received), len(payload), err, reason)
			}
			if err = <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

// close() may win the lock against a policy iteration whose timer already
// fired. That iteration must not close the change channel a second time.
func TestRecoveryPolicyStopsAfterClose(t *testing.T) {
	o := &Outbound{
		ctx: context.Background(), tags: []string{"leg0", "leg1"}, logger: L.NOP(),
		cfg:             coreConfig{Memory: newMemoryBudget(8<<20, false)},
		failoverTimeout: 5 * time.Second, failbackDelay: 30 * time.Second,
	}
	r, err := newRecoveryClient(o)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	// Healthy paths change the usable mask on the first policy iteration.
	r.health[0].lastTCP, r.health[0].lastUDP = now, now
	r.health[1].lastTCP, r.health[1].lastUDP = now, now

	r.mu.Lock()
	result := make(chan any, 1)
	r.workers.Add(1)
	go func() {
		defer r.workers.Done()
		defer func() { result <- recover() }()
		r.runPolicy()
	}()
	closed := make(chan struct{})
	go func() { r.close(); close(closed) }()
	// close() queues on the mutex first; the policy timer fires after it.
	time.Sleep(250 * time.Millisecond)
	r.mu.Unlock()

	select {
	case value := <-result:
		if value != nil {
			t.Fatalf("policy loop panicked after close: %v", value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("policy loop did not stop after close")
	}
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("recovery close did not return")
	}
}

type routeRecorder struct {
	adapter.ConnectionRouter
	routed chan net.Conn
}

func (r *routeRecorder) RouteConnectionEx(_ context.Context, conn net.Conn, _ adapter.InboundContext, _ N.CloseHandlerFunc) {
	r.routed <- conn
}

func (r *routeRecorder) RoutePacketConnectionEx(context.Context, N.PacketConn, adapter.InboundContext, N.CloseHandlerFunc) {
	panic("unexpected packet route")
}

type firstReadSignal struct {
	net.Conn
	once    sync.Once
	reading chan struct{}
}

func (c *firstReadSignal) Read(p []byte) (int, error) {
	c.once.Do(func() { close(c.reading) })
	return c.Conn.Read(p)
}

func newClosableInbound(router *routeRecorder) *Inbound {
	i := &Inbound{
		ctx: context.Background(), logger: L.NOP(), router: router,
		cfg:              coreConfig{Memory: newMemoryBudget(8<<20, false)},
		handshakeTimeout: 2 * time.Second,
		sessions:         make(map[[16]byte]*serverSession),
		recoveryGroups:   make(map[[16]byte]*recoveryServerGroup),
		statusWake:       make(chan struct{}, 1),
	}
	i.listener = listener.New(listener.Options{Context: i.ctx, Logger: L.NOP()})
	return i
}

// A connection accepted before Close but still reading its hello must not
// create a session or route a target connection after Close returns.
func TestInboundCloseStopsPendingHandshakes(t *testing.T) {
	router := &routeRecorder{routed: make(chan net.Conn, 1)}
	i := newClosableInbound(router)
	client, server := net.Pipe()
	defer client.Close()
	accepted := &firstReadSignal{Conn: server, reading: make(chan struct{})}
	handled := make(chan struct{})
	go func() { i.NewConnection(i.ctx, accepted, adapter.InboundContext{}, nil); close(handled) }()
	<-accepted.reading
	if err := i.Close(); err != nil {
		t.Fatal(err)
	}
	_ = client.SetDeadline(time.Now().Add(time.Second))
	hello := helloMessage{Session: [16]byte{1}, FrameSize: 65536, Destination: "example.com:443", Create: true}
	if err := writeHello(client, hello); err == nil {
		if response, err := readHelloResponse(client); err == nil && response.Status == helloStatusOK {
			t.Fatal("closed inbound accepted a pending hello")
		}
	}
	select {
	case <-handled:
	case <-time.After(2 * time.Second):
		t.Fatal("pending handshake outlived Close")
	}

	// Connections accepted after Close are refused at once.
	late, lateServer := net.Pipe()
	defer late.Close()
	go i.NewConnection(i.ctx, lateServer, adapter.InboundContext{}, nil)
	_ = late.SetDeadline(time.Now().Add(time.Second))
	if err := writeHello(late, hello); err == nil {
		if response, err := readHelloResponse(late); err == nil && response.Status == helloStatusOK {
			t.Fatal("closed inbound accepted a new hello")
		}
	}
	select {
	case <-router.routed:
		t.Fatal("closed inbound routed a target connection")
	default:
	}
	i.access.Lock()
	sessions := len(i.sessions)
	i.access.Unlock()
	if sessions != 0 {
		t.Fatalf("closed inbound holds %d sessions", sessions)
	}
}

// A lost fragment leaves its datagram incomplete. Neither later complete
// datagrams nor later fragmented ones may wait for it to expire.
func TestRecoveryUDPLostFragmentsDoNotBlock(t *testing.T) {
	budget := newMemoryBudget(8<<20, false)
	c, err := newRecoveryPacketConn([16]byte{1}, budget, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	now := time.Now()
	address := M.ParseSocksaddr("127.0.0.1:12345")
	for seq := uint64(1); seq <= 2*recoveryAssemblySlots; seq++ {
		// Only the first fragment of each 1400-byte datagram arrives.
		c.deliver(recoveryDatagram{kind: recoveryUDPData, seq: seq, total: 1400, address: address, data: make([]byte, recoveryFragment)}, now)
	}
	if len(c.parts) > recoveryAssemblySlots {
		t.Fatalf("%d incomplete datagrams retained", len(c.parts))
	}
	seq := uint64(2*recoveryAssemblySlots + 1)
	c.deliver(recoveryDatagram{kind: recoveryUDPData, seq: seq, total: 20, address: address, data: make([]byte, 20)}, now)
	seq++
	// A fragmented datagram whose last fragment arrives first.
	c.deliver(recoveryDatagram{kind: recoveryUDPData, seq: seq, offset: recoveryFragment, total: 1400, address: address, data: make([]byte, 500)}, now)
	c.deliver(recoveryDatagram{kind: recoveryUDPData, seq: seq, total: 1400, address: address, data: make([]byte, recoveryFragment)}, now)
	if len(c.queue) != 2 {
		t.Fatalf("delivered %d of 2 datagrams while lost fragments were pending", len(c.queue))
	}
	for len(c.queue) > 0 {
		budget.releaseOther((<-c.queue).data)
	}
}

func TestRecoveryUDPDuplicateAtWindowBoundary(t *testing.T) {
	budget := newMemoryBudget(8<<20, false)
	c, err := newRecoveryPacketConn([16]byte{1}, budget, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	now := time.Now()
	d := recoveryDatagram{kind: recoveryUDPData, total: 1, address: M.ParseSocksaddr("127.0.0.1:12345"), data: []byte{42}}
	for seq := uint64(1); seq <= 129; seq++ {
		d.seq = seq
		c.deliver(d, now)
		budget.releaseOther((<-c.queue).data)
	}
	// Datagram 129 took datagram 1's slot; 1 must still count as old.
	d.seq = 1
	c.deliver(d, now)
	if len(c.queue) != 0 {
		t.Fatal("datagram 1 was delivered twice")
	}
	// The newest 127 earlier datagrams remain distinguishable.
	d.seq = 2
	c.deliver(d, now)
	if len(c.queue) != 0 {
		t.Fatal("datagram 2 was delivered twice")
	}
}
