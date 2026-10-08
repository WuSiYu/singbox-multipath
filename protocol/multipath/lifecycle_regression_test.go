package multipath

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/listener"
	L "github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/sagernet/sing-box/protocol/multipath/stream"
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

// When leg0 cannot reach the server (or failover marks it unavailable), the
// session is created on leg1. With early write, that leg sends its hello with
// the first data and must carry data before the server answers.
func TestEarlyWriteSessionCreatedOnLeg1(t *testing.T) {
	cfg := testCoreConfig()
	clientCore, clientApp := newCore(context.Background(), cfg)
	serverCore, _ := newCore(context.Background(), cfg)
	defer clientCore.Close()
	defer serverCore.Close()

	clientWire, serverWire := net.Pipe()
	defer serverWire.Close()
	_ = serverWire.SetDeadline(time.Now().Add(5 * time.Second))
	message := helloMessage{Session: [16]byte{1}, LegID: 1, FrameSize: uint32(cfg.FrameSize), Destination: "example.com:443", Create: true}
	fastOpenConn, err := newClientFastOpenConn(clientWire, message, time.Now().Add(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	readResponse := func(conn net.Conn) error {
		if err := fastOpenConn.waitStarted(); err != nil {
			return err
		}
		if _, err := readHelloResponse(conn); err != nil {
			return err
		}
		return conn.SetDeadline(time.Time{})
	}
	if _, err = clientCore.addLegWithReadPreamble(1, fastOpenConn, nil, readResponse); err != nil {
		t.Fatal(err)
	}
	payload := []byte("first bytes on leg1")
	if _, err = clientApp.Write(payload); err != nil {
		t.Fatal(err)
	}
	received, err := readHello(serverWire)
	if err != nil {
		t.Fatal("hello was not sent with the first data: ", err)
	}
	if received != message {
		t.Fatal("unexpected hello")
	}
	// The creating leg announces the client's receive window first.
	frame, err := readStartupDataFrame(serverWire, serverCore)
	if err == nil && (frame.typ != frameTypeData || !bytes.Equal(frame.data, payload)) {
		err = errors.New("unexpected early data frame")
	}
	if err != nil {
		t.Fatal(err)
	}
}

// A session may run on leg1 alone (leg0 lost, or created on leg1). A full
// Close must still deliver the data the application wrote, then DATA_FIN.
func TestCloseOnLeg1OnlyDeliversAcceptedData(t *testing.T) {
	cfg := testCoreConfig()
	left, leftApp := newCore(context.Background(), cfg)
	right, rightApp := newCore(context.Background(), cfg)
	defer left.Close()
	defer right.Close()
	a, b := net.Pipe()
	connectTestLeg(t, left, right, 1, a, b)
	payload := flowPayload(256 << 10)
	if n, err := leftApp.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("write: n=%d err=%v", n, err)
	}
	if err := leftApp.Close(); err != nil {
		t.Fatal(err)
	}
	_ = rightApp.SetReadDeadline(time.Now().Add(10 * time.Second))
	received, err := io.ReadAll(rightApp)
	if err != nil || !bytes.Equal(received, payload) {
		t.Fatalf("received %d of %d bytes: %v", len(received), len(payload), err)
	}
}

// Unread in-order data hold receive pages too; they count toward what the
// session holds when the receive region is shared.
func TestReceiveShareCountsUnreadPages(t *testing.T) {
	budget := newMemoryBudget(64<<20, false)
	cfg := testCoreConfig()
	cfg.FrameSize = 64 << 10
	cfg.ReceiveWindowBytes = 32 << 20
	cfg.Memory = budget
	core, _ := newCore(context.Background(), cfg)
	defer core.Close()
	const payload = 4 << 20
	core.stateMu.Lock()
	core.rx.Advertise(32 << 20)
	if accepted, err := core.rx.Insert(0, make([]byte, payload)); err != nil || accepted != payload {
		core.stateMu.Unlock()
		t.Fatalf("insert: accepted=%d err=%v", accepted, err)
	}
	core.receiveTargetLocked(time.Now())
	core.stateMu.Unlock()
	budget.access.Lock()
	held := budget.windows.seen[core].held
	budget.access.Unlock()
	// The head page is prepaid by the session, not taken from the region.
	if held < (payload/stream.PageSize-1)*stream.PageCharge {
		t.Fatalf("4 MiB of unread pages reported as %d held bytes", held)
	}
}

// Repeated feedback must not queue copies of a repair that is still waiting
// to be sent, nor of one sent within a retransmission timeout, and the queue
// stays bounded.
func TestRepairQueueStaysBounded(t *testing.T) {
	c := &mpCore{cfg: flowTestConfig(), tx: stream.NewSender(1 << 20), legs: make(map[uint8]*mpLeg), pumpWake: make(chan struct{}, 1)}
	c.tx.WriteNext, c.tx.Next = 1<<20, 1<<20
	leg := &mpLeg{id: 0, busy: true}
	leg.ready.Store(true)
	c.legs[0] = leg
	now := time.Now()
	for i := 0; i < 1000; i++ {
		c.queueRepairsLocked([]stream.Range{{Start: 0, End: 4096}}, now.Add(time.Duration(i)*time.Second))
	}
	if len(c.repairQueue) != 1 {
		t.Fatalf("one unsent repair queued %d times", len(c.repairQueue))
	}
	// A repair sent just now is not queued again until its timeout passes.
	c.repairQueue = nil
	c.repairSent = []repairRecord{{Range: stream.Range{Start: 0, End: 4096}, at: now}}
	c.queueRepairsLocked([]stream.Range{{Start: 0, End: 4096}}, now.Add(time.Millisecond))
	if len(c.repairQueue) != 0 {
		t.Fatal("a repair still in flight was queued again")
	}
	// A gap that moved past a partial repair is queued at once, minus the
	// part already in flight.
	c.queueRepairsLocked([]stream.Range{{Start: 2048, End: 8192}}, now.Add(time.Millisecond))
	if len(c.repairQueue) != 1 || c.repairQueue[0] != (stream.Range{Start: 4096, End: 8192}) {
		t.Fatalf("moved gap queued as %+v", c.repairQueue)
	}
	c.repairQueue = nil
	c.queueRepairsLocked([]stream.Range{{Start: 0, End: 4096}}, now.Add(time.Minute))
	if len(c.repairQueue) != 1 {
		t.Fatal("a repair was not queued again after its timeout")
	}
	for i := 0; i < 200; i++ {
		start := uint64(8192 + i*4096)
		c.queueRepairsLocked([]stream.Range{{Start: start, End: start + 1024}}, now.Add(time.Minute))
	}
	if len(c.repairQueue) > maxRepairRecords {
		t.Fatalf("repair queue grew to %d entries", len(c.repairQueue))
	}
}

// Like a socket, a Read whose deadline already passed fails even when data
// are buffered.
func TestExpiredReadDeadlineFailsWithBufferedData(t *testing.T) {
	core, app := newCore(context.Background(), flowTestConfig())
	defer core.Close()
	core.stateMu.Lock()
	_, err := core.rx.Insert(0, []byte{42})
	core.stateMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	_ = app.SetReadDeadline(time.Now().Add(-time.Second))
	if n, err := app.Read(make([]byte, 1)); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("read after the deadline: n=%d err=%v", n, err)
	}
	_ = app.SetReadDeadline(time.Time{})
	if n, err := app.Read(make([]byte, 1)); n != 1 || err != nil {
		t.Fatalf("read after clearing the deadline: n=%d err=%v", n, err)
	}
}

// With early write, CloseWrite before any payload must still send the hello:
// DATA_FIN needs a ready leg, and the leg is ready only after its hello.
func TestEarlyCloseWriteStartsHandshake(t *testing.T) {
	cfg := testCoreConfig()
	core, app := newCore(context.Background(), cfg)
	defer core.Close()
	local, peer := net.Pipe()
	defer peer.Close()
	message := helloMessage{Session: [16]byte{9}, FrameSize: uint32(cfg.FrameSize), Destination: "example.com:443", Create: true}
	early, err := newClientFastOpenConn(local, message, time.Now().Add(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	preamble := func(conn net.Conn) error {
		if err := early.waitStarted(); err != nil {
			return err
		}
		_, err := readHelloResponse(conn)
		return err
	}
	if _, err = core.addLegWithReadPreamble(0, early, nil, preamble); err != nil {
		t.Fatal(err)
	}
	logical := &earlyLogicalConn{Conn: app, core: core, primary: early}
	if err = logical.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	_ = peer.SetReadDeadline(time.Now().Add(2 * time.Second))
	got, err := readHello(peer)
	if err != nil {
		t.Fatal("CloseWrite before payload did not send the hello: ", err)
	}
	if got.Session != message.Session {
		t.Fatal("unexpected hello")
	}
}

// Remembered hello nonces are charged to the memory budget and released when
// they expire; a nonce that cannot be recorded is refused, never accepted.
func TestHelloNoncesChargeBudget(t *testing.T) {
	budget := newMemoryBudget(1<<20, false)
	i := &Inbound{cfg: coreConfig{Memory: budget}}
	now := time.Now()
	for n := 0; n < 1024; n++ {
		if err := i.rememberNonce([16]byte{byte(n), byte(n >> 8)}, now); err != nil {
			t.Fatalf("nonce %d: %v", n, err)
		}
	}
	if used := budget.snapshot().UsedBytes; used < 1024*nonceCharge {
		t.Fatalf("1024 nonces charged %d bytes", used)
	}
	if err := i.rememberNonce([16]byte{1}, now); !errors.Is(err, errHelloReplayed) {
		t.Fatal("replayed nonce accepted: ", err)
	}
	i.expireClosedSessions(now.Add(2*helloAuthSkew + time.Second))
	if used := budget.snapshot().UsedBytes; used != 0 {
		t.Fatalf("expired nonces still charge %d bytes", used)
	}
	if !budget.reserveSession(budget.limit - budget.snapshot().UsedBytes) {
		t.Fatal("fill budget")
	}
	if err := i.rememberNonce([16]byte{0xee}, now); !errors.Is(err, errHelloNonceStorage) {
		t.Fatal("nonce accepted without being recorded: ", err)
	}
}

// A hello whose nonce cannot be recorded for lack of memory is refused, but as
// a retryable resource rejection: an authentication failure would end the
// client's healthy session when a leg rejoins under memory pressure.
func TestNonceStorageRejectionIsRetryable(t *testing.T) {
	router := &routeRecorder{routed: make(chan net.Conn, 1)}
	i := newClosableInbound(router)
	i.psk = "secret"
	defer i.Close()
	budget := i.cfg.Memory
	for budget.reservePage(4096, false) {
	}
	client, server := net.Pipe()
	defer client.Close()
	go i.NewConnection(i.ctx, server, adapter.InboundContext{}, nil)
	_ = client.SetDeadline(time.Now().Add(2 * time.Second))
	if err := writeHello(client, helloMessage{Session: [16]byte{9}, LegID: 1, FrameSize: 65536, Destination: "example.com:443", PSK: "secret"}); err != nil {
		t.Fatal(err)
	}
	_, err := readHelloResponse(client)
	if reason, rejected := helloRejectReasonFromError(err); !rejected || reason != helloRejectLegUnavailable || reason.fatal(true) {
		t.Fatalf("nonce storage failure answered with %v", err)
	}
	select {
	case <-router.routed:
		t.Fatal("unrecorded hello was accepted")
	default:
	}
}

// The closed-session record is reserved when a session is admitted, so its ID
// is remembered even if memory is full when the session ends: a retried
// Create with that ID must never dial the target a second time.
func TestClosedSessionTombstoneReservedAtAdmission(t *testing.T) {
	router := &routeRecorder{routed: make(chan net.Conn, 1)}
	i := newClosableInbound(router)
	defer i.Close()
	client, server := net.Pipe()
	defer client.Close()
	go i.NewConnection(i.ctx, server, adapter.InboundContext{}, nil)
	_ = client.SetDeadline(time.Now().Add(2 * time.Second))
	id := [16]byte{5}
	if err := writeHello(client, helloMessage{Session: id, FrameSize: 65536, Destination: "example.com:443", Create: true}); err != nil {
		t.Fatal(err)
	}
	if response, err := readHelloResponse(client); err != nil || response.Status != helloStatusOK {
		t.Fatalf("create: %+v %v", response, err)
	}
	<-router.routed
	i.access.Lock()
	session := i.sessions[id]
	i.access.Unlock()
	if session == nil || !session.tombstone {
		t.Fatal("admitted session has no reserved closed-session record")
	}
	budget := i.cfg.Memory
	if !budget.reserveSession(budget.limit - budget.snapshot().UsedBytes) {
		t.Fatal("fill budget")
	}
	i.removeSession(id, session)
	i.access.Lock()
	_, remembered := i.closedSessions[id]
	i.access.Unlock()
	if !remembered {
		t.Fatal("closed session forgotten while memory was full")
	}
}
