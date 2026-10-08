package multipath

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sagernet/sing-box/protocol/multipath/stream"
)

func TestHelloAuthentication(t *testing.T) {
	message := helloMessage{Session: [16]byte{7}, LegID: 1, FrameSize: 64 << 10, Destination: "example.com:443", Create: true, PSK: "secret"}
	read := func(m helloMessage) (helloMessage, *helloAuth) {
		t.Helper()
		encoded, err := encodeHello(m)
		if err != nil {
			t.Fatal(err)
		}
		a, b := net.Pipe()
		go func() { defer a.Close(); _, _ = a.Write(encoded) }()
		got, auth, err := readHelloWithAuth(b)
		b.Close()
		if err != nil {
			t.Fatal(err)
		}
		return got, auth
	}
	nonces := make(map[[16]byte]bool)
	remember := func(nonce [16]byte, _ time.Time) error {
		if nonces[nonce] {
			return errHelloReplayed
		}
		nonces[nonce] = true
		return nil
	}
	got, auth := read(message)
	if got.Session != message.Session || !got.Create || got.LegID != 1 || got.Destination != message.Destination || got.PSK != "" {
		t.Fatalf("hello fields: %+v", got)
	}
	if err := verifyHelloAuth(auth, "secret", time.Now(), remember); err != nil {
		t.Fatal(err)
	}
	if err := verifyHelloAuth(auth, "secret", time.Now(), remember); err == nil {
		t.Fatal("replayed hello accepted")
	}
	_, auth = read(message)
	if err := verifyHelloAuth(auth, "other", time.Now(), remember); err == nil {
		t.Fatal("wrong psk accepted")
	}
	if err := verifyHelloAuth(auth, "secret", time.Now().Add(3*time.Minute), remember); err == nil {
		t.Fatal("stale hello accepted")
	}
	message.PSK = ""
	if _, auth = read(message); verifyHelloAuth(auth, "secret", time.Now(), remember) == nil {
		t.Fatal("unauthenticated hello accepted by a psk server")
	}
	// Any change to the signed bytes breaks the MAC.
	message.PSK = "secret"
	encoded, _ := encodeHello(message)
	encoded[helloHeaderSize] ^= 1
	a, b := net.Pipe()
	go func() { defer a.Close(); _, _ = a.Write(encoded) }()
	_, auth, err := readHelloWithAuth(b)
	b.Close()
	if err != nil || verifyHelloAuth(auth, "secret", time.Now(), remember) == nil {
		t.Fatal("tampered hello accepted", err)
	}
}

func TestHelloVersionMismatch(t *testing.T) {
	encoded, err := encodeHello(helloMessage{FrameSize: 64 << 10, Destination: "example.com:443"})
	if err != nil {
		t.Fatal(err)
	}
	encoded[4] = 12
	a, b := net.Pipe()
	go func() { defer a.Close(); _, _ = a.Write(encoded) }()
	_, err = readHello(b)
	b.Close()
	var versionErr *helloVersionError
	if !errors.As(err, &versionErr) || versionErr.version != 12 {
		t.Fatalf("version mismatch not reported: %v", err)
	}
	if !helloRejectVersion.valid() || !helloRejectVersion.fatal(false) || helloRejectLegUnavailable.fatal(true) || helloRejectSessionUnavailable.fatal(false) || !helloRejectSessionUnavailable.fatal(true) {
		t.Fatal("rejection classification")
	}
}

// Losing leg0 is not fatal: feedback, data and FIN continue on leg1.
func TestLeg0LossKeepsSession(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := flowTestConfig()
		cfg.ReceiveWindowBytes = 64 << 10
		left, app := newCore(context.Background(), cfg)
		right, peer := newCore(context.Background(), cfg)
		defer closeFlowCores(left, right)
		a0, b0 := net.Pipe()
		connectTestLeg(t, left, right, 0, a0, b0)
		a1, b1 := net.Pipe()
		connectTestLeg(t, left, right, 1, a1, b1)
		left.activate(activationInfo{Reason: activationReasonBytes})
		payload := flowPayload(512 << 10)
		written := flowSend(app, payload, true)
		go func() {
			time.Sleep(20 * time.Millisecond)
			a0.Close()
		}()
		_ = peer.SetReadDeadline(time.Now().Add(30 * time.Second))
		got, err := io.ReadAll(peer)
		if err != nil {
			t.Fatal(err)
		}
		if err = <-written; err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatal("payload mismatch after leg0 loss")
		}
		if left.getLeg(0) != nil || right.getLeg(0) != nil {
			t.Fatal("test did not remove leg0")
		}
		assertFlowAlive(t, left, right)
	})
}

// Before activation new data use leg0; once leg0 is gone they move to leg1.
func TestPreferredDataLegFallsBackToLeg1(t *testing.T) {
	core, _ := newCore(context.Background(), testCoreConfig())
	defer core.Close()
	primary, secondary := &mpLeg{id: 0}, &mpLeg{id: 1}
	primary.ready.Store(true)
	secondary.ready.Store(true)
	core.stateMu.Lock()
	defer core.stateMu.Unlock()
	core.legsMu.Lock()
	core.legs[0], core.legs[1] = primary, secondary
	core.legsMu.Unlock()
	if got := core.choosePathLocked(1024); got != primary || core.dataModeLocked() != 1 {
		t.Fatal("pre-activation data must use leg0")
	}
	primary.path.Stale = true
	if got := core.choosePathLocked(1024); got != secondary || dataModeName(core.dataModeLocked()) != "failover" {
		t.Fatal("stalled leg0 must hand new data to leg1")
	}
	core.legsMu.Lock()
	delete(core.legs, 0)
	core.legsMu.Unlock()
	if got := core.choosePathLocked(1024); got != secondary {
		t.Fatal("missing leg0 must hand new data to leg1")
	}
	core.legsMu.Lock()
	delete(core.legs, 1)
	core.legsMu.Unlock()
}

func TestSessionEndsAfterLegsAbsent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := flowTestConfig()
		cfg.LegAbsentTimeout = 30 * time.Second
		core, app := newCore(context.Background(), cfg)
		defer app.Close()
		a, b := net.Pipe()
		leg, err := core.addLeg(0, a, nil)
		if err != nil {
			t.Fatal(err)
		}
		go func() { _, _ = io.Copy(io.Discard, b) }()
		core.legFailed(leg, legFailureReadData, io.EOF)
		b.Close()
		time.Sleep(25 * time.Second)
		if core.isDone() {
			t.Fatal("session ended before the leg-absent timeout")
		}
		time.Sleep(10 * time.Second)
		if !core.isDone() {
			t.Fatal("session without legs outlived its timeout")
		}
		waitCoreRelease(t, core)
	})
}

// Feedback copies may arrive out of order on different legs: monotonic fields
// merge, while flags only follow the newest sequence.
func TestFeedbackMergeAcrossLegs(t *testing.T) {
	core, _ := newCore(context.Background(), flowTestConfig())
	defer core.Close()
	if err := core.handleWindow(flowMessage{Next: 0, Limit: 4096, Seq: 5, Flags: flowFlagPressure}); err != nil {
		t.Fatal(err)
	}
	if err := core.handleWindow(flowMessage{Next: 0, Limit: 2048, Seq: 4}); err != nil {
		t.Fatal(err)
	}
	core.stateMu.Lock()
	window, pressure := core.tx.WindowEnd, core.peerPressure
	core.stateMu.Unlock()
	if window != 4096 || !pressure {
		t.Fatalf("stale feedback overrode newer state: window=%d pressure=%v", window, pressure)
	}
	// A newer limit may be lower: the receiver took back an unused window.
	if err := core.handleWindow(flowMessage{Next: 0, Limit: 1024, Seq: 6}); err != nil {
		t.Fatal(err)
	}
	core.stateMu.Lock()
	window = core.tx.WindowEnd
	core.stateMu.Unlock()
	if window != 1024 {
		t.Fatalf("newer feedback did not lower the window: %d", window)
	}
}

// While the application is backlogged, a slower path whose completion is far
// behind (a lossy QUIC child with data waiting for retransmissions) still
// takes data when the faster one is busy, within the window and skew limits.
func TestBackloggedSlowerPathKeepsCarrying(t *testing.T) {
	core, _ := newCore(context.Background(), testCoreConfig())
	defer core.Close()
	core.active.Store(true)
	core.writerWaiting.Store(true)
	fast, slow := &mpLeg{id: 0}, &mpLeg{id: 1}
	fast.ready.Store(true)
	slow.ready.Store(true)
	fast.busy = true
	fast.path = stream.Path{Rate: 100e6, SRTT: 40 * time.Millisecond, MinimumRTT: 40 * time.Millisecond, Sent: 8 << 20}
	slow.path = stream.Path{Rate: 45e6, SRTT: 220 * time.Millisecond, MinimumRTT: 57 * time.Millisecond, Sent: 12 << 20}
	core.stateMu.Lock()
	defer core.stateMu.Unlock()
	core.tx.WindowEnd = 1 << 30
	core.legsMu.Lock()
	core.legs[0], core.legs[1] = fast, slow
	core.legsMu.Unlock()
	defer func() {
		core.legsMu.Lock()
		clear(core.legs)
		core.legsMu.Unlock()
	}()
	// Completion 0.30 s against 0.10 s: three times the best path, but the
	// segment arrives about 0.2 s later, within eight 57 ms round trips.
	if got := core.choosePathLocked(65536); got != slow {
		t.Fatal("backlogged slower path was starved")
	}
	// The same backlog on a 10 ms path is seconds of queue for it.
	slow.path.MinimumRTT, slow.path.SRTT = 10*time.Millisecond, 10*time.Millisecond
	if got := core.choosePathLocked(65536); got != nil {
		t.Fatal("slower path took data beyond the skew limit")
	}
	// Without a backlog the tail rule stays strict.
	slow.path.MinimumRTT, slow.path.SRTT = 57*time.Millisecond, 220*time.Millisecond
	core.writerWaiting.Store(false)
	core.writerReleasedAt.Store(0)
	if got := core.choosePathLocked(65536); got != nil {
		t.Fatal("idle application handed the tail to a slower path")
	}
}
