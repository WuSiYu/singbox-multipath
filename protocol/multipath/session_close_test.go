package multipath

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	L "github.com/sagernet/sing/common/logger"
)

func TestCoreCloseWritesSessionCloseOnEveryLeg(t *testing.T) {
	core, appConn := newCore(context.Background(), testCoreConfig())
	t.Cleanup(func() { appConn.Close() })
	peers := make([]net.Conn, 0, 2)
	for legID := uint8(0); legID < 2; legID++ {
		coreConn, peerConn := net.Pipe()
		if _, err := core.addLeg(legID, coreConn, nil); err != nil {
			t.Fatal(err)
		}
		peers = append(peers, peerConn)
		t.Cleanup(func() { peerConn.Close() })
	}

	startedAt := time.Now()
	if err := core.Close(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(startedAt); elapsed > 100*time.Millisecond {
		t.Fatalf("logical close waited for physical leg drain: %s", elapsed)
	}
	for legID, peerConn := range peers {
		if err := peerConn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		var frameType [1]byte
		if _, err := io.ReadFull(peerConn, frameType[:]); err != nil {
			t.Fatalf("leg%d did not receive session-close: %v", legID, err)
		}
		if legID == 0 && frameType[0] == frameTypeWindow {
			if _, err := readFlow(peerConn); err != nil {
				t.Fatal(err)
			}
			if _, err := io.ReadFull(peerConn, frameType[:]); err != nil {
				t.Fatal(err)
			}
		}
		if frameType[0] != frameTypeSessionClose {
			t.Fatalf("leg%d received frame type %d instead of session-close", legID, frameType[0])
		}
		var reason [1]byte
		if _, err := io.ReadFull(peerConn, reason[:]); err != nil || reason[0] != closeReasonShutdown {
			t.Fatalf("leg%d close reason: %v %v", legID, reason, err)
		}
	}
}

// Session close is sent only when the peer terminates, after its DATA_FIN was
// acknowledged on a normal close, so it is ordered by the byte stream and ends
// the session from any leg.
func TestSessionCloseOnAnyLegEndsSession(t *testing.T) {
	core, appConn := newCore(context.Background(), testCoreConfig())
	t.Cleanup(func() { core.Close() })
	t.Cleanup(func() { appConn.Close() })
	peers := make([]net.Conn, 0, 2)
	for legID := uint8(0); legID < 2; legID++ {
		coreConn, peerConn := net.Pipe()
		if _, err := core.addLeg(legID, coreConn, nil); err != nil {
			t.Fatal(err)
		}
		peers = append(peers, peerConn)
		t.Cleanup(func() { peerConn.Close() })
	}
	go func() { _, _ = io.Copy(io.Discard, peers[0]) }()
	if err := writeWireFrame(peers[1], wireFrame{typ: frameTypeSessionClose}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-core.Done():
	case <-time.After(time.Second):
		t.Fatal("session close on leg1 was ignored")
	}
}

// A confirmed session that the server reports as unavailable has ended there;
// the leg manager closes the local session instead of retrying forever.
func TestMaintainLegEndsSessionClosedByServer(t *testing.T) {
	core, appConn := newCore(context.Background(), testCoreConfig())
	t.Cleanup(func() { appConn.Close() })
	primaryConn, primaryPeer := net.Pipe()
	if _, err := core.addLeg(0, primaryConn, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { primaryPeer.Close() })
	go func() { _, _ = io.Copy(io.Discard, primaryPeer) }()

	var dialCount atomic.Uint64
	secondary := &tfoMatrixChild{
		tag: "secondary",
		dial: func(context.Context) (net.Conn, error) {
			dialCount.Add(1)
			clientConn, serverConn := net.Pipe()
			go func() {
				defer serverConn.Close()
				if _, err := readHello(serverConn); err == nil {
					_ = writeHelloResponse(serverConn, helloResponse{Status: helloStatusRejected, RejectReason: helloRejectSessionUnavailable})
				}
			}()
			return clientConn, nil
		},
	}
	outbound := &Outbound{
		ctx:              context.Background(),
		logger:           L.NOP(),
		tags:             []string{"primary", secondary.tag},
		children:         []adapter.Outbound{nil, secondary},
		handshakeTimeout: time.Second,
		cfg:              testCoreConfig(),
	}
	var confirmed atomic.Bool
	confirmed.Store(true)
	done := make(chan struct{})
	go func() {
		outbound.maintainLeg(core, [16]byte{1}, 1, "example.com:443", &confirmed, time.Now().Add(time.Second), nil)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("leg manager kept retrying a closed session")
	}
	if !core.isDone() || dialCount.Load() != 1 {
		t.Fatalf("closed session: done=%v dials=%d", core.isDone(), dialCount.Load())
	}
}

func TestLegBackoff(t *testing.T) {
	backoff := time.Duration(0)
	var got []time.Duration
	for range 7 {
		backoff = nextLegBackoff(backoff, false)
		got = append(got, backoff)
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("backoff %v, want %v", got, want)
		}
	}
	if nextLegBackoff(time.Minute, true) != 250*time.Millisecond {
		t.Fatal("failover sessions rely on shared health checks and retry quickly")
	}
}

func TestInitialFrameEncoderSupportsSessionClose(t *testing.T) {
	encoded, err := encodeWireFrame(wireFrame{typ: frameTypeSessionClose})
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != 2 || encoded[0] != frameTypeSessionClose || encoded[1] != closeReasonUnknown {
		t.Fatalf("unexpected encoded session-close frame: %v", encoded)
	}
}
