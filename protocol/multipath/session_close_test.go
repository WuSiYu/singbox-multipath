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
		if frameType[0] != frameTypeSessionClose {
			t.Fatalf("leg%d received frame type %d instead of session-close", legID, frameType[0])
		}
	}
}

func TestSessionCloseOnBoosterTerminatesCore(t *testing.T) {
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

	writeResult := make(chan error, 1)
	go func() {
		writeResult <- writeWireFrame(peers[1], wireFrame{typ: frameTypeSessionClose})
	}()
	select {
	case <-core.Done():
	case <-time.After(time.Second):
		t.Fatal("session-close on booster leg did not terminate the core")
	}
	if err := <-writeResult; err != nil {
		t.Fatal(err)
	}
	if !core.isDone() {
		t.Fatal("core remained active after peer session-close")
	}
}

func TestJoinSecondaryDefersSessionNotFound(t *testing.T) {
	core, appConn := newCore(context.Background(), testCoreConfig())
	t.Cleanup(func() { appConn.Close() })
	primaryConn, primaryPeer := net.Pipe()
	if _, err := core.addLeg(0, primaryConn, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { primaryPeer.Close() })

	var dialCount atomic.Uint64
	serverResult := make(chan error, 1)
	secondary := &tfoMatrixChild{
		tag: "secondary",
		dial: func(context.Context) (net.Conn, error) {
			dialCount.Add(1)
			clientConn, serverConn := net.Pipe()
			go func() {
				defer serverConn.Close()
				_, err := readHello(serverConn)
				if err == nil {
					err = writeHelloResponse(serverConn, helloResponse{
						Status:       helloStatusRejected,
						RejectReason: helloRejectSessionUnavailable,
					})
				}
				serverResult <- err
			}()
			return clientConn, nil
		},
	}
	outbound := &Outbound{
		logger:           L.NOP(),
		tags:             []string{"primary", secondary.tag},
		children:         []adapter.Outbound{nil, secondary},
		handshakeTimeout: time.Second,
	}
	var sessionID [16]byte
	joinDone := make(chan struct{})
	go func() {
		outbound.joinSecondary(core, sessionID, uint32(testCoreConfig().ChunkSize), "example.com:443", nil)
		close(joinDone)
	}()

	if err := <-serverResult; err != nil {
		t.Fatal(err)
	}
	select {
	case <-joinDone:
		t.Fatal("session-not-found incorrectly terminated the secondary join")
	case <-time.After(100 * time.Millisecond):
	}
	if count := dialCount.Load(); count != 1 {
		t.Fatalf("session-not-found caused an immediate retry: %d dials", count)
	}
	if core.isDone() {
		t.Fatal("session-not-found incorrectly closed the local core")
	}

	core.peerSessionClosed(io.EOF)
	select {
	case <-joinDone:
	case <-time.After(time.Second):
		t.Fatal("explicit peer session close did not stop the secondary join")
	}
}

func TestInitialFrameEncoderSupportsSessionClose(t *testing.T) {
	encoded, err := encodeWireFrame(wireFrame{typ: frameTypeSessionClose})
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != 1 || encoded[0] != frameTypeSessionClose {
		t.Fatalf("unexpected encoded session-close frame: %v", encoded)
	}
}
