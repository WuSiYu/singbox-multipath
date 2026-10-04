package multipath

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"testing/synctest"
	"time"
)

func TestCloseReasonCodec(t *testing.T) {
	for reason := closeReasonUnknown; reason <= closeReasonShutdown; reason++ {
		frame := wireFrame{typ: frameTypeSessionClose, closeReason: reason}
		encoded, err := encodeWireFrame(frame)
		if err != nil || !bytes.Equal(encoded, []byte{frameTypeSessionClose, reason}) {
			t.Fatalf("initial encoding: %v %v", encoded, err)
		}
		a, b := net.Pipe()
		result := make(chan error, 1)
		go func() { defer a.Close(); result <- writeWireFrame(a, frame) }()
		got, err := readFrame(b, nil)
		b.Close()
		if err != nil || got.closeReason != reason || got.typ != frameTypeSessionClose {
			t.Fatalf("round trip: %+v %v", got, err)
		}
		if err = <-result; err != nil {
			t.Fatal(err)
		}
	}
	for _, payload := range [][]byte{{frameTypeSessionClose}, {frameTypeSessionClose, 255}} {
		a, b := net.Pipe()
		go func() { defer a.Close(); _, _ = a.Write(payload) }()
		_, err := readFrame(b, nil)
		b.Close()
		if err == nil {
			t.Fatalf("accepted truncated/invalid close reason: %v", payload)
		}
	}
	if _, err := encodeWireFrame(wireFrame{typ: frameTypeSessionClose, closeReason: 255}); err == nil {
		t.Fatal("accepted invalid initial close reason")
	}
}

func TestEndpointCloseProvenanceAcrossLegs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		left, app := newCore(context.Background(), flowTestConfig())
		right, peer := newCore(context.Background(), flowTestConfig())
		defer closeFlowCores(left, right)
		for id := uint8(0); id < 2; id++ {
			a, b := net.Pipe()
			connectTestLeg(t, left, right, id, a, b)
		}
		if err := app.Close(); err != nil {
			t.Fatal(err)
		}
		if data, err := io.ReadAll(peer); err != nil || len(data) != 0 {
			t.Fatalf("endpoint close changed FIN semantics: %v %v", data, err)
		}
		waitCoreRelease(t, left, right)
		if got := closeSource(left.closeSource.Load()); got != closeSourceLocalEndpoint {
			t.Fatalf("local origin: %v", got)
		}
		if got := closeSource(right.closeSource.Load()); got != closeSourceRemoteEndpoint {
			t.Fatalf("peer origin: %v", got)
		}
	})
}

func TestTransportFailureCannotBecomeEndpointClose(t *testing.T) {
	for _, peerClose := range []bool{false, true} {
		core, app := newCore(context.Background(), flowTestConfig())
		if peerClose {
			core.peerSessionClosed(io.EOF)
		} else {
			core.fail(io.ErrUnexpectedEOF)
		}
		app.Close() // relay cleanup after an MP-originated error
		source := closeSource(core.closeSource.Load())
		if source == closeSourceLocalEndpoint || source == closeSourceRemoteEndpoint {
			t.Fatalf("transport failure hidden as endpoint close: %v", source)
		}
		waitCoreRelease(t, core)
	}
}

func TestLegEventEndpointAttribution(t *testing.T) {
	for _, source := range []closeSource{closeSourceUnknown, closeSourceLocalEndpoint, closeSourceRemoteEndpoint, closeSourceTransport} {
		core, app := newCore(context.Background(), flowTestConfig())
		core.noteCloseSource(source)
		for _, cause := range []error{io.ErrUnexpectedEOF, syscall.ECONNRESET, syscall.EPIPE, context.Canceled} {
			err := core.sourcedLegError(cause)
			if !errors.Is(err, cause) {
				t.Fatal("source annotation changed error identity")
			}
			status := newOutboundStatus("", outboundStatusConfig{})
			status.recordLegError(1, "read_data", "example.com:443", "test", 0, err, time.Now())
			if got := status.legErrors[1].source; got != source.String() {
				t.Fatalf("status source %q, want %q", got, source.String())
			}
			wantCount := uint64(1)
			if source == closeSourceLocalEndpoint || source == closeSourceRemoteEndpoint {
				wantCount = 0
			}
			if got := status.legErrors[1].count; got != wantCount {
				t.Fatalf("event count %d, want %d", got, wantCount)
			}
		}
		for _, cause := range []error{os.ErrDeadlineExceeded, errors.New("invalid multipath data mapping")} {
			err := core.sourcedLegError(cause).(*sourcedLegError)
			if err.source != closeSourceUnknown {
				t.Fatalf("independent timeout/protocol error hidden by endpoint close: %v", err.source)
			}
		}
		core.Close()
		app.Close()
		waitCoreRelease(t, core)
	}
}

func TestLegFailureCountsExcludeConfirmedEndpointClose(t *testing.T) {
	for _, source := range []closeSource{closeSourceUnknown, closeSourceLocalEndpoint, closeSourceRemoteEndpoint, closeSourceTransport} {
		for _, cause := range []error{io.ErrUnexpectedEOF, os.ErrDeadlineExceeded} {
			core, _ := newCore(context.Background(), flowTestConfig())
			core.noteCloseSource(source)
			a, b := net.Pipe()
			leg, err := core.addLeg(1, a, nil)
			if err != nil {
				t.Fatal(err)
			}
			core.legFailed(leg, legFailureReadData, cause)
			want := uint64(1)
			if cause == io.ErrUnexpectedEOF && (source == closeSourceLocalEndpoint || source == closeSourceRemoteEndpoint) {
				want = 0
			}
			core.legFailureMu.Lock()
			got := core.legFailures[1]
			core.legFailureMu.Unlock()
			if got != want {
				t.Fatalf("source=%v err=%v: count=%d want=%d", source, cause, got, want)
			}
			b.Close()
			core.Close()
			waitCoreRelease(t, core)
		}
	}
}

func TestPeerEndpointMarkerOnAnyLeg(t *testing.T) {
	core, app := newCore(context.Background(), flowTestConfig())
	defer core.Close()
	defer app.Close()
	a, b := net.Pipe()
	defer b.Close()
	if _, err := core.addLeg(1, a, nil); err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = io.Copy(io.Discard, b) }()
	if err := writeWireFrame(b, wireFrame{typ: frameTypeSessionClose, closeReason: closeReasonEndpoint}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-core.Done():
	case <-time.After(time.Second):
		t.Fatal("peer session close on leg1 was ignored")
	}
	if source := closeSource(core.closeSource.Load()); source != closeSourceRemoteEndpoint {
		t.Fatalf("missing peer close provenance: %v", source)
	}
}
