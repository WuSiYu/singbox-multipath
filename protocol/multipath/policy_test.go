package multipath

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
)

func TestPolicyHelloRoundTrip(t *testing.T) {
	p := sessionPolicy{
		Upload:   directionPolicy{QueueFrames: 16, ActivationWindow: time.Second, SendBufferBytes: 1 << 20, ReceiveWindowBytes: 2 << 20},
		Download: directionPolicy{AggregationEnabled: true, Leg0TrafficSaving: true, ActivationOnQueue: true, QueueFrames: 64, ThresholdBytesPS: 2500000, ActivationAfterBytes: 2 << 20, ActivationAfterBytesMinBytesPS: 125000, ActivationWindow: 500 * time.Millisecond, SendBufferBytes: 4 << 20, ReceiveWindowBytes: 8 << 20, PathStallTimeoutMin: 300 * time.Millisecond},
	}
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	expected := helloMessage{FrameSize: 16384, Destination: "example.com:443", Policy: p}
	done := make(chan error, 1)
	go func() { done <- writeHello(a, expected) }()
	actual, err := readHello(b)
	if err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if actual != expected {
		t.Fatalf("policy round trip mismatch: %+v", actual)
	}
}

func TestPolicyDigestMismatchRejected(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	message := helloMessage{FrameSize: 65536, Destination: "example.com:443"}
	message.Policy.Download.Leg0TrafficSaving = true
	done := make(chan error, 1)
	go func() {
		got, err := readHello(b)
		if err == nil {
			got.Policy.Download.Leg0TrafficSaving = false
			err = writeHelloResponse(b, helloResponse{Status: helloStatusOK, FrameSize: got.FrameSize, PolicyDigest: got.Policy.digest()})
		}
		done <- err
	}()
	out := &Outbound{handshakeTimeout: time.Second}
	if err := out.clientHandshake(context.Background(), a, message); err == nil {
		t.Fatal("changed policy accepted")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestJoinCannotChangeDirectionPolicy(t *testing.T) {
	original := helloMessage{Session: [16]byte{1}, LegID: 1, FrameSize: 65536, Destination: "example.com:443"}
	for _, change := range []func(*sessionPolicy){
		func(p *sessionPolicy) { p.Upload.AggregationEnabled = true },
		func(p *sessionPolicy) { p.Download.Leg0TrafficSaving = true },
		func(p *sessionPolicy) { p.Upload.ReceiveWindowBytes = 1 << 20 },
		func(p *sessionPolicy) { p.Download.QueueFrames = 64 },
	} {
		a, b := net.Pipe()
		_ = a.SetDeadline(time.Now().Add(time.Second))
		server := &Inbound{ctx: context.Background(), logger: log.NewNOPFactory().Logger(), handshakeTimeout: time.Second,
			sessions: map[[16]byte]*serverSession{original.Session: {frameSize: original.FrameSize, policy: original.Policy, destination: M.ParseSocksaddr(original.Destination)}},
		}
		done := make(chan struct{})
		go func() { server.NewConnection(context.Background(), b, adapter.InboundContext{}, nil); close(done) }()
		changed := original
		change(&changed.Policy)
		if err := writeHello(a, changed); err != nil {
			t.Fatal(err)
		}
		_, err := readHelloResponse(a)
		if reason, ok := helloRejectReasonFromError(err); !ok || reason != helloRejectSessionMismatch {
			t.Fatalf("join accepted changed policy: %v", err)
		}
		a.Close()
		b.Close()
		<-done
	}
}
