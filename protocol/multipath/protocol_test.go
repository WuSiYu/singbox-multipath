package multipath

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

type pendingHandshakeConn struct {
	net.Conn
	pending               bool
	deadlineSetAfterWrite bool
}

func (c *pendingHandshakeConn) NeedHandshake() bool {
	return c.pending
}

func (c *pendingHandshakeConn) Write(payload []byte) (int, error) {
	c.pending = false
	return c.Conn.Write(payload)
}

func (c *pendingHandshakeConn) SetDeadline(deadline time.Time) error {
	if c.pending {
		return os.ErrInvalid
	}
	if !deadline.IsZero() {
		c.deadlineSetAfterWrite = true
	}
	return c.Conn.SetDeadline(deadline)
}

type invalidDeadlineConn struct {
	net.Conn
}

func (c *invalidDeadlineConn) SetDeadline(time.Time) error {
	return os.ErrInvalid
}

func TestHelloResponseCarriesChunkSize(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	message := helloMessage{
		LegID:         0,
		RequestStatus: true,
		FrameSize:     64 * 1024,
		Destination:   "example.com:443",
	}
	serverResult := make(chan error, 1)
	go func() {
		received, err := readHello(server)
		if err == nil && received != message {
			t.Errorf("hello mismatch: %#v != %#v", received, message)
		}
		if err == nil {
			err = writeHelloResponse(server, helloResponse{Status: helloStatusOK, FrameSize: 32 * 1024})
		}
		serverResult <- err
	}()
	if err := writeHello(client, message); err != nil {
		t.Fatal(err)
	}
	response, err := readHelloResponse(client)
	if err != nil {
		t.Fatal(err)
	}
	if response.FrameSize != 32*1024 {
		t.Fatalf("unexpected negotiated chunk size: %d", response.FrameSize)
	}
	if err = <-serverResult; err != nil {
		t.Fatal(err)
	}
}

func TestClientHandshakeRejectsChunkSizeChange(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	message := helloMessage{LegID: 1, FrameSize: 64 * 1024, Destination: "example.com:443"}
	serverResult := make(chan error, 1)
	go func() {
		_, err := readHello(server)
		if err == nil {
			err = writeHelloResponse(server, helloResponse{Status: helloStatusOK, FrameSize: 32 * 1024})
		}
		serverResult <- err
	}()
	outbound := &Outbound{handshakeTimeout: time.Second}
	if err := outbound.clientHandshake(context.Background(), client, message); err == nil {
		t.Fatal("expected changed chunk size to be rejected")
	}
	if err := <-serverResult; err != nil {
		t.Fatal(err)
	}
}

func TestClientHandshakeWaitsForResponse(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	message := helloMessage{LegID: 0, FrameSize: 64 * 1024, Destination: "example.com:443"}
	helloRead := make(chan struct{})
	responseGate := make(chan struct{})
	serverResult := make(chan error, 1)
	go func() {
		received, err := readHello(server)
		if err == nil && received != message {
			err = errors.New("hello mismatch")
		}
		close(helloRead)
		if err == nil {
			<-responseGate
			err = writeHelloResponse(server, helloResponse{Status: helloStatusOK, FrameSize: message.FrameSize, PolicyDigest: message.Policy.digest()})
		}
		serverResult <- err
	}()
	outbound := &Outbound{handshakeTimeout: time.Second}
	clientResult := make(chan error, 1)
	go func() {
		clientResult <- outbound.clientHandshake(context.Background(), client, message)
	}()
	<-helloRead
	select {
	case err := <-clientResult:
		t.Fatalf("client handshake returned before response: %v", err)
	default:
	}
	close(responseGate)
	if err := <-clientResult; err != nil {
		t.Fatal(err)
	}
	if err := <-serverResult; err != nil {
		t.Fatal(err)
	}
}

func TestClientHandshakeSupportsPendingChild(t *testing.T) {
	clientWire, server := net.Pipe()
	client := &pendingHandshakeConn{Conn: clientWire, pending: true}
	defer client.Close()
	defer server.Close()
	message := helloMessage{LegID: 0, FrameSize: 64 * 1024, Destination: "example.com:443"}
	serverResult := make(chan error, 1)
	go func() {
		received, err := readHello(server)
		if err == nil && received != message {
			err = errors.New("hello mismatch")
		}
		if err == nil {
			err = writeHelloResponse(server, helloResponse{Status: helloStatusOK, FrameSize: message.FrameSize, PolicyDigest: message.Policy.digest()})
		}
		serverResult <- err
	}()
	outbound := &Outbound{handshakeTimeout: time.Second}
	if err := outbound.clientHandshake(context.Background(), client, message); err != nil {
		t.Fatal(err)
	}
	if client.pending {
		t.Fatal("child handshake was not completed by the multipath hello")
	}
	if !client.deadlineSetAfterWrite {
		t.Fatal("handshake deadline was not applied after the child first write")
	}
	if err := <-serverResult; err != nil {
		t.Fatal(err)
	}
}

func TestClientHandshakeDoesNotIgnoreInvalidDeadline(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	outbound := &Outbound{handshakeTimeout: time.Second}
	err := outbound.clientHandshake(context.Background(), &invalidDeadlineConn{Conn: client}, helloMessage{
		LegID:       0,
		FrameSize:   64 * 1024,
		Destination: "example.com:443",
	})
	if !errors.Is(err, os.ErrInvalid) {
		t.Fatalf("expected invalid deadline error, got %v", err)
	}
}

func TestHelloRejectsBoosterStatus(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	go func() {
		_ = writeHelloResponse(server, helloResponse{Status: helloStatusRejected, RejectReason: helloRejectSessionUnavailable})
	}()
	if _, err := readHelloResponse(client); err == nil {
		t.Fatal("expected rejected hello response")
	} else if !strings.Contains(err.Error(), "not established yet or is already closed") {
		t.Fatalf("rejection reason missing from error: %v", err)
	} else if reason, loaded := helloRejectReasonFromError(err); !loaded || reason != helloRejectSessionUnavailable {
		t.Fatalf("structured rejection reason missing from error: %v", err)
	}
}

func TestProtocolVersionTwelveHello(t *testing.T) {
	encoded, err := encodeHello(helloMessage{
		LegID:       0,
		FrameSize:   64 * 1024,
		Destination: "example.com:443",
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded[:4]) != "SMPA" || encoded[4] != 12 {
		t.Fatalf("unexpected multipath protocol header: %q version=%d", encoded[:4], encoded[4])
	}
}

func TestWriteHelloResponseRejectsInvalidValues(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	for _, response := range []helloResponse{
		{Status: helloStatusRejected},
		{Status: 2, FrameSize: 64 * 1024},
	} {
		if err := writeHelloResponse(client, response); err == nil {
			t.Fatalf("accepted invalid hello response: %+v", response)
		}
	}
}

func TestReadHelloResponseRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name    string
		version byte
		status  byte
		value   uint32
	}{
		{"v4 response", 4, helloStatusOK, 64 * 1024},
		{"v5 response", 5, helloStatusOK, 64 * 1024},
		{"v6 response", 6, helloStatusOK, 64 * 1024},
		{"v7 response", 7, helloStatusOK, 64 * 1024},
		{"v8 response", 8, helloStatusOK, 64 * 1024},
		{"v9 response", 9, helloStatusOK, 64 * 1024},
		{"v10 response", 10, helloStatusOK, 64 * 1024},
		{"v11 response", 11, helloStatusOK, 64 * 1024},
		{"unknown status", helloVersion, 2, 64 * 1024},
		{"missing rejection reason", helloVersion, helloStatusRejected, 0},
		{"unknown rejection reason", helloVersion, helloStatusRejected, 255},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			var header [responseHeaderSize]byte
			copy(header[0:4], responseMagic[:])
			header[4] = test.version
			header[5] = test.status
			binary.BigEndian.PutUint32(header[6:10], test.value)
			writeDone := make(chan error, 1)
			go func() {
				writeDone <- writeAll(server, header[:])
			}()
			if _, err := readHelloResponse(client); err == nil {
				t.Fatal("accepted invalid hello response")
			}
			client.Close()
			if err := <-writeDone; err != nil && !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, net.ErrClosed) {
				t.Fatal(err)
			}
		})
	}
}

func TestReadHelloRejectsOldProtocolVersions(t *testing.T) {
	for _, version := range []byte{4, 5, 6, 7, 8, 9, 10, 11} {
		header, err := encodeHelloHeader(helloMessage{LegID: 0, FrameSize: 64 * 1024, Destination: "example.com:443"})
		if err != nil {
			t.Fatal(err)
		}
		header[4] = version
		client, server := net.Pipe()
		writeDone := make(chan error, 1)
		go func() { writeDone <- writeAll(client, header[:]) }()
		_, err = readHello(server)
		if err == nil {
			t.Fatalf("accepted protocol version %d", version)
		}
		server.Close()
		if err = <-writeDone; err != nil && !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
		client.Close()
		server.Close()
	}
}

func TestReadHelloRejectsUnknownFlags(t *testing.T) {
	header, err := encodeHelloHeader(helloMessage{
		LegID:       0,
		FrameSize:   64 * 1024,
		Destination: "example.com:443",
	})
	if err != nil {
		t.Fatal(err)
	}
	header[6] = 0x80
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- writeAll(server, append(header[:], "example.com:443"...))
	}()
	if _, err = readHello(client); err == nil {
		t.Fatal("accepted unknown multipath hello flags")
	}
	client.Close()
	if err = <-writeDone; err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
}

func TestSenderStatusRoundTrip(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	expected := senderStatus{
		DataMode: 3, SendBufferLimit: 64 << 20, ReceiveWindowLimit: 128 << 20,
		Sequence:                 42,
		Flags:                    senderStatusFlagActive | senderStatusFlagLeg0Present | senderStatusFlagMemoryPressure,
		LogicalTX:                1000,
		LegTX:                    [2]uint64{400, 600},
		LegTXFrames:              [2]uint64{4, 6},
		LegBacklog:               [2]uint64{100, 200},
		LegWriting:               [2]uint64{10, 20},
		LegWriteBlockedNanos:     [2]uint64{30, 40},
		LegPeakBacklog:           [2]uint64{300, 400},
		SendBufferBytes:          500,
		ReplayPeakBytes:          600,
		FallbackBytes:            700,
		FallbackFrames:           8,
		FallbackEvents:           9,
		ReplayTimeouts:           10,
		BackpressureEvents:       11,
		BackpressureNanos:        12,
		MemoryUsed:               13,
		MemoryPeakUsed:           14,
		MemoryPressureEvents:     15,
		MemoryBackpressureEvents: 16,
		LegFailures:              [2]uint64{17, 18},
		LastFailureLeg:           1,
		LastFailureStage:         senderStatusStageCode(legFailureReplay),
	}
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- writeSenderStatus(server, expected)
	}()
	var frameType [1]byte
	if _, err := io.ReadFull(client, frameType[:]); err != nil {
		t.Fatal(err)
	}
	if frameType[0] != frameTypeSenderStatus {
		t.Fatalf("unexpected frame type: %d", frameType[0])
	}
	actual, err := readSenderStatus(client)
	if err != nil {
		t.Fatal(err)
	}
	if actual != expected {
		t.Fatalf("sender status mismatch:\nactual:   %+v\nexpected: %+v", actual, expected)
	}
	if err = <-writeDone; err != nil {
		t.Fatal(err)
	}
}
