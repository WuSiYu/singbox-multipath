package multipath

import (
	"context"
	"encoding/binary"
	"errors"
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
		LegID:       0,
		ChunkSize:   64 * 1024,
		Destination: "example.com:443",
	}
	serverResult := make(chan error, 1)
	go func() {
		received, err := readHello(server)
		if err == nil && received != message {
			t.Errorf("hello mismatch: %#v != %#v", received, message)
		}
		if err == nil {
			err = writeHelloResponse(server, helloResponse{Status: helloStatusOK, ChunkSize: 32 * 1024})
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
	if response.ChunkSize != 32*1024 {
		t.Fatalf("unexpected negotiated chunk size: %d", response.ChunkSize)
	}
	if err = <-serverResult; err != nil {
		t.Fatal(err)
	}
}

func TestClientHandshakeRejectsChunkSizeChange(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	message := helloMessage{LegID: 1, ChunkSize: 64 * 1024, Destination: "example.com:443"}
	serverResult := make(chan error, 1)
	go func() {
		_, err := readHello(server)
		if err == nil {
			err = writeHelloResponse(server, helloResponse{Status: helloStatusOK, ChunkSize: 32 * 1024})
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
	message := helloMessage{LegID: 0, ChunkSize: 64 * 1024, Destination: "example.com:443"}
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
			err = writeHelloResponse(server, helloResponse{Status: helloStatusOK, ChunkSize: message.ChunkSize})
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
	message := helloMessage{LegID: 0, ChunkSize: 64 * 1024, Destination: "example.com:443"}
	serverResult := make(chan error, 1)
	go func() {
		received, err := readHello(server)
		if err == nil && received != message {
			err = errors.New("hello mismatch")
		}
		if err == nil {
			err = writeHelloResponse(server, helloResponse{Status: helloStatusOK, ChunkSize: message.ChunkSize})
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
		ChunkSize:   64 * 1024,
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

func TestProtocolVersionFourHello(t *testing.T) {
	encoded, err := encodeHello(helloMessage{
		LegID:       0,
		ChunkSize:   64 * 1024,
		Destination: "example.com:443",
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded[:4]) != "SMP4" || encoded[4] != 4 {
		t.Fatalf("unexpected multipath protocol header: %q version=%d", encoded[:4], encoded[4])
	}
}

func TestWriteHelloResponseRejectsInvalidV4Values(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	for _, response := range []helloResponse{
		{Status: helloStatusRejected},
		{Status: 2, ChunkSize: 64 * 1024},
	} {
		if err := writeHelloResponse(client, response); err == nil {
			t.Fatalf("accepted invalid v4 hello response: %+v", response)
		}
	}
}

func TestReadHelloResponseRejectsInvalidV4Values(t *testing.T) {
	tests := []struct {
		name    string
		version byte
		status  byte
		value   uint32
	}{
		{"v3 response", 3, helloStatusOK, 64 * 1024},
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
				t.Fatal("accepted invalid v4 hello response")
			}
			if err := <-writeDone; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReadHelloRejectsProtocolVersionThree(t *testing.T) {
	header, err := encodeHelloHeader(helloMessage{
		LegID:       0,
		ChunkSize:   64 * 1024,
		Destination: "example.com:443",
	})
	if err != nil {
		t.Fatal(err)
	}
	copy(header[0:4], []byte("SMP3"))
	header[4] = 3
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- writeAll(client, header[:])
	}()
	if _, err = readHello(server); err == nil {
		t.Fatal("accepted a v3 multipath hello")
	}
	if err = <-writeDone; err != nil {
		t.Fatal(err)
	}
}
