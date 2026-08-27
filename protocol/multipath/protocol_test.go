package multipath

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

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

func TestHelloRejectsBoosterStatus(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	go func() {
		_ = writeHelloResponse(server, helloResponse{Status: helloStatusRejected, RejectReason: helloRejectSessionNotFound})
	}()
	if _, err := readHelloResponse(client); err == nil {
		t.Fatal("expected rejected hello response")
	} else if !strings.Contains(err.Error(), "session no longer exists") {
		t.Fatalf("rejection reason missing from error: %v", err)
	}
}

func TestHelloRejectWithoutReasonRemainsCompatible(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	go func() {
		_ = writeHelloResponse(server, helloResponse{Status: helloStatusRejected})
	}()
	if _, err := readHelloResponse(client); err == nil {
		t.Fatal("expected rejected hello response")
	} else if !strings.Contains(err.Error(), "unspecified by server") {
		t.Fatalf("unexpected legacy rejection error: %v", err)
	}
}
