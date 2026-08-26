package multipath

import (
	"net"
	"testing"
)

func TestHelloNegotiatesChunkSize(t *testing.T) {
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

func TestHelloRejectsBoosterStatus(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	go func() {
		_ = writeHelloResponse(server, helloResponse{Status: helloStatusRejected})
	}()
	if _, err := readHelloResponse(client); err == nil {
		t.Fatal("expected rejected hello response")
	}
}
