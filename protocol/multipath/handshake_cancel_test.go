package multipath

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"testing/synctest"
	"time"
)

func TestClientHandshakeCancellationInterruptsIO(t *testing.T) {
	for _, blockedResponse := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			a, b := net.Pipe()
			defer a.Close()
			defer b.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if blockedResponse {
				go func() { _, _ = readHello(b) }()
			}
			o := &Outbound{handshakeTimeout: time.Minute}
			done := make(chan error, 1)
			go func() {
				done <- o.clientHandshake(ctx, a, helloMessage{FrameSize: 65536, Destination: "example.com:443"})
			}()
			synctest.Wait() // hello write or response read is blocked
			cancel()
			synctest.Wait()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancel: %v", err)
				}
			default:
				t.Fatal("handshake outlived its owner until the socket deadline")
			}
		})
	}
}

func TestClientHandshakeDetachesCancellationOnSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := net.Pipe()
		defer a.Close()
		defer b.Close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		message := helloMessage{FrameSize: 65536, Destination: "example.com:443"}
		done := make(chan error, 1)
		go func() {
			_, err := readHello(b)
			if err == nil {
				err = writeHelloResponse(b, helloResponse{Status: helloStatusOK, FrameSize: message.FrameSize, PolicyDigest: message.Policy.digest()})
			}
			if err == nil {
				var p [1]byte
				_, err = io.ReadFull(b, p[:])
				if p[0] != 42 {
					err = errors.New("payload changed")
				}
			}
			done <- err
		}()
		o := &Outbound{handshakeTimeout: time.Minute}
		if err := o.clientHandshake(ctx, a, message); err != nil {
			t.Fatal(err)
		}
		cancel()
		synctest.Wait()
		if _, err := a.Write([]byte{42}); err != nil {
			t.Fatal("canceled completed handshake closed transport", err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestCoreReleaseWaitsForPendingHandshake(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		core, _ := newCore(context.Background(), flowTestConfig())
		a, b := net.Pipe()
		defer a.Close()
		defer b.Close()
		done := make(chan error, 1)
		o := &Outbound{handshakeTimeout: time.Minute}
		core.startWorkers(func() {
			done <- o.clientHandshake(core.ctx, a, helloMessage{FrameSize: 65536, Destination: "example.com:443"})
		})
		synctest.Wait()
		core.Close()
		<-core.released
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		default:
			t.Fatal("session reservation was released before its join worker")
		}
	})
}
