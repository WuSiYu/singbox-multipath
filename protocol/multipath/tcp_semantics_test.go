package multipath

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestApplicationReceivesResetError(t *testing.T) {
	for _, cause := range []error{errors.New("test peer reset"), io.EOF} {
		t.Run(cause.Error(), func(t *testing.T) {
			core, app := newCore(context.Background(), flowTestConfig())
			defer core.Close()
			core.peerSessionClosed(cause)
			_, err := app.Read(make([]byte, 1))
			want := cause
			if cause == io.EOF {
				want = io.ErrUnexpectedEOF // No FIN was received.
			}
			if !errors.Is(err, want) {
				t.Fatalf("Read lost abnormal close reason: got %v, want %v", err, want)
			}
		})
	}
}

func TestEarlyWriteDeadline(t *testing.T) {
	for _, size := range []int{0, 1} {
		for _, update := range []bool{false, true} {
			t.Run(fmt.Sprintf("bytes=%d/update=%t", size, update), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					core, app := newCore(context.Background(), flowTestConfig())
					defer core.Close()
					a, b := net.Pipe()
					defer b.Close()
					primary, err := newClientFastOpenConn(a, helloMessage{FrameSize: 1024, Destination: "example.com:80"}, time.Now().Add(5*time.Second))
					if err != nil {
						t.Fatal(err)
					}
					if _, err = core.addLegWithReadPreamble(0, primary, nil, func(net.Conn) error { return primary.waitStarted() }); err != nil {
						t.Fatal(err)
					}
					conn := &earlyLogicalConn{Conn: app, core: core, primary: primary}
					if !update {
						_ = conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
					}
					result := make(chan error, 1)
					go func() { _, writeErr := conn.Write(make([]byte, size)); result <- writeErr }()
					if update {
						time.Sleep(50 * time.Millisecond)
						_ = conn.SetDeadline(time.Now().Add(50 * time.Millisecond))
					}
					time.Sleep(150 * time.Millisecond)
					select {
					case err = <-result:
						if !errors.Is(err, os.ErrDeadlineExceeded) {
							t.Fatalf("Write: %v", err)
						}
					default:
						t.Fatal("first TFO write ignored application deadline")
					}
					if core.isDone() {
						t.Fatal("application timeout terminated the transport")
					}
					b.Close()
					core.Close()
					waitCoreRelease(t, core)
				})
			})
		}
	}
}

func TestDeadlineRecoveryPreservesStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := flowTestConfig()
		cfg.ReceiveWindowBytes = int64(cfg.FrameSize)
		cfg.SendBufferBytes = 8 << 10
		left, app := newCore(context.Background(), cfg)
		right, peer := newCore(context.Background(), cfg)
		defer closeFlowCores(left, right)
		a, b := net.Pipe()
		connectTestLeg(t, left, right, 0, a, b)
		_ = app.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		if _, err := app.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("Read: %v", err)
		}
		_ = app.SetReadDeadline(time.Time{})
		payload := auditStream(916, 16<<10)
		_ = app.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
		n, err := app.Write(payload)
		if n <= 0 || n >= len(payload) || !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("partial Write: %d %v", n, err)
		}
		_ = app.SetWriteDeadline(time.Time{})
		written := flowSend(app, payload[n:], true)
		got, err := io.ReadAll(peer)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("resumed stream: %d %v", len(got), err)
		}
		if err = <-written; err != nil {
			t.Fatal(err)
		}
		response := flowSend(peer, []byte("still alive"), true)
		got, err = io.ReadAll(app)
		if err != nil || string(got) != "still alive" {
			t.Fatalf("resumed Read: %q %v", got, err)
		}
		if err = <-response; err != nil {
			t.Fatal(err)
		}
	})
}

func TestEarlyWriteDeadlineExtensionAndClose(t *testing.T) {
	for _, action := range []string{"extend", "clear", "close"} {
		for _, size := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s/bytes=%d", action, size), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					core, app := newCore(context.Background(), flowTestConfig())
					defer core.Close()
					a, b := net.Pipe()
					defer b.Close()
					primary, err := newClientFastOpenConn(a, helloMessage{FrameSize: 1024, Destination: "example.com:80"}, time.Now().Add(5*time.Second))
					if err != nil {
						t.Fatal(err)
					}
					if _, err = core.addLegWithReadPreamble(0, primary, nil, func(net.Conn) error { return primary.waitStarted() }); err != nil {
						t.Fatal(err)
					}
					conn := &earlyLogicalConn{Conn: app, core: core, primary: primary}
					_ = conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
					result := make(chan error, 1)
					go func() { _, writeErr := conn.Write(make([]byte, size)); result <- writeErr }()
					time.Sleep(50 * time.Millisecond)
					if action == "close" {
						conn.Close()
						if err = <-result; !errors.Is(err, net.ErrClosed) {
							t.Fatalf("Close did not interrupt Write: %v", err)
						}
					} else {
						deadline := time.Time{}
						if action == "extend" {
							deadline = time.Now().Add(250 * time.Millisecond)
						}
						_ = conn.SetWriteDeadline(deadline)
						time.Sleep(100 * time.Millisecond)
						select {
						case err = <-result:
							t.Fatalf("old deadline fired: %v", err)
						default:
						}
						go io.Copy(io.Discard, b)
						if err = <-result; err != nil {
							t.Fatal(err)
						}
					}
					b.Close()
					core.Close()
					waitCoreRelease(t, core)
				})
			})
		}
	}
}

func TestLocalCloseKeepsClosedError(t *testing.T) {
	core, app := newCore(context.Background(), flowTestConfig())
	defer core.Close()
	app.Close()
	if _, err := app.Read(make([]byte, 1)); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Read: %v", err)
	}
	if _, err := app.Write([]byte{1}); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Write: %v", err)
	}
}

func TestConcurrentWritesPreserveRecords(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		left, app := newCore(context.Background(), flowTestConfig())
		right, peer := newCore(context.Background(), flowTestConfig())
		defer closeFlowCores(left, right)
		a, b := net.Pipe()
		connectTestLeg(t, left, right, 0, a, b)
		const count, size = 16, 4096
		var writers sync.WaitGroup
		results := make(chan error, count)
		for id := 0; id < count; id++ {
			writers.Add(1)
			go func() {
				defer writers.Done()
				_, err := app.Write(bytes.Repeat([]byte{byte(id)}, size))
				results <- err
			}()
		}
		go func() { writers.Wait(); app.(closeWriter).CloseWrite() }()
		got, err := io.ReadAll(peer)
		if err != nil || len(got) != count*size {
			t.Fatalf("Read: %d %v", len(got), err)
		}
		seen := make(map[byte]bool)
		for offset := 0; offset < len(got); offset += size {
			id := got[offset]
			if seen[id] || !bytes.Equal(got[offset:offset+size], bytes.Repeat([]byte{id}, size)) {
				t.Fatal("concurrent writes interleaved or repeated")
			}
			seen[id] = true
		}
		for range count {
			if err = <-results; err != nil {
				t.Fatal(err)
			}
		}
	})
}
