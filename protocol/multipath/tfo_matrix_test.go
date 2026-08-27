package multipath

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	L "github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type tfoMatrixChild struct {
	tag  string
	dial func(context.Context) (net.Conn, error)
}

func (c *tfoMatrixChild) Type() string           { return "test" }
func (c *tfoMatrixChild) Tag() string            { return c.tag }
func (c *tfoMatrixChild) Network() []string      { return []string{N.NetworkTCP} }
func (c *tfoMatrixChild) Dependencies() []string { return nil }

func (c *tfoMatrixChild) DialContext(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
	return c.dial(ctx)
}

func (c *tfoMatrixChild) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("packet connection is not supported")
}

type tfoMatrixConn struct {
	net.Conn
	access    sync.Mutex
	lazy      bool
	pending   bool
	writes    [][]byte
	deadlines []time.Time
}

func newTFOMatrixConn(conn net.Conn, lazy bool) *tfoMatrixConn {
	return &tfoMatrixConn{Conn: conn, lazy: lazy, pending: lazy}
}

func (c *tfoMatrixConn) NeedHandshake() bool {
	c.access.Lock()
	defer c.access.Unlock()
	return c.pending
}

func (c *tfoMatrixConn) Write(payload []byte) (int, error) {
	c.access.Lock()
	c.pending = false
	c.writes = append(c.writes, bytes.Clone(payload))
	c.access.Unlock()
	return c.Conn.Write(payload)
}

func (c *tfoMatrixConn) SetDeadline(deadline time.Time) error {
	c.access.Lock()
	if c.pending {
		c.access.Unlock()
		return os.ErrInvalid
	}
	c.deadlines = append(c.deadlines, deadline)
	c.access.Unlock()
	return c.Conn.SetDeadline(deadline)
}

func (c *tfoMatrixConn) snapshot() ([][]byte, []time.Time) {
	c.access.Lock()
	defer c.access.Unlock()
	writes := make([][]byte, len(c.writes))
	for index, payload := range c.writes {
		writes[index] = bytes.Clone(payload)
	}
	return writes, append([]time.Time(nil), c.deadlines...)
}

func TestTFOCombinationMatrix(t *testing.T) {
	for _, multipathTFO := range []bool{false, true} {
		for _, childTFO := range []bool{false, true} {
			name := fmt.Sprintf("multipath=%t/child=%t", multipathTFO, childTFO)
			t.Run(name, func(t *testing.T) {
				testTFOCombination(t, multipathTFO, childTFO)
			})
		}
	}
}

func testTFOCombination(t *testing.T, multipathTFO, childTFO bool) {
	t.Helper()
	clientWire, serverWire := net.Pipe()
	clientConn := newTFOMatrixConn(clientWire, childTFO)
	payload := []byte("matrix payload")
	messageResult := make(chan helloMessage, 1)
	serverResult := make(chan error, 1)
	cfg := testCoreConfig()
	cfg.ActivationAfterBytes = 1 << 20
	serverCore, serverApp := newCore(context.Background(), cfg)
	t.Cleanup(func() {
		serverApp.Close()
		serverCore.Close()
		serverWire.Close()
	})

	readFrame := func() error {
		frame, err := readWireFrame(serverWire, serverCore)
		if err != nil {
			return err
		}
		defer serverCore.putBuffer(frame.data)
		if frame.typ != frameTypeData || frame.seq != 0 || !bytes.Equal(frame.data, payload) {
			return errors.New("unexpected first multipath data frame")
		}
		return nil
	}
	go func() {
		message, err := readHello(serverWire)
		if err == nil {
			messageResult <- message
		}
		if err == nil && multipathTFO {
			err = readFrame()
		}
		if err == nil {
			err = writeHelloResponse(serverWire, helloResponse{Status: helloStatusOK, ChunkSize: message.ChunkSize})
		}
		if err == nil && !multipathTFO {
			err = readFrame()
		}
		serverResult <- err
	}()

	primary := &tfoMatrixChild{
		tag: "primary",
		dial: func(context.Context) (net.Conn, error) {
			return clientConn, nil
		},
	}
	secondary := &tfoMatrixChild{
		tag: "secondary",
		dial: func(ctx context.Context) (net.Conn, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	outbound := &Outbound{
		ctx:              context.Background(),
		logger:           L.NOP(),
		tags:             []string{primary.tag, secondary.tag},
		children:         []adapter.Outbound{primary, secondary},
		tcpFastOpen:      multipathTFO,
		cfg:              cfg,
		handshakeTimeout: time.Second,
	}
	destination := M.ParseSocksaddr("example.com:443")
	logical, err := outbound.DialContext(context.Background(), N.NetworkTCP, destination)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { logical.Close() })
	if _, err = logical.Write(payload); err != nil {
		t.Fatal(err)
	}

	select {
	case err = <-serverResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the multipath exchange")
	}
	message := <-messageResult
	if message.Destination != destination.String() || message.LegID != 0 || message.ChunkSize != uint32(cfg.ChunkSize) {
		t.Fatalf("unexpected multipath hello: %#v", message)
	}
	if N.NeedHandshakeForWrite(logical) {
		t.Fatal("logical connection still reports a pending handshake")
	}

	hello, err := encodeHello(message)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := encodeWireFrame(wireFrame{typ: frameTypeData, data: payload})
	if err != nil {
		t.Fatal(err)
	}
	writes, deadlines := clientConn.snapshot()
	if !bytes.Equal(bytes.Join(writes, nil), append(hello, frame...)) {
		t.Fatal("physical writes do not contain exactly the hello and first data frame")
	}
	if multipathTFO && len(writes) != 1 {
		t.Fatalf("multipath TFO did not combine the hello and first frame: %d writes", len(writes))
	}
	if !multipathTFO && len(writes) <= 1 {
		t.Fatal("disabled multipath TFO unexpectedly combined the hello and first frame")
	}
	if len(deadlines) == 0 {
		t.Fatal("handshake deadline was never applied")
	}
}
