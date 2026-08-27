package multipath

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	N "github.com/sagernet/sing/common/network"
)

type fastOpenSpyConn struct {
	access    sync.Mutex
	writes    [][]byte
	wrote     bool
	closed    bool
	deadlines []time.Time
}

func (c *fastOpenSpyConn) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (c *fastOpenSpyConn) Write(payload []byte) (int, error) {
	c.access.Lock()
	defer c.access.Unlock()
	if c.closed {
		return 0, net.ErrClosed
	}
	c.wrote = true
	c.writes = append(c.writes, bytes.Clone(payload))
	return len(payload), nil
}

func (c *fastOpenSpyConn) NeedHandshake() bool {
	c.access.Lock()
	defer c.access.Unlock()
	return !c.wrote
}

func (c *fastOpenSpyConn) Close() error {
	c.access.Lock()
	c.closed = true
	c.access.Unlock()
	return nil
}

func (c *fastOpenSpyConn) LocalAddr() net.Addr  { return testAddr("local") }
func (c *fastOpenSpyConn) RemoteAddr() net.Addr { return testAddr("remote") }

func (c *fastOpenSpyConn) SetDeadline(deadline time.Time) error {
	c.access.Lock()
	defer c.access.Unlock()
	if !c.wrote {
		return os.ErrInvalid
	}
	c.deadlines = append(c.deadlines, deadline)
	return nil
}

func (c *fastOpenSpyConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fastOpenSpyConn) SetWriteDeadline(time.Time) error { return nil }

func (c *fastOpenSpyConn) snapshotWrites() [][]byte {
	c.access.Lock()
	defer c.access.Unlock()
	writes := make([][]byte, len(c.writes))
	for index, payload := range c.writes {
		writes[index] = bytes.Clone(payload)
	}
	return writes
}

type testAddr string

func (a testAddr) Network() string { return string(a) }
func (a testAddr) String() string  { return string(a) }

func TestClientFastOpenCombinesHelloAndFirstFrame(t *testing.T) {
	message := helloMessage{
		Session:     [16]byte{1, 2, 3, 4},
		LegID:       0,
		ChunkSize:   64 * 1024,
		Destination: "1.1.1.1:80",
	}
	payload := []byte("GET / HTTP/1.1\r\nHost: 1.1.1.1\r\n\r\n")
	frame := wireFrame{typ: frameTypeData, seq: 0, data: payload}
	spy := new(fastOpenSpyConn)
	deadline := time.Now().Add(time.Second)
	conn, err := newClientFastOpenConn(spy, message, deadline)
	if err != nil {
		t.Fatal(err)
	}
	if err = writeWireFrame(conn, frame); err != nil {
		t.Fatal(err)
	}
	hello, err := encodeHello(message)
	if err != nil {
		t.Fatal(err)
	}
	encodedFrame, err := encodeWireFrame(frame)
	if err != nil {
		t.Fatal(err)
	}
	writes := spy.snapshotWrites()
	if len(writes) != 1 {
		t.Fatalf("expected one physical write, got %d", len(writes))
	}
	expected := append(bytes.Clone(hello), encodedFrame...)
	if !bytes.Equal(writes[0], expected) {
		t.Fatal("first physical write does not contain the complete hello and data frame")
	}
	if conn.NeedHandshakeForWrite() {
		t.Fatal("fast-open handshake still pending after first frame")
	}
	if len(spy.deadlines) != 1 {
		t.Fatalf("expected deadline to be applied after lazy connect, got %d", len(spy.deadlines))
	}
	if !spy.deadlines[0].Equal(deadline) {
		t.Fatal("fast-open handshake did not retain the caller's deadline")
	}
}

func TestClientFastOpenSupportsOpaqueLazyChild(t *testing.T) {
	message := helloMessage{
		Session:     [16]byte{5, 6, 7, 8},
		LegID:       0,
		ChunkSize:   64 * 1024,
		Destination: "1.1.1.1:443",
	}
	spy := new(fastOpenSpyConn)
	child := struct{ net.Conn }{Conn: spy}
	if N.NeedHandshakeForWrite(&child) {
		t.Fatal("opaque child unexpectedly exposes its lazy-connect state")
	}
	deadline := time.Now().Add(time.Second)
	conn, err := newClientFastOpenConn(&child, message, deadline)
	if err != nil {
		t.Fatal(err)
	}
	if err = writeWireFrame(conn, wireFrame{typ: frameTypeData, data: []byte("payload")}); err != nil {
		t.Fatal(err)
	}
	if writes := spy.snapshotWrites(); len(writes) != 1 {
		t.Fatalf("expected one physical write, got %d", len(writes))
	}
	if len(spy.deadlines) != 1 || !spy.deadlines[0].Equal(deadline) {
		t.Fatal("handshake deadline was not applied after the opaque child first write")
	}
}

func TestWriteHelloWithoutMultipathTFOUsesSeparateWrites(t *testing.T) {
	message := helloMessage{
		Session:     [16]byte{2, 4, 6, 8},
		LegID:       0,
		ChunkSize:   64 * 1024,
		Destination: "example.com:443",
	}
	spy := new(fastOpenSpyConn)
	if err := writeHello(spy, message); err != nil {
		t.Fatal(err)
	}
	writes := spy.snapshotWrites()
	if len(writes) != 2 {
		t.Fatalf("non-fast-open hello path changed its physical write count: got %d", len(writes))
	}
	hello, err := encodeHello(message)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bytes.Join(writes, nil), hello) {
		t.Fatal("non-fast-open hello path changed its wire representation")
	}
}

func TestClientFastOpenEmptyWriteSendsHelloOnly(t *testing.T) {
	message := helloMessage{
		Session:     [16]byte{9, 8, 7, 6},
		LegID:       0,
		ChunkSize:   64 * 1024,
		Destination: "example.com:443",
	}
	spy := new(fastOpenSpyConn)
	conn, err := newClientFastOpenConn(spy, message, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err = conn.writeHelloOnly(); err != nil {
		t.Fatal(err)
	}
	hello, err := encodeHello(message)
	if err != nil {
		t.Fatal(err)
	}
	writes := spy.snapshotWrites()
	if len(writes) != 1 || !bytes.Equal(writes[0], hello) {
		t.Fatal("empty early write did not send exactly one complete hello")
	}
}

func TestClientFastOpenCloseBeforeWriteUnblocksHandshake(t *testing.T) {
	message := helloMessage{
		Session:     [16]byte{4, 3, 2, 1},
		LegID:       0,
		ChunkSize:   64 * 1024,
		Destination: "example.com:80",
	}
	conn, err := newClientFastOpenConn(new(fastOpenSpyConn), message, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err = conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err = conn.waitStarted(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("expected closed handshake, got %v", err)
	}
}

func TestEarlyLogicalConnSendsPayloadWithHello(t *testing.T) {
	cfg := testCoreConfig()
	cfg.ActivationAfterBytes = 1 << 20
	clientCore, clientApp := newCore(context.Background(), cfg)
	serverCore, _ := newCore(context.Background(), cfg)
	defer clientCore.Close()
	defer serverCore.Close()

	clientWire, serverWire := net.Pipe()
	defer serverWire.Close()
	message := helloMessage{
		Session:     [16]byte{1, 3, 3, 7},
		LegID:       0,
		ChunkSize:   uint32(cfg.ChunkSize),
		Destination: "example.com:443",
	}
	fastOpenConn, err := newClientFastOpenConn(clientWire, message, time.Now().Add(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	readResponse := func(conn net.Conn) error {
		if err := fastOpenConn.waitStarted(); err != nil {
			return err
		}
		response, err := readHelloResponse(conn)
		if err != nil {
			return err
		}
		if response.ChunkSize != message.ChunkSize {
			return errors.New("unexpected response chunk size")
		}
		return conn.SetDeadline(time.Time{})
	}
	if _, err = clientCore.addLegWithReadPreamble(0, fastOpenConn, nil, readResponse); err != nil {
		t.Fatal(err)
	}
	earlyConn := &earlyLogicalConn{Conn: clientApp, core: clientCore, primary: fastOpenConn}
	if !N.NeedHandshakeForWrite(earlyConn) {
		t.Fatal("early logical connection did not advertise its pending handshake")
	}

	payload := []byte("early application payload")
	serverResult := make(chan error, 1)
	go func() {
		receivedHello, readErr := readHello(serverWire)
		if readErr == nil && receivedHello != message {
			readErr = errors.New("unexpected hello")
		}
		var frame wireFrame
		if readErr == nil {
			frame, readErr = readWireFrame(serverWire, serverCore)
		}
		if readErr == nil && (frame.typ != frameTypeData || frame.seq != 0 || !bytes.Equal(frame.data, payload)) {
			readErr = errors.New("unexpected early data frame")
		}
		if len(frame.data) > 0 {
			serverCore.putBuffer(frame.data)
		}
		if readErr == nil {
			readErr = writeHelloResponse(serverWire, helloResponse{Status: helloStatusOK, ChunkSize: message.ChunkSize})
		}
		serverResult <- readErr
	}()

	if _, err = earlyConn.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err = <-serverResult; err != nil {
		t.Fatal(err)
	}
	if N.NeedHandshakeForWrite(earlyConn) {
		t.Fatal("early logical connection still reports a pending handshake")
	}
}

func TestEarlyLogicalConnRejectClosesConnection(t *testing.T) {
	cfg := testCoreConfig()
	clientCore, clientApp := newCore(context.Background(), cfg)
	serverCore, _ := newCore(context.Background(), cfg)
	defer clientCore.Close()
	defer serverCore.Close()

	clientWire, serverWire := net.Pipe()
	defer serverWire.Close()
	message := helloMessage{
		Session:     [16]byte{8, 6, 4, 2},
		LegID:       0,
		ChunkSize:   uint32(cfg.ChunkSize),
		Destination: "example.com:80",
	}
	fastOpenConn, err := newClientFastOpenConn(clientWire, message, time.Now().Add(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	readResponse := func(conn net.Conn) error {
		if err := fastOpenConn.waitStarted(); err != nil {
			return err
		}
		_, err := readHelloResponse(conn)
		return err
	}
	if _, err = clientCore.addLegWithReadPreamble(0, fastOpenConn, nil, readResponse); err != nil {
		t.Fatal(err)
	}
	earlyConn := &earlyLogicalConn{Conn: clientApp, core: clientCore, primary: fastOpenConn}

	serverResult := make(chan error, 1)
	go func() {
		receivedHello, readErr := readHello(serverWire)
		if readErr == nil && receivedHello != message {
			readErr = errors.New("unexpected hello")
		}
		var frame wireFrame
		if readErr == nil {
			frame, readErr = readWireFrame(serverWire, serverCore)
		}
		if len(frame.data) > 0 {
			serverCore.putBuffer(frame.data)
		}
		if readErr == nil {
			readErr = writeHelloResponse(serverWire, helloResponse{
				Status:       helloStatusRejected,
				RejectReason: helloRejectInvalidDestination,
			})
		}
		serverResult <- readErr
	}()

	if _, err = earlyConn.Write([]byte("must be rejected")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-clientCore.Done():
	case <-time.After(time.Second):
		t.Fatal("rejected early-write handshake did not close the logical connection")
	}
	if err = <-serverResult; err != nil {
		t.Fatal(err)
	}
	if _, err = earlyConn.Write([]byte("must fail")); err == nil {
		t.Fatal("write succeeded after the early-write handshake was rejected")
	}
}

func TestClientHandshakeDeadlineUsesContext(t *testing.T) {
	contextDeadline := time.Now().Add(time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), contextDeadline)
	defer cancel()
	outbound := &Outbound{handshakeTimeout: 10 * time.Second}
	if deadline := outbound.clientHandshakeDeadline(ctx); !deadline.Equal(contextDeadline) {
		t.Fatalf("expected context deadline %v, got %v", contextDeadline, deadline)
	}
}

func TestEarlyLogicalConnPreservesHalfClose(t *testing.T) {
	core, appConn := newCore(context.Background(), testCoreConfig())
	defer core.Close()
	earlyConn := &earlyLogicalConn{Conn: appConn, core: core}
	if _, loaded := any(earlyConn).(N.ReadCloser); !loaded {
		t.Fatal("early logical connection lost CloseRead support")
	}
	if _, loaded := any(earlyConn).(N.WriteCloser); !loaded {
		t.Fatal("early logical connection lost CloseWrite support")
	}
}
