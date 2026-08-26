package multipath

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type closeWriter interface {
	CloseWrite() error
}

type delayedWriteConn struct {
	net.Conn
	delay time.Duration
}

type countingConn struct {
	net.Conn
	written atomic.Int64
}

type stallingConn struct {
	net.Conn
	started    chan struct{}
	release    chan struct{}
	startOne   sync.Once
	releaseOne sync.Once
}

func newStallingConn(conn net.Conn) *stallingConn {
	return &stallingConn{Conn: conn, started: make(chan struct{}), release: make(chan struct{})}
}

func (c *stallingConn) Write([]byte) (int, error) {
	c.startOne.Do(func() { close(c.started) })
	<-c.release
	return 0, net.ErrClosed
}

func (c *stallingConn) Close() error {
	c.releaseOne.Do(func() { close(c.release) })
	return c.Conn.Close()
}

func (c *countingConn) Write(buffer []byte) (int, error) {
	n, err := c.Conn.Write(buffer)
	c.written.Add(int64(n))
	return n, err
}

func (c *delayedWriteConn) Write(buffer []byte) (int, error) {
	time.Sleep(c.delay)
	return c.Conn.Write(buffer)
}

func testCoreConfig() coreConfig {
	return coreConfig{
		ChunkSize:        4 * 1024,
		QueueFrames:      64,
		QueueBytes:       256 * 1024,
		BandwidthMbps:    []uint32{1, 16},
		MaxReorderFrames: 4096,
		MaxReorderBytes:  16 << 20,
		ReplayBytes:      16 << 20,
		ReplayTimeout:    time.Second,
	}
}

func connectTestLeg(t *testing.T, left, right *mpCore, id uint8, leftConn, rightConn net.Conn) (*mpLeg, *mpLeg) {
	t.Helper()
	leftLeg, err := left.addLeg(id, leftConn, nil)
	if err != nil {
		t.Fatal(err)
	}
	rightLeg, err := right.addLeg(id, rightConn, nil)
	if err != nil {
		t.Fatal(err)
	}
	return leftLeg, rightLeg
}

func closeTestWrite(t *testing.T, conn net.Conn) {
	t.Helper()
	writer, loaded := conn.(closeWriter)
	if !loaded {
		t.Fatal("logical connection does not implement CloseWrite")
	}
	if err := writer.CloseWrite(); err != nil {
		t.Fatal(err)
	}
}

type leg1ActiveEvent struct {
	info      activationInfo
	reconnect bool
}

func TestCoreLeg1ActiveNotification(t *testing.T) {
	cfg := testCoreConfig()
	cfg.ActivationAfterBytes = 1
	events := make(chan leg1ActiveEvent, 3)
	cfg.OnLeg1Active = func(info activationInfo, reconnect bool) {
		events <- leg1ActiveEvent{info: info, reconnect: reconnect}
	}
	core, _ := newCore(context.Background(), cfg)
	defer core.Close()

	firstCoreConn, firstPeerConn := net.Pipe()
	firstLeg, err := core.addLeg(1, firstCoreConn, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer firstPeerConn.Close()
	select {
	case <-events:
		t.Fatal("leg1 was reported before activation")
	default:
	}

	trigger := activationInfo{
		Reason:           activationReasonThroughput,
		WindowBytes:      16 << 20,
		RateBytesPS:      20_000_000,
		ThresholdBytesPS: 15_000_000,
		Elapsed:          time.Second,
	}
	core.activate(trigger)
	firstEvent := <-events
	if firstEvent.info != trigger || firstEvent.reconnect {
		t.Fatalf("unexpected first leg1 event: %+v", firstEvent)
	}
	core.notifyLeg1Active()
	select {
	case event := <-events:
		t.Fatalf("duplicate leg1 notification: %+v", event)
	default:
	}

	core.legFailed(firstLeg, net.ErrClosed)
	secondCoreConn, secondPeerConn := net.Pipe()
	defer secondPeerConn.Close()
	if _, err = core.addLeg(1, secondCoreConn, nil); err != nil {
		t.Fatal(err)
	}
	secondEvent := <-events
	if secondEvent.info != trigger || !secondEvent.reconnect {
		t.Fatalf("unexpected reconnected leg1 event: %+v", secondEvent)
	}
}

func TestCoreLeg1ActiveNotificationWaitsForLeg(t *testing.T) {
	cfg := testCoreConfig()
	cfg.ActivationAfterBytes = 1
	events := make(chan leg1ActiveEvent, 1)
	cfg.OnLeg1Active = func(info activationInfo, reconnect bool) {
		events <- leg1ActiveEvent{info: info, reconnect: reconnect}
	}
	core, _ := newCore(context.Background(), cfg)
	defer core.Close()
	trigger := activationInfo{
		Reason:         activationReasonBytes,
		CurrentBytes:   4096,
		ThresholdBytes: 1024,
	}
	core.activate(trigger)
	select {
	case <-events:
		t.Fatal("leg1 was reported before it joined")
	default:
	}

	coreConn, peerConn := net.Pipe()
	defer peerConn.Close()
	if _, err := core.addLeg(1, coreConn, nil); err != nil {
		t.Fatal(err)
	}
	event := <-events
	if event.info != trigger || event.reconnect {
		t.Fatalf("unexpected delayed leg1 event: %+v", event)
	}
}

func TestActivationInfoString(t *testing.T) {
	tests := []struct {
		name     string
		info     activationInfo
		expected string
	}{
		{
			name: "bytes",
			info: activationInfo{
				Reason:         activationReasonBytes,
				CurrentBytes:   2048,
				ThresholdBytes: 1024,
			},
			expected: "reason=bytes current_bytes=2048 threshold_bytes=1024",
		},
		{
			name: "throughput",
			info: activationInfo{
				Reason:           activationReasonThroughput,
				WindowBytes:      16 << 20,
				RateBytesPS:      17_100_000,
				ThresholdBytesPS: 15_000_000,
				Elapsed:          time.Second,
			},
			expected: "reason=throughput measured_mbps=136.80 threshold_mbps=120.00 window=1s window_bytes=16777216",
		},
		{
			name: "leg0 queue",
			info: activationInfo{
				Reason:           activationReasonLeg0Queue,
				BacklogBytes:     13 << 20,
				QueueBytes:       16 << 20,
				Elapsed:          time.Second,
				RequiredDuration: time.Second,
			},
			expected: "reason=leg0_queue backlog_bytes=13631488 queue_bytes=16777216 ratio=81.2% duration=1s required_duration=1s",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if actual := test.info.String(); actual != test.expected {
				t.Fatalf("unexpected activation info: %q", actual)
			}
		})
	}
}

func TestCoreHalfClosePreservesTail(t *testing.T) {
	left, leftApp := newCore(context.Background(), testCoreConfig())
	right, rightApp := newCore(context.Background(), testCoreConfig())
	defer left.Close()
	defer right.Close()
	leg0Left, leg0Right := net.Pipe()
	connectTestLeg(t, left, right, 0, leg0Left, leg0Right)
	leg1Left, leg1Right := net.Pipe()
	connectTestLeg(t, left, right, 1, leg1Left, leg1Right)

	payload := bytes.Repeat([]byte("multipath-tail-"), 512*1024)
	writeResult := make(chan error, 1)
	go func() {
		_, err := leftApp.Write(payload)
		if err == nil {
			err = leftApp.(closeWriter).CloseWrite()
		}
		writeResult <- err
	}()
	_ = rightApp.SetReadDeadline(time.Now().Add(15 * time.Second))
	received, err := io.ReadAll(rightApp)
	if err != nil {
		t.Fatal(err)
	}
	if err = <-writeResult; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(received, payload) {
		t.Fatalf("tail mismatch: received %d of %d bytes", len(received), len(payload))
	}
}

func TestCoreBidirectionalHalfClose(t *testing.T) {
	left, leftApp := newCore(context.Background(), testCoreConfig())
	right, rightApp := newCore(context.Background(), testCoreConfig())
	defer left.Close()
	defer right.Close()
	leg0Left, leg0Right := net.Pipe()
	connectTestLeg(t, left, right, 0, leg0Left, leg0Right)

	request := bytes.Repeat([]byte("request"), 8192)
	response := bytes.Repeat([]byte("response"), 8192)
	serverResult := make(chan error, 1)
	go func() {
		readRequest, err := io.ReadAll(rightApp)
		if err == nil && !bytes.Equal(readRequest, request) {
			err = io.ErrUnexpectedEOF
		}
		if err == nil {
			_, err = rightApp.Write(response)
		}
		if err == nil {
			err = rightApp.(closeWriter).CloseWrite()
		}
		serverResult <- err
	}()

	if _, err := leftApp.Write(request); err != nil {
		t.Fatal(err)
	}
	closeTestWrite(t, leftApp)
	_ = leftApp.SetReadDeadline(time.Now().Add(10 * time.Second))
	readResponse, err := io.ReadAll(leftApp)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readResponse, response) {
		t.Fatalf("response mismatch: received %d of %d bytes", len(readResponse), len(response))
	}
	if err = <-serverResult; err != nil {
		t.Fatal(err)
	}
}

func TestCorePrimaryFastOpenSendsDataBeforeHelloResponse(t *testing.T) {
	cfg := testCoreConfig()
	cfg.ActivationAfterBytes = 1 << 20
	clientCore, clientApp := newCore(context.Background(), cfg)
	serverCore, serverApp := newCore(context.Background(), cfg)
	defer clientCore.Close()
	defer serverCore.Close()

	clientWire, serverWire := net.Pipe()
	dataReceived := make(chan struct{})
	responseGate := make(chan struct{})
	serverResult := make(chan error, 1)
	hello := helloMessage{LegID: 0, ChunkSize: uint32(cfg.ChunkSize), Destination: "example.com:443"}
	go func() {
		received, err := readHello(serverWire)
		if err == nil && received != hello {
			err = errors.New("fast-open hello mismatch")
		}
		if err == nil {
			err = serverCore.reserveLeg(0)
		}
		if err == nil {
			for {
				var earlyFrame wireFrame
				earlyFrame, err = readWireFrame(serverWire, serverCore)
				if err != nil {
					break
				}
				serverCore.incoming <- earlyFrame
				if earlyFrame.typ == frameTypeData {
					close(dataReceived)
					break
				}
			}
		}
		if err == nil {
			<-responseGate
			err = writeHelloResponse(serverWire, helloResponse{Status: helloStatusOK, ChunkSize: uint32(cfg.ChunkSize)})
		}
		if err == nil {
			_, err = serverCore.commitLeg(0, serverWire, nil)
		}
		serverResult <- err
	}()

	outbound := &Outbound{handshakeTimeout: 5 * time.Second}
	readResponse, err := outbound.beginClientHandshake(context.Background(), clientWire, hello)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = clientCore.addLegWithReadPreamble(0, clientWire, nil, readResponse); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("fast-open-data"), 1024)
	writeResult := make(chan error, 1)
	go func() {
		_, writeErr := clientApp.Write(payload)
		if writeErr == nil {
			writeErr = clientApp.(closeWriter).CloseWrite()
		}
		writeResult <- writeErr
	}()

	select {
	case <-dataReceived:
	case <-time.After(time.Second):
		t.Fatal("server did not receive application data before the hello response")
	}
	close(responseGate)
	if err = <-serverResult; err != nil {
		t.Fatal(err)
	}
	_ = serverApp.SetReadDeadline(time.Now().Add(5 * time.Second))
	received, err := io.ReadAll(serverApp)
	if err != nil {
		t.Fatal(err)
	}
	if err = <-writeResult; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(received, payload) {
		t.Fatalf("fast-open payload mismatch: received %d of %d bytes", len(received), len(payload))
	}
}

func TestCorePrimaryFastOpenRejectClosesLogicalConnection(t *testing.T) {
	cfg := testCoreConfig()
	core, appConn := newCore(context.Background(), cfg)
	defer core.Close()
	clientWire, serverWire := net.Pipe()
	hello := helloMessage{LegID: 0, ChunkSize: uint32(cfg.ChunkSize), Destination: "example.com:443"}
	serverResult := make(chan error, 1)
	go func() {
		_, err := readHello(serverWire)
		if err == nil {
			err = writeHelloResponse(serverWire, helloResponse{Status: helloStatusRejected})
		}
		serverResult <- err
	}()
	outbound := &Outbound{handshakeTimeout: 5 * time.Second}
	readResponse, err := outbound.beginClientHandshake(context.Background(), clientWire, hello)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = core.addLegWithReadPreamble(0, clientWire, nil, readResponse); err != nil {
		t.Fatal(err)
	}
	select {
	case <-core.Done():
	case <-time.After(time.Second):
		t.Fatal("rejected fast-open handshake did not close the logical connection")
	}
	if err = <-serverResult; err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		if _, err = appConn.Write([]byte("must fail")); err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("writes kept succeeding after rejected handshake")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCoreSmallFlowUsesOnlyLeg0(t *testing.T) {
	cfg := testCoreConfig()
	cfg.ActivationAfterBytes = 1 << 20
	left, leftApp := newCore(context.Background(), cfg)
	right, rightApp := newCore(context.Background(), cfg)
	defer left.Close()
	defer right.Close()
	leg0Left, leg0Right := net.Pipe()
	connectTestLeg(t, left, right, 0, leg0Left, leg0Right)
	boosterLeft, boosterRight := net.Pipe()
	stalledBooster := newStallingConn(boosterLeft)
	countedBooster := &countingConn{Conn: stalledBooster}
	connectTestLeg(t, left, right, 1, countedBooster, boosterRight)

	payload := bytes.Repeat([]byte("small-flow"), 16*1024)
	writeResult := make(chan error, 1)
	go func() {
		_, err := leftApp.Write(payload)
		if err == nil {
			err = leftApp.(closeWriter).CloseWrite()
		}
		writeResult <- err
	}()
	_ = rightApp.SetReadDeadline(time.Now().Add(10 * time.Second))
	received, err := io.ReadAll(rightApp)
	if err != nil {
		t.Fatal(err)
	}
	if err = <-writeResult; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(received, payload) {
		t.Fatal("small flow payload mismatch")
	}
	if written := countedBooster.written.Load(); written != 0 {
		t.Fatalf("small flow wrote %d bytes to booster leg", written)
	}
	select {
	case <-stalledBooster.started:
		t.Fatal("small flow attempted to use the stalled booster leg")
	default:
	}
}

func TestCoreLeg1FailureFallsBackToLeg0(t *testing.T) {
	left, leftApp := newCore(context.Background(), testCoreConfig())
	right, rightApp := newCore(context.Background(), testCoreConfig())
	defer left.Close()
	defer right.Close()
	leg0Left, leg0Right := net.Pipe()
	connectTestLeg(t, left, right, 0, leg0Left, leg0Right)
	boosterLeft, boosterRight := net.Pipe()
	connectTestLeg(t, left, right, 1, &delayedWriteConn{Conn: boosterLeft, delay: 2 * time.Millisecond}, boosterRight)

	payload := bytes.Repeat([]byte("fallback-data-"), 512*1024)
	writeResult := make(chan error, 1)
	readResult := make(chan []byte, 1)
	readError := make(chan error, 1)
	go func() {
		_ = rightApp.SetReadDeadline(time.Now().Add(20 * time.Second))
		data, err := io.ReadAll(rightApp)
		readResult <- data
		readError <- err
	}()
	go func() {
		_, err := leftApp.Write(payload)
		if err == nil {
			err = leftApp.(closeWriter).CloseWrite()
		}
		writeResult <- err
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		left.replayMu.Lock()
		replayBytes := left.replayBytes
		left.replayMu.Unlock()
		if replayBytes > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("booster leg did not receive replay-tracked data")
		}
		time.Sleep(time.Millisecond)
	}
	if err := boosterLeft.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-writeResult; err != nil {
		t.Fatal(err)
	}
	received := <-readResult
	if err := <-readError; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(received, payload) {
		t.Fatalf("fallback mismatch: received %d of %d bytes", len(received), len(payload))
	}
}

func TestCoreLeg1StallFallsBackToLeg0(t *testing.T) {
	cfg := testCoreConfig()
	cfg.ReplayTimeout = 100 * time.Millisecond
	left, leftApp := newCore(context.Background(), cfg)
	right, rightApp := newCore(context.Background(), cfg)
	defer left.Close()
	defer right.Close()
	leg0Left, leg0Right := net.Pipe()
	connectTestLeg(t, left, right, 0, leg0Left, leg0Right)
	boosterLeft, boosterRight := net.Pipe()
	stalledBooster := newStallingConn(boosterLeft)
	connectTestLeg(t, left, right, 1, stalledBooster, boosterRight)

	payload := bytes.Repeat([]byte("stalled-fallback-"), 128*1024)
	writeResult := make(chan error, 1)
	go func() {
		_, err := leftApp.Write(payload)
		if err == nil {
			err = leftApp.(closeWriter).CloseWrite()
		}
		writeResult <- err
	}()
	_ = rightApp.SetReadDeadline(time.Now().Add(10 * time.Second))
	received, err := io.ReadAll(rightApp)
	if err != nil {
		t.Fatal(err)
	}
	if err = <-writeResult; err != nil {
		t.Fatal(err)
	}
	select {
	case <-stalledBooster.started:
	default:
		t.Fatal("booster leg never entered the stalled write")
	}
	if !bytes.Equal(received, payload) {
		t.Fatalf("stall fallback mismatch: received %d of %d bytes", len(received), len(payload))
	}
}
