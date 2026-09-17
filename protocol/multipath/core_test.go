package multipath

import (
	"bytes"
	"context"
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
		AggregationEnabled:  true,
		ActivationOnQueue:   true,
		FrameSize:           4 * 1024,
		QueueFrames:         64,
		QueueBytes:          256 * 1024,
		ReceiveWindowBytes:  16 << 20,
		SendBufferBytes:     16 << 20,
		PathStallTimeoutMin: time.Second,
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

	core.legFailed(firstLeg, legFailureReadData, net.ErrClosed)
	<-firstLeg.readerDone
	<-firstLeg.writerDone
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

func TestCoreSenderStatusAndRTTProbe(t *testing.T) {
	left, _ := newCore(context.Background(), testCoreConfig())
	rightConfig := testCoreConfig()
	rightConfig.SendStatus = true
	right, _ := newCore(context.Background(), rightConfig)
	defer left.Close()
	defer right.Close()
	leftWire, rightWire := net.Pipe()
	connectTestLeg(t, left, right, 0, leftWire, rightWire)

	right.ingressBytes.Store(4096)
	right.fallbackB.Store(1024)
	if !right.queueSenderStatus(time.Now(), true) {
		t.Fatal("sender status was not queued")
	}
	waitForStatus(t, func() bool {
		return left.peerSenderStatusSnapshot().status.Sequence > 0
	})
	remote := left.peerSenderStatusSnapshot().status
	if remote.LogicalTX != 4096 || remote.FallbackBytes != 1024 {
		t.Fatalf("unexpected remote sender status: %+v", remote)
	}

	left.scheduleProbes(time.Now())
	waitForStatus(t, func() bool {
		return left.rttSnapshot()[0].Samples > 0
	})
	rtt := left.rttSnapshot()[0]
	if rtt.ProbeSent != 1 || rtt.Latest <= 0 || rtt.EWMA <= 0 {
		t.Fatalf("unexpected RTT status: %+v", rtt)
	}
}

func TestCoreSenderStatusDisabled(t *testing.T) {
	core, _ := newCore(context.Background(), testCoreConfig())
	defer core.Close()
	if core.queueSenderStatus(time.Now(), true) {
		t.Fatal("sender status was queued while disabled")
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

func TestActivationAfterBytesMinRate(t *testing.T) {
	cfg := testCoreConfig()
	cfg.ActivationAfterBytes = 2 << 20
	cfg.ActivationAfterBytesMinBytesPS = 8 << 20
	cfg.ActivationWindow = time.Second

	if _, ok := activationAfterBytes(cfg, 2<<20, 0, time.Second); ok {
		t.Fatal("activation should wait for the minimum rate")
	}
	info, ok := activationAfterBytes(cfg, 10<<20, 0, time.Second)
	if !ok {
		t.Fatal("activation should pass the minimum rate")
	}
	if info.RateBytesPS != 10<<20 || info.MinRateBytesPS != 8<<20 || info.Elapsed != time.Second {
		t.Fatalf("unexpected activation info: %+v", info)
	}

	cfg.ActivationAfterBytesMinBytesPS = 0
	if _, ok = activationAfterBytes(cfg, 2<<20, 0, 0); !ok {
		t.Fatal("activation without a minimum rate should keep the old immediate behavior")
	}
}

func TestCoreHalfClosePreservesTail(t *testing.T) {
	left, leftApp := newCore(context.Background(), testCoreConfig())
	right, rightApp := newCore(context.Background(), testCoreConfig())
	left.activate(activationInfo{Reason: activationReasonBytes})
	right.activate(activationInfo{Reason: activationReasonBytes})
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
	left.activate(activationInfo{Reason: activationReasonBytes})
	right.activate(activationInfo{Reason: activationReasonBytes})
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
	testCoreLeg1FailureFallsBackToLeg0(t, false)
}

func TestTrafficSavingLeg1FailureFallsBackToLeg0(t *testing.T) {
	testCoreLeg1FailureFallsBackToLeg0(t, true)
}

func testCoreLeg1FailureFallsBackToLeg0(t *testing.T, saving bool) {
	cfg := testCoreConfig()
	cfg.Leg0TrafficSaving = saving
	left, leftApp := newCore(context.Background(), cfg)
	right, rightApp := newCore(context.Background(), cfg)
	left.activate(activationInfo{Reason: activationReasonBytes})
	right.activate(activationInfo{Reason: activationReasonBytes})
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
			t.Fatalf("booster leg did not receive replay-tracked data: left=%s right=%s tx=%d rx=%d", left.failure, right.failure, left.txSeq.Load(), right.rxExpected.Load())
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
	testCoreLeg1StallFallsBackToLeg0(t, false)
}

func TestTrafficSavingLeg1StallFallsBackToLeg0(t *testing.T) {
	testCoreLeg1StallFallsBackToLeg0(t, true)
}

func testCoreLeg1StallFallsBackToLeg0(t *testing.T, saving bool) {
	cfg := testCoreConfig()
	cfg.Leg0TrafficSaving = saving
	cfg.PathStallTimeoutMin = 100 * time.Millisecond
	left, leftApp := newCore(context.Background(), cfg)
	right, rightApp := newCore(context.Background(), cfg)
	left.activate(activationInfo{Reason: activationReasonBytes})
	right.activate(activationInfo{Reason: activationReasonBytes})
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
