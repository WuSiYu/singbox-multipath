package multipath

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/protocol/multipath/stream"
)

// Wire-format fixtures own their scratch independently of runtime page storage.
func readWireFrame(conn net.Conn, core *mpCore) (wireFrame, error) {
	return readFrame(conn, make([]byte, core.cfg.FrameSize))
}

func beta5Pair(t *testing.T, memory int64) (*mpCore, *mpCore, net.Conn, net.Conn, [2]net.Conn) {
	t.Helper()
	cfg := coreConfig{AggregationEnabled: true, FrameSize: 64 << 10, QueueFrames: 256, ReceiveWindowBytes: 8 << 20}
	cfg.Memory = newMemoryBudget(memory, false)
	a, appA, err := newCoreWithError(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Memory = newMemoryBudget(memory, false)
	b, appB, err := newCoreWithError(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
		for _, core := range []*mpCore{a, b} {
			select {
			case <-core.released:
			case <-time.After(3 * time.Second):
				t.Error("worker/memory release did not complete")
			}
			if got := core.memory.snapshot(); got.UsedBytes != got.CachedBytes {
				t.Errorf("unreleased budget: %+v", got)
			}
		}
	})
	var transports [2]net.Conn
	for id := uint8(0); id < 2; id++ {
		left, right := net.Pipe()
		transports[id] = left
		if _, err = a.addLeg(id, left, nil); err != nil {
			t.Fatal(err)
		}
		if _, err = b.addLeg(id, right, nil); err != nil {
			t.Fatal(err)
		}
	}
	a.activate(activationInfo{Reason: activationReasonBytes})
	b.activate(activationInfo{Reason: activationReasonBytes})
	_ = appA.SetDeadline(time.Now().Add(10 * time.Second))
	_ = appB.SetDeadline(time.Now().Add(10 * time.Second))
	return a, b, appA, appB, transports
}

func TestBeta5BidirectionalByteStream(t *testing.T) {
	for _, budget := range []int64{64 << 20, 8 << 20} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			_, _, a, b, _ := beta5Pair(t, budget)
			up := bytes.Repeat([]byte("upload-0123456789"), 128*1024)
			down := bytes.Repeat([]byte("download-ABCDEFGHIJK"), 128*1024)
			results := make(chan error, 4)
			write := func(conn net.Conn, data []byte) {
				_, err := conn.Write(data)
				if err == nil {
					err = conn.(*logicalConn).CloseWrite()
				}
				results <- err
			}
			read := func(conn net.Conn, data []byte) {
				got, err := io.ReadAll(conn)
				if err == nil && !bytes.Equal(data, got) {
					err = fmt.Errorf("byte mismatch: got %d want %d", len(got), len(data))
				}
				results <- err
			}
			go write(a, up)
			go write(b, down)
			go read(a, down)
			go read(b, up)
			for i := 0; i < 4; i++ {
				if err := <-results; err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestBeta5SecondaryFailureDoesNotCloseSession(t *testing.T) {
	a, _, appA, appB, transports := beta5Pair(t, 64<<20)
	payload := bytes.Repeat([]byte("retained-logical-stream"), 1024*1024)
	result := make(chan error, 1)
	go func() {
		_, err := appA.Write(payload)
		if err == nil {
			err = appA.(*logicalConn).CloseWrite()
		}
		result <- err
	}()
	go func() {
		for a.legCounters[1].txBytes.Load() < 64<<10 && !a.isDone() {
			time.Sleep(time.Millisecond)
		}
		_ = transports[1].Close()
	}()
	got, err := io.ReadAll(appB)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("recovery corrupted stream: got %d want %d", len(got), len(payload))
	}
	if a.isDone() {
		t.Fatal("secondary failure terminated live primary session")
	}
}

func TestBeta5ManyTinyWrites(t *testing.T) {
	_, _, a, b, _ := beta5Pair(t, 16<<20)
	const count = 4096
	result := make(chan error, 1)
	go func() {
		for i := 0; i < count; i++ {
			if _, err := a.Write([]byte{byte(i)}); err != nil {
				result <- err
				return
			}
		}
		result <- a.(*logicalConn).CloseWrite()
	}()
	got, err := io.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if err = <-result; err != nil {
		t.Fatal(err)
	}
	if len(got) != count {
		t.Fatal(len(got))
	}
	for i, value := range got {
		if value != byte(i) {
			t.Fatal("tiny-write corruption", i)
		}
	}
}

// Receiving a DATA mapping is not atomic: a prefix is readable and Data-ACKed
// while its suffix is still blocked in the child. A failed secondary prefix
// remains valid when the complete mapping is reinjected through the primary.
func TestBeta5IncrementalMapping(t *testing.T) {
	for _, secondaryFailure := range []bool{false, true} {
		t.Run(fmt.Sprint(secondaryFailure), func(t *testing.T) {
			core, app := newCore(context.Background(), coreConfig{FrameSize: 65536, Memory: newMemoryBudget(16<<20, false)})
			defer core.Close()
			_ = app.SetDeadline(time.Now().Add(3 * time.Second))
			var peers [2]net.Conn
			for id := uint8(0); id < 2; id++ {
				peer, child := net.Pipe()
				peers[id] = peer
				defer peer.Close()
				if _, err := core.addLeg(id, child, nil); err != nil {
					t.Fatal(err)
				}
				go func(peer net.Conn) {
					for {
						if _, err := readFrame(peer, make([]byte, 65536)); err != nil {
							return
						}
					}
				}(peer)
			}
			payload := make([]byte, 65536)
			for i := range payload {
				payload[i] = byte(i*31 + i/251)
			}
			encoded, err := encodeWireFrame(wireFrame{typ: frameTypeData, generation: 1, data: payload})
			if err != nil {
				t.Fatal(err)
			}
			id := 0
			if secondaryFailure {
				id = 1
			}
			prefix := 777
			if err = writeAll(peers[id], encoded[:dataFrameHeaderSize+prefix]); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len(payload))
			if _, err = io.ReadFull(app, got[:prefix]); err != nil {
				t.Fatal("prefix waited for suffix:", err)
			}
			core.stateMu.Lock()
			ack := core.rx.Ack()
			receipt := core.getLeg(uint8(id)).received.Next
			core.stateMu.Unlock()
			if ack != uint64(prefix) || receipt != uint64(prefix) {
				t.Fatalf("premature or missing ACK: %d/%d", ack, receipt)
			}
			if core.legCounters[id].rxFrames.Load() != 0 {
				t.Fatal("partial mapping counted as complete")
			}
			result := make(chan error, 1)
			go func() {
				if secondaryFailure {
					_ = peers[1].Close()
					err = writeAll(peers[0], encoded)
				} else {
					err = writeAll(peers[0], encoded[dataFrameHeaderSize+prefix:])
				}
				if err == nil {
					err = writeWireFrame(peers[0], wireFrame{typ: frameTypeFIN, seq: uint64(len(payload))})
				}
				result <- err
			}()
			if _, err := io.ReadFull(app, got[prefix:]); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatal("partial mapping or reinjection corrupted bytes")
			}
			if n, err := app.Read(make([]byte, 1)); n != 0 || err != io.EOF {
				t.Fatalf("FIN: n=%d err=%v", n, err)
			}
			if err := <-result; err != nil {
				t.Fatal(err)
			}
			if core.legCounters[0].rxFrames.Load() != 1 {
				t.Fatal("wire frame counted more than once")
			}
		})
	}
}

func TestBeta5MappingOverflowRejectedBeforeDelivery(t *testing.T) {
	for _, offset := range []int{1, 9} {
		left, right := net.Pipe()
		var header [dataFrameHeaderSize]byte
		header[0] = frameTypeData
		binary.BigEndian.PutUint64(header[offset:offset+8], math.MaxUint64)
		binary.BigEndian.PutUint64(header[17:25], 1)
		binary.BigEndian.PutUint32(header[25:29], 1)
		go func() { defer left.Close(); _ = writeAll(left, header[:]) }()
		called := false
		_, err := readFrameData(right, make([]byte, 65536), func(wireFrame) error { called = true; return nil })
		_ = right.Close()
		if err == nil || called {
			t.Fatal("overflowing mapping was published")
		}
	}
}

func TestBeta5LocalQueueIsNotAPathFlightLimit(t *testing.T) {
	core, _ := newCore(context.Background(), coreConfig{AggregationEnabled: true, QueueFrames: 8})
	defer core.Close()
	core.active.Store(true)
	leg := &mpLeg{id: 1, path: stream.Path{Generation: 1, Rate: 100 << 20, SRTT: time.Second}}
	leg.ready.Store(true)
	// Many small mappings may traverse a large-BDP child. Their metadata and
	// the byte pipeline are bounded separately; queue_frames is not a quota.
	for i := 0; i < 1024; i++ {
		_, _ = leg.path.Submitted(1024, time.Now())
	}
	core.stateMu.Lock()
	core.legsMu.Lock()
	core.legs[1] = leg
	core.legsMu.Unlock()
	chosen := core.choosePathLocked(65536)
	core.legsMu.Lock()
	delete(core.legs, 1)
	core.legsMu.Unlock()
	core.stateMu.Unlock()
	if chosen != leg {
		t.Fatal("local queue size incorrectly capped path flight")
	}
}

func TestPeerPressureDoesNotGatePaths(t *testing.T) {
	core, _ := newCore(context.Background(), coreConfig{AggregationEnabled: true})
	defer core.Close()
	core.active.Store(true)
	primary := &mpLeg{id: 0}
	secondary := &mpLeg{id: 1}
	primary.ready.Store(true)
	secondary.ready.Store(true)
	core.stateMu.Lock()
	core.legsMu.Lock()
	core.legs[0], core.legs[1] = primary, secondary
	core.legsMu.Unlock()
	// The receiver backs its whole window; its pressure flag is informational.
	core.peerPressure = true
	if got := core.choosePathLocked(65536); got != primary {
		t.Error("peer pressure blocked primary progress")
	}
	primary.busy = true
	if got := core.choosePathLocked(65536); got != secondary {
		t.Error("peer pressure blocked the secondary")
	}
	core.legsMu.Lock()
	delete(core.legs, 0)
	delete(core.legs, 1)
	core.legsMu.Unlock()
	core.stateMu.Unlock()
}

type beta5HeldConn struct {
	net.Conn
	hold    chan struct{}
	entered chan struct{}
	once    sync.Once
}

// Write holds the first DATA frame; feedback and other control frames pass.
func (c *beta5HeldConn) Write(data []byte) (int, error) {
	if len(data) > dataFrameHeaderSize && data[0] == frameTypeData {
		c.once.Do(func() { close(c.entered); <-c.hold })
	}
	return c.Conn.Write(data)
}

func TestBeta5HeldSecondaryWriterDoesNotOwnACKedBuffer(t *testing.T) {
	cfg := coreConfig{AggregationEnabled: true, FrameSize: 64 << 10, QueueFrames: 256, Memory: newMemoryBudget(64<<20, false)}
	a, appA := newCore(context.Background(), cfg)
	cfg.Memory = newMemoryBudget(64<<20, false)
	b, appB := newCore(context.Background(), cfg)
	defer a.Close()
	defer b.Close()
	left, right := net.Pipe()
	_, _ = a.addLeg(0, left, nil)
	_, _ = b.addLeg(0, right, nil)
	left, right = net.Pipe()
	held := &beta5HeldConn{Conn: left, hold: make(chan struct{}), entered: make(chan struct{})}
	_, _ = a.addLeg(1, held, nil)
	_, _ = b.addLeg(1, right, nil)
	a.activate(activationInfo{Reason: activationReasonBytes})
	payload := bytes.Repeat([]byte("stalled-secondary"), 128*1024)
	_ = appA.SetDeadline(time.Now().Add(8 * time.Second))
	_ = appB.SetDeadline(time.Now().Add(8 * time.Second))
	defer close(held.hold)
	result := make(chan error, 1)
	go func() {
		_, err := appA.Write(payload)
		if err == nil {
			err = appA.(*logicalConn).CloseWrite()
		}
		result <- err
	}()
	select {
	case <-held.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("secondary not exercised")
	}
	got, err := io.ReadAll(appB)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("reinjection or buffer ownership corrupted data")
	}
	// Whichever repair reached the data first, opportunistic (fallbackB) or
	// at the tail (tailB), the held writer's buffer was resent.
	if a.fallbackB.Load()+a.tailB.Load() == 0 {
		t.Fatal("held secondary did not exercise reinjection")
	}
}
