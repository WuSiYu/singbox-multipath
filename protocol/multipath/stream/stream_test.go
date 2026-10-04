package stream

import (
	"bytes"
	"math/rand"
	"testing"
	"time"
)

func TestReceiptAndReadAreIndependent(t *testing.T) {
	r := NewReceiver(4*PageSize, nil)
	payload := bytes.Repeat([]byte{7}, 3*PageSize+19)
	if _, err := r.Insert(PageSize, payload[PageSize:]); err != nil {
		t.Fatal(err)
	}
	if r.Ack() != 0 || len(r.Readable()) != 0 {
		t.Fatal("out-of-order receipt advanced Data ACK")
	}
	if err := r.SetFIN(uint64(len(payload))); err != nil {
		t.Fatal(err)
	}
	if r.Complete() {
		t.Fatal("FIN bypassed missing prefix")
	}
	if _, err := r.Insert(0, payload[:PageSize]); err != nil {
		t.Fatal(err)
	}
	if r.Ack() != uint64(len(payload)+1) || r.ReadNext != 0 || r.EOF() {
		t.Fatal("receipt/consumption/FIN conflated")
	}
	if r.Advertise(r.Capacity) != 4*PageSize {
		t.Fatal("ACK alone slid window")
	}
	var received []byte
	for data := r.Readable(); len(data) > 0; data = r.Readable() {
		received = append(received, data...)
		r.Consume(len(data))
	}
	if !bytes.Equal(received, payload) || !r.EOF() {
		t.Fatal("stream changed")
	}
	if r.Advertise(r.Capacity) != uint64(4*PageSize+len(payload)) {
		t.Fatal("window not tied to consumption")
	}
}

func TestByteFragmentsDuplicatesAndOverlaps(t *testing.T) {
	for seed := int64(0); seed < 100; seed++ {
		rng := rand.New(rand.NewSource(seed))
		payload := make([]byte, 7*PageSize+127)
		_, _ = rng.Read(payload)
		r := NewReceiver(16*PageSize, nil)
		var offsets []int
		for start := 0; start < len(payload); start += 127 {
			offsets = append(offsets, start)
		}
		rng.Shuffle(len(offsets), func(i, j int) { offsets[i], offsets[j] = offsets[j], offsets[i] })
		var received []byte
		for _, start := range offsets {
			end := min(len(payload), start+127+rng.Intn(300))
			if _, err := r.Insert(uint64(start), payload[start:end]); err != nil {
				t.Fatal(seed, err)
			}
			if _, err := r.Insert(uint64(start), payload[start:end]); err != nil {
				t.Fatal(seed, err)
			}
			if rng.Intn(3) == 0 {
				data := r.Readable()
				if len(data) > 0 {
					n := 1 + rng.Intn(len(data))
					received = append(received, data[:n]...)
					r.Consume(n)
				}
			}
		}
		if err := r.SetFIN(uint64(len(payload))); err != nil {
			t.Fatal(seed, err)
		}
		for data := r.Readable(); len(data) > 0; data = r.Readable() {
			received = append(received, data...)
			r.Consume(len(data))
		}
		if !bytes.Equal(received, payload) || !r.EOF() {
			t.Fatalf("seed %d corrupted stream", seed)
		}
		buffered, reordered, _ := r.Buffered()
		if buffered != 0 || reordered != 0 {
			t.Fatal(seed, buffered, reordered)
		}
		r.Close()
	}
}

type testMemory struct{ used, limit int }

func (m *testMemory) Acquire(bool) bool {
	if m.used == m.limit {
		return false
	}
	m.used++
	return true
}
func (m *testMemory) Release(bool) { m.used-- }

func TestPressureKeepsAcknowledgedBytesAndAdmitsHead(t *testing.T) {
	memory := &testMemory{limit: 2}
	r := NewReceiver(8*PageSize, memory)
	payload := bytes.Repeat([]byte{9}, PageSize)
	_, _ = r.Insert(0, payload)
	_, _ = r.Insert(3*PageSize, payload)
	if r.Ack() != PageSize {
		t.Fatal("bad prefix")
	}
	_, err := r.Insert(PageSize, payload)
	if err != nil || r.Ack() != 2*PageSize || r.Pruned != PageSize {
		t.Fatal("head did not evict unacknowledged tail", err, r.Ack(), r.Pruned)
	}
	if !bytes.Equal(r.Readable(), payload) {
		t.Fatal("acknowledged unread bytes evicted")
	}
	_, _ = r.Insert(2*PageSize, payload)
	if r.Ack() != 2*PageSize {
		t.Fatal("acknowledged unowned bytes under pressure")
	}
	r.Consume(PageSize)
	_, _ = r.Insert(2*PageSize, payload)
	if r.Ack() != 3*PageSize {
		t.Fatal("receiver failed to resume after application read")
	}
	end := r.WindowEnd
	if r.Advertise(0) != end {
		t.Fatal("pressure shrank window")
	}
	r.Close()
	if memory.used != 0 {
		t.Fatal("receive page leak", memory.used)
	}
}

func TestFirstReceivedBytesWin(t *testing.T) {
	r := NewReceiver(PageSize, nil)
	_, _ = r.Insert(8, []byte("original"))
	_, _ = r.Insert(0, []byte("prefix!!changed!"))
	if got := string(r.Readable()); got != "prefix!!original" {
		t.Fatal(got)
	}
}

func TestSenderPartialACKAndWriterOwnership(t *testing.T) {
	s := NewSender(100)
	released := 0
	buffer := NewBuffer([]byte("abcdefghijklmnop"), func() { released++ })
	if err := s.Append(buffer); err != nil {
		t.Fatal(err)
	}
	part, _ := s.NextRange(10)
	part.Buffer.Retain()
	if err := s.Sent(part); err != nil {
		t.Fatal(err)
	}
	if err := s.Acknowledge(6, 100); err != nil {
		t.Fatal(err)
	}
	if data, ok := s.Range(6, 100); !ok || string(data.Data()) != "ghijklmnop" {
		t.Fatal("partial ACK lost suffix")
	}
	if s.Buffered() != 10 || released != 0 {
		t.Fatal("premature release")
	}
	part2, _ := s.NextRange(100)
	if err := s.Sent(part2); err != nil {
		t.Fatal(err)
	}
	if err := s.Acknowledge(16, 100); err != nil {
		t.Fatal(err)
	}
	if released != 0 {
		t.Fatal("ACK released bytes still referenced by writer")
	}
	if string(part.Data()) != "abcdefghij" {
		t.Fatal("writer slice mutated")
	}
	part.Buffer.Release()
	if released != 1 {
		t.Fatal("buffer leak")
	}
}

func TestSenderWindowAndFIN(t *testing.T) {
	s := NewSender(3)
	_ = s.Append(NewBuffer([]byte("abcdef"), nil))
	s.CloseWrite()
	part, _ := s.NextRange(100)
	if part.Length != 3 {
		t.Fatal("ignored shared window")
	}
	_ = s.Sent(part)
	if _, ok := s.NextRange(100); ok || s.SendFIN() {
		t.Fatal("overran window")
	}
	if err := s.Acknowledge(3, 7); err != nil {
		t.Fatal(err)
	}
	if err := s.Acknowledge(2, 4); err != nil || s.WindowEnd != 7 || s.Una != 3 {
		t.Fatal("stale ACK shrank state")
	}
	part, _ = s.NextRange(100)
	_ = s.Sent(part)
	if !s.SendFIN() || s.Next != 7 {
		t.Fatal("FIN did not consume one byte")
	}
	if err := s.Acknowledge(8, 9); err == nil {
		t.Fatal("accepted future ACK")
	}
	if err := s.Acknowledge(7, 7); err != nil || !s.FINAcked {
		t.Fatal("FIN acknowledgement lost")
	}
}

func TestFINValidation(t *testing.T) {
	r := NewReceiver(2*PageSize, nil)
	_, _ = r.Insert(100, []byte("tail"))
	if err := r.SetFIN(99); err == nil {
		t.Fatal("accepted FIN before buffered data")
	}
	if err := r.SetFIN(104); err != nil {
		t.Fatal(err)
	}
	if err := r.SetFIN(105); err == nil {
		t.Fatal("accepted inconsistent FIN")
	}
	if _, err := r.Insert(104, []byte("x")); err == nil {
		t.Fatal("accepted data after FIN")
	}
}

func TestPathReceiptsAreNotDataACK(t *testing.T) {
	s := NewSender(1 << 20)
	_ = s.Append(NewBuffer(make([]byte, 4096), nil))
	part, _ := s.NextRange(4096)
	_ = s.Sent(part)
	p := Path{Generation: 8}
	now := time.Unix(1000, 0)
	_, _ = p.Submitted(4096, now)
	if err := p.Feedback(Receipt{Generation: 7, Next: 1 << 40}, now); err != nil || p.Received != 0 {
		t.Fatal("old generation changed state")
	}
	if err := p.Feedback(Receipt{Generation: 8, Next: 4096, ReceivedAt: 10000}, now.Add(100*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if p.Outstanding() != 0 || s.Una != 0 || s.Buffered() != 4096 {
		t.Fatal("path receipt released connection data")
	}
	if err := p.Feedback(Receipt{Generation: 8, Next: 4097}, now); err == nil {
		t.Fatal("future path ACK accepted")
	}
}

func TestPathRateUsesRemoteDeliveryClock(t *testing.T) {
	p := Path{Generation: 1}
	now := time.Unix(1000, 0)
	_, _ = p.Submitted(1_000_000, now)
	_ = p.Feedback(Receipt{Generation: 1, Next: 100_000, ReceivedAt: 10_000}, now.Add(100*time.Millisecond))
	_ = p.Feedback(Receipt{Generation: 1, Next: 600_000, ReceivedAt: 20_000}, now.Add(101*time.Millisecond))
	if p.Rate != 50_000_000 {
		t.Fatal("ACK compression inflated rate", p.Rate)
	}
	if p.Stalled(now.Add(150*time.Millisecond), 0) {
		t.Fatal("short retransmission classified stale")
	}
	if !p.Stalled(now.Add(2*time.Second), 0) {
		t.Fatal("stalled path not detected")
	}
	p.Stale = true
	_ = p.Feedback(Receipt{Generation: 1, Next: 700_000, ReceivedAt: 30_000}, now.Add(3*time.Second))
	if p.Stale {
		t.Fatal("fresh path receipt failed to resume path")
	}
}

func TestFirstCoalescedReceiptInitializesDelivery(t *testing.T) {
	p := Path{Generation: 1}
	now := time.Now()
	for i := 0; i < 4; i++ {
		_, _ = p.Submitted(65536, now)
	}
	err := p.Feedback(Receipt{Generation: 1, Next: 4 * 65536, FirstNext: 65536, FirstReceivedAt: 1000, ReceivedAt: 2572}, now.Add(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if p.Rate < 120_000_000 || p.Rate > 130_000_000 {
		t.Fatal("first burst did not initialize delivery rate", p.Rate)
	}
}

func TestPathFlightOwnershipAndClose(t *testing.T) {
	released, prepaid := 0, 0
	p := Path{Generation: 3, ReleaseFlight: func(reserved bool) {
		released++
		if reserved {
			prepaid++
		}
	}}
	now := time.Now()
	_, _ = p.Submitted(10, now, true)
	_, _ = p.Submitted(10, now, false)
	_, _ = p.Submitted(10, now, false)
	_ = p.Feedback(Receipt{Generation: 2, Next: 30, ReceivedAt: 10}, now)
	if released != 0 {
		t.Fatal("old generation released live records")
	}
	_ = p.Feedback(Receipt{Generation: 3, Next: 15, ReceivedAt: 10}, now)
	if released != 1 || prepaid != 1 {
		t.Fatal("partial receipt released wrong records")
	}
	p.Close()
	p.Close()
	if released != 3 || prepaid != 1 {
		t.Fatal("flight ownership leaked or double released")
	}
}

func TestPathRateDoesNotTreatQUICBurstsAsCapacity(t *testing.T) {
	p := Path{Generation: 1, SRTT: 100 * time.Millisecond, MinimumRTT: 100 * time.Millisecond}
	now := time.Now()
	_, _ = p.Submitted(100<<20, now)
	var received uint64
	var stamp uint64 = 1
	// 100 batches/s, each 64 KiB, are delivered in 10-frame bursts after
	// a 100 ms reliable-stream stall: 6.55 MB/s, not the burst's 6.55 GB/s.
	for round := 0; round < 100; round++ {
		stamp += 100000
		for i := 0; i < 10; i++ {
			received += 65536
			stamp += 10
			if err := p.Feedback(Receipt{Generation: 1, Next: received, ReceivedAt: stamp}, now.Add(time.Duration(stamp)*time.Microsecond)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if p.Rate < 3_000_000 || p.Rate > 11_000_000 {
		t.Fatal("burst-biased delivery rate", p.Rate)
	}
}
