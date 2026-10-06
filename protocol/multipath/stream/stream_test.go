package stream

import (
	"bytes"
	"math"
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
	far := uint64(NearHeadPages + 5)
	r := NewReceiver((far+2)*PageSize, memory)
	if r.WindowEnd != InitialWindow {
		t.Fatal("accounted receiver must start with a small window")
	}
	r.Advertise(r.Capacity)
	payload := bytes.Repeat([]byte{9}, PageSize)
	_, _ = r.Insert(0, payload)
	_, _ = r.Insert(far*PageSize, payload)
	if r.Ack() != PageSize || memory.used != 2 {
		t.Fatal("bad prefix")
	}
	// A page near the head evicts the farthest unacknowledged page.
	_, err := r.Insert(PageSize, payload)
	if err != nil || r.Ack() != 2*PageSize || r.Pruned != PageSize {
		t.Fatal("near-head data did not evict the unacknowledged tail", err, r.Ack(), r.Pruned)
	}
	if got := r.DroppedRanges(4, math.MaxUint64); len(got) != 1 || got[0].Start != far*PageSize {
		t.Fatalf("evicted tail not reported: %v", got)
	}
	if !bytes.Equal(r.Readable(), payload) {
		t.Fatal("acknowledged unread bytes evicted")
	}
	// Nothing left to evict: acknowledged unread bytes are never dropped.
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

func TestDroppedRangesAreReportedAndCleared(t *testing.T) {
	memory := &testMemory{limit: 1}
	r := NewReceiver(16*PageSize, memory)
	payload := bytes.Repeat([]byte{1}, PageSize)
	_, _ = r.Insert(PageSize, payload)   // admitted (first speculative page)
	_, _ = r.Insert(3*PageSize, payload) // refused: memory exhausted
	_, _ = r.Insert(5*PageSize, payload) // refused
	got := r.DroppedRanges(4, math.MaxUint64)
	if len(got) != 2 || got[0] != (Range{3 * PageSize, 4 * PageSize}) || got[1] != (Range{5 * PageSize, 6 * PageSize}) {
		t.Fatalf("dropped ranges %v", got)
	}
	memory.limit = 16
	_, _ = r.Insert(3*PageSize, payload)
	if got = r.DroppedRanges(4, math.MaxUint64); len(got) != 1 || got[0].Start != 5*PageSize {
		t.Fatalf("repaired range still reported: %v", got)
	}
	_, _ = r.Insert(0, bytes.Repeat([]byte{1}, 6*PageSize))
	if got = r.DroppedRanges(4, math.MaxUint64); len(got) != 0 {
		t.Fatalf("acknowledged range still reported: %v", got)
	}
}

func TestRangeSetBounded(t *testing.T) {
	var s rangeSet
	for i := uint64(0); i < 40; i++ {
		s.add(Range{i * 10, i*10 + 1 + i%3})
	}
	if len(s) > maxDroppedRanges || s[0] != (Range{0, 1}) {
		t.Fatal("range set unbounded or lost its lowest range", s)
	}
	for _, r := range s {
		if r.End-r.Start > 3 {
			t.Fatal("ranges merged across gaps", s)
		}
	}
	for i := 1; i < len(s); i++ {
		if s[i-1].End >= s[i].Start {
			t.Fatal("ranges overlap or unsorted", s)
		}
	}
	s.remove(Range{0, 400})
	if len(s) != 0 {
		t.Fatal("remove", s)
	}
}

// Delivery after an idle period must not average throughput over the gap.
func TestPathRateRestartsAfterIdle(t *testing.T) {
	var p Path
	p.Generation = 1
	start := time.Unix(1000, 0)
	_, _ = p.Submitted(1000, start)
	_ = p.Feedback(Receipt{Generation: 1, Next: 500, ReceivedAt: 1_000_000, FirstNext: 100, FirstReceivedAt: 900_000}, start.Add(time.Millisecond))
	_ = p.Feedback(Receipt{Generation: 1, Next: 1000, ReceivedAt: 1_001_000}, start.Add(2*time.Millisecond))
	rate := p.Rate
	// Ten seconds idle, then another burst at the same delivery rate.
	_, _ = p.Submitted(1000, start.Add(10*time.Second))
	_ = p.Feedback(Receipt{Generation: 1, Next: 1500, ReceivedAt: 11_000_000}, start.Add(10*time.Second+time.Millisecond))
	_ = p.Feedback(Receipt{Generation: 1, Next: 2000, ReceivedAt: 11_001_000}, start.Add(10*time.Second+2*time.Millisecond))
	if p.Rate < rate/2 {
		t.Fatalf("idle gap collapsed the delivery rate: %.0f -> %.0f", rate, p.Rate)
	}
}

func TestBufferHeadroom(t *testing.T) {
	buffer := NewBufferWithHeadroom(make([]byte, 64), 32, 16, nil)
	copy(buffer.Data, "0123456789abcdef")
	contiguous, ok := buffer.TakeHeadroom(buffer.Data[:8], 29)
	if !ok || len(contiguous) != 37 || &contiguous[29] != &buffer.Data[0] {
		t.Fatal("headroom does not directly precede the payload")
	}
	if _, ok = buffer.TakeHeadroom(buffer.Data, 29); ok {
		t.Fatal("two writers shared one headroom")
	}
	buffer.ReturnHeadroom()
	if _, ok = buffer.TakeHeadroom(buffer.Data[4:], 29); ok {
		t.Fatal("headroom used for a payload that does not begin the buffer")
	}
	if _, ok = buffer.TakeHeadroom(buffer.Data, 33); ok {
		t.Fatal("header larger than the headroom")
	}
	plain := NewBuffer(make([]byte, 8), nil)
	if _, ok = plain.TakeHeadroom(plain.Data, 1); ok {
		t.Fatal("buffer without headroom offered one")
	}
}

func TestPathRemainingDelivery(t *testing.T) {
	cold := Path{Generation: 1}
	_, _ = cold.Submitted(128<<10, time.Now())
	// Without a rate sample 128 KiB need four slow-start rounds of 100 ms.
	if got := cold.RemainingDelivery(128<<10, time.Now(), 100*time.Millisecond, time.Now()); got < 0.4 || got > 0.5 {
		t.Fatalf("cold path estimate %.3f s", got)
	}
	warm := Path{Generation: 1, Rate: 1 << 20, MinimumRTT: 20 * time.Millisecond}
	_, _ = warm.Submitted(256<<10, time.Now())
	if got := warm.RemainingDelivery(256<<10, time.Now(), 0, time.Now()); got < 0.25 || got > 0.27 {
		t.Fatalf("warm path estimate %.3f s", got)
	}
	if got := warm.RemainingDelivery(0, time.Now(), 0, time.Now()); got != 0 {
		t.Fatalf("delivered bytes still pending: %.3f", got)
	}
}

func TestPathMinimumRTTExpires(t *testing.T) {
	now := time.Unix(100, 0)
	p := Path{Generation: 1}
	sample := func(rtt time.Duration) {
		_, _ = p.Submitted(1000, now)
		now = now.Add(rtt)
		_ = p.Feedback(Receipt{Generation: 1, Next: p.Sent}, now)
	}
	sample(10 * time.Millisecond)
	for range 5 {
		sample(300 * time.Millisecond)
	}
	if p.MinimumRTT != 10*time.Millisecond {
		t.Fatalf("minimum replaced inside its window: %v", p.MinimumRTT)
	}
	for range 40 {
		sample(300 * time.Millisecond)
	}
	if p.MinimumRTT != 300*time.Millisecond {
		t.Fatalf("stale minimum kept after the path's delay grew: %v", p.MinimumRTT)
	}
}

func TestPathObservedDelayBoundsEstimates(t *testing.T) {
	now := time.Now()
	// Bursts suggest 100 MB/s, but frames have been taking 600 ms.
	p := Path{Generation: 1, Rate: 100 << 20, SRTT: 610 * time.Millisecond, recentRTT: 610 * time.Millisecond, MinimumRTT: 20 * time.Millisecond}
	_, _ = p.Submitted(64<<10, now)
	if got := p.CompletionTime(64<<10, 0, 0, now); got < 0.59 {
		t.Fatalf("queued path completion ignores observed delay: %.3f s", got)
	}
	if got := p.RemainingDelivery(64<<10, now.Add(-500*time.Millisecond), 0, now); got < 0.09 || got > 0.11 {
		t.Fatalf("remaining delivery ignores elapsed time: %.3f s", got)
	}
	idle := Path{Generation: 1, Rate: 100 << 20, SRTT: 610 * time.Millisecond, MinimumRTT: 20 * time.Millisecond}
	if got := idle.CompletionTime(64<<10, 0, 0, now); got > 0.02 {
		t.Fatalf("empty path keeps old queueing delay: %.3f s", got)
	}
}

// Frames that sat out an outage must not keep a recovered path looking slow.
func TestPathObservedDelayRecovers(t *testing.T) {
	now := time.Unix(100, 0)
	p := Path{Generation: 1}
	sample := func(rtt time.Duration) {
		_, _ = p.Submitted(1000, now)
		now = now.Add(rtt)
		_ = p.Feedback(Receipt{Generation: 1, Next: p.Sent}, now)
	}
	for range 10 {
		sample(10 * time.Millisecond)
	}
	sample(2 * time.Second)
	if p.observedDelay() < 0.9 {
		t.Fatalf("outage sample ignored: %.3f s", p.observedDelay())
	}
	for range 4 {
		sample(10 * time.Millisecond)
	}
	if got := p.observedDelay(); got > 0.15 {
		t.Fatalf("recovered path still looks slow after four samples: %.3f s", got)
	}
}

func TestPathRateSurvivesOutage(t *testing.T) {
	p := Path{Generation: 1}
	start := time.Unix(1000, 0)
	_, _ = p.Submitted(100<<20, start)
	var received, stamp uint64 = 0, 1_000_000
	for i := 0; i < 100; i++ {
		received += 50_000
		stamp += 1000
		_ = p.Feedback(Receipt{Generation: 1, Next: received, ReceivedAt: stamp}, start.Add(time.Duration(i+10)*time.Millisecond))
	}
	rate := p.Rate
	// Nothing arrives for two seconds; then the path delivers again.
	received += 100_000
	stamp += 2_000_000
	_ = p.Feedback(Receipt{Generation: 1, Next: received, ReceivedAt: stamp}, start.Add(2200*time.Millisecond))
	if p.Rate < rate/2 {
		t.Fatalf("outage collapsed the delivery rate: %.0f -> %.0f", rate, p.Rate)
	}
	received += 50_000
	stamp += 1000
	_ = p.Feedback(Receipt{Generation: 1, Next: received, ReceivedAt: stamp}, start.Add(2201*time.Millisecond))
	if p.Rate < rate/2 {
		t.Fatalf("delivery after the outage collapsed the rate: %.0f -> %.0f", rate, p.Rate)
	}
}

func TestPathProbeRaisesCollapsedRate(t *testing.T) {
	p := Path{Generation: 1}
	start := time.Unix(1000, 0)
	_, _ = p.Submitted(65536, start)
	_ = p.Feedback(Receipt{Generation: 1, Next: 65536, ReceivedAt: 1_000_000, FirstNext: 1, FirstReceivedAt: 400_000}, start.Add(10*time.Millisecond))
	p.Rate = 100_000 // collapsed estimate
	// One frame at a time: each starts from idle and arrives in one receipt.
	at := start.Add(100 * time.Millisecond)
	for i := 0; i < 3; i++ {
		_, _ = p.Submitted(65536, at)
		_ = p.Feedback(Receipt{Generation: 1, Next: p.Sent, ReceivedAt: uint64(1_100_000 + i*20_000)}, at.Add(14*time.Millisecond))
		at = at.Add(20 * time.Millisecond)
	}
	if p.Rate < 4_000_000 {
		t.Fatalf("frames delivered in 14 ms left the rate at %.0f B/s", p.Rate)
	}
}
