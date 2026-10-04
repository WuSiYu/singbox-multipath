package stream

import (
	"math"
	"sync/atomic"
)

// Buffer ownership is shared only by the connection send queue and active
// writers. ACK processing must not recycle bytes referenced by a blocked Write.
type Buffer struct {
	Data    []byte
	refs    int
	release func()

	// raw[:headroom] directly precedes Data. One writer at a time may place a
	// frame header there to send header and payload in a single child write.
	raw      []byte
	headroom int
	headBusy atomic.Bool
}

func NewBuffer(data []byte, release func()) *Buffer {
	return &Buffer{Data: data, refs: 1, release: release}
}

// NewBufferWithHeadroom uses raw[headroom:headroom+length] as Data.
func NewBufferWithHeadroom(raw []byte, headroom, length int, release func()) *Buffer {
	return &Buffer{Data: raw[headroom : headroom+length], refs: 1, release: release, raw: raw, headroom: headroom}
}

// TakeHeadroom returns header bytes immediately followed by data, when data
// begins this buffer and no other writer holds the headroom. The caller
// fills the first header bytes and calls ReturnHeadroom after the write.
func (b *Buffer) TakeHeadroom(data []byte, header int) ([]byte, bool) {
	if header > b.headroom || len(data) == 0 || len(b.Data) == 0 || &data[0] != &b.Data[0] || !b.headBusy.CompareAndSwap(false, true) {
		return nil, false
	}
	return b.raw[b.headroom-header : b.headroom+len(data)], true
}

func (b *Buffer) ReturnHeadroom() { b.headBusy.Store(false) }

func (b *Buffer) Retain() {
	if b.refs <= 0 {
		panic("multipath: retaining released buffer")
	}
	b.refs++
}
func (b *Buffer) Release() {
	if b.refs <= 0 {
		panic("multipath: releasing unowned buffer")
	}
	b.refs--
	if b.refs == 0 && b.release != nil {
		b.release()
	}
}

type Segment struct {
	Seq    uint64
	Buffer *Buffer
	Offset int
	Length int
}

func (s Segment) End() uint64  { return s.Seq + uint64(s.Length) }
func (s Segment) Data() []byte { return s.Buffer.Data[s.Offset : s.Offset+s.Length] }

// Sender is the single connection-level retransmission queue. Path receipts
// deliberately have no API which removes its data.
type Sender struct {
	Una       uint64
	Next      uint64
	WriteNext uint64
	WindowEnd uint64
	FIN       uint64
	HasFIN    bool
	FINSent   bool
	FINAcked  bool
	segments  []Segment
	head      int
}

func NewSender(initialWindow uint64) *Sender { return &Sender{WindowEnd: initialWindow} }

// Append transfers the caller's one buffer reference on success only.
func (s *Sender) Append(buffer *Buffer) error {
	length := len(buffer.Data)
	if s.HasFIN || length == 0 || uint64(length) >= math.MaxUint64-s.WriteNext {
		return ErrSequence
	}
	s.segments = append(s.segments, Segment{Seq: s.WriteNext, Buffer: buffer, Length: length})
	s.WriteNext += uint64(length)
	return nil
}

func (s *Sender) CloseWrite() {
	if !s.HasFIN {
		s.FIN, s.HasFIN = s.WriteNext, true
	}
}

// Range borrows a subrange of one retained segment. A writer must Retain its
// buffer before releasing the connection lock and Release after Write returns.
func (s *Sender) Range(seq uint64, maxLength int) (Segment, bool) {
	if seq < s.Una || seq >= s.WriteNext || maxLength <= 0 {
		return Segment{}, false
	}
	lo, hi := s.head, len(s.segments)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if s.segments[mid].End() <= seq {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == len(s.segments) {
		return Segment{}, false
	}
	segment := s.segments[lo]
	if seq < segment.Seq {
		return Segment{}, false
	}
	offset := int(seq - segment.Seq)
	segment.Seq = seq
	segment.Offset += offset
	segment.Length = min(segment.Length-offset, maxLength)
	return segment, true
}

func (s *Sender) NextRange(maxLength int) (Segment, bool) {
	if s.Next >= s.WindowEnd {
		return Segment{}, false
	}
	return s.Range(s.Next, int(min(uint64(maxLength), s.WindowEnd-s.Next)))
}

func (s *Sender) Sent(segment Segment) error {
	if segment.Seq != s.Next || segment.Length <= 0 || segment.End() > s.WriteNext || segment.End() > s.WindowEnd {
		return ErrSequence
	}
	s.Next = segment.End()
	return nil
}

func (s *Sender) SendFIN() bool {
	if !s.HasFIN || s.FINSent || s.Next != s.FIN || s.Next >= s.WindowEnd {
		return false
	}
	s.FINSent = true
	s.Next++
	return true
}

// Acknowledge handles duplicate/reordered window updates without shrinking the
// right edge. Partial byte ACKs trim the queue without retaining frame indexes.
func (s *Sender) Acknowledge(next, windowEnd uint64) error {
	if next > s.Next || windowEnd < next {
		return ErrSequence
	}
	s.WindowEnd = max(s.WindowEnd, windowEnd)
	if next <= s.Una {
		return nil
	}
	s.Una = next
	for s.head < len(s.segments) {
		segment := &s.segments[s.head]
		if segment.Seq >= next {
			break
		}
		if segment.End() > next {
			consumed := int(next - segment.Seq)
			segment.Seq = next
			segment.Offset += consumed
			segment.Length -= consumed
			break
		}
		segment.Buffer.Release()
		*segment = Segment{}
		s.head++
	}
	if s.head == len(s.segments) {
		s.segments = s.segments[:0]
		if cap(s.segments) > 1024 {
			s.segments = nil
		}
		s.head = 0
	} else if s.head >= 1024 && s.head*2 >= len(s.segments) {
		n := copy(s.segments, s.segments[s.head:])
		clear(s.segments[n:])
		s.segments = s.segments[:n]
		s.head = 0
	}
	s.FINAcked = s.FINSent && next == s.FIN+1
	return nil
}

func (s *Sender) Buffered() uint64 {
	return s.WriteNext - min(s.Una, s.WriteNext)
}

func (s *Sender) Close() {
	for i := s.head; i < len(s.segments); i++ {
		s.segments[i].Buffer.Release()
	}
	s.segments = nil
	s.head = 0
}
