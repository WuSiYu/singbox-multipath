// Package stream implements the connection-level byte sequence space. It has
// no transport, timer, goroutine, or path-activation policy. Callers serialize
// state transitions; returned readable bytes remain immutable until Consume.
package stream

import (
	"errors"
	"math"
	"math/bits"
	"sync"
)

const (
	PageSize = 16 << 10
	// Charge the payload, the presence bitmap, and the page/map bookkeeping.
	PageCharge = PageSize + PageSize/8 + 256
)

// Range is a half-open byte interval of the connection sequence space.
type Range struct{ Start, End uint64 }

// maxDroppedRanges bounds the dropped-range set. When exceeded, the highest
// ranges are forgotten rather than merged: a merge would cover bytes that did
// arrive and turn one repair into a large duplicate. Forgotten ranges are
// still recovered by the sender's head and timeout repairs.
const maxDroppedRanges = 16

// rangeSet is a sorted list of disjoint ranges.
type rangeSet []Range

func (s *rangeSet) add(r Range) {
	if r.Start >= r.End {
		return
	}
	out := (*s)[:0:0]
	for _, x := range *s {
		if x.End < r.Start || x.Start > r.End {
			out = append(out, x)
			continue
		}
		r.Start, r.End = min(r.Start, x.Start), max(r.End, x.End)
	}
	index := len(out)
	for i, x := range out {
		if x.Start > r.Start {
			index = i
			break
		}
	}
	out = append(out, Range{})
	copy(out[index+1:], out[index:])
	out[index] = r
	if len(out) > maxDroppedRanges {
		out = out[:maxDroppedRanges]
	}
	*s = out
}

func (s *rangeSet) remove(r Range) {
	if len(*s) == 0 || r.Start >= r.End {
		return
	}
	out := (*s)[:0:0]
	for _, x := range *s {
		if x.End <= r.Start || x.Start >= r.End {
			out = append(out, x)
			continue
		}
		if x.Start < r.Start {
			out = append(out, Range{x.Start, r.Start})
		}
		if x.End > r.End {
			out = append(out, Range{r.End, x.End})
		}
	}
	*s = out
}

// NearHeadPages pages from the next expected byte are admitted like the head
// itself (using the emergency margin and evicting the farthest data): a whole
// repaired frame can then advance in-order delivery in one round trip.
const NearHeadPages = 16

var (
	ErrSequence = errors.New("invalid multipath byte sequence")
	ErrWindow   = errors.New("multipath data exceeds receive window")
	ErrFIN      = errors.New("inconsistent multipath data FIN")
)

// PageMemory is an admission interface, not a blocking allocator. Acquire must
// not wait: a missing head mapping and its controls may be behind this frame on
// the same reliable transport. A receiver can evict only unacknowledged pages.
// The caller reserves at least one head page independently of speculative data.
type PageMemory interface {
	Acquire(head bool) bool
	Release(head bool)
}

type receivePage struct {
	data    [PageSize]byte
	present [PageSize / 64]uint64
	count   int
	head    bool
}

// Pages are recycled: a 16 KiB allocation per arriving page otherwise
// dominates garbage collection at gigabit rates.
var pagePool = sync.Pool{New: func() any { return new(receivePage) }}

func newPage(head bool) *receivePage {
	page := pagePool.Get().(*receivePage)
	clear(page.present[:])
	page.count, page.head = 0, head
	return page
}

func freePage(page *receivePage) { pagePool.Put(page) }

// Receiver separates received-prefix acknowledgement from application reads.
// Its sparse, byte-addressed pages bound metadata even for one-byte frames.
// Individual path receipts do not belong to this state machine.
type Receiver struct {
	Next      uint64
	ReadNext  uint64
	WindowEnd uint64
	Capacity  uint64
	FIN       uint64
	HasFIN    bool
	Pruned    uint64
	Dropped   uint64
	MaxSeen   uint64 // highest byte end that arrived, including duplicates
	dropped   rangeSet
	buffered  uint64
	pages     map[uint64]*receivePage
	memory    PageMemory
}

func NewReceiver(capacity uint64, memory PageMemory) *Receiver {
	if capacity == 0 {
		capacity = PageSize
	}
	return &Receiver{WindowEnd: capacity, Capacity: capacity, pages: make(map[uint64]*receivePage), memory: memory}
}

func (r *Receiver) Ack() uint64 {
	if r.HasFIN && r.Next == r.FIN {
		return r.Next + 1
	}
	return r.Next
}

func (r *Receiver) Complete() bool { return r.HasFIN && r.Next == r.FIN }
func (r *Receiver) EOF() bool      { return r.HasFIN && r.ReadNext == r.FIN }

func (r *Receiver) SetFIN(seq uint64) error {
	if seq == math.MaxUint64 || seq < r.Next || seq >= r.WindowEnd || r.HasFIN && seq != r.FIN {
		return ErrFIN
	}
	for id, page := range r.pages {
		start := id * PageSize
		if start >= seq {
			if page.count != 0 {
				return ErrFIN
			}
			continue
		}
		if start+PageSize > seq {
			for offset := int(seq - start); offset < PageSize; {
				index, bit := offset/64, offset%64
				if page.present[index]&(math.MaxUint64<<bit) != 0 {
					return ErrFIN
				}
				offset = (index + 1) * 64
			}
		}
	}
	r.FIN, r.HasFIN = seq, true
	return nil
}

// Insert accepts only bytes not already received. A duplicate never overwrites
// data, including out-of-order data. Admission failures leave holes, not a false
// acknowledgement; the sender still owns every byte beyond Ack().
func (r *Receiver) Insert(seq uint64, data []byte) (int, error) {
	if len(data) == 0 || uint64(len(data)) > math.MaxUint64-seq {
		return 0, ErrSequence
	}
	end := seq + uint64(len(data))
	if r.HasFIN && end > r.FIN {
		return 0, ErrFIN
	}
	if end > r.WindowEnd {
		return 0, ErrWindow
	}
	r.MaxSeen = max(r.MaxSeen, end)
	if end <= r.Next {
		return 0, nil
	}
	if seq < r.Next {
		data = data[r.Next-seq:]
		seq = r.Next
	}
	accepted := 0
	for len(data) > 0 {
		id, offset := seq/PageSize, int(seq%PageSize)
		length := min(len(data), PageSize-offset)
		page := r.pages[id]
		if page == nil {
			head := id < r.Next/PageSize+NearHeadPages
			admitted := r.memory == nil || r.memory.Acquire(head)
			if !admitted && head {
				// Linux's receive-pressure rule: discard the farthest data
				// which have NOT been Data-ACKed, then admit the missing head.
				if r.pruneTail(id) {
					admitted = r.memory.Acquire(head)
				}
			}
			if admitted {
				page = newPage(head)
				r.pages[id] = page
			}
		}
		if page == nil {
			r.Dropped += uint64(length)
			r.dropped.add(Range{seq, seq + uint64(length)})
		} else {
			added := page.insert(offset, data[:length])
			accepted += added
			r.buffered += uint64(added)
			if len(r.dropped) > 0 {
				r.dropped.remove(Range{seq, seq + uint64(length)})
			}
		}
		seq += uint64(length)
		data = data[length:]
		r.advance()
	}
	return accepted, nil
}

func (p *receivePage) insert(offset int, data []byte) int {
	added := 0
	for len(data) > 0 {
		word, bit := offset/64, offset%64
		n := min(len(data), 64-bit)
		mask := (uint64(1)<<n - 1) << bit
		if n == 64 {
			mask = math.MaxUint64
		}
		fresh := mask &^ p.present[word]
		if fresh == mask {
			copy(p.data[offset:offset+n], data[:n])
			added += n
		} else if fresh != 0 {
			for todo := fresh; todo != 0; todo &= todo - 1 {
				index := bits.TrailingZeros64(todo) - bit
				p.data[offset+index] = data[index]
				added++
			}
		}
		p.present[word] |= mask
		offset += n
		data = data[n:]
	}
	p.count += added
	return added
}

func (r *Receiver) advance() {
	for {
		page := r.pages[r.Next/PageSize]
		if page == nil {
			return
		}
		offset := int(r.Next % PageSize)
		if page.count == PageSize {
			r.Next += uint64(PageSize - offset)
			continue
		}
		word, bit := offset/64, offset%64
		missing := ^page.present[word] & (math.MaxUint64 << bit)
		if missing != 0 {
			r.Next += uint64(bits.TrailingZeros64(missing) - bit)
			return
		}
		r.Next += uint64(64 - bit)
	}
}

// Readable returns at most one page. Insert will not modify the returned range;
// its presence bits are already set. Consume is called only after the reader
// finishes using this slice (including partial writes to the application).
func (r *Receiver) Readable() []byte {
	if r.ReadNext == r.Next {
		return nil
	}
	page := r.pages[r.ReadNext/PageSize]
	if page == nil {
		panic("multipath: acknowledged receive page lost")
	}
	offset := int(r.ReadNext % PageSize)
	n := int(min(uint64(PageSize-offset), r.Next-r.ReadNext))
	return page.data[offset : offset+n]
}

func (r *Receiver) Consume(length int) {
	if length < 0 || uint64(length) > r.Next-r.ReadNext {
		panic("multipath: invalid receive consumption")
	}
	old := r.ReadNext
	r.ReadNext += uint64(length)
	r.buffered -= uint64(length)
	for id := old / PageSize; id < r.ReadNext/PageSize; id++ {
		if page := r.pages[id]; page != nil {
			delete(r.pages, id)
			if r.memory != nil {
				r.memory.Release(page.head)
			}
			freePage(page)
		}
	}
}

// Advertise slides the window to ReadNext+target, bounded by Capacity. The
// right edge never retracts. Storage is allocated lazily as bytes arrive.
func (r *Receiver) Advertise(target uint64) uint64 {
	target = min(target, r.Capacity)
	end := uint64(math.MaxUint64)
	if target <= math.MaxUint64-r.ReadNext {
		end = r.ReadNext + target
	}
	r.WindowEnd = max(r.WindowEnd, end)
	return r.WindowEnd
}

func (r *Receiver) pruneTail(keep uint64) bool {
	var last uint64
	found := false
	for id := range r.pages {
		if id != keep && id >= r.Next/PageSize+NearHeadPages && (!found || id > last) {
			last, found = id, true
		}
	}
	if !found {
		return false
	}
	page := r.pages[last]
	start := last * PageSize
	r.dropped.add(Range{max(start, r.Next), min(start+PageSize, r.MaxSeen)})
	r.Pruned += uint64(page.count)
	r.buffered -= uint64(page.count)
	delete(r.pages, last)
	if r.memory != nil {
		r.memory.Release(page.head)
	}
	freePage(page)
	return true
}

// DroppedRanges returns up to n of the lowest byte ranges above Next that this
// receiver refused or evicted for lack of memory, ending no later than limit.
// The sender repairs them without waiting for a timeout.
func (r *Receiver) DroppedRanges(n int, limit uint64) []Range {
	if len(r.dropped) == 0 {
		return nil
	}
	r.dropped.remove(Range{0, r.Next})
	var out []Range
	for _, x := range r.dropped {
		if len(out) == n || x.Start >= limit {
			break
		}
		x.End = min(x.End, limit)
		out = append(out, x)
	}
	return out
}

// HeadPageEnd is the end of the page holding the next expected byte. Bytes
// below it are always admitted, even under memory pressure.
func (r *Receiver) HeadPageEnd() uint64 { return (r.Next/PageSize + 1) * PageSize }

func (r *Receiver) Buffered() (bytes, outOfOrder uint64, pages int) {
	if len(r.pages) == 0 {
		return 0, 0, 0
	}
	bytes = r.buffered
	outOfOrder = bytes - (r.Next - r.ReadNext)
	pages = len(r.pages) - int(r.Next/PageSize-r.ReadNext/PageSize)
	if page := r.pages[r.Next/PageSize]; page != nil && page.count == int(r.Next%PageSize) {
		pages--
	}
	return bytes, outOfOrder, pages
}

func (r *Receiver) Close() {
	for id, page := range r.pages {
		delete(r.pages, id)
		if r.memory != nil {
			r.memory.Release(page.head)
		}
		freePage(page)
	}
	r.buffered = 0
}
