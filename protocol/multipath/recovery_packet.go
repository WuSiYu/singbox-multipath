package multipath

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing/common/buf"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/common/pipe"
)

const (
	recoveryUDPHeader = 64
	recoveryFragment  = 900
	recoveryUDPPing   = 1
	recoveryUDPPong   = 2
	recoveryUDPData   = 3
	recoveryUDPClose  = 4
)

// recoveryAssemblySlots bounds incomplete datagrams per association.
const recoveryAssemblySlots = 4

// Recovery UDP is datagram transport, never a reliable MP DATA stream. Small
// fragments avoid depending on outer IP fragmentation; incomplete packets are
// discarded without retransmission or head-of-line blocking.
type recoveryDatagram struct {
	kind, leg      byte
	group, session [16]byte
	epoch          uint64
	mask, path     byte
	seq            uint64
	offset, total  uint16
	address        M.Socksaddr
	data           []byte
}

func (d recoveryDatagram) encode() ([]byte, error) {
	address := ""
	if d.kind == recoveryUDPData {
		address = d.address.String()
	}
	if len(address) > 256 || len(d.data) > recoveryFragment {
		return nil, errors.New("invalid recovery datagram length")
	}
	p := make([]byte, recoveryUDPHeader+len(address)+len(d.data))
	copy(p, "SMU6")
	p[4], p[5] = d.kind, d.leg
	copy(p[6:22], d.group[:])
	copy(p[22:38], d.session[:])
	binary.BigEndian.PutUint64(p[38:46], d.epoch)
	p[46], p[47] = d.mask, d.path
	binary.BigEndian.PutUint64(p[48:56], d.seq)
	binary.BigEndian.PutUint16(p[56:58], d.offset)
	binary.BigEndian.PutUint16(p[58:60], d.total)
	binary.BigEndian.PutUint16(p[60:62], uint16(len(address)))
	copy(p[64:], address)
	copy(p[64+len(address):], d.data)
	return p, nil
}

func decodeRecoveryDatagram(p []byte) (d recoveryDatagram, err error) {
	if len(p) < recoveryUDPHeader || string(p[:4]) != "SMU6" || p[5] > 1 || p[46] > 3 || p[47] > 1 || p[62] != 0 || p[63] != 0 {
		return d, errors.New("invalid recovery datagram")
	}
	d.kind, d.leg = p[4], p[5]
	copy(d.group[:], p[6:22])
	copy(d.session[:], p[22:38])
	d.epoch = binary.BigEndian.Uint64(p[38:46])
	d.mask, d.path = p[46], p[47]
	d.seq = binary.BigEndian.Uint64(p[48:56])
	d.offset = binary.BigEndian.Uint16(p[56:58])
	d.total = binary.BigEndian.Uint16(p[58:60])
	n := int(binary.BigEndian.Uint16(p[60:62]))
	if n > 256 || 64+n > len(p) {
		return d, errors.New("invalid recovery address")
	}
	d.data = p[64+n:]
	if d.kind != recoveryUDPData {
		if d.kind < recoveryUDPPing || d.kind > recoveryUDPClose || n != 0 || len(d.data) != 0 || d.offset != 0 || d.total != 0 {
			return d, errors.New("invalid recovery control")
		}
		return d, nil
	}
	d.address = M.ParseSocksaddr(string(p[64 : 64+n]))
	if !d.address.IsValid() || d.address.Port == 0 || d.total > 65507 || int(d.offset) > int(d.total) || int(d.offset)%recoveryFragment != 0 || len(d.data) != min(recoveryFragment, int(d.total)-int(d.offset)) || (d.total > 0 && d.offset == d.total) {
		return d, errors.New("invalid recovery fragment")
	}
	return d, nil
}

type recoveryAssembly struct {
	data      []byte
	address   M.Socksaddr
	seen      [73]bool
	remaining int
	at        time.Time
	legBytes  [2]int
}

type recoveryPacketConn struct {
	id                          [16]byte
	memory                      *memoryBudget
	mu                          sync.Mutex
	parts                       map[uint64]*recoveryAssembly
	recent                      [128]uint64
	highest                     uint64
	queue                       chan *recoveryAssembly
	done                        chan struct{}
	once                        sync.Once
	readDeadline, writeDeadline pipe.Deadline
	send                        func(recoveryDatagram) error
	onClose                     func()
	onReceive                   func(byte, int)
	sequence                    atomic.Uint64
	lastActivity                atomic.Int64
}

func newRecoveryPacketConn(id [16]byte, memory *memoryBudget, send func(recoveryDatagram) error, closeFunc func()) (*recoveryPacketConn, error) {
	if !memory.reservePage(4096, true) {
		return nil, errMemoryLimit
	}
	c := &recoveryPacketConn{id: id, memory: memory, parts: make(map[uint64]*recoveryAssembly), queue: make(chan *recoveryAssembly, 32), done: make(chan struct{}), readDeadline: pipe.MakeDeadline(), writeDeadline: pipe.MakeDeadline(), send: send, onClose: closeFunc}
	c.lastActivity.Store(time.Now().UnixNano())
	return c, nil
}

func (c *recoveryPacketConn) deliver(d recoveryDatagram, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.done:
		return
	default:
	}
	// recent remembers one sequence per slot, so only the last 128 numbers
	// can be told apart; highest-128 already shares highest's slot.
	if d.seq == 0 || c.recent[d.seq%128] == d.seq || (c.highest >= 128 && d.seq <= c.highest-128) {
		return
	}
	c.expireLocked(now)
	p := c.parts[d.seq]
	if p == nil {
		// A datagram that fits one fragment completes at once and needs no
		// slot. Otherwise the oldest incomplete datagram gives way: one whose
		// fragment was lost must not block later datagrams until it expires.
		if len(c.parts) >= recoveryAssemblySlots && (d.offset != 0 || len(d.data) != int(d.total)) {
			c.evictOldestLocked()
		}
		storage := c.memory.tryAcquireOther(max(1, int(d.total)) + 512)
		if storage == nil {
			return
		}
		p = &recoveryAssembly{data: storage[:int(d.total)], address: d.address, remaining: max(1, (int(d.total)+recoveryFragment-1)/recoveryFragment), at: now}
		c.parts[d.seq] = p
	}
	if len(p.data) != int(d.total) || p.address != d.address {
		return
	}
	index := int(d.offset) / recoveryFragment
	if p.seen[index] {
		return
	}
	p.seen[index] = true
	p.remaining--
	p.legBytes[d.leg] += len(d.data)
	copy(p.data[int(d.offset):], d.data)
	if p.remaining != 0 {
		return
	}
	delete(c.parts, d.seq)
	c.highest = max(c.highest, d.seq)
	c.recent[d.seq%128] = d.seq
	c.lastActivity.Store(now.UnixNano())
	select {
	case c.queue <- p:
		if c.onReceive != nil {
			for id, n := range p.legBytes {
				c.onReceive(byte(id), n)
			}
		}
	default:
		c.memory.releaseOther(p.data)
	}
}

func (c *recoveryPacketConn) evictOldestLocked() {
	var oldest uint64
	var found *recoveryAssembly
	for seq, p := range c.parts {
		if found == nil || p.at.Before(found.at) || p.at.Equal(found.at) && seq < oldest {
			oldest, found = seq, p
		}
	}
	if found != nil {
		c.memory.releaseOther(found.data)
		delete(c.parts, oldest)
	}
}

func (c *recoveryPacketConn) expireLocked(now time.Time) {
	for id, p := range c.parts {
		if now.Sub(p.at) >= 5*time.Second {
			c.memory.releaseOther(p.data)
			delete(c.parts, id)
		}
	}
}
func (c *recoveryPacketConn) expire(now time.Time) { c.mu.Lock(); c.expireLocked(now); c.mu.Unlock() }

func (c *recoveryPacketConn) read() (*recoveryAssembly, error) {
	select {
	case <-c.done:
		return nil, net.ErrClosed
	default:
	}
	select {
	case p := <-c.queue:
		return p, nil
	case <-c.done:
		return nil, net.ErrClosed
	case <-c.readDeadline.Wait():
		return nil, os.ErrDeadlineExceeded
	}
}
func (c *recoveryPacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	p, err := c.read()
	if err != nil {
		return 0, nil, err
	}
	defer c.memory.releaseOther(p.data)
	return copy(b, p.data), p.address.UDPAddr(), nil
}
func (c *recoveryPacketConn) ReadPacket(b *buf.Buffer) (M.Socksaddr, error) {
	p, err := c.read()
	if err != nil {
		return M.Socksaddr{}, err
	}
	defer c.memory.releaseOther(p.data)
	if b.FreeLen() < len(p.data) {
		return M.Socksaddr{}, io.ErrShortBuffer
	}
	_, err = b.Write(p.data)
	return p.address, err
}
func (c *recoveryPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	if len(p) > 65507 {
		return 0, errors.New("UDP payload exceeds 65507 bytes")
	}
	select {
	case <-c.done:
		return 0, net.ErrClosed
	case <-c.writeDeadline.Wait():
		return 0, os.ErrDeadlineExceeded
	default:
	}
	d := recoveryDatagram{kind: recoveryUDPData, session: c.id, seq: c.sequence.Add(1), total: uint16(len(p)), address: M.SocksaddrFromNet(addr)}
	if !d.address.IsValid() || d.address.Port == 0 {
		return 0, errors.New("invalid UDP destination")
	}
	for offset := 0; ; offset += recoveryFragment {
		d.offset = uint16(offset)
		d.data = p[offset:min(len(p), offset+recoveryFragment)]
		if err := c.send(d); err != nil {
			return 0, err
		}
		if offset+recoveryFragment >= len(p) {
			break
		}
	}
	c.lastActivity.Store(time.Now().UnixNano())
	return len(p), nil
}
func (c *recoveryPacketConn) WritePacket(b *buf.Buffer, addr M.Socksaddr) error {
	defer b.Release()
	_, err := c.WriteTo(b.Bytes(), addr)
	return err
}
func (c *recoveryPacketConn) Close() error {
	c.once.Do(func() {
		c.mu.Lock()
		close(c.done)
		for _, p := range c.parts {
			c.memory.releaseOther(p.data)
		}
		clear(c.parts)
		for {
			select {
			case p := <-c.queue:
				c.memory.releaseOther(p.data)
			default:
				c.mu.Unlock()
				c.memory.releaseSession(4096)
				if c.onClose != nil {
					c.onClose()
				}
				return
			}
		}
	})
	return nil
}
func (c *recoveryPacketConn) LocalAddr() net.Addr                { return &net.UDPAddr{} }
func (c *recoveryPacketConn) SetReadDeadline(t time.Time) error  { c.readDeadline.Set(t); return nil }
func (c *recoveryPacketConn) SetWriteDeadline(t time.Time) error { c.writeDeadline.Set(t); return nil }
func (c *recoveryPacketConn) SetDeadline(t time.Time) error {
	c.readDeadline.Set(t)
	c.writeDeadline.Set(t)
	return nil
}
