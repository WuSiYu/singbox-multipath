package multipath

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	frameTypeData   byte = 1
	frameHeaderSize      = 13 // type(1) + seq(8) + len(4)
	maxFramePayload      = 1 << 20
)

var errCoreClosed = errors.New("multipath core closed")

type coreConfig struct {
	ChunkSize        int
	QueueFrames      int
	ThresholdBytesPS uint64
	ActivationWindow time.Duration
	BandwidthMbps    []uint32
	MaxReorderFrames int
	OnActivate       func()
}

type dataFrame struct {
	seq  uint64
	data []byte
}

type mpLeg struct {
	id      uint8
	conn    net.Conn
	send    chan dataFrame
	onClose func(error)
	once    sync.Once
}

func (l *mpLeg) close(err error) {
	l.once.Do(func() {
		_ = l.conn.Close()
		if l.onClose != nil {
			l.onClose(err)
		}
	})
}

type mpCore struct {
	cfg      coreConfig
	appConn  net.Conn
	pipeConn net.Conn

	legsMu sync.RWMutex
	legs   map[uint8]*mpLeg

	incoming chan dataFrame
	done     chan struct{}
	closeErr atomic.Value
	closeOne sync.Once

	txSeq atomic.Uint64

	ingressBytes atomic.Uint64
	active       atomic.Bool
	activeCh     chan struct{}
	activateOnce sync.Once

	bufferPool sync.Pool
}

func newCore(cfg coreConfig) (*mpCore, net.Conn) {
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = 64 * 1024
	}
	if cfg.QueueFrames <= 0 {
		cfg.QueueFrames = 256
	}
	if cfg.ActivationWindow <= 0 {
		cfg.ActivationWindow = time.Second
	}
	if cfg.MaxReorderFrames <= 0 {
		cfg.MaxReorderFrames = 2048
	}
	appConn, pipeConn := net.Pipe()
	c := &mpCore{
		cfg:      cfg,
		appConn:  appConn,
		pipeConn: pipeConn,
		legs:     make(map[uint8]*mpLeg),
		incoming: make(chan dataFrame, cfg.QueueFrames*2),
		done:     make(chan struct{}),
		activeCh: make(chan struct{}),
	}
	c.bufferPool.New = func() any {
		return make([]byte, cfg.ChunkSize)
	}
	if cfg.ThresholdBytesPS == 0 {
		c.activate()
	}
	go c.txLoop()
	go c.rxLoop()
	go c.activationLoop()
	return c, appConn
}

func (c *mpCore) addLeg(id uint8, conn net.Conn, onClose func(error)) error {
	c.legsMu.Lock()
	if _, exists := c.legs[id]; exists {
		c.legsMu.Unlock()
		return errors.New("duplicate multipath leg")
	}
	leg := &mpLeg{
		id:      id,
		conn:    conn,
		send:    make(chan dataFrame, c.cfg.QueueFrames),
		onClose: onClose,
	}
	c.legs[id] = leg
	c.legsMu.Unlock()
	go c.legWriteLoop(leg)
	go c.legReadLoop(leg)
	return nil
}

func (c *mpCore) AppConn() net.Conn {
	return c.appConn
}

func (c *mpCore) Done() <-chan struct{} {
	return c.done
}

func (c *mpCore) Close() error {
	c.fail(io.EOF)
	return nil
}

func (c *mpCore) fail(err error) {
	if err == nil {
		err = errCoreClosed
	}
	c.closeOne.Do(func() {
		c.closeErr.Store(err)
		close(c.done)
		_ = c.pipeConn.Close()
		_ = c.appConn.Close()
		c.legsMu.RLock()
		legs := make([]*mpLeg, 0, len(c.legs))
		for _, leg := range c.legs {
			legs = append(legs, leg)
		}
		c.legsMu.RUnlock()
		for _, leg := range legs {
			leg.close(err)
		}
	})
}

func (c *mpCore) getBuffer() []byte {
	return c.bufferPool.Get().([]byte)
}

func (c *mpCore) putBuffer(buffer []byte) {
	if cap(buffer) < c.cfg.ChunkSize {
		return
	}
	c.bufferPool.Put(buffer[:c.cfg.ChunkSize])
}

func (c *mpCore) txLoop() {
	for {
		buffer := c.getBuffer()
		n, err := c.pipeConn.Read(buffer[:c.cfg.ChunkSize])
		if n > 0 {
			c.ingressBytes.Add(uint64(n))
			frame := dataFrame{
				seq:  c.txSeq.Add(1) - 1,
				data: buffer[:n],
			}
			if enqueueErr := c.enqueue(frame); enqueueErr != nil {
				c.putBuffer(buffer)
				c.fail(enqueueErr)
				return
			}
		} else {
			c.putBuffer(buffer)
		}
		if err != nil {
			c.fail(err)
			return
		}
	}
}

func (c *mpCore) activationLoop() {
	if c.active.Load() {
		return
	}
	interval := c.cfg.ActivationWindow / 10
	if interval < 50*time.Millisecond {
		interval = 50 * time.Millisecond
	}
	if interval > 200*time.Millisecond {
		interval = 200 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	windowStart := time.Now()
	windowBase := c.ingressBytes.Load()
	var queueHighSince time.Time
	for {
		select {
		case <-c.done:
			return
		case now := <-ticker.C:
			if c.active.Load() {
				return
			}
			if now.Sub(windowStart) >= c.cfg.ActivationWindow {
				bytesNow := c.ingressBytes.Load()
				delta := bytesNow - windowBase
				elapsed := now.Sub(windowStart)
				if elapsed > 0 && uint64(float64(delta)/elapsed.Seconds()) >= c.cfg.ThresholdBytesPS {
					c.activate()
					return
				}
				windowStart = now
				windowBase = bytesNow
			}
			primary := c.getLeg(0)
			if primary == nil {
				continue
			}
			if len(primary.send)*5 >= cap(primary.send)*4 {
				if queueHighSince.IsZero() {
					queueHighSince = now
				} else if now.Sub(queueHighSince) >= c.cfg.ActivationWindow {
					c.activate()
					return
				}
			} else {
				queueHighSince = time.Time{}
			}
		}
	}
}

func (c *mpCore) activate() {
	c.activateOnce.Do(func() {
		c.active.Store(true)
		close(c.activeCh)
		if c.cfg.OnActivate != nil {
			c.cfg.OnActivate()
		}
	})
}

func (c *mpCore) getLeg(id uint8) *mpLeg {
	c.legsMu.RLock()
	leg := c.legs[id]
	c.legsMu.RUnlock()
	return leg
}

func (c *mpCore) availableLegs() []*mpLeg {
	c.legsMu.RLock()
	legs := make([]*mpLeg, 0, len(c.legs))
	for _, leg := range c.legs {
		legs = append(legs, leg)
	}
	c.legsMu.RUnlock()
	return legs
}

func (c *mpCore) weightFor(id uint8) uint32 {
	if int(id) < len(c.cfg.BandwidthMbps) && c.cfg.BandwidthMbps[id] > 0 {
		return c.cfg.BandwidthMbps[id]
	}
	return 1
}

func (c *mpCore) chooseLeg(active bool) *mpLeg {
	if !active {
		return c.getLeg(0)
	}
	legs := c.availableLegs()
	var best *mpLeg
	var bestNum, bestDen uint64
	for _, leg := range legs {
		weight := uint64(c.weightFor(leg.id))
		num := uint64(len(leg.send) + 1)
		if best == nil || num*bestDen < bestNum*weight {
			best = leg
			bestNum = num
			bestDen = weight
		}
	}
	return best
}

func (c *mpCore) enqueue(frame dataFrame) error {
	for {
		select {
		case <-c.done:
			return errCoreClosed
		default:
		}
		active := c.active.Load()
		leg := c.chooseLeg(active)
		if leg == nil {
			select {
			case <-c.done:
				return errCoreClosed
			case <-time.After(5 * time.Millisecond):
				continue
			}
		}
		select {
		case leg.send <- frame:
			return nil
		default:
		}
		if !active {
			select {
			case leg.send <- frame:
				return nil
			case <-c.activeCh:
				continue
			case <-c.done:
				return errCoreClosed
			}
		}
		// Work-conserving fallback: if the predicted-best leg is full, try any
		// other active leg before blocking.
		for _, other := range c.availableLegs() {
			if other == leg {
				continue
			}
			select {
			case other.send <- frame:
				return nil
			default:
			}
		}
		select {
		case leg.send <- frame:
			return nil
		case <-c.done:
			return errCoreClosed
		case <-time.After(time.Millisecond):
		}
	}
}

func (c *mpCore) legWriteLoop(leg *mpLeg) {
	for {
		select {
		case <-c.done:
			return
		case frame := <-leg.send:
			err := writeDataFrame(leg.conn, frame)
			c.putBuffer(frame.data)
			if err != nil {
				leg.close(err)
				c.fail(err)
				return
			}
		}
	}
}

func (c *mpCore) legReadLoop(leg *mpLeg) {
	for {
		frame, err := readDataFrame(leg.conn, c)
		if err != nil {
			leg.close(err)
			c.fail(err)
			return
		}
		select {
		case c.incoming <- frame:
		case <-c.done:
			c.putBuffer(frame.data)
			return
		}
	}
}

func (c *mpCore) rxLoop() {
	expected := uint64(0)
	pending := make(map[uint64]dataFrame)
	for {
		select {
		case <-c.done:
			for _, frame := range pending {
				c.putBuffer(frame.data)
			}
			return
		case frame := <-c.incoming:
			if frame.seq < expected {
				c.putBuffer(frame.data)
				continue
			}
			if frame.seq > expected {
				if _, exists := pending[frame.seq]; exists {
					c.putBuffer(frame.data)
					continue
				}
				if len(pending) >= c.cfg.MaxReorderFrames {
					c.putBuffer(frame.data)
					c.fail(errors.New("multipath reorder buffer exceeded"))
					return
				}
				pending[frame.seq] = frame
				continue
			}
			for {
				if err := writeAll(c.pipeConn, frame.data); err != nil {
					c.putBuffer(frame.data)
					c.fail(err)
					return
				}
				c.putBuffer(frame.data)
				expected++
				next, exists := pending[expected]
				if !exists {
					break
				}
				delete(pending, expected)
				frame = next
			}
		}
	}
}

func writeDataFrame(conn net.Conn, frame dataFrame) error {
	var header [frameHeaderSize]byte
	header[0] = frameTypeData
	binary.BigEndian.PutUint64(header[1:9], frame.seq)
	binary.BigEndian.PutUint32(header[9:13], uint32(len(frame.data)))
	buffers := net.Buffers{header[:], frame.data}
	_, err := buffers.WriteTo(conn)
	return err
}

func readDataFrame(conn net.Conn, core *mpCore) (dataFrame, error) {
	var header [frameHeaderSize]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return dataFrame{}, err
	}
	if header[0] != frameTypeData {
		return dataFrame{}, errors.New("unknown multipath frame type")
	}
	seq := binary.BigEndian.Uint64(header[1:9])
	length := int(binary.BigEndian.Uint32(header[9:13]))
	if length < 0 || length > maxFramePayload {
		return dataFrame{}, errors.New("invalid multipath frame length")
	}
	var buffer []byte
	if length <= core.cfg.ChunkSize {
		buffer = core.getBuffer()[:length]
	} else {
		buffer = make([]byte, length)
	}
	if _, err := io.ReadFull(conn, buffer); err != nil {
		core.putBuffer(buffer)
		return dataFrame{}, err
	}
	return dataFrame{seq: seq, data: buffer}, nil
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
