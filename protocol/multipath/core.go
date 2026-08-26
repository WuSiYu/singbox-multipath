package multipath

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	frameTypeData  byte = 1
	frameTypeACK   byte = 2
	frameTypeFIN   byte = 3
	frameTypeReset byte = 4

	dataFrameHeaderSize    = 13 // type(1) + seq(8) + len(4)
	controlFrameHeaderSize = 9  // type(1) + seq(8)
	maxFramePayload        = 1 << 20
	maxQueueBytes          = 64 << 20
	maxReorderBytes        = 512 << 20
	maxReplayBytes         = 512 << 20
)

var (
	errCoreClosed  = errors.New("multipath core closed")
	errLeg1Stalled = errors.New("multipath booster leg stalled")
)

type coreConfig struct {
	ChunkSize            int
	QueueFrames          int
	QueueBytes           int64
	ThresholdBytesPS     uint64
	ActivationAfterBytes uint64
	ActivationWindow     time.Duration
	BandwidthMbps        []uint32
	MaxReorderFrames     int
	MaxReorderBytes      int64
	ReplayBytes          int64
	ReplayTimeout        time.Duration
	OnLeg1Active         func(activationInfo, bool)
	OnLegFailure         func(uint8, error)
}

type activationReason string

const (
	activationReasonImmediate  activationReason = "immediate"
	activationReasonBytes      activationReason = "bytes"
	activationReasonThroughput activationReason = "throughput"
	activationReasonLeg0Queue  activationReason = "leg0_queue"
)

type activationInfo struct {
	Reason           activationReason
	CurrentBytes     uint64
	ThresholdBytes   uint64
	WindowBytes      uint64
	RateBytesPS      uint64
	ThresholdBytesPS uint64
	Elapsed          time.Duration
	BacklogBytes     int64
	QueueBytes       int64
	RequiredDuration time.Duration
}

func (i activationInfo) String() string {
	switch i.Reason {
	case activationReasonBytes:
		return fmt.Sprintf("reason=%s current_bytes=%d threshold_bytes=%d", i.Reason, i.CurrentBytes, i.ThresholdBytes)
	case activationReasonThroughput:
		return fmt.Sprintf(
			"reason=%s measured_mbps=%.2f threshold_mbps=%.2f window=%s window_bytes=%d",
			i.Reason,
			float64(i.RateBytesPS)*8/1_000_000,
			float64(i.ThresholdBytesPS)*8/1_000_000,
			i.Elapsed.Round(time.Millisecond),
			i.WindowBytes,
		)
	case activationReasonLeg0Queue:
		ratio := float64(0)
		if i.QueueBytes > 0 {
			ratio = float64(i.BacklogBytes) * 100 / float64(i.QueueBytes)
		}
		return fmt.Sprintf(
			"reason=%s backlog_bytes=%d queue_bytes=%d ratio=%.1f%% duration=%s required_duration=%s",
			i.Reason,
			i.BacklogBytes,
			i.QueueBytes,
			ratio,
			i.Elapsed.Round(time.Millisecond),
			i.RequiredDuration,
		)
	default:
		return fmt.Sprintf("reason=%s", i.Reason)
	}
}

type wireFrame struct {
	typ    byte
	seq    uint64
	data   []byte
	replay bool
}

type replayEntry struct {
	frame          wireFrame
	sentAt         time.Time
	fallbackQueued bool
	acked          bool
}

type mpLegCounters struct {
	txBytes  atomic.Uint64
	rxBytes  atomic.Uint64
	txFrames atomic.Uint64
	rxFrames atomic.Uint64
}

// logicalConn uses one net.Pipe per direction. Unlike a single net.Pipe, this
// lets sing-box half-close one direction without tearing down queued data in
// the other direction.
type logicalConn struct {
	readConn  net.Conn
	writeConn net.Conn
	onClose   func()
	closeOne  sync.Once
}

func newLogicalPipe() (*logicalConn, net.Conn, net.Conn) {
	appWrite, coreRead := net.Pipe()
	coreWrite, appRead := net.Pipe()
	return &logicalConn{readConn: appRead, writeConn: appWrite}, coreRead, coreWrite
}

func (c *logicalConn) Read(buffer []byte) (int, error) {
	return c.readConn.Read(buffer)
}

func (c *logicalConn) Write(buffer []byte) (int, error) {
	return c.writeConn.Write(buffer)
}

func (c *logicalConn) CloseRead() error {
	return c.readConn.Close()
}

func (c *logicalConn) CloseWrite() error {
	return c.writeConn.Close()
}

func (c *logicalConn) closeInternal() (error, bool) {
	var closeErr error
	closed := false
	c.closeOne.Do(func() {
		closed = true
		closeErr = errors.Join(c.readConn.Close(), c.writeConn.Close())
	})
	return closeErr, closed
}

func (c *logicalConn) Close() error {
	closeErr, closed := c.closeInternal()
	if closed && c.onClose != nil {
		c.onClose()
	}
	return closeErr
}

func (c *logicalConn) LocalAddr() net.Addr {
	return c.readConn.LocalAddr()
}

func (c *logicalConn) RemoteAddr() net.Addr {
	return c.readConn.RemoteAddr()
}

func (c *logicalConn) SetDeadline(deadline time.Time) error {
	return errors.Join(c.readConn.SetReadDeadline(deadline), c.writeConn.SetWriteDeadline(deadline))
}

func (c *logicalConn) SetReadDeadline(deadline time.Time) error {
	return c.readConn.SetReadDeadline(deadline)
}

func (c *logicalConn) SetWriteDeadline(deadline time.Time) error {
	return c.writeConn.SetWriteDeadline(deadline)
}

type mpLeg struct {
	id           uint8
	conn         net.Conn
	readPreamble func(net.Conn) error
	send         chan wireFrame
	control      chan wireFrame
	onClose      func(error)
	done         chan struct{}
	writerDone   chan struct{}
	closeOne     sync.Once
	queuedBytes  atomic.Int64
	writingBytes atomic.Int64
}

func (l *mpLeg) Done() <-chan struct{} {
	return l.done
}

func (l *mpLeg) close(err error) {
	l.closeOne.Do(func() {
		close(l.done)
		_ = l.conn.Close()
		if l.onClose != nil {
			l.onClose(err)
		}
	})
}

func (l *mpLeg) backlogBytes() int64 {
	return l.queuedBytes.Load() + l.writingBytes.Load()
}

func (l *mpLeg) reserveQueue(length int64, limit int64) bool {
	for {
		queued := l.queuedBytes.Load()
		if queued+length > limit {
			return false
		}
		if l.queuedBytes.CompareAndSwap(queued, queued+length) {
			return true
		}
	}
}

func (l *mpLeg) tryQueue(frame wireFrame, limit int64) bool {
	length := int64(len(frame.data))
	if !l.reserveQueue(length, limit) {
		return false
	}
	select {
	case <-l.done:
		l.queuedBytes.Add(-length)
		return false
	case l.send <- frame:
		return true
	default:
		l.queuedBytes.Add(-length)
		return false
	}
}

func (l *mpLeg) queueControl(coreDone <-chan struct{}, frame wireFrame) error {
	select {
	case <-coreDone:
		return errCoreClosed
	case <-l.done:
		return errCoreClosed
	case l.control <- frame:
		return nil
	}
}

func (l *mpLeg) tryControl(frame wireFrame) {
	select {
	case <-l.done:
	case l.control <- frame:
	default:
	}
}

type mpCore struct {
	cfg          coreConfig
	ctx          context.Context
	cancel       context.CancelFunc
	appConn      *logicalConn
	txPipe       net.Conn
	rxPipe       net.Conn
	legsMu       sync.RWMutex
	legs         map[uint8]*mpLeg
	reserved     map[uint8]bool
	incoming     chan wireFrame
	done         chan struct{}
	closeOne     sync.Once
	txSeq        atomic.Uint64
	ingressBytes atomic.Uint64
	egressBytes  atomic.Uint64
	legCounters  [2]mpLegCounters
	active       atomic.Bool
	activeCh     chan struct{}
	activateOnce sync.Once
	activationMu sync.Mutex
	activation   activationInfo
	activationAt time.Time
	notifiedLeg1 *mpLeg
	leg1Joins    uint64
	localFIN     atomic.Bool
	remoteFIN    atomic.Bool
	ackNext      atomic.Uint64
	ackedNext    atomic.Uint64
	ackWake      chan struct{}
	replayMu     sync.Mutex
	replay       map[uint64]*replayEntry
	replayBytes  int64
	reorderBytes atomic.Int64
	reorderCount atomic.Int64
	failureMu    sync.Mutex
	failure      string
	failureAt    time.Time
	bufferPool   sync.Pool
}

func newCore(parent context.Context, cfg coreConfig) (*mpCore, net.Conn) {
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = 64 * 1024
	}
	if cfg.QueueFrames <= 0 {
		cfg.QueueFrames = 256
	}
	if cfg.QueueBytes <= 0 {
		cfg.QueueBytes = int64(cfg.ChunkSize) * int64(cfg.QueueFrames)
	}
	if cfg.ActivationWindow <= 0 {
		cfg.ActivationWindow = time.Second
	}
	if cfg.MaxReorderFrames <= 0 {
		cfg.MaxReorderFrames = 2048
	}
	if cfg.MaxReorderBytes <= 0 {
		cfg.MaxReorderBytes = 64 << 20
	}
	if cfg.ReplayBytes <= 0 {
		cfg.ReplayBytes = 64 << 20
	}
	if cfg.ReplayTimeout <= 0 {
		cfg.ReplayTimeout = 5 * time.Second
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	appConn, txPipe, rxPipe := newLogicalPipe()
	c := &mpCore{
		cfg:      cfg,
		ctx:      ctx,
		cancel:   cancel,
		appConn:  appConn,
		txPipe:   txPipe,
		rxPipe:   rxPipe,
		legs:     make(map[uint8]*mpLeg),
		reserved: make(map[uint8]bool),
		incoming: make(chan wireFrame, cfg.QueueFrames*2),
		done:     make(chan struct{}),
		activeCh: make(chan struct{}),
		ackWake:  make(chan struct{}, 1),
		replay:   make(map[uint64]*replayEntry),
	}
	appConn.onClose = func() { c.fail(io.EOF) }
	c.bufferPool.New = func() any {
		return make([]byte, cfg.ChunkSize)
	}
	if cfg.ThresholdBytesPS == 0 && cfg.ActivationAfterBytes == 0 {
		c.activate(activationInfo{Reason: activationReasonImmediate})
	}
	go c.txLoop()
	go c.rxLoop()
	go c.activationLoop()
	go c.ackLoop()
	go c.replayLoop()
	return c, appConn
}

func (c *mpCore) Context() context.Context {
	return c.ctx
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

func (c *mpCore) isDone() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func (c *mpCore) fail(err error) {
	if err == nil {
		err = errCoreClosed
	}
	c.closeOne.Do(func() {
		c.failureMu.Lock()
		c.failure = err.Error()
		c.failureAt = time.Now()
		c.failureMu.Unlock()
		c.cancel()
		close(c.done)
		_ = c.txPipe.Close()
		_ = c.rxPipe.Close()
		_, _ = c.appConn.closeInternal()
		c.legsMu.RLock()
		legs := make([]*mpLeg, 0, len(c.legs))
		for _, leg := range c.legs {
			legs = append(legs, leg)
		}
		c.legsMu.RUnlock()
		for _, leg := range legs {
			leg.close(err)
		}
		// Buffers possibly referenced by blocked writers are deliberately left to
		// the garbage collector instead of being returned to the pool here.
		c.replayMu.Lock()
		c.replay = make(map[uint64]*replayEntry)
		c.replayBytes = 0
		c.replayMu.Unlock()
	})
}

func (c *mpCore) protocolFail(err error) {
	if leg := c.getLeg(0); leg != nil {
		leg.tryControl(wireFrame{typ: frameTypeReset})
	}
	c.fail(err)
}

func (c *mpCore) reserveLeg(id uint8) error {
	if id > 1 {
		return errors.New("invalid multipath leg id")
	}
	c.legsMu.Lock()
	defer c.legsMu.Unlock()
	if c.isDone() {
		return errCoreClosed
	}
	if c.legs[id] != nil || c.reserved[id] {
		return errors.New("duplicate multipath leg")
	}
	c.reserved[id] = true
	return nil
}

func (c *mpCore) cancelLegReservation(id uint8) {
	c.legsMu.Lock()
	delete(c.reserved, id)
	c.legsMu.Unlock()
}

func (c *mpCore) commitLeg(id uint8, conn net.Conn, onClose func(error)) (*mpLeg, error) {
	return c.commitLegWithReadPreamble(id, conn, onClose, nil)
}

func (c *mpCore) commitLegWithReadPreamble(id uint8, conn net.Conn, onClose func(error), readPreamble func(net.Conn) error) (*mpLeg, error) {
	c.legsMu.Lock()
	if !c.reserved[id] {
		c.legsMu.Unlock()
		return nil, errors.New("multipath leg is not reserved")
	}
	delete(c.reserved, id)
	if c.isDone() {
		c.legsMu.Unlock()
		return nil, errCoreClosed
	}
	if c.legs[id] != nil {
		c.legsMu.Unlock()
		return nil, errors.New("duplicate multipath leg")
	}
	leg := &mpLeg{
		id:           id,
		conn:         conn,
		readPreamble: readPreamble,
		send:         make(chan wireFrame, c.cfg.QueueFrames),
		control:      make(chan wireFrame, 32),
		onClose:      onClose,
		done:         make(chan struct{}),
		writerDone:   make(chan struct{}),
	}
	c.legs[id] = leg
	c.legsMu.Unlock()
	go c.legWriteLoop(leg)
	go c.legReadLoop(leg)
	if id == 1 {
		c.notifyLeg1Active()
	}
	return leg, nil
}

func (c *mpCore) addLeg(id uint8, conn net.Conn, onClose func(error)) (*mpLeg, error) {
	return c.addLegWithReadPreamble(id, conn, onClose, nil)
}

func (c *mpCore) addLegWithReadPreamble(id uint8, conn net.Conn, onClose func(error), readPreamble func(net.Conn) error) (*mpLeg, error) {
	if err := c.reserveLeg(id); err != nil {
		return nil, err
	}
	leg, err := c.commitLegWithReadPreamble(id, conn, onClose, readPreamble)
	if err != nil {
		c.cancelLegReservation(id)
	}
	return leg, err
}

func (c *mpCore) getLeg(id uint8) *mpLeg {
	c.legsMu.RLock()
	leg := c.legs[id]
	c.legsMu.RUnlock()
	return leg
}

func (c *mpCore) availableLegs() []*mpLeg {
	c.legsMu.RLock()
	legs := make([]*mpLeg, 0, 2)
	if leg := c.legs[0]; leg != nil {
		legs = append(legs, leg)
	}
	if leg := c.legs[1]; leg != nil {
		legs = append(legs, leg)
	}
	c.legsMu.RUnlock()
	return legs
}

func (c *mpCore) getBuffer() []byte {
	return c.bufferPool.Get().([]byte)
}

func (c *mpCore) putBuffer(buffer []byte) {
	if cap(buffer) != c.cfg.ChunkSize {
		return
	}
	c.bufferPool.Put(buffer[:c.cfg.ChunkSize])
}

func (c *mpCore) txLoop() {
	for {
		buffer := c.getBuffer()
		n, err := c.txPipe.Read(buffer[:c.cfg.ChunkSize])
		if n > 0 {
			c.ingressBytes.Add(uint64(n))
			frame := wireFrame{
				typ:  frameTypeData,
				seq:  c.txSeq.Add(1) - 1,
				data: buffer[:n],
			}
			if enqueueErr := c.enqueue(frame); enqueueErr != nil {
				c.putBuffer(buffer)
				if !c.isDone() {
					c.fail(enqueueErr)
				}
				return
			}
		} else {
			c.putBuffer(buffer)
		}
		if err != nil {
			if c.isDone() {
				return
			}
			if errors.Is(err, io.EOF) {
				if finErr := c.sendFIN(); finErr != nil && !c.isDone() {
					c.fail(finErr)
				}
				return
			}
			c.protocolFail(err)
			return
		}
	}
}

func (c *mpCore) sendFIN() error {
	if !c.localFIN.CompareAndSwap(false, true) {
		return nil
	}
	leg := c.getLeg(0)
	if leg == nil {
		return errors.New("multipath control leg is unavailable")
	}
	return leg.queueControl(c.done, wireFrame{typ: frameTypeFIN, seq: c.txSeq.Load()})
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
			bytesNow := c.ingressBytes.Load()
			if c.cfg.ActivationAfterBytes > 0 && bytesNow >= c.cfg.ActivationAfterBytes {
				c.activate(activationInfo{
					Reason:         activationReasonBytes,
					CurrentBytes:   bytesNow,
					ThresholdBytes: c.cfg.ActivationAfterBytes,
				})
				return
			}
			if c.cfg.ThresholdBytesPS > 0 && now.Sub(windowStart) >= c.cfg.ActivationWindow {
				delta := bytesNow - windowBase
				elapsed := now.Sub(windowStart)
				rate := uint64(0)
				if elapsed > 0 {
					rate = uint64(float64(delta) / elapsed.Seconds())
				}
				if rate >= c.cfg.ThresholdBytesPS {
					c.activate(activationInfo{
						Reason:           activationReasonThroughput,
						WindowBytes:      delta,
						RateBytesPS:      rate,
						ThresholdBytesPS: c.cfg.ThresholdBytesPS,
						Elapsed:          elapsed,
					})
					return
				}
				windowStart = now
				windowBase = bytesNow
			}
			primary := c.getLeg(0)
			if primary == nil {
				continue
			}
			backlogBytes := primary.backlogBytes()
			if backlogBytes*5 >= c.cfg.QueueBytes*4 {
				if queueHighSince.IsZero() {
					queueHighSince = now
				} else if now.Sub(queueHighSince) >= c.cfg.ActivationWindow {
					c.activate(activationInfo{
						Reason:           activationReasonLeg0Queue,
						BacklogBytes:     backlogBytes,
						QueueBytes:       c.cfg.QueueBytes,
						Elapsed:          now.Sub(queueHighSince),
						RequiredDuration: c.cfg.ActivationWindow,
					})
					return
				}
			} else {
				queueHighSince = time.Time{}
			}
		}
	}
}

func (c *mpCore) activate(info activationInfo) {
	c.activateOnce.Do(func() {
		c.activationMu.Lock()
		c.activation = info
		c.activationAt = time.Now()
		c.activationMu.Unlock()
		c.active.Store(true)
		close(c.activeCh)
		c.notifyLeg1Active()
	})
}

func (c *mpCore) notifyLeg1Active() {
	if !c.active.Load() || c.cfg.OnLeg1Active == nil {
		return
	}
	leg := c.getLeg(1)
	if leg == nil {
		return
	}
	c.activationMu.Lock()
	if c.notifiedLeg1 == leg {
		c.activationMu.Unlock()
		return
	}
	info := c.activation
	reconnect := c.leg1Joins > 0
	c.notifiedLeg1 = leg
	c.leg1Joins++
	callback := c.cfg.OnLeg1Active
	c.activationMu.Unlock()
	callback(info, reconnect)
}

func (c *mpCore) weightFor(id uint8) uint32 {
	if int(id) < len(c.cfg.BandwidthMbps) && c.cfg.BandwidthMbps[id] > 0 {
		return c.cfg.BandwidthMbps[id]
	}
	return 1
}

func (c *mpCore) chooseLeg(frameLength int) *mpLeg {
	if !c.active.Load() {
		return c.getLeg(0)
	}
	legs := c.availableLegs()
	var best *mpLeg
	var bestNum, bestDen uint64
	for _, leg := range legs {
		weight := uint64(c.weightFor(leg.id))
		num := uint64(leg.backlogBytes() + int64(frameLength))
		if best == nil || num*bestDen < bestNum*weight {
			best = leg
			bestNum = num
			bestDen = weight
		}
	}
	return best
}

func (c *mpCore) tryQueueNewFrame(leg *mpLeg, frame wireFrame) bool {
	if leg == nil {
		return false
	}
	queuedFrame := frame
	if leg.id == 1 {
		if !c.trackReplay(&queuedFrame) {
			return false
		}
	}
	if leg.tryQueue(queuedFrame, c.cfg.QueueBytes) {
		return true
	}
	if queuedFrame.replay {
		c.untrackReplay(queuedFrame.seq, false)
	}
	return false
}

func (c *mpCore) enqueue(frame wireFrame) error {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return errCoreClosed
		default:
		}
		leg := c.chooseLeg(len(frame.data))
		if c.tryQueueNewFrame(leg, frame) {
			return nil
		}
		if c.active.Load() {
			for _, other := range c.availableLegs() {
				if other != leg && c.tryQueueNewFrame(other, frame) {
					return nil
				}
			}
		}
		select {
		case <-c.done:
			return errCoreClosed
		case <-ticker.C:
		}
	}
}

func (c *mpCore) legWriteLoop(leg *mpLeg) {
	defer close(leg.writerDone)
	for {
		select {
		case control := <-leg.control:
			if err := writeWireFrame(leg.conn, control); err != nil {
				c.legFailed(leg, err)
				return
			}
			continue
		default:
		}
		select {
		case <-c.done:
			return
		case <-leg.done:
			return
		case control := <-leg.control:
			if err := writeWireFrame(leg.conn, control); err != nil {
				c.legFailed(leg, err)
				return
			}
		case frame := <-leg.send:
			length := int64(len(frame.data))
			leg.queuedBytes.Add(-length)
			leg.writingBytes.Add(length)
			if leg.id == 1 {
				c.markReplaySent(frame.seq)
			}
			err := writeWireFrame(leg.conn, frame)
			leg.writingBytes.Add(-length)
			if err != nil {
				c.legFailed(leg, err)
				return
			}
			c.legCounters[leg.id].txBytes.Add(uint64(length))
			c.legCounters[leg.id].txFrames.Add(1)
			if leg.id == 0 {
				if frame.replay {
					c.completeFallback(frame.seq)
				} else {
					c.putBuffer(frame.data)
				}
			}
		}
	}
}

func (c *mpCore) legReadLoop(leg *mpLeg) {
	if leg.readPreamble != nil {
		if err := leg.readPreamble(leg.conn); err != nil {
			if !c.isDone() {
				c.legFailed(leg, err)
			}
			return
		}
	}
	for {
		frame, err := readWireFrame(leg.conn, c)
		if err != nil {
			if !c.isDone() {
				c.legFailed(leg, err)
			}
			return
		}
		switch frame.typ {
		case frameTypeACK:
			if leg.id != 0 {
				c.protocolFail(errors.New("multipath ACK received on booster leg"))
				return
			}
			c.handleACK(frame.seq)
		case frameTypeReset:
			c.fail(errors.New("multipath peer reset"))
			return
		case frameTypeData, frameTypeFIN:
			if frame.typ == frameTypeData {
				c.legCounters[leg.id].rxBytes.Add(uint64(len(frame.data)))
				c.legCounters[leg.id].rxFrames.Add(1)
			}
			select {
			case c.incoming <- frame:
			case <-c.done:
				if len(frame.data) > 0 {
					c.putBuffer(frame.data)
				}
				return
			}
		default:
			c.protocolFail(errors.New("unknown multipath frame type"))
			return
		}
	}
}

func (c *mpCore) legFailed(leg *mpLeg, err error) {
	c.legsMu.Lock()
	if c.legs[leg.id] != leg {
		c.legsMu.Unlock()
		leg.close(err)
		return
	}
	delete(c.legs, leg.id)
	c.legsMu.Unlock()
	leg.close(err)
	if c.cfg.OnLegFailure != nil {
		c.cfg.OnLegFailure(leg.id, err)
	}
	if leg.id == 0 {
		c.fail(err)
		return
	}
	go func() {
		<-leg.writerDone
		if !c.isDone() {
			c.reinjectLeg1()
		}
	}()
}

func (c *mpCore) rxLoop() {
	expected := uint64(0)
	pending := make(map[uint64]wireFrame)
	var pendingBytes int64
	var finSeq *uint64
	cleanup := func() {
		for _, frame := range pending {
			c.putBuffer(frame.data)
		}
		c.reorderBytes.Store(0)
		c.reorderCount.Store(0)
	}
	defer cleanup()
	for {
		select {
		case <-c.done:
			return
		case frame := <-c.incoming:
			switch frame.typ {
			case frameTypeFIN:
				if finSeq != nil && *finSeq != frame.seq {
					c.protocolFail(errors.New("conflicting multipath FIN sequence"))
					return
				}
				if frame.seq < expected {
					continue
				}
				if frame.seq-expected > uint64(c.cfg.MaxReorderFrames) {
					c.protocolFail(errors.New("multipath FIN sequence gap exceeded"))
					return
				}
				for sequence := range pending {
					if sequence >= frame.seq {
						c.protocolFail(errors.New("multipath buffered data after FIN"))
						return
					}
				}
				value := frame.seq
				finSeq = &value
			case frameTypeData:
				if finSeq != nil && frame.seq >= *finSeq {
					c.putBuffer(frame.data)
					c.protocolFail(errors.New("multipath data after FIN"))
					return
				}
				if frame.seq < expected {
					c.putBuffer(frame.data)
					continue
				}
				if frame.seq > expected {
					if frame.seq-expected > uint64(c.cfg.MaxReorderFrames) {
						c.putBuffer(frame.data)
						c.protocolFail(errors.New("multipath sequence gap exceeded"))
						return
					}
					if _, exists := pending[frame.seq]; exists {
						c.putBuffer(frame.data)
						continue
					}
					if len(pending) >= c.cfg.MaxReorderFrames || pendingBytes+int64(len(frame.data)) > c.cfg.MaxReorderBytes {
						c.putBuffer(frame.data)
						c.protocolFail(errors.New("multipath reorder buffer exceeded"))
						return
					}
					pending[frame.seq] = frame
					pendingBytes += int64(len(frame.data))
					c.reorderBytes.Add(int64(len(frame.data)))
					c.reorderCount.Add(1)
					continue
				}
				for {
					if err := writeAll(c.rxPipe, frame.data); err != nil {
						c.putBuffer(frame.data)
						if !c.isDone() {
							c.fail(err)
						}
						return
					}
					c.egressBytes.Add(uint64(len(frame.data)))
					c.putBuffer(frame.data)
					expected++
					c.requestACK(expected)
					next, exists := pending[expected]
					if !exists {
						break
					}
					delete(pending, expected)
					pendingBytes -= int64(len(next.data))
					c.reorderBytes.Add(-int64(len(next.data)))
					c.reorderCount.Add(-1)
					frame = next
				}
			}
			if finSeq != nil && expected == *finSeq {
				c.remoteFIN.Store(true)
				_ = c.rxPipe.Close()
				return
			}
		}
	}
}

func (c *mpCore) requestACK(next uint64) {
	for {
		current := c.ackNext.Load()
		if next <= current || c.ackNext.CompareAndSwap(current, next) {
			break
		}
	}
	select {
	case c.ackWake <- struct{}{}:
	default:
	}
}

func (c *mpCore) ackLoop() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	lastSent := uint64(0)
	for {
		select {
		case <-c.done:
			return
		case <-c.ackWake:
		case <-ticker.C:
		}
		next := c.ackNext.Load()
		if next == 0 || next == lastSent {
			continue
		}
		leg := c.getLeg(0)
		if leg == nil {
			c.fail(errors.New("multipath control leg is unavailable"))
			return
		}
		if err := leg.queueControl(c.done, wireFrame{typ: frameTypeACK, seq: next}); err != nil {
			if !c.isDone() {
				c.fail(err)
			}
			return
		}
		lastSent = next
	}
}

func (c *mpCore) trackReplay(frame *wireFrame) bool {
	length := int64(len(frame.data))
	c.replayMu.Lock()
	defer c.replayMu.Unlock()
	if c.replayBytes+length > c.cfg.ReplayBytes {
		return false
	}
	if _, exists := c.replay[frame.seq]; exists {
		return false
	}
	frame.replay = true
	c.replay[frame.seq] = &replayEntry{frame: *frame}
	c.replayBytes += length
	return true
}

func (c *mpCore) untrackReplay(seq uint64, release bool) {
	var buffer []byte
	c.replayMu.Lock()
	if entry := c.replay[seq]; entry != nil {
		delete(c.replay, seq)
		c.replayBytes -= int64(len(entry.frame.data))
		if release {
			buffer = entry.frame.data
		}
	}
	c.replayMu.Unlock()
	if buffer != nil {
		c.putBuffer(buffer)
	}
}

func (c *mpCore) markReplaySent(seq uint64) {
	c.replayMu.Lock()
	if entry := c.replay[seq]; entry != nil && entry.sentAt.IsZero() {
		entry.sentAt = time.Now()
	}
	c.replayMu.Unlock()
}

func (c *mpCore) handleACK(next uint64) {
	if next > c.txSeq.Load() {
		c.protocolFail(errors.New("invalid multipath ACK sequence"))
		return
	}
	for {
		current := c.ackedNext.Load()
		if next <= current {
			return
		}
		if c.ackedNext.CompareAndSwap(current, next) {
			break
		}
	}
	var buffers [][]byte
	c.replayMu.Lock()
	for seq, entry := range c.replay {
		if seq >= next {
			continue
		}
		if entry.fallbackQueued {
			entry.acked = true
			continue
		}
		delete(c.replay, seq)
		c.replayBytes -= int64(len(entry.frame.data))
		buffers = append(buffers, entry.frame.data)
	}
	c.replayMu.Unlock()
	for _, buffer := range buffers {
		c.putBuffer(buffer)
	}
}

func (c *mpCore) completeFallback(seq uint64) {
	c.untrackReplay(seq, true)
}

func (c *mpCore) replayLoop() {
	interval := c.cfg.ReplayTimeout / 4
	if interval < 50*time.Millisecond {
		interval = 50 * time.Millisecond
	}
	if interval > 500*time.Millisecond {
		interval = 500 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case now := <-ticker.C:
			stalled := false
			c.replayMu.Lock()
			for _, entry := range c.replay {
				if !entry.fallbackQueued && !entry.sentAt.IsZero() && now.Sub(entry.sentAt) >= c.cfg.ReplayTimeout {
					stalled = true
					break
				}
			}
			c.replayMu.Unlock()
			if stalled {
				if leg := c.getLeg(1); leg != nil {
					c.legFailed(leg, errLeg1Stalled)
				}
			}
		}
	}
}

func (c *mpCore) reinjectLeg1() {
	c.replayMu.Lock()
	sequences := make([]uint64, 0, len(c.replay))
	for seq, entry := range c.replay {
		if !entry.fallbackQueued {
			entry.fallbackQueued = true
			sequences = append(sequences, seq)
		}
	}
	c.replayMu.Unlock()
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for _, seq := range sequences {
		for {
			if c.isDone() {
				return
			}
			c.replayMu.Lock()
			entry := c.replay[seq]
			var frame wireFrame
			if entry != nil {
				frame = entry.frame
				frame.replay = true
			}
			c.replayMu.Unlock()
			if entry == nil {
				break
			}
			leg0 := c.getLeg(0)
			if leg0 == nil {
				c.fail(errors.New("multipath control leg unavailable during fallback"))
				return
			}
			if leg0.tryQueue(frame, c.cfg.QueueBytes) {
				break
			}
			select {
			case <-c.done:
				return
			case <-ticker.C:
			}
		}
	}
}

func writeWireFrame(conn net.Conn, frame wireFrame) error {
	if writer, isInitialWriter := conn.(initialFrameWriter); isInitialWriter {
		handled, err := writer.writeInitialFrame(frame)
		if handled {
			return err
		}
	}
	switch frame.typ {
	case frameTypeData:
		if len(frame.data) == 0 || len(frame.data) > maxFramePayload {
			return errors.New("invalid multipath data frame")
		}
		var header [dataFrameHeaderSize]byte
		header[0] = frameTypeData
		binary.BigEndian.PutUint64(header[1:9], frame.seq)
		binary.BigEndian.PutUint32(header[9:13], uint32(len(frame.data)))
		buffers := net.Buffers{header[:], frame.data}
		_, err := buffers.WriteTo(conn)
		return err
	case frameTypeACK, frameTypeFIN:
		var header [controlFrameHeaderSize]byte
		header[0] = frame.typ
		binary.BigEndian.PutUint64(header[1:9], frame.seq)
		return writeAll(conn, header[:])
	case frameTypeReset:
		return writeAll(conn, []byte{frameTypeReset})
	default:
		return errors.New("unknown multipath frame type")
	}
}

func readWireFrame(conn net.Conn, core *mpCore) (wireFrame, error) {
	var frame wireFrame
	var frameType [1]byte
	if _, err := io.ReadFull(conn, frameType[:]); err != nil {
		return frame, err
	}
	frame.typ = frameType[0]
	switch frame.typ {
	case frameTypeData:
		var header [dataFrameHeaderSize - 1]byte
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			return wireFrame{}, err
		}
		frame.seq = binary.BigEndian.Uint64(header[0:8])
		length := int(binary.BigEndian.Uint32(header[8:12]))
		if length <= 0 || length > core.cfg.ChunkSize || length > maxFramePayload {
			return wireFrame{}, errors.New("invalid multipath frame length")
		}
		buffer := core.getBuffer()[:length]
		if _, err := io.ReadFull(conn, buffer); err != nil {
			core.putBuffer(buffer)
			return wireFrame{}, err
		}
		frame.data = buffer
		return frame, nil
	case frameTypeACK, frameTypeFIN:
		var sequence [8]byte
		if _, err := io.ReadFull(conn, sequence[:]); err != nil {
			return wireFrame{}, err
		}
		frame.seq = binary.BigEndian.Uint64(sequence[:])
		return frame, nil
	case frameTypeReset:
		return frame, nil
	default:
		return wireFrame{}, errors.New("unknown multipath frame type")
	}
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
