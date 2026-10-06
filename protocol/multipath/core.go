package multipath

import (
	"context"
	"errors"
	"net"
	"os"
	"time"

	"github.com/sagernet/sing-box/protocol/multipath/stream"
)

func newCore(parent context.Context, cfg coreConfig) (*mpCore, net.Conn) {
	c, conn, err := newCoreWithError(parent, cfg)
	if err != nil {
		panic(err)
	}
	return c, conn
}

func newCoreWithError(parent context.Context, cfg coreConfig) (*mpCore, net.Conn, error) {
	if cfg.FrameSize <= 0 {
		cfg.FrameSize = 64 << 10
	}
	if cfg.QueueFrames <= 0 {
		cfg.QueueFrames = 256
	}
	if cfg.QueueBytes <= 0 {
		cfg.QueueBytes = int64(cfg.FrameSize) * int64(cfg.QueueFrames)
	}
	if cfg.ActivationWindow <= 0 {
		cfg.ActivationWindow = defaultActivationWindow
	}
	if cfg.HandshakeTimeout <= 0 {
		cfg.HandshakeTimeout = 10 * time.Second
	}
	if cfg.LegAbsentTimeout <= 0 {
		cfg.LegAbsentTimeout = legAbsentTimeout(cfg.HandshakeTimeout)
	}
	if parent == nil {
		parent = context.Background()
	}
	budget := cfg.Memory
	if budget == nil {
		budget = newMemoryBudget(1<<62, false)
	}
	if cfg.ReceiveWindowBytes <= 0 {
		cfg.ReceiveWindowBytes = maxReorderBytes
	}
	if cfg.SendBufferBytes <= 0 {
		cfg.SendBufferBytes = maxReplayBytes
	}
	reservation := minimumSessionMemory(cfg)
	if !budget.reserveSession(reservation) {
		return nil, nil, errMemoryLimit
	}
	budget.sessions.Add(1)
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	c := &mpCore{
		cfg: cfg, ctx: ctx, cancel: cancel,
		legs: make(map[uint8]*mpLeg), reserved: make(map[uint8]bool), retiring: make(map[uint8]*mpLeg),
		done: make(chan struct{}), released: make(chan struct{}), activeCh: make(chan struct{}),
		memory: budget, sessionBytes: reservation, txReserve: make(chan []byte, 1),
		pumpWake: make(chan struct{}, 1), txWake: make(chan struct{}, 1), drained: make(chan struct{}),
		startedAt: time.Now(),
	}
	app := newLogicalConn(c)
	c.appConn = app
	// The first chunk is usable before feedback, preserving the early-write
	// fast path. Advertised byte capacity is separate from allocated storage.
	c.tx = stream.NewSender(uint64(cfg.FrameSize))
	capacity := uint64(cfg.ReceiveWindowBytes)
	c.rx = stream.NewReceiver(capacity, c)
	// Accept the peer's first frame even if no feedback precedes it, as when
	// a session is created by leg1, which sends no startup window.
	c.rx.Advertise(uint64(cfg.FrameSize))
	c.txReserve <- budget.takeReservedBuffer(cfg.FrameSize + txHeadroom)
	app.onClose = c.closeApplication
	app.onCloseRead = c.closeApplicationRead
	c.startWorkers(c.pumpLoop, c.activationLoop)
	return c, app, nil
}

// txHeadroom precedes every transmit payload so a data frame header can be
// written contiguously with it.
const txHeadroom = 32

func minimumSessionMemory(cfg coreConfig) int64 {
	return sessionMemoryReservation(cfg)
}

// Acquire and Release implement the receive-page admission interface. They run
// under stateMu. One prepaid head page is transferable between live head pages;
// it is never available to speculative out-of-order storage.
func (c *mpCore) Acquire(head bool) bool {
	if head && c.headPages == 0 {
		c.headPages++
		return true
	}
	if !c.memory.rxAcquire(stream.PageCharge, head) {
		return false
	}
	if head {
		c.headPages++
	}
	return true
}

func (c *mpCore) Release(head bool) {
	if head {
		c.headPages--
		if c.headPages == 0 {
			return
		}
	}
	c.memory.rxRelease(stream.PageCharge)
}

func (c *mpCore) releaseAfterShutdown(legs []*mpLeg, err error) {
	closeLegsAfterDrain(legs, err)
	c.workerGroup.Wait()
	// A peer close after FIN keeps the receive half readable until the
	// application has taken every byte, reached EOF, or closed reading.
	<-c.drained
	c.appConn.terminate(false)
	c.stateMu.Lock()
	for _, leg := range legs {
		leg.path.Close()
	}
	c.tx.Close()
	c.rx.Close()
	c.rxClosed = true
	c.signalReadersLocked()
	c.mappings = nil
	c.replayMu.Lock()
	c.replayBytes = 0
	c.replayMu.Unlock()
	c.reorderBytes.Store(0)
	c.reorderCount.Store(0)
	c.stateMu.Unlock()
	select {
	case buffer := <-c.txReserve:
		c.memory.putReservedBuffer(buffer)
	default:
	}
	c.memory.forget(c)
	c.memory.releaseSession(c.sessionBytes)
	c.memory.sessions.Add(-1)
	close(c.released)
}

func (c *mpCore) commitLeg(id uint8, conn net.Conn, onClose func(error)) (*mpLeg, error) {
	return c.commitLegWithReadPreamble(id, conn, onClose, nil)
}

func (c *mpCore) commitLegWithReadPreamble(id uint8, conn net.Conn, onClose func(error), preamble func(net.Conn) error) (*mpLeg, error) {
	c.stateMu.Lock()
	c.legsMu.Lock()
	if !c.reserved[id] || c.legs[id] != nil || c.isDone() {
		c.legsMu.Unlock()
		c.stateMu.Unlock()
		return nil, errors.New("multipath leg reservation is not available")
	}
	delete(c.reserved, id)
	ctx, cancel := context.WithCancel(c.ctx)
	c.nextGeneration++
	leg := &mpLeg{
		id: id, ctx: ctx, cancel: cancel, conn: conn, readPreamble: preamble,
		send: make(chan wireFrame, 1), control: make(chan wireFrame, 32), feedback: make(chan wireFrame, 1),
		telemetry: make(chan struct{}, 1), shutdown: make(chan legShutdownRequest, 1),
		onClose: onClose, done: make(chan struct{}), writerDone: make(chan struct{}), readerDone: make(chan struct{}),
		path: stream.Path{Generation: c.nextGeneration},
	}
	leg.ready.Store(preamble == nil)
	leg.path.ReleaseFlight = func(prepaid bool) {
		if prepaid {
			leg.prepaidFlights--
		} else {
			c.memory.releaseSession(128)
		}
	}
	c.legs[id] = leg
	if id == 0 || c.cfg.Recovery != nil {
		initial := wireFrame{typ: frameTypeWindow, flow: c.feedbackLockedWithoutLegs()}
		if early, ok := conn.(*clientFastOpenConn); ok {
			early.initialWindow = encodeFlow(initial)
		} else if preamble == nil {
			leg.startupFeedback = &initial
		}
	}
	if !c.startWorkers(func() { c.legWriteLoop(leg) }, func() { c.legReadLoop(leg) }) {
		delete(c.legs, id)
		c.legsMu.Unlock()
		c.stateMu.Unlock()
		cancel()
		return nil, errCoreClosed
	}
	c.legCounters[id].joins.Add(1)
	c.legsMu.Unlock()
	c.stateMu.Unlock()
	wakeFlow(c.pumpWake)
	if id == 1 {
		c.notifyLeg1Active()
	}
	return leg, nil
}

func (c *mpCore) addLeg(id uint8, conn net.Conn, onClose func(error)) (*mpLeg, error) {
	return c.addLegWithReadPreamble(id, conn, onClose, nil)
}

func (c *mpCore) addLegWithReadPreamble(id uint8, conn net.Conn, onClose func(error), preamble func(net.Conn) error) (*mpLeg, error) {
	if err := c.reserveLeg(id); err != nil {
		return nil, err
	}
	leg, err := c.commitLegWithReadPreamble(id, conn, onClose, preamble)
	if err != nil {
		c.cancelLegReservation(id)
	}
	return leg, err
}

// nextTXBuffer returns storage for length application bytes. Small writes
// use a size class instead of charging a whole frame slab until Data ACK; a
// small write is still sent at once, never held back to wait for more.
func (c *mpCore) nextTXBuffer(app *logicalConn, length int) (*stream.Buffer, error) {
	size := c.cfg.FrameSize
	if length <= c.cfg.FrameSize/2 {
		size = 256
		for size < length {
			size *= 2
		}
	}
	// Charge fixed metadata as well as payload; one-byte application writes
	// cannot create an unbounded list of uncharged send records. The charge
	// also covers the frame-header headroom in front of the payload.
	size += 512
	for {
		if c.isDone() {
			return nil, app.terminalWriteError()
		}
		buffer, changed := c.memory.tryAcquireTX(size)
		if buffer != nil {
			return stream.NewBufferWithHeadroom(buffer, txHeadroom, length, func() { c.memory.releaseTX(buffer) }), nil
		}
		select {
		case <-c.done:
			return nil, app.terminalWriteError()
		case <-app.writeClosed:
			return nil, app.terminalWriteError()
		case <-app.writeDeadline.Wait():
			return nil, os.ErrDeadlineExceeded
		case buffer = <-c.txReserve:
			return stream.NewBufferWithHeadroom(buffer, txHeadroom, length, func() { c.txReserve <- buffer }), nil
		case <-changed:
		}
	}
}

// waitTXSpace blocks a Write until the unsent queue and the send history both
// have room for another frame.
func (c *mpCore) waitTXSpace(app *logicalConn) error {
	var blocked time.Time
	defer func() {
		if !blocked.IsZero() {
			c.writerReleasedAt.Store(time.Now().UnixNano())
			c.writerWaiting.Store(false)
			c.backpressNS.Add(uint64(time.Since(blocked)))
		}
	}()
	for {
		select {
		case <-app.writeClosed:
			return app.terminalWriteError()
		case <-app.writeDeadline.Wait():
			return os.ErrDeadlineExceeded
		default:
		}
		c.stateMu.Lock()
		now := time.Now()
		pending := c.tx.WriteNext - min(c.tx.Next, c.tx.WriteNext)
		frame := uint64(c.cfg.FrameSize)
		unsentOK := pending+frame <= c.unsentLimitLocked()
		historyOK := c.tx.Buffered()+frame <= c.historyLimitLocked(now)
		if unsentOK && !historyOK {
			c.growHistoryLocked(now)
		}
		available := unsentOK && historyOK
		c.stateMu.Unlock()
		if c.isDone() {
			return app.terminalWriteError()
		}
		if available {
			return nil
		}
		if blocked.IsZero() {
			blocked = time.Now()
			c.backpressE.Add(1)
			c.writerWaiting.Store(true)
		}
		select {
		case <-c.done:
			return app.terminalWriteError()
		case <-app.writeClosed:
			return app.terminalWriteError()
		case <-app.writeDeadline.Wait():
			return os.ErrDeadlineExceeded
		case <-c.txWake:
		}
	}
}

// writerBacklogged reports whether the application recently had more data
// than the session would accept, so it is not yet at the end of a transfer.
func (c *mpCore) writerBacklogged(now time.Time) bool {
	return c.writerWaiting.Load() || now.UnixNano()-c.writerReleasedAt.Load() < int64(writerBacklogHold)
}

// writerBacklogHold bridges the gap between consecutive blocked Writes.
const writerBacklogHold = 10 * time.Millisecond

const (
	// unsentQueueTime sizes the application-to-assigner cushion, like MPTCP's
	// msk notsent_lowat: enough to keep the pump fed between relay wakeups.
	// Bytes handed to a child are bounded by that child's own backpressure.
	unsentQueueTime = 10 * time.Millisecond
	unsentFloor     = 1 << 20
	historyFloor    = 4 << 20
)

// carryingLegsLocked returns the legs that currently receive new data, and
// their summed delivery rate (bytes/s) and largest smoothed delivery RTT.
func (c *mpCore) carryingRateLocked() (float64, time.Duration) {
	rate, rtt := float64(0), time.Duration(0)
	saving := c.trafficSavingSecondaryLocked()
	preferred := c.preferredDataLeg()
	for _, leg := range c.availableLegs() {
		if !c.active.Load() && leg != preferred {
			continue
		}
		if saving != nil && leg != saving {
			continue
		}
		rate += leg.path.Rate
		rtt = max(rtt, leg.path.SRTT)
	}
	return rate, rtt
}

// Caller holds stateMu. queue_frames is a ceiling; the working limit follows
// the measured delivery rate of the paths currently carrying new data.
func (c *mpCore) unsentLimitLocked() uint64 {
	maximum := uint64(max(c.cfg.QueueBytes, int64(c.cfg.FrameSize)))
	floor := min(maximum, max(uint64(c.cfg.FrameSize)*4, unsentFloor))
	rate, _ := c.carryingRateLocked()
	return min(maximum, max(floor, uint64(rate*unsentQueueTime.Seconds())))
}

// historyLimitLocked bounds bytes not yet covered by Data ACK, like MPTCP's
// msk sndbuf: about twice the bandwidth-delay product of the carrying paths,
// grown while history (not the network) limits sending, and capped by the
// configured send_buffer_bytes and this session's share of the node's
// transmit region.
func (c *mpCore) historyLimitLocked(now time.Time) uint64 {
	rate, rtt := c.carryingRateLocked()
	target := max(uint64(historyFloor), uint64(2*rate*rtt.Seconds())) + c.unsentLimitLocked()
	if !c.historyAt.IsZero() && c.historyGrant > target {
		elapsed := now.Sub(c.historyAt).Seconds()
		c.historyGrant = max(target, c.historyGrant-uint64(float64(c.historyGrant-target)*min(1, elapsed*0.1)))
	}
	c.historyGrant = max(c.historyGrant, target)
	c.historyAt = now
	limit := min(c.historyGrant, uint64(max(c.cfg.SendBufferBytes, int64(c.cfg.FrameSize))))
	share := uint64(max(0, c.memory.transmitShare(c, now)))
	return min(limit, max(share, c.unsentLimitLocked()+uint64(c.cfg.FrameSize)))
}

// growHistoryLocked expands the history grant once per RTT while an idle path
// shows that history, not the network, is the bottleneck.
func (c *mpCore) growHistoryLocked(now time.Time) {
	_, rtt := c.carryingRateLocked()
	if now.Sub(c.historyGrownAt) < max(rtt, 10*time.Millisecond) {
		return
	}
	for _, leg := range c.availableLegs() {
		if leg.ready.Load() && !leg.busy && !leg.path.Stale {
			c.historyGrant = min(uint64(c.cfg.SendBufferBytes), c.historyGrant+c.historyGrant/4)
			c.historyGrownAt = now
			return
		}
	}
}

func (c *mpCore) receiveComplete() bool {
	c.stateMu.Lock()
	complete := c.rx.Complete()
	c.stateMu.Unlock()
	return complete
}

func (c *mpCore) updateStateCountersLocked() {
	c.txSeq.Store(c.tx.Next)
	c.ackedNext.Store(c.tx.Una)
	c.rxExpected.Store(c.rx.Next)
	c.ackedFIN.Store(c.tx.FINAcked)
	c.receivedFIN.Store(c.rx.Complete())
	_, reordered, pages := c.rx.Buffered()
	c.reorderBytes.Store(int64(reordered))
	if reordered == 0 {
		pages = 0
	}
	c.reorderCount.Store(int64(pages))
	updateAtomicPeak(&c.reorderPeak, int64(reordered))
	updateAtomicPeak(&c.reorderFPeak, int64(pages))
	c.replayMu.Lock()
	c.replayBytes = int64(c.tx.Buffered())
	updateAtomicPeak(&c.replayPeak, c.replayBytes)
	c.replayMu.Unlock()
}
