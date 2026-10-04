package multipath

import (
	"context"
	"errors"
	"io"
	"net"
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
	app, txPipe, rxPipe := newLogicalPipe()
	c := &mpCore{
		cfg: cfg, ctx: ctx, cancel: cancel, appConn: app, txPipe: txPipe, rxPipe: rxPipe,
		legs: make(map[uint8]*mpLeg), reserved: make(map[uint8]bool), retiring: make(map[uint8]*mpLeg),
		done: make(chan struct{}), released: make(chan struct{}), activeCh: make(chan struct{}),
		memory: budget, sessionBytes: reservation, txReserve: make(chan []byte, 1),
		pumpWake: make(chan struct{}, 1), txWake: make(chan struct{}, 1), rxWake: make(chan struct{}, 1),
		startedAt: time.Now(),
	}
	// The first chunk is usable before feedback, preserving the early-write
	// fast path. Advertised byte capacity is separate from allocated storage.
	c.tx = stream.NewSender(uint64(cfg.FrameSize))
	capacity := uint64(cfg.ReceiveWindowBytes)
	c.rx = stream.NewReceiver(capacity, c)
	c.txReserve <- budget.takeReservedBuffer(cfg.FrameSize)
	app.onClose = c.closeApplication
	app.onCloseRead = func() error { c.localReadClosed.Store(true); return app.readConn.Close() }
	c.startWorkers(c.txLoop, c.rxLoop, c.pumpLoop, c.activationLoop)
	return c, app, nil
}

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
	c.stateMu.Lock()
	for _, leg := range legs {
		leg.path.Close()
	}
	c.tx.Close()
	c.rx.Close()
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

func (c *mpCore) nextTXBuffer() (*stream.Buffer, error) {
	for {
		if c.isDone() {
			return nil, errCoreClosed
		}
		// Charge fixed metadata as well as payload; one-byte application writes
		// cannot create an unbounded list of uncharged send records.
		buffer, changed := c.memory.tryAcquireTX(c.cfg.FrameSize + 512)
		if buffer != nil {
			return stream.NewBuffer(buffer[:c.cfg.FrameSize], func() { c.memory.releaseTX(buffer) }), nil
		}
		select {
		case <-c.done:
			return nil, errCoreClosed
		case buffer = <-c.txReserve:
			return stream.NewBuffer(buffer, func() { c.txReserve <- buffer }), nil
		case <-changed:
		}
	}
}

func (c *mpCore) waitTXSpace() bool {
	var blocked time.Time
	defer func() {
		if !blocked.IsZero() {
			c.backpressNS.Add(uint64(time.Since(blocked)))
		}
	}()
	for {
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
		if available {
			return !c.isDone()
		}
		if blocked.IsZero() {
			blocked = time.Now()
			c.backpressE.Add(1)
		}
		select {
		case <-c.done:
			return false
		case <-c.txWake:
		}
	}
}

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

func (c *mpCore) txLoop() {
	for c.waitTXSpace() {
		buffer, err := c.nextTXBuffer()
		if err != nil {
			return
		}
		n, readErr := c.txPipe.Read(buffer.Data)
		if n > 0 && n <= c.cfg.FrameSize/2 {
			// Retain small writes in a size class instead of charging an entire
			// maximum-sized slab until Data ACK. This is copying, not batching:
			// a small first write is never delayed waiting for more application data.
			size := 256
			for size < n {
				size *= 2
			}
			compact, _ := c.memory.tryAcquireTX(size + 512)
			if compact != nil {
				copy(compact, buffer.Data[:n])
				buffer.Release()
				buffer = stream.NewBuffer(compact[:n], func() { c.memory.releaseTX(compact) })
			}
		}
		c.stateMu.Lock()
		if n > 0 && !c.isDone() {
			buffer.Data = buffer.Data[:n]
			err = c.tx.Append(buffer)
			if err == nil {
				c.ingressBytes.Add(uint64(n))
			} else {
				buffer.Release()
			}
		} else {
			buffer.Release()
		}
		if errors.Is(readErr, io.EOF) && !c.isDone() {
			c.tx.CloseWrite()
			c.localFIN.Store(true)
		}
		c.updateStateCountersLocked()
		c.stateMu.Unlock()
		wakeFlow(c.pumpWake)
		if err != nil {
			c.protocolFail(err)
			return
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) && !c.isDone() {
				c.fail(readErr)
			}
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

func (c *mpCore) rxLoop() {
	defer c.rxPipe.Close()
	for {
		c.stateMu.Lock()
		data, finished := c.rx.Readable(), c.rx.EOF()
		c.stateMu.Unlock()
		if finished {
			c.remoteFIN.Store(true)
			return
		}
		if len(data) == 0 {
			select {
			case <-c.done:
				return
			case <-c.rxWake:
			}
			continue
		}
		n, err := len(data), error(nil)
		discard := c.localReadClosed.Load()
		if !discard {
			n, err = c.rxPipe.Write(data)
		}
		if n > 0 {
			c.stateMu.Lock()
			c.rx.Consume(n)
			c.feedbackDirty = true
			c.updateStateCountersLocked()
			c.stateMu.Unlock()
			if !discard {
				c.egressBytes.Add(uint64(n))
			}
			wakeFlow(c.pumpWake)
		}
		if err != nil && !c.localReadClosed.Load() {
			if !c.isDone() {
				c.fail(err)
			}
			return
		}
	}
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
