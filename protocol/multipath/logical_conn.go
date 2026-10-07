package multipath

import (
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing/common/pipe"
)

// logicalConn is the application side of a session. Reads copy straight from
// the receive pages and writes straight into transmit buffers: there is no
// intermediate pipe or relay goroutine between the application and the core.
type logicalConn struct {
	core *mpCore

	writeMu                     sync.Mutex
	readDeadline, writeDeadline pipe.Deadline
	readClosed                  chan struct{} // application closed reading
	writeClosed                 chan struct{} // writing ended (application or termination)
	readTerminated              chan struct{} // termination without receive drain
	readCloseOne, writeCloseOne sync.Once
	terminateOne, closeOne      sync.Once

	onClose     func() error
	onCloseRead func() error

	errorMu                           sync.RWMutex
	readError, writeError             error
	readClosedByApp, writeClosedByApp atomic.Bool
}

type logicalAddr struct{}

func (logicalAddr) Network() string { return "multipath" }
func (logicalAddr) String() string  { return "multipath" }

func newLogicalConn(core *mpCore) *logicalConn {
	return &logicalConn{
		core:           core,
		readDeadline:   pipe.MakeDeadline(),
		writeDeadline:  pipe.MakeDeadline(),
		readClosed:     make(chan struct{}),
		writeClosed:    make(chan struct{}),
		readTerminated: make(chan struct{}),
	}
}

func (c *logicalConn) Read(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	core := c.core
	for {
		if c.readClosedByApp.Load() {
			return 0, net.ErrClosed
		}
		select {
		case <-c.readTerminated:
			return 0, c.terminalReadError()
		case <-c.readDeadline.Wait():
			// As with a socket, an expired deadline fails the Read even when
			// data are buffered.
			return 0, os.ErrDeadlineExceeded
		default:
		}
		core.stateMu.Lock()
		if core.rxClosed {
			core.stateMu.Unlock()
			return 0, c.terminalReadError()
		}
		n := 0
		for n < len(buffer) {
			data := core.rx.Readable()
			if len(data) == 0 {
				break
			}
			copied := copy(buffer[n:], data)
			core.rx.Consume(copied)
			n += copied
		}
		eof := core.rx.EOF()
		var ready <-chan struct{}
		wake := false
		if n > 0 {
			wake = core.markFeedbackLocked()
			core.updateStateCountersLocked()
		} else if !eof {
			ready = core.rxReadyLocked()
		}
		core.stateMu.Unlock()
		if n > 0 {
			core.egressBytes.Add(uint64(n))
			if wake {
				wakeFlow(core.pumpWake)
			}
			return n, nil
		}
		if eof {
			core.remoteFIN.Store(true)
			core.finishDrain()
			return 0, io.EOF
		}
		select {
		case <-ready:
		case <-c.readClosed:
		case <-c.readTerminated:
		case <-c.readDeadline.Wait():
			return 0, os.ErrDeadlineExceeded
		}
	}
}

func (c *logicalConn) terminalReadError() error {
	c.errorMu.RLock()
	defer c.errorMu.RUnlock()
	if c.readError != nil {
		return c.readError
	}
	return io.EOF
}

func (c *logicalConn) terminalWriteError() error {
	if c.writeClosedByApp.Load() {
		return net.ErrClosed
	}
	c.errorMu.RLock()
	defer c.errorMu.RUnlock()
	if c.writeError != nil {
		return c.writeError
	}
	return net.ErrClosed
}

// Write accepts application bytes into the session's send history in
// frame-sized segments. It returns the accepted prefix on deadline or close,
// like a partially successful TCP write; accepted bytes remain queued.
func (c *logicalConn) Write(buffer []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	core := c.core
	written := 0
	for written < len(buffer) {
		if err := core.waitTXSpace(c); err != nil {
			return written, err
		}
		chunk := min(len(buffer)-written, core.cfg.FrameSize)
		segment, err := core.nextTXBuffer(c, chunk)
		if err != nil {
			return written, err
		}
		copy(segment.Data, buffer[written:written+chunk])
		core.stateMu.Lock()
		if core.isDone() || c.writeClosedByApp.Load() {
			core.stateMu.Unlock()
			segment.Release()
			return written, c.terminalWriteError()
		}
		err = core.tx.Append(segment)
		if err == nil {
			core.ingressBytes.Add(uint64(chunk))
			core.updateStateCountersLocked()
		}
		core.stateMu.Unlock()
		if err != nil {
			segment.Release()
			core.protocolFail(err)
			return written, err
		}
		wakeFlow(core.pumpWake)
		written += chunk
	}
	return written, nil
}

func (c *logicalConn) setTerminalError(err error, drainReceive bool) {
	readErr, writeErr := err, err
	if errors.Is(err, io.EOF) {
		readErr, writeErr = io.ErrUnexpectedEOF, net.ErrClosed
	}
	c.errorMu.Lock()
	if !drainReceive {
		c.readError = readErr
	}
	c.writeError = writeErr
	c.errorMu.Unlock()
}

// terminate stops application writes and, unless buffered receive data must
// still drain, application reads.
func (c *logicalConn) terminate(drainReceive bool) {
	c.closeWriteSignal()
	if !drainReceive {
		c.terminateOne.Do(func() { close(c.readTerminated) })
	}
}

func (c *logicalConn) closeWriteSignal() {
	c.writeCloseOne.Do(func() { close(c.writeClosed); c.writeDeadline.Set(time.Time{}) })
}

func (c *logicalConn) closeReadSignal() {
	c.readCloseOne.Do(func() { close(c.readClosed) })
}

func (c *logicalConn) CloseRead() error {
	c.readClosedByApp.Store(true)
	if c.onCloseRead != nil {
		return c.onCloseRead()
	}
	c.closeReadSignal()
	return nil
}

// CloseWrite ends the application's sending direction with DATA_FIN after
// every accepted byte. A concurrently blocked Write returns its prefix.
func (c *logicalConn) CloseWrite() error {
	c.writeClosedByApp.Store(true)
	return c.closeWriteInternal()
}

func (c *logicalConn) closeWriteInternal() error {
	c.closeWriteSignal()
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.core.closeWriteLocked()
	return nil
}

func (c *logicalConn) closeInternal() (error, bool) {
	closed := false
	c.closeOne.Do(func() {
		closed = true
		c.closeReadSignal()
		_ = c.closeWriteInternal()
		c.core.finishDrain()
	})
	return nil, closed
}

func (c *logicalConn) Close() error {
	c.readClosedByApp.Store(true)
	c.writeClosedByApp.Store(true)
	if c.onClose != nil {
		return c.onClose()
	}
	closeErr, _ := c.closeInternal()
	return closeErr
}

func (c *logicalConn) LocalAddr() net.Addr  { return logicalAddr{} }
func (c *logicalConn) RemoteAddr() net.Addr { return logicalAddr{} }

func (c *logicalConn) SetDeadline(deadline time.Time) error {
	return errors.Join(c.SetReadDeadline(deadline), c.SetWriteDeadline(deadline))
}

func (c *logicalConn) SetReadDeadline(deadline time.Time) error {
	c.readDeadline.Set(deadline)
	return nil
}

func (c *logicalConn) SetWriteDeadline(deadline time.Time) error {
	c.writeDeadline.Set(deadline)
	return nil
}

// rxReadyLocked returns a channel closed by the next delivery to readers.
func (c *mpCore) rxReadyLocked() <-chan struct{} {
	if c.rxReady == nil {
		c.rxReady = make(chan struct{})
	}
	return c.rxReady
}

// signalReadersLocked wakes blocked readers after new in-order data or FIN.
func (c *mpCore) signalReadersLocked() {
	if c.rxReady != nil {
		close(c.rxReady)
		c.rxReady = nil
	}
}

// markFeedbackLocked records that the peer should hear about new receive
// progress. Only the first mark since the last feedback wakes the pump, which
// then paces feedback itself; waking it for every arrival or read costs a
// goroutine switch per chunk and sends nothing sooner.
func (c *mpCore) markFeedbackLocked() bool {
	wake := !c.feedbackDirty
	c.feedbackDirty = true
	return wake
}

// discardLocked consumes everything readable after the application closed
// its read side, so the window keeps moving and the peer is never blocked.
func (c *mpCore) discardLocked() {
	for data := c.rx.Readable(); len(data) > 0; data = c.rx.Readable() {
		c.rx.Consume(len(data))
		c.feedbackDirty = true
	}
}

// closeWriteLocked appends DATA_FIN after all accepted bytes. Callers hold
// the application's write lock so no Write can still be appending.
func (c *mpCore) closeWriteLocked() {
	c.stateMu.Lock()
	if !c.isDone() && !c.tx.HasFIN {
		c.tx.CloseWrite()
		c.localFIN.Store(true)
		c.updateStateCountersLocked()
	}
	c.stateMu.Unlock()
	wakeFlow(c.pumpWake)
}

// finishDrain releases a terminated session that kept its receive half open
// for the application: called on EOF delivery or when reading is abandoned.
func (c *mpCore) finishDrain() {
	c.drainOne.Do(func() { close(c.drained) })
}
