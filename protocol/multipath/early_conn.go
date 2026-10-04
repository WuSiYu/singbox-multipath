package multipath

import (
	"encoding/binary"
	"errors"
	"net"
	"os"
	"sync"
	"time"

	N "github.com/sagernet/sing/common/network"
)

type initialFrameWriter interface {
	writeInitialFrame(frame wireFrame) (handled bool, err error)
}

// clientFastOpenConn keeps the SMP hello pending until the first frame so the
// child outbound can place both in its first TCP Fast Open write.
type clientFastOpenConn struct {
	net.Conn
	hello             []byte
	initialWindow     []byte // Set before the core starts either leg worker.
	handshakeDeadline time.Time
	startOnce         sync.Once
	startDone         chan struct{}
	startErr          error
}

func newClientFastOpenConn(conn net.Conn, message helloMessage, handshakeDeadline time.Time) (*clientFastOpenConn, error) {
	hello, err := encodeHello(message)
	if err != nil {
		return nil, err
	}
	return &clientFastOpenConn{
		Conn:              conn,
		hello:             hello,
		handshakeDeadline: handshakeDeadline,
		startDone:         make(chan struct{}),
	}, nil
}

func (c *clientFastOpenConn) NeedHandshakeForWrite() bool {
	select {
	case <-c.startDone:
		return false
	default:
		return true
	}
}

func (c *clientFastOpenConn) start(payload []byte) (bool, error) {
	started := false
	c.startOnce.Do(func() {
		started = true
		c.startErr = c.writeInitial(payload)
		close(c.startDone)
	})
	if started {
		return true, c.startErr
	}
	<-c.startDone
	if c.startErr != nil {
		return true, c.startErr
	}
	return false, nil
}

func (c *clientFastOpenConn) writeInitial(payload []byte) error {
	deadlineSet := true
	if err := c.Conn.SetDeadline(c.handshakeDeadline); err != nil {
		// Some child wrappers hide their lazy-connect state. EINVAL before the
		// first write is therefore treated as a deferred deadline; the deadline
		// is applied again immediately after that write completes.
		if !errors.Is(err, os.ErrInvalid) {
			return err
		}
		deadlineSet = false
	}
	initial := make([]byte, 0, len(c.hello)+len(c.initialWindow)+len(payload))
	initial = append(initial, c.hello...)
	initial = append(initial, c.initialWindow...)
	initial = append(initial, payload...)
	if err := writeAll(c.Conn, initial); err != nil {
		return err
	}
	if !deadlineSet {
		if err := c.Conn.SetDeadline(c.handshakeDeadline); err != nil {
			return err
		}
	}
	return nil
}

func (c *clientFastOpenConn) waitStarted() error {
	<-c.startDone
	return c.startErr
}

func (c *clientFastOpenConn) writeHelloOnly() error {
	_, err := c.start(nil)
	return err
}

func (c *clientFastOpenConn) writeInitialFrame(frame wireFrame) (bool, error) {
	if !c.NeedHandshakeForWrite() {
		if err := c.waitStarted(); err != nil {
			return true, err
		}
		return false, nil
	}
	encoded, err := encodeWireFrame(frame)
	if err != nil {
		return true, err
	}
	return c.start(encoded)
}

func (c *clientFastOpenConn) Write(payload []byte) (int, error) {
	started, err := c.start(payload)
	if started {
		if err != nil {
			return 0, err
		}
		return len(payload), nil
	}
	return c.Conn.Write(payload)
}

func (c *clientFastOpenConn) Close() error {
	err := c.Conn.Close()
	c.startOnce.Do(func() {
		c.startErr = net.ErrClosed
		close(c.startDone)
	})
	return err
}

func encodeWireFrame(frame wireFrame) ([]byte, error) {
	switch frame.typ {
	case frameTypeWindow:
		return encodeFlow(frame), nil
	case frameTypeData:
		if len(frame.data) == 0 || len(frame.data) > maxFramePayload {
			return nil, errors.New("invalid multipath data frame")
		}
		encoded := make([]byte, dataFrameHeaderSize+len(frame.data))
		encoded[0] = frameTypeData
		binary.BigEndian.PutUint64(encoded[1:9], frame.seq)
		binary.BigEndian.PutUint64(encoded[9:17], frame.pathSeq)
		binary.BigEndian.PutUint64(encoded[17:25], frame.generation)
		binary.BigEndian.PutUint32(encoded[25:29], uint32(len(frame.data)))
		copy(encoded[dataFrameHeaderSize:], frame.data)
		return encoded, nil
	case frameTypeFIN:
		encoded := make([]byte, controlFrameHeaderSize)
		encoded[0] = frame.typ
		binary.BigEndian.PutUint64(encoded[1:9], frame.seq)
		return encoded, nil
	case frameTypeSessionClose:
		if frame.closeReason > closeReasonShutdown {
			return nil, errors.New("invalid multipath close reason")
		}
		return []byte{frame.typ, frame.closeReason}, nil
	case frameTypeReset:
		return []byte{frame.typ}, nil
	default:
		return nil, errors.New("unknown multipath frame type")
	}
}

var (
	_ N.EarlyWriter = (*earlyLogicalConn)(nil)
	_ N.ReadCloser  = (*earlyLogicalConn)(nil)
	_ N.WriteCloser = (*earlyLogicalConn)(nil)
)

type earlyLogicalConn struct {
	net.Conn
	core      *mpCore
	primary   *clientFastOpenConn
	helloOnce sync.Once
}

func (c *earlyLogicalConn) NeedHandshakeForWrite() bool {
	return c.primary.NeedHandshakeForWrite()
}

func (c *earlyLogicalConn) Write(payload []byte) (int, error) {
	if len(payload) == 0 && c.primary.NeedHandshakeForWrite() {
		// The empty early-write kick must be interruptible just like DATA.
		select {
		case <-c.core.appConn.writeClosed:
			return 0, net.ErrClosed
		case <-c.core.appConn.writeDeadline.Wait():
			return 0, os.ErrDeadlineExceeded
		default:
		}
		c.helloOnce.Do(func() {
			c.core.startWorkers(func() {
				if err := c.primary.writeHelloOnly(); err != nil {
					// The session can still be created on the other leg.
					for _, leg := range c.core.availableLegs() {
						if leg.conn == c.primary {
							c.core.legFailed(leg, legFailureHandshake, err)
						}
					}
				}
			})
		})
		return 0, c.waitInitialWrite()
	}
	pending := c.primary.NeedHandshakeForWrite()
	n, err := c.Conn.Write(payload)
	if err == nil && pending {
		if startErr := c.waitInitialWrite(); startErr != nil {
			return n, startErr
		}
	}
	return n, err
}

func (c *earlyLogicalConn) waitInitialWrite() error {
	select {
	case <-c.primary.startDone:
		// A failed first write leaves the session to the other leg; only a
		// terminated session is an application error.
		if !c.core.isDone() {
			return nil
		}
		return c.primary.startErr
	case <-c.core.appConn.writeClosed:
		return net.ErrClosed
	case <-c.core.appConn.writeDeadline.Wait():
		// Bytes already accepted into the logical stream remain queued, as
		// with a partially successful TCP Write. A timeout does not reset it.
		return os.ErrDeadlineExceeded
	case <-c.core.done:
		return net.ErrClosed
	}
}

func (c *earlyLogicalConn) CloseRead() error {
	return N.CloseRead(c.Conn)
}

func (c *earlyLogicalConn) CloseWrite() error {
	return N.CloseWrite(c.Conn)
}

func (c *earlyLogicalConn) Upstream() any {
	return c.Conn
}
