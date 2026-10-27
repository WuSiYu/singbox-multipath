package route

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	L "github.com/sagernet/sing/common/logger"
)

type halfCloseTestConn struct {
	net.Conn
	fullCloses  int
	writeCloses int
}

func (c *halfCloseTestConn) CloseWrite() error { c.writeCloses++; return nil }
func (c *halfCloseTestConn) Close() error      { c.fullCloses++; return c.Conn.Close() }

func TestConnectionCopyUnwrapsHalfClose(t *testing.T) {
	manager := NewConnectionManager(context.Background(), L.NOP())
	source, sourcePeer := net.Pipe()
	defer source.Close()
	sourcePeer.Close()
	target, targetPeer := net.Pipe()
	defer target.Close()
	defer targetPeer.Close()
	spy := &halfCloseTestConn{Conn: target}
	wrapped := manager.TrackConn(spy)
	var done atomic.Bool
	manager.connectionCopy(context.Background(), source, wrapped, false, &done, nil)
	if spy.writeCloses != 1 || spy.fullCloses != 0 {
		t.Fatalf("half-close became full-close: write=%d full=%d", spy.writeCloses, spy.fullCloses)
	}
	wrapped.Close()
}
