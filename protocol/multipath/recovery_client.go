package multipath

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

const recoveryControlSize = 30

type recoveryClient struct {
	attempts             [2]atomic.Uint64
	o                    *Outbound
	id                   [16]byte
	ctx                  context.Context
	cancel               context.CancelFunc
	policy               recoveryPolicy
	timeout, delay       time.Duration
	preferredUDP         byte
	mu                   sync.Mutex
	health               [2]recoveryHealth
	udp                  [2]net.Conn
	control              [2]net.Conn
	udpWrite             [2]sync.Mutex
	udpChallenges        [2]map[uint64]time.Time
	packets              map[[16]byte]*recoveryPacketConn
	tcpSessions          map[[16]byte]struct{}
	changed              chan struct{}
	tcpChoice, udpChoice byte
	closed               bool
	workers              sync.WaitGroup
}

func newRecoveryClient(o *Outbound) (*recoveryClient, error) {
	id, err := newSessionID()
	if err != nil {
		return nil, err
	}
	if !o.cfg.Memory.reservePage(128<<10, true) {
		return nil, errMemoryLimit
	}
	ctx, cancel := context.WithCancel(o.ctx)
	r := &recoveryClient{o: o, id: id, ctx: ctx, cancel: cancel, timeout: o.failoverTimeout, delay: o.failbackDelay, packets: make(map[[16]byte]*recoveryPacketConn), tcpSessions: make(map[[16]byte]struct{}), changed: make(chan struct{})}
	if o.udpTag == o.tags[1] {
		r.preferredUDP = 1
		r.udpChoice = 1
	}
	r.policy.update(1, 0, r.udpChoice)
	for id := byte(0); id < 2; id++ {
		r.udpChallenges[id] = make(map[uint64]time.Time)
	}
	return r, nil
}

func (r *recoveryClient) start() {
	for id := byte(0); id < 2; id++ {
		r.workers.Add(2)
		go func(id byte) { defer r.workers.Done(); r.runControl(id) }(id)
		go func(id byte) { defer r.workers.Done(); r.runUDP(id) }(id)
	}
	r.workers.Add(1)
	go func() { defer r.workers.Done(); r.runPolicy() }()
}

func recoveryWait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
func (r *recoveryClient) close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	r.cancel()
	close(r.changed)
	var conns []net.Conn
	for _, c := range r.control {
		if c != nil {
			conns = append(conns, c)
		}
	}
	for _, c := range r.udp {
		if c != nil {
			conns = append(conns, c)
		}
	}
	var packets []*recoveryPacketConn
	for _, c := range r.packets {
		packets = append(packets, c)
	}
	r.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
	for _, c := range packets {
		c.Close()
	}
	r.workers.Wait()
	r.o.cfg.Memory.releaseSession(128 << 10)
}

func (r *recoveryClient) controlMessage() [recoveryControlSize]byte {
	var b [recoveryControlSize]byte
	binary.BigEndian.PutUint64(b[:8], uint64(time.Now().UnixNano()))
	epoch, mask, udp := r.policy.snapshot()
	binary.BigEndian.PutUint64(b[8:16], epoch)
	b[16], b[17] = mask, udp
	binary.BigEndian.PutUint64(b[18:26], uint64(max(2*time.Minute, 4*r.timeout+r.delay)))
	return b
}

func (r *recoveryClient) runControl(id byte) {
	for r.ctx.Err() == nil {
		ctx, cancel := context.WithTimeout(r.ctx, r.timeout)
		conn, err := r.o.children[id].DialContext(ctx, N.NetworkTCP, r.o.aggregation)
		if err == nil {
			r.mu.Lock()
			if r.closed {
				r.mu.Unlock()
				conn.Close()
				cancel()
				return
			}
			r.control[id] = conn
			r.mu.Unlock()
			err = r.o.clientHandshake(ctx, conn, helloMessage{Session: r.id, Group: r.id, Recovery: true, Control: true, LegID: id, FrameSize: 1, Destination: r.o.aggregation.String(), PSK: r.o.psk})
		}
		cancel()
		if err == nil {
			var queries recoverySessionQueries
			for r.ctx.Err() == nil {
				message := r.controlMessage()
				started := time.Now()
				_ = conn.SetDeadline(started.Add(r.timeout))
				if err = writeRecoveryRequest(conn, message, r.absentSessions(queries)); err != nil {
					break
				}
				var response [recoveryControlSize]byte
				response, queries, err = readRecoveryResponse(conn)
				if err != nil {
					break
				}
				if response != message || time.Since(started) >= r.timeout {
					break
				}
				r.mu.Lock()
				r.health[id].lastTCP = time.Now()
				r.mu.Unlock()
				if !recoveryWait(r.ctx, min(time.Second, r.timeout/3)) {
					break
				}
			}
		}
		if conn != nil {
			conn.Close()
		}
		r.mu.Lock()
		if r.control[id] == conn {
			r.control[id] = nil
		}
		r.mu.Unlock()
		if !recoveryWait(r.ctx, 250*time.Millisecond) {
			return
		}
	}
}

func (r *recoveryClient) runUDP(id byte) {
	for r.ctx.Err() == nil {
		ctx, cancel := context.WithTimeout(r.ctx, r.timeout)
		conn, err := r.o.children[id].DialContext(ctx, N.NetworkUDP, r.o.aggregation)
		cancel()
		if err != nil {
			if !recoveryWait(r.ctx, time.Second) {
				return
			}
			continue
		}
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			conn.Close()
			return
		}
		r.udp[id] = conn
		r.mu.Unlock()
		readDone := make(chan struct{})
		go func() {
			defer close(readDone)
			buffer := make([]byte, 2048)
			for {
				n, err := conn.Read(buffer)
				if err != nil {
					return
				}
				d, err := decodeRecoveryDatagram(buffer[:n])
				if err != nil || d.group != r.id || d.leg != id {
					continue
				}
				now := time.Now()
				r.mu.Lock()
				if d.kind == recoveryUDPPong {
					if sent, ok := r.udpChallenges[id][d.seq]; ok && now.Sub(sent) < r.timeout {
						r.health[id].lastUDP = now
					}
					delete(r.udpChallenges[id], d.seq)
				}
				packet := r.packets[d.session]
				r.mu.Unlock()
				if d.kind == recoveryUDPData && packet != nil {
					packet.deliver(d, now)
				}
				if d.kind == recoveryUDPClose && packet != nil {
					packet.Close()
				}
			}
		}()
		timer := time.NewTicker(min(time.Second, r.timeout/3))
		alive := true
		for alive {
			now := time.Now()
			sequence := uint64(now.UnixNano())
			r.mu.Lock()
			for seq, sent := range r.udpChallenges[id] {
				if now.Sub(sent) >= r.timeout {
					delete(r.udpChallenges[id], seq)
				}
			}
			r.udpChallenges[id][sequence] = now
			r.mu.Unlock()
			_ = r.sendOn(id, recoveryDatagram{kind: recoveryUDPPing, seq: sequence})
			select {
			case <-r.ctx.Done():
				alive = false
			case <-readDone:
				alive = false
			case <-timer.C:
			}
		}
		timer.Stop()
		conn.Close()
		<-readDone
		r.mu.Lock()
		if r.udp[id] == conn {
			r.udp[id] = nil
		}
		r.mu.Unlock()
		if !recoveryWait(r.ctx, 250*time.Millisecond) {
			return
		}
	}
}

func (r *recoveryClient) sendOn(id byte, d recoveryDatagram) error {
	r.udpWrite[id].Lock()
	defer r.udpWrite[id].Unlock()
	r.mu.Lock()
	conn := r.udp[id]
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return net.ErrClosed
	}
	if conn == nil {
		return nil
	}
	d.group = r.id
	d.leg = id
	d.epoch, d.mask, d.path = r.policy.snapshot()
	p, err := d.encode()
	if err != nil {
		return err
	}
	_ = conn.SetWriteDeadline(time.Now().Add(r.timeout))
	_, err = conn.Write(p)
	if err == nil && d.kind == recoveryUDPData && r.o.status != nil {
		r.o.status.countRecoveryUDP(id, false, int64(len(d.data)))
	}
	// A transient datagram transport error is packet loss, not closure of the
	// logical association. Health checks decide which socket is used next.
	return nil
}

func (r *recoveryClient) runPolicy() {
	for recoveryWait(r.ctx, 100*time.Millisecond) {
		now := time.Now()
		r.mu.Lock()
		for id := range r.health {
			r.health[id].refresh(now, r.timeout, r.delay)
		}
		r.tcpChoice = recoveryChoice(r.tcpChoice, 0, r.health, now, r.delay)
		r.udpChoice = recoveryChoice(r.udpChoice, r.preferredUDP, r.health, now, r.delay)
		mask := byte(0)
		if r.health[0].healthy && r.tcpChoice == 0 {
			mask |= 1
		}
		if r.health[1].healthy {
			mask |= 2
		}
		epoch, oldMask, oldUDP := r.policy.snapshot()
		changed := mask != oldMask || r.udpChoice != oldUDP
		if changed {
			r.policy.update(epoch+1, mask, r.udpChoice)
			close(r.changed)
			r.changed = make(chan struct{})
		}
		var packets []*recoveryPacketConn
		for _, c := range r.packets {
			packets = append(packets, c)
		}
		r.mu.Unlock()
		for _, c := range packets {
			c.expire(now)
		}
		if changed {
			r.o.logger.Info("multipath recovery: TCP leg", r.tcpPath(), " UDP leg", r.policy.udp.Load(), " usable_mask=", mask)
		}
	}
}

func (r *recoveryClient) tcpPath() byte {
	if r.policy.allows(0) {
		return 0
	}
	return 1
}
func (r *recoveryClient) waitReady(ctx context.Context) (byte, error) {
	for {
		r.mu.Lock()
		changed := r.changed
		closed := r.closed
		r.mu.Unlock()
		if closed {
			return 0, net.ErrClosed
		}
		if r.policy.mask.Load() != 0 {
			return r.tcpPath(), nil
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-r.ctx.Done():
			return 0, net.ErrClosed
		case <-changed:
		}
	}
}

func (r *recoveryClient) listenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	ctx, cancel := context.WithTimeout(ctx, r.o.handshakeTimeout)
	defer cancel()
	if _, err := r.waitReady(ctx); err != nil {
		return nil, err
	}
	id, err := newSessionID()
	if err != nil {
		return nil, err
	}
	c, err := newRecoveryPacketConn(id, r.o.cfg.Memory, func(d recoveryDatagram) error { return r.sendOn(byte(r.policy.udp.Load()), d) }, func() {
		r.mu.Lock()
		delete(r.packets, id)
		r.mu.Unlock()
		_ = r.sendOn(byte(r.policy.udp.Load()), recoveryDatagram{kind: recoveryUDPClose, session: id})
	})
	if err != nil {
		return nil, err
	}
	c.onReceive = func(id byte, n int) {
		if r.o.status != nil {
			r.o.status.countRecoveryUDP(id, true, int64(n))
		}
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		c.Close()
		return nil, net.ErrClosed
	}
	r.packets[id] = c
	r.mu.Unlock()
	return c, nil
}

var errRecoveryNotReady = errors.New("multipath recovery group is not established")
