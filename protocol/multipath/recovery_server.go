package multipath

import (
	"container/list"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common/buf"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type recoveryServerGroup struct {
	i              *Inbound
	id             [16]byte
	policy         recoveryPolicy
	mu             sync.Mutex
	controls       [2]net.Conn
	sources        [2]M.Socksaddr
	sourceSequence [2]uint64
	packets        map[[16]byte]*recoveryPacketConn
	closedPackets  map[[16]byte]time.Time
	closedTCP      map[[16]byte]time.Time
	tcpSessions    list.List // round-robin ownership queries; protected by mu
	lastSeen       time.Time
	lease          time.Duration
	closed         bool
}

func (i *Inbound) recoveryGroup(id [16]byte) *recoveryServerGroup {
	i.recoveryMu.Lock()
	defer i.recoveryMu.Unlock()
	return i.recoveryGroups[id]
}

func (i *Inbound) serveRecoveryControl(conn net.Conn, h helloMessage, onClose N.CloseHandlerFunc) {
	if !h.Recovery || h.Group == [16]byte{} || h.Group != h.Session || h.Create || h.FrameSize != 1 {
		i.rejectHello(conn, onClose, helloRejectSessionMismatch, errors.New("invalid multipath recovery control hello"))
		return
	}
	i.recoveryMu.Lock()
	g := i.recoveryGroups[h.Group]
	if g == nil {
		if i.recoveryClosed || !i.cfg.Memory.reservePage(32768, true) {
			i.recoveryMu.Unlock()
			i.rejectHello(conn, onClose, helloRejectLegUnavailable, errMemoryLimit)
			return
		}
		g = &recoveryServerGroup{i: i, id: h.Group, packets: make(map[[16]byte]*recoveryPacketConn), closedPackets: make(map[[16]byte]time.Time), closedTCP: make(map[[16]byte]time.Time), lastSeen: time.Now(), lease: 2 * time.Minute}
		i.recoveryGroups[h.Group] = g
	}
	i.recoveryMu.Unlock()
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		N.CloseOnHandshakeFailure(conn, onClose, net.ErrClosed)
		return
	}
	previous := g.controls[h.LegID]
	g.controls[h.LegID] = conn
	g.mu.Unlock()
	if previous != nil {
		previous.Close()
	}
	defer func() {
		conn.Close()
		g.mu.Lock()
		if g.controls[h.LegID] == conn {
			g.controls[h.LegID] = nil
		}
		g.mu.Unlock()
		if onClose != nil {
			onClose(nil)
		}
	}()
	if err := writeHelloResponse(conn, helloResponse{Status: helloStatusOK, FrameSize: 1, PolicyDigest: h.Policy.digest()}); err != nil {
		return
	}
	var queries recoverySessionQueries
	for {
		g.mu.Lock()
		lease := g.lease
		g.mu.Unlock()
		_ = conn.SetDeadline(time.Now().Add(lease))
		message, absent, err := readRecoveryRequest(conn)
		if err != nil || (queries.count < 64 && absent>>queries.count != 0) {
			return
		}
		lease = time.Duration(binary.BigEndian.Uint64(message[18:26]))
		if lease < 2*time.Minute || lease > 2*time.Hour || message[16] > 3 || message[17] > 1 || binary.BigEndian.Uint32(message[26:]) != 0 {
			return
		}
		g.mu.Lock()
		g.lease = lease
		g.lastSeen = time.Now()
		g.mu.Unlock()
		g.policy.update(binary.BigEndian.Uint64(message[8:16]), message[16], message[17])
		g.releaseAbsentSessions(queries, absent)
		queries = g.sessionQueries()
		if err := writeRecoveryResponse(conn, message, queries); err != nil {
			return
		}
	}
}

func (i *Inbound) NewPacket(buffer *buf.Buffer, source M.Socksaddr) {
	defer buffer.Release()
	d, err := decodeRecoveryDatagram(buffer.Bytes())
	if err != nil {
		return
	}
	g := i.recoveryGroup(d.group)
	if g == nil {
		return
	}
	if d.kind == recoveryUDPPong {
		return
	}
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return
	}
	g.lastSeen = time.Now()
	if d.kind == recoveryUDPPing && d.seq > g.sourceSequence[d.leg] {
		g.sources[d.leg] = source
		g.sourceSequence[d.leg] = d.seq
	}
	// Native UDP addresses are established only by a challenge on that leg.
	// Old packets cannot retarget server replies after rebinding or failback.
	known := g.sources[d.leg] == source
	g.mu.Unlock()
	if !known {
		return
	}
	g.policy.update(d.epoch, d.mask, d.path)
	switch d.kind {
	case recoveryUDPPing:
		d.kind = recoveryUDPPong
		_ = g.writeTo(d, source)
	case recoveryUDPData:
		g.receive(d, source)
	case recoveryUDPClose:
		g.mu.Lock()
		c := g.packets[d.session]
		g.mu.Unlock()
		if c != nil {
			c.Close()
		}
	}
}

func (g *recoveryServerGroup) writeTo(d recoveryDatagram, source M.Socksaddr) error {
	p, err := d.encode()
	if err != nil {
		return err
	}
	return g.i.listener.PacketWriter().WritePacket(buf.As(p), source)
}

func (g *recoveryServerGroup) receive(d recoveryDatagram, source M.Socksaddr) {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return
	}
	if _, closed := g.closedPackets[d.session]; closed {
		g.mu.Unlock()
		_ = g.writeTo(recoveryDatagram{kind: recoveryUDPClose, group: g.id, session: d.session, leg: d.leg}, source)
		return
	}
	c := g.packets[d.session]
	created := false
	if c == nil {
		if !g.i.cfg.Memory.reservePage(128, true) {
			g.mu.Unlock()
			return
		}
		var err error
		c, err = newRecoveryPacketConn(d.session, g.i.cfg.Memory, func(reply recoveryDatagram) error {
			reply.group = g.id
			reply.epoch, reply.mask, reply.path = g.policy.snapshot()
			reply.leg = reply.path
			g.mu.Lock()
			address := g.sources[reply.leg]
			closed := g.closed
			g.mu.Unlock()
			if closed {
				return net.ErrClosed
			}
			if !address.IsValid() {
				return nil
			}
			// An outer send error does not destroy the target-side UDP socket.
			_ = g.writeTo(reply, address)
			return nil
		}, func() {
			g.mu.Lock()
			delete(g.packets, d.session)
			if !g.closed {
				g.closedPackets[d.session] = time.Now()
			}
			closed := g.closed
			path := byte(g.policy.udp.Load())
			address := g.sources[path]
			g.mu.Unlock()
			if !closed && address.IsValid() {
				_ = g.writeTo(recoveryDatagram{kind: recoveryUDPClose, group: g.id, session: d.session, leg: path}, address)
			}
		})
		if err != nil {
			g.i.cfg.Memory.releaseSession(128)
			g.mu.Unlock()
			return
		}
		g.packets[d.session] = c
		created = true
	}
	g.mu.Unlock()
	c.deliver(d, time.Now())
	if created {
		metadata := adapter.InboundContext{Inbound: g.i.Tag(), InboundType: g.i.Type(), Source: source, Destination: d.address, Network: N.NetworkUDP}
		go g.i.router.RoutePacketConnectionEx(log.ContextWithNewID(g.i.ctx), c, metadata, func(error) { c.Close() })
	}
}

func (g *recoveryServerGroup) close() {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return
	}
	g.closed = true
	recordBytes := int64(len(g.closedPackets)+len(g.packets)+len(g.closedTCP)) * 128
	var packets []*recoveryPacketConn
	for _, c := range g.packets {
		packets = append(packets, c)
	}
	controls := g.controls
	g.mu.Unlock()
	for _, c := range controls {
		if c != nil {
			c.Close()
		}
	}
	for _, c := range packets {
		c.Close()
	}
	g.i.access.Lock()
	var cores []*mpCore
	for _, s := range g.i.sessions {
		if s.group == g {
			cores = append(cores, s.core)
		}
	}
	g.i.access.Unlock()
	for _, c := range cores {
		c.Close()
	}
	g.i.cfg.Memory.releaseSession(32768 + recordBytes)
}

func (i *Inbound) recoveryMaintenance(ctx context.Context) {
	for recoveryWait(ctx, time.Second) {
		now := time.Now()
		i.expireClosedSessions(now)
		i.recoveryMu.Lock()
		var expired []*recoveryServerGroup
		var live []*recoveryServerGroup
		for id, g := range i.recoveryGroups {
			g.mu.Lock()
			stale := now.Sub(g.lastSeen) >= g.lease
			g.mu.Unlock()
			if stale {
				delete(i.recoveryGroups, id)
				expired = append(expired, g)
			} else {
				live = append(live, g)
			}
		}
		i.recoveryMu.Unlock()
		for _, g := range expired {
			g.close()
		}
		for _, g := range live {
			g.mu.Lock()
			if g.closed {
				g.mu.Unlock()
				continue
			}
			for id, at := range g.closedTCP {
				if !at.IsZero() && now.Sub(at) >= 2*time.Minute {
					delete(g.closedTCP, id)
					g.i.cfg.Memory.releaseSession(128)
				}
			}
			var packets []*recoveryPacketConn
			for _, c := range g.packets {
				packets = append(packets, c)
			}
			for id, at := range g.closedPackets {
				if now.Sub(at) >= 2*time.Minute {
					delete(g.closedPackets, id)
					g.i.cfg.Memory.releaseSession(128)
				}
			}
			g.mu.Unlock()
			for _, c := range packets {
				c.expire(now)
				if now.Sub(time.Unix(0, c.lastActivity.Load())) >= 5*time.Minute {
					c.Close()
				}
			}
		}
	}
}
