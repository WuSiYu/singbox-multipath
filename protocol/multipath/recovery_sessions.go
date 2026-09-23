package multipath

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
)

const recoveryQueryLimit = 64

// Each control connection owns one bounded query batch. A reply is an absence
// bitmap for that exact batch, not an inventory snapshot or a timeout inference.
// Reconnecting discards the batch and re-queries; no close queue can accumulate.
type recoverySessionQueries struct {
	count uint16
	ids   [recoveryQueryLimit][16]byte
}

func writeRecoveryRequest(w io.Writer, message [recoveryControlSize]byte, absent uint64) error {
	var wire [recoveryControlSize + 8]byte
	copy(wire[:], message[:])
	binary.BigEndian.PutUint64(wire[recoveryControlSize:], absent)
	return writeAll(w, wire[:])
}

func readRecoveryRequest(r io.Reader) (message [recoveryControlSize]byte, absent uint64, err error) {
	var wire [recoveryControlSize + 8]byte
	if _, err = io.ReadFull(r, wire[:]); err != nil {
		return
	}
	copy(message[:], wire[:])
	absent = binary.BigEndian.Uint64(wire[recoveryControlSize:])
	return
}

func writeRecoveryResponse(w io.Writer, message [recoveryControlSize]byte, queries recoverySessionQueries) error {
	if queries.count > recoveryQueryLimit {
		return errors.New("invalid multipath session query count")
	}
	var wire [recoveryControlSize + 2 + recoveryQueryLimit*16]byte
	copy(wire[:], message[:])
	binary.BigEndian.PutUint16(wire[recoveryControlSize:], queries.count)
	for n := range int(queries.count) {
		copy(wire[recoveryControlSize+2+n*16:], queries.ids[n][:])
	}
	return writeAll(w, wire[:recoveryControlSize+2+int(queries.count)*16])
}

func readRecoveryResponse(r io.Reader) (message [recoveryControlSize]byte, queries recoverySessionQueries, err error) {
	var header [recoveryControlSize + 2]byte
	if _, err = io.ReadFull(r, header[:]); err != nil {
		return
	}
	copy(message[:], header[:])
	queries.count = binary.BigEndian.Uint16(header[recoveryControlSize:])
	if queries.count > recoveryQueryLimit {
		err = errors.New("invalid multipath session query count")
		return
	}
	var payload [recoveryQueryLimit * 16]byte
	if _, err = io.ReadFull(r, payload[:int(queries.count)*16]); err != nil {
		return
	}
	for n := range int(queries.count) {
		copy(queries.ids[n][:], payload[n*16:(n+1)*16])
	}
	return
}

func (r *recoveryClient) registerTCP(id [16]byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return net.ErrClosed
	}
	if !r.o.cfg.Memory.reservePage(128, true) {
		return errMemoryLimit
	}
	r.tcpSessions[id] = struct{}{}
	return nil
}

func (r *recoveryClient) unregisterTCP(id [16]byte) {
	r.mu.Lock()
	if _, exists := r.tcpSessions[id]; exists {
		delete(r.tcpSessions, id)
		r.o.cfg.Memory.releaseSession(128)
	}
	r.mu.Unlock()
}

func (r *recoveryClient) absentSessions(queries recoverySessionQueries) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	var absent uint64
	for n := range int(queries.count) {
		if _, exists := r.tcpSessions[queries.ids[n]]; !exists {
			absent |= uint64(1) << n
		}
	}
	return absent
}

func (g *recoveryServerGroup) sessionQueries() (queries recoverySessionQueries) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return
	}
	queries.count = uint16(min(recoveryQueryLimit, g.tcpSessions.Len()))
	for n := range int(queries.count) {
		entry := g.tcpSessions.Front()
		queries.ids[n] = entry.Value.([16]byte)
		g.tcpSessions.MoveToBack(entry)
	}
	return
}

func (g *recoveryServerGroup) releaseAbsentSessions(queries recoverySessionQueries, absent uint64) {
	for n := range int(queries.count) {
		if absent&(uint64(1)<<n) == 0 {
			continue
		}
		g.i.access.Lock()
		session := g.i.sessions[queries.ids[n]]
		g.i.access.Unlock()
		if session != nil && session.group == g {
			// Ownership has ended even if a terminal frame could not cross a
			// failed data path. No application or transport timeout is guessed.
			session.core.noteCloseSource(closeSourcePeerUnknown)
			session.core.terminate(net.ErrClosed, frameTypeSessionClose)
		}
	}
}
