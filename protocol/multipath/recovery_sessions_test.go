package multipath

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"testing/synctest"
	"time"
)

func TestRecoverySessionQueryCodec(t *testing.T) {
	var message [recoveryControlSize]byte
	message[0] = 123
	for _, count := range []uint16{0, 1, 64} {
		queries := recoverySessionQueries{count: count}
		for n := range int(count) {
			queries.ids[n][0] = byte(n + 1)
		}
		var wire bytes.Buffer
		if err := writeRecoveryResponse(&wire, message, queries); err != nil {
			t.Fatal(err)
		}
		m, q, err := readRecoveryResponse(&wire)
		if err != nil || m != message || q != queries {
			t.Fatalf("response roundtrip: %v", err)
		}
		if err := writeRecoveryRequest(&wire, message, ^uint64(0)); err != nil {
			t.Fatal(err)
		}
		m, absent, err := readRecoveryRequest(&wire)
		if err != nil || m != message || absent != ^uint64(0) {
			t.Fatalf("request roundtrip: %v", err)
		}
	}
	var malformed [recoveryControlSize + 2]byte
	binary.BigEndian.PutUint16(malformed[recoveryControlSize:], 65)
	if _, _, err := readRecoveryResponse(bytes.NewReader(malformed[:])); err == nil {
		t.Fatal("accepted oversized query")
	}
	binary.BigEndian.PutUint16(malformed[recoveryControlSize:], 1)
	if _, _, err := readRecoveryResponse(bytes.NewReader(malformed[:])); err == nil {
		t.Fatal("accepted truncated query")
	}
}

func TestRecoveryControlReclaimsAbandonedSessions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		budget := newMemoryBudget(64<<20, false)
		o := &Outbound{ctx: context.Background(), tags: []string{"leg0", "leg1"}, cfg: coreConfig{Memory: budget}, failoverTimeout: 5 * time.Second, failbackDelay: 30 * time.Second}
		r, err := newRecoveryClient(o)
		if err != nil {
			t.Fatal(err)
		}
		defer r.close()
		i := &Inbound{cfg: coreConfig{Memory: budget}, sessions: make(map[[16]byte]*serverSession), recoveryGroups: make(map[[16]byte]*recoveryServerGroup)}
		connect := func() net.Conn {
			a, b := net.Pipe()
			go i.serveRecoveryControl(a, helloMessage{Recovery: true, Control: true, Group: r.id, Session: r.id, FrameSize: 1}, nil)
			if _, err := readHelloResponse(b); err != nil {
				t.Fatal(err)
			}
			return b
		}
		conn := connect()
		g := i.recoveryGroup(r.id)
		defer g.close()
		defer func() { conn.Close() }()
		var cores []*mpCore
		for n := range 150 {
			var id [16]byte
			binary.BigEndian.PutUint16(id[:], uint16(n+1))
			cfg := flowTestConfig()
			cfg.Memory, cfg.Recovery = budget, &g.policy
			core, app := newCore(context.Background(), cfg)
			cores = append(cores, core)
			session := &serverSession{id: id, group: g, core: core, appConn: app}
			i.access.Lock()
			i.sessions[id] = session
			g.mu.Lock()
			if !budget.reservePage(128, true) {
				t.Fatal("tombstone admission")
			}
			g.closedTCP[id] = time.Time{}
			session.recoveryEntry = g.tcpSessions.PushBack(id)
			g.mu.Unlock()
			i.access.Unlock()
			go func() { <-core.released; i.removeSession(id, session) }()
			// Retain the last logical connection despite losing BOTH data legs.
			if n == 149 {
				if err := r.registerTCP(id); err != nil {
					t.Fatal(err)
				}
				defer r.unregisterTCP(id)
			}
		}
		defer func() {
			for _, c := range cores {
				c.Close()
			}
			for _, c := range cores {
				<-c.released
			}
		}()
		var queries recoverySessionQueries
		exchange := func() {
			message := r.controlMessage()
			if err := writeRecoveryRequest(conn, message, r.absentSessions(queries)); err != nil {
				t.Fatal(err)
			}
			response, q, err := readRecoveryResponse(conn)
			if err != nil || response != message {
				t.Fatalf("exchange: %v", err)
			}
			queries = q
			synctest.Wait()
		}
		exchange()
		// Lose an outstanding query batch on a control disconnect. No closure
		// is inferred, and the next connection must query those IDs again.
		conn.Close()
		conn = connect()
		queries = recoverySessionQueries{}
		for range 8 {
			exchange()
		}
		for n, c := range cores {
			if c.isDone() != (n != 149) {
				t.Fatalf("wrong ownership result for session %d", n)
			}
		}
		for range 3 {
			time.Sleep(time.Minute)
			exchange()
		}
		if cores[149].isDone() {
			t.Fatal("live client session expired during a long data-path outage")
		}
		if budget.sessions.Load() != 1 {
			t.Fatalf("orphan cores retained: %d", budget.sessions.Load())
		}
	})
}

func TestApplicationCloseIdleBound(t *testing.T) {
	for _, closeWriteOnly := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			cfg := flowTestConfig()
			cfg.Recovery = &recoveryPolicy{}
			c, app := newCore(context.Background(), cfg)
			defer func() { c.Close(); <-c.released }()
			if closeWriteOnly {
				app.(interface{ CloseWrite() error }).CloseWrite()
			} else {
				app.Close()
			}
			time.Sleep(applicationCloseIdleTimeout - time.Second)
			if c.isDone() {
				t.Fatal("drain terminated early")
			}
			time.Sleep(24 * time.Hour)
			if c.isDone() == closeWriteOnly {
				t.Fatalf("wrong close semantics, half-close=%v", closeWriteOnly)
			}
			if !closeWriteOnly && c.memory.sessions.Load() != 0 {
				t.Fatal("expired close retained session")
			}
		})
	}
}

func TestApplicationCloseIdleBoundWaitsForFINReceipt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		left, app := newCore(context.Background(), flowTestConfig())
		right, peer := newCore(context.Background(), flowTestConfig())
		defer closeFlowCores(left, right)
		a, b := net.Pipe()
		gate := &finAckGate{Conn: b, started: make(chan struct{}), release: make(chan struct{}), stopped: make(chan struct{})}
		connectTestLeg(t, left, right, 0, a, gate)
		go func() { _, _ = app.Write(make([]byte, 32<<10)); app.Close() }()
		if _, err := io.Copy(io.Discard, peer); err != nil {
			t.Fatal(err)
		}
		<-gate.started
		time.Sleep(applicationCloseIdleTimeout / 2)
		if left.isDone() {
			t.Fatal("full Close did not allow a delayed FIN ACK")
		}
		close(gate.release)
		synctest.Wait()
		if !left.isDone() {
			t.Fatal("FIN acknowledgement did not complete close")
		}
	})
}

func TestApplicationCloseProgressExtendsDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := flowTestConfig()
		cfg.Recovery = &recoveryPolicy{}
		c, app := newCore(context.Background(), cfg)
		defer func() { c.Close(); <-c.released }()
		app.Close()
		for range 3 {
			time.Sleep(applicationCloseIdleTimeout / 2)
			c.stateMu.Lock()
			c.tx.Una++ // inject cumulative Data ACK progress, not a probe/window update
			c.stateMu.Unlock()
			c.finishApplicationClose()
			if c.isDone() {
				t.Fatal("progressing drain was terminated")
			}
		}
		time.Sleep(applicationCloseIdleTimeout + time.Second)
		if !c.isDone() {
			t.Fatal("stopped drain did not expire")
		}
	})
}
