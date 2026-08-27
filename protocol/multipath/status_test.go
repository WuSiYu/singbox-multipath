package multipath

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
)

func waitForStatus(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for status counters")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCoreStatusCounters(t *testing.T) {
	left, leftApp := newCore(context.Background(), testCoreConfig())
	right, rightApp := newCore(context.Background(), testCoreConfig())
	defer left.Close()
	defer right.Close()
	leftWire, rightWire := net.Pipe()
	connectTestLeg(t, left, right, 0, leftWire, rightWire)

	payload := bytes.Repeat([]byte("status-counter"), 4096)
	writeDone := make(chan error, 1)
	go func() {
		_, err := leftApp.Write(payload)
		writeDone <- err
	}()
	received := make([]byte, len(payload))
	if _, err := io.ReadFull(rightApp, received); err != nil {
		t.Fatal(err)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(received, payload) {
		t.Fatal("payload mismatch")
	}
	waitForStatus(t, func() bool {
		return right.statusSnapshot().counters.logicalRX == uint64(len(payload))
	})

	leftSnapshot := left.statusSnapshot()
	rightSnapshot := right.statusSnapshot()
	if leftSnapshot.counters.logicalTX != uint64(len(payload)) {
		t.Fatalf("unexpected logical TX: %d", leftSnapshot.counters.logicalTX)
	}
	if leftSnapshot.counters.legTX[0] != uint64(len(payload)) {
		t.Fatalf("unexpected leg0 TX: %d", leftSnapshot.counters.legTX[0])
	}
	if rightSnapshot.counters.logicalRX != uint64(len(payload)) {
		t.Fatalf("unexpected logical RX: %d", rightSnapshot.counters.logicalRX)
	}
	if rightSnapshot.counters.legRX[0] != uint64(len(payload)) {
		t.Fatalf("unexpected leg0 RX: %d", rightSnapshot.counters.legRX[0])
	}
}

func TestOutboundStatusDocument(t *testing.T) {
	cfg := testCoreConfig()
	cfg.ActivationAfterBytes = 1
	left, leftApp := newCore(context.Background(), cfg)
	right, rightApp := newCore(context.Background(), cfg)
	defer left.Close()
	defer right.Close()
	leg0Left, leg0Right := net.Pipe()
	connectTestLeg(t, left, right, 0, leg0Left, leg0Right)
	leg1Left, leg1Right := net.Pipe()
	connectTestLeg(t, left, right, 1, leg1Left, leg1Right)

	status := newOutboundStatus(filepath.Join(t.TempDir(), "status.json"), outboundStatusConfig{
		tag:              "mp-out",
		aggregation:      "10.0.0.1:39000",
		udpOutbound:      "leg0",
		handshakeTimeout: 10 * time.Second,
		legTags:          [2]string{"leg0", "leg1"},
		legTypes:         [2]string{"direct", "hysteria2"},
		cfg:              cfg,
	})
	var id [16]byte
	id[0] = 0x42
	session := status.addSession(id, "1.1.1.1:443", left, leg1PhaseReady)
	session.leg1Attempts.Store(1)
	status.buildDocument(time.Now())

	payload := bytes.Repeat([]byte("multipath-status"), 8192)
	writeDone := make(chan error, 1)
	go func() {
		_, err := leftApp.Write(payload)
		writeDone <- err
	}()
	received := make([]byte, len(payload))
	if _, err := io.ReadFull(rightApp, received); err != nil {
		t.Fatal(err)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, func() bool {
		snapshot := left.statusSnapshot()
		return snapshot.counters.legTX[0]+snapshot.counters.legTX[1] == uint64(len(payload))
	})

	document := status.buildDocument(time.Now().Add(time.Second))
	if document.Node.Logical.Connections != 1 {
		t.Fatalf("unexpected connection count: %d", document.Node.Logical.Connections)
	}
	if document.Node.Logical.Cumulative.TXBytes != uint64(len(payload)) {
		t.Fatalf("unexpected cumulative TX: %d", document.Node.Logical.Cumulative.TXBytes)
	}
	if document.Node.Logical.Current.TXBytesPS == 0 {
		t.Fatal("logical TX rate was not sampled")
	}
	if document.Node.Legs[0].Cumulative.TXBytes+document.Node.Legs[1].Cumulative.TXBytes != uint64(len(payload)) {
		t.Fatal("leg traffic does not add up to logical traffic")
	}
	topFlowCount := len(document.Node.Legs[0].TopFlows) + len(document.Node.Legs[1].TopFlows)
	if topFlowCount == 0 {
		t.Fatal("active flow missing from leg Top 10")
	}
}

func TestWriteStatusDocument(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "status.json")
	expected := statusDocument{
		SchemaVersion: statusSchemaVersion,
		GeneratedAt:   time.Now().Format(time.RFC3339Nano),
		Node:          statusNode{Tag: "mp-out", Type: "multipath"},
	}
	if err := writeStatusDocument(path, expected); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var actual statusDocument
	if err = json.Unmarshal(content, &actual); err != nil {
		t.Fatal(err)
	}
	if actual.SchemaVersion != statusSchemaVersion || actual.Node.Tag != expected.Node.Tag {
		t.Fatalf("unexpected status document: %+v", actual)
	}
}

func TestOutboundStatusPreservesClosedSessionCounters(t *testing.T) {
	core, appConn := newCore(context.Background(), testCoreConfig())
	status := newOutboundStatus(filepath.Join(t.TempDir(), "status.json"), outboundStatusConfig{
		legTags: [2]string{"leg0", "leg1"},
		cfg:     testCoreConfig(),
	})
	var id [16]byte
	id[0] = 1
	session := status.addSession(id, "1.1.1.1:443", core, leg1PhaseReady)
	session.leg1Attempts.Store(3)
	core.activationMu.Lock()
	core.leg1Joins = 2
	core.activationMu.Unlock()
	if err := appConn.Close(); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, func() bool {
		status.access.Lock()
		defer status.access.Unlock()
		return len(status.sessions) == 0
	})

	document := status.buildDocument(time.Now())
	if document.Node.Legs[1].JoinCount != 2 {
		t.Fatalf("unexpected closed-session join count: %d", document.Node.Legs[1].JoinCount)
	}
	if document.Node.Legs[1].AttemptCount != 3 {
		t.Fatalf("unexpected closed-session attempt count: %d", document.Node.Legs[1].AttemptCount)
	}
}

func TestOutboundStatusWriterLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "multipath", "status.json")
	status := newOutboundStatus(path, outboundStatusConfig{
		tag:     "mp-out",
		legTags: [2]string{"leg0", "leg1"},
		cfg:     testCoreConfig(),
	})
	errors := make(chan error, 1)
	status.start(context.Background(), func(err error) { errors <- err })
	waitForStatus(t, func() bool {
		_, err := os.Stat(path)
		return err == nil
	})
	select {
	case err := <-errors:
		t.Fatal(err)
	default:
	}
	if err := status.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("status file still exists after close: %v", err)
	}
}

func TestOutboundStatusDoesNotCountFailedLegAsCarrying(t *testing.T) {
	core, appConn := newCore(context.Background(), testCoreConfig())
	defer appConn.Close()
	status := newOutboundStatus(filepath.Join(t.TempDir(), "status.json"), outboundStatusConfig{
		legTags: [2]string{"leg0", "leg1"},
		cfg:     testCoreConfig(),
	})
	var id [16]byte
	status.addSession(id, "1.1.1.1:443", core, leg1PhaseRetrying)
	now := time.Now()
	status.buildDocument(now)
	core.legCounters[1].txBytes.Add(1024)

	document := status.buildDocument(now.Add(time.Second))
	leg1 := document.Node.Legs[1]
	if leg1.Connections != 0 || leg1.CarryingConnections != 0 || leg1.StandbyConnections != 0 {
		t.Fatalf("failed leg reported impossible connection counts: %+v", leg1)
	}
	if len(leg1.TopFlows) != 1 {
		t.Fatal("failed leg's traffic from the latest sample should remain visible")
	}
}

func TestOutboundStatusErrorDetails(t *testing.T) {
	core, appConn := newCore(context.Background(), testCoreConfig())
	defer appConn.Close()
	status := newOutboundStatus(filepath.Join(t.TempDir(), "status.json"), outboundStatusConfig{
		legTags: [2]string{"leg0", "leg1"},
		cfg:     testCoreConfig(),
	})
	var id [16]byte
	copy(id[:], []byte{0x12, 0x34, 0x56, 0x78})
	session := status.addSession(id, "1.1.1.1:443", core, leg1PhaseRetrying)
	session.leg1Attempts.Store(3)
	err := &helloRejectedError{reason: helloRejectSessionUnavailable}
	session.recordLegError(1, "secondary_handshake", err)
	session.recordLegError(1, "secondary_handshake", err)

	document := status.buildDocument(time.Now())
	leg := document.Node.Legs[1]
	if leg.LastErrorCategory != "hello_rejected" || leg.LastErrorStage != "secondary_handshake" {
		t.Fatalf("unexpected error classification: %+v", leg)
	}
	if !leg.LastErrorTransient {
		t.Fatal("late secondary hello rejection should be transient")
	}
	if !leg.LastErrorHarmless {
		t.Fatal("late secondary hello rejection should be harmless")
	}
	if leg.LastErrorDestination != "1.1.1.1:443" || leg.LastErrorSessionID != "12345678" {
		t.Fatalf("unexpected error context: %+v", leg)
	}
	if leg.LastErrorAttempt != 3 || leg.ErrorCount != 2 {
		t.Fatalf("unexpected error counters: %+v", leg)
	}
}

func TestClassifyStatusError(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		category  string
		transient bool
		harmless  bool
	}{
		{"eof", io.EOF, "peer_closed", true, true},
		{"unexpected eof", io.ErrUnexpectedEOF, "peer_closed", true, false},
		{"unavailable session", &helloRejectedError{reason: helloRejectSessionUnavailable}, "hello_rejected", true, true},
		{"leg unavailable", &helloRejectedError{reason: helloRejectLegUnavailable}, "hello_rejected", true, true},
		{"replay timeout", errLeg1Stalled, "replay_timeout", true, false},
		{"dial timeout", errors.New("dial tcp: i/o timeout"), "timeout", true, false},
		{"transport", errors.New("connection reset by peer"), "transport_error", false, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			category, transient, harmless := classifyStatusError(test.err)
			if category != test.category || transient != test.transient || harmless != test.harmless {
				t.Fatalf("classifyStatusError(%v) = %q, transient=%v, harmless=%v", test.err, category, transient, harmless)
			}
		})
	}
}

func TestOutboundStatusIncludesUDPInLogicalAndSelectedLeg(t *testing.T) {
	status := newOutboundStatus(filepath.Join(t.TempDir(), "status.json"), outboundStatusConfig{
		udpOutbound: "leg1",
		legTags:     [2]string{"leg0", "leg1"},
		cfg:         testCoreConfig(),
	})
	now := time.Now()
	status.buildDocument(now)
	status.countUDPTX(1500)
	status.countUDPRX(3000)

	document := status.buildDocument(now.Add(time.Second))
	logical := document.Node.Logical
	if logical.Current.TXBytesPS != 1500 || logical.Current.RXBytesPS != 3000 {
		t.Fatalf("unexpected logical UDP rate: %+v", logical.Current)
	}
	if logical.Cumulative.TXBytes != 1500 || logical.Cumulative.RXBytes != 3000 {
		t.Fatalf("unexpected logical UDP cumulative traffic: %+v", logical.Cumulative)
	}
	leg0, leg1 := document.Node.Legs[0], document.Node.Legs[1]
	if leg0.UDPSelected || leg0.Cumulative.TXBytes+leg0.Cumulative.RXBytes != 0 {
		t.Fatalf("UDP traffic was attributed to leg0: %+v", leg0)
	}
	if !leg1.UDPSelected || leg1.UDPCurrent != logical.Current || leg1.UDPCumulative != logical.Cumulative {
		t.Fatalf("UDP traffic missing from selected leg: %+v", leg1)
	}
	if leg1.Current != logical.Current || leg1.Cumulative != logical.Cumulative {
		t.Fatalf("UDP traffic missing from leg totals: %+v", leg1)
	}
	if logical.State != "preferred_only" || leg1.State != "carrying" {
		t.Fatalf("UDP activity did not update states: logical=%s leg1=%s", logical.State, leg1.State)
	}
}

func TestUDPConnectionCounters(t *testing.T) {
	status := newOutboundStatus(filepath.Join(t.TempDir(), "status.json"), outboundStatusConfig{})
	outbound := &Outbound{status: status}
	rawConn, peerConn := net.Pipe()
	defer rawConn.Close()
	defer peerConn.Close()
	conn := outbound.trackUDPConnection(rawConn)

	upload := []byte("udp-upload")
	uploadDone := make(chan error, 1)
	go func() {
		_, err := conn.Write(upload)
		uploadDone <- err
	}()
	receivedUpload := make([]byte, len(upload))
	if _, err := io.ReadFull(peerConn, receivedUpload); err != nil {
		t.Fatal(err)
	}
	if err := <-uploadDone; err != nil {
		t.Fatal(err)
	}

	download := []byte("udp-download")
	downloadDone := make(chan error, 1)
	go func() {
		_, err := peerConn.Write(download)
		downloadDone <- err
	}()
	receivedDownload := make([]byte, len(download))
	if _, err := io.ReadFull(conn, receivedDownload); err != nil {
		t.Fatal(err)
	}
	if err := <-downloadDone; err != nil {
		t.Fatal(err)
	}

	counters := status.udpSnapshot()
	if counters.TXBytes != uint64(len(upload)) || counters.RXBytes != uint64(len(download)) {
		t.Fatalf("unexpected UDP connection counters: %+v", counters)
	}
}

func TestConnectionCoreConfigAttributesPreferredFailureToLeg0(t *testing.T) {
	status := newOutboundStatus(filepath.Join(t.TempDir(), "status.json"), outboundStatusConfig{
		legTags: [2]string{"leg0", "leg1"},
		cfg:     testCoreConfig(),
	})
	outbound := &Outbound{status: status}
	var id [16]byte
	id[0] = 0x42
	cfg := outbound.connectionCoreConfig(context.Background(), M.ParseSocksaddr("1.1.1.1:443"), id)
	cfg.OnLegFailure(0, legFailureHandshake, io.EOF)

	document := status.buildDocument(time.Now())
	if document.Node.Legs[0].ErrorCount != 1 || document.Node.Legs[0].LastErrorStage != "handshake_response" {
		t.Fatalf("preferred failure missing from leg0: %+v", document.Node.Legs[0])
	}
	if document.Node.Legs[1].ErrorCount != 0 {
		t.Fatalf("preferred failure was incorrectly attributed to leg1: %+v", document.Node.Legs[1])
	}
}
