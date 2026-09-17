package multipath

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	statusSchemaVersion = 4
	statusTopFlowCount  = 10

	leg1PhaseWaiting int32 = iota
	leg1PhaseConnecting
	leg1PhaseReady
	leg1PhaseRetrying
)

type coreTrafficCounters struct {
	logicalTX uint64
	logicalRX uint64
	legJoins  [2]uint64
	legTX     [2]uint64
	legRX     [2]uint64
	legTXF    [2]uint64
	legRXF    [2]uint64
}

type senderTotals struct {
	logicalTX          uint64
	legTX              [2]uint64
	legTXF             [2]uint64
	fallbackBytes      uint64
	fallbackFrames     uint64
	fallbackEvents     uint64
	replayTimeouts     uint64
	backpressureEvents uint64
	backpressureNanos  uint64
	legFailures        [2]uint64
}

func (t *senderTotals) add(other senderTotals) {
	t.logicalTX += other.logicalTX
	t.fallbackBytes += other.fallbackBytes
	t.fallbackFrames += other.fallbackFrames
	t.fallbackEvents += other.fallbackEvents
	t.replayTimeouts += other.replayTimeouts
	t.backpressureEvents += other.backpressureEvents
	t.backpressureNanos += other.backpressureNanos
	for index := range t.legTX {
		t.legTX[index] += other.legTX[index]
		t.legTXF[index] += other.legTXF[index]
		t.legFailures[index] += other.legFailures[index]
	}
}

func (s coreStatusSnapshot) localSenderTotals() senderTotals {
	return senderTotals{
		logicalTX:          s.counters.logicalTX,
		legTX:              s.counters.legTX,
		legTXF:             s.counters.legTXF,
		fallbackBytes:      s.fallbackBytes,
		fallbackFrames:     s.fallbackFrames,
		fallbackEvents:     s.fallbackEvents,
		replayTimeouts:     s.replayTimeouts,
		backpressureEvents: s.backpressureEvents,
		backpressureNanos:  s.backpressureNanos,
		legFailures:        s.legFailures,
	}
}

func senderTotalsFromPeer(status senderStatus) senderTotals {
	return senderTotals{
		logicalTX:          status.LogicalTX,
		legTX:              status.LegTX,
		legTXF:             status.LegTXFrames,
		fallbackBytes:      status.FallbackBytes,
		fallbackFrames:     status.FallbackFrames,
		fallbackEvents:     status.FallbackEvents,
		replayTimeouts:     status.ReplayTimeouts,
		backpressureEvents: status.BackpressureEvents,
		backpressureNanos:  status.BackpressureNanos,
		legFailures:        status.LegFailures,
	}
}

func (c *coreTrafficCounters) add(other coreTrafficCounters) {
	c.logicalTX += other.logicalTX
	c.logicalRX += other.logicalRX
	for index := range c.legTX {
		c.legJoins[index] += other.legJoins[index]
		c.legTX[index] += other.legTX[index]
		c.legRX[index] += other.legRX[index]
		c.legTXF[index] += other.legTXF[index]
		c.legRXF[index] += other.legRXF[index]
	}
}

type coreStatusSnapshot struct {
	dataMode           uint64
	counters           coreTrafficCounters
	active             bool
	activation         activationInfo
	activationAt       time.Time
	legPresent         [2]bool
	legBacklog         [2]int64
	legWriting         [2]int64
	legWriteBlock      [2]time.Duration
	legPeak            [2]int64
	replayBytes        int64
	replayPeak         int64
	reorderBytes       int64
	reorderFrames      int64
	reorderPeak        int64
	reorderFPeak       int64
	fallbackBytes      uint64
	fallbackFrames     uint64
	fallbackEvents     uint64
	replayTimeouts     uint64
	backpressureEvents uint64
	backpressureNanos  uint64
	legFailures        [2]uint64
	peerSender         peerSenderStatus
	rtt                [2]legRTTSnapshot
	failure            string
	failureAt          time.Time
}

func (c *mpCore) statusSnapshot() coreStatusSnapshot {
	snapshot := coreStatusSnapshot{
		active:             c.active.Load(),
		reorderBytes:       c.reorderBytes.Load(),
		reorderFrames:      c.reorderCount.Load(),
		replayPeak:         c.replayPeak.Load(),
		reorderPeak:        c.reorderPeak.Load(),
		reorderFPeak:       c.reorderFPeak.Load(),
		fallbackBytes:      c.fallbackB.Load(),
		fallbackFrames:     c.fallbackF.Load(),
		fallbackEvents:     c.fallbackE.Load(),
		replayTimeouts:     c.replayTO.Load(),
		backpressureEvents: c.backpressE.Load(),
		backpressureNanos:  c.backpressNS.Load(),
		peerSender:         c.peerSenderStatusSnapshot(),
		rtt:                c.rttSnapshot(),
		counters: coreTrafficCounters{
			logicalTX: c.ingressBytes.Load(),
			logicalRX: c.egressBytes.Load(),
		},
	}
	c.stateMu.Lock()
	snapshot.dataMode = c.dataModeLocked()
	c.stateMu.Unlock()
	for index := range snapshot.legPresent {
		snapshot.counters.legJoins[index] = c.legCounters[index].joins.Load()
		leg := c.getLeg(uint8(index))
		snapshot.legPresent[index] = leg != nil
		if leg != nil {
			snapshot.legBacklog[index] = leg.backlogBytes()
			snapshot.legWriting[index], snapshot.legWriteBlock[index] = leg.writingSnapshot(time.Now())
		}
		snapshot.legPeak[index] = c.legPeak[index].Load()
		snapshot.counters.legTX[index] = c.legCounters[index].txBytes.Load()
		snapshot.counters.legRX[index] = c.legCounters[index].rxBytes.Load()
		snapshot.counters.legTXF[index] = c.legCounters[index].txFrames.Load()
		snapshot.counters.legRXF[index] = c.legCounters[index].rxFrames.Load()
	}
	c.activationMu.Lock()
	snapshot.activation = c.activation
	snapshot.activationAt = c.activationAt
	c.activationMu.Unlock()
	c.legFailureMu.Lock()
	snapshot.legFailures = c.legFailures
	c.legFailureMu.Unlock()
	c.replayMu.Lock()
	snapshot.replayBytes = c.replayBytes
	c.replayMu.Unlock()
	c.failureMu.Lock()
	snapshot.failure = c.failure
	snapshot.failureAt = c.failureAt
	c.failureMu.Unlock()
	return snapshot
}

type statusSession struct {
	parent       *outboundStatus
	id           string
	destination  string
	startedAt    time.Time
	core         *mpCore
	leg1Phase    atomic.Int32
	leg1Attempts atomic.Uint64
}

func statusSessionID(id [16]byte) string {
	return hex.EncodeToString(id[:4])
}

func (s *statusSession) setLeg1Phase(phase int32) {
	if s != nil {
		s.leg1Phase.Store(phase)
	}
}

func (s *statusSession) beginLeg1Attempt() {
	if s == nil {
		return
	}
	s.leg1Attempts.Add(1)
	s.leg1Phase.Store(leg1PhaseConnecting)
}

func (s *statusSession) recordLegError(legID uint8, stage string, err error) {
	if s == nil || err == nil {
		return
	}
	attempt := uint64(0)
	if legID == 1 {
		attempt = s.leg1Attempts.Load()
	}
	s.parent.recordLegError(legID, stage, s.destination, s.id[:8], attempt, err, time.Now())
}

type outboundStatusConfig struct {
	recovery         *recoveryClient
	tag              string
	aggregation      string
	udpOutbound      string
	tcpFastOpen      bool
	handshakeTimeout time.Duration
	legTags          [2]string
	legTypes         [2]string
	cfg              coreConfig
	policy           sessionPolicy
}

type statusErrorEvent struct {
	message     string
	source      string
	category    string
	stage       string
	destination string
	sessionID   string
	attempt     uint64
	count       uint64
	transient   bool
	harmless    bool
	at          time.Time
}

type udpTrafficCounters struct {
	txBytes atomic.Uint64
	rxBytes atomic.Uint64
}

type outboundStatus struct {
	file      string
	startedAt time.Time
	config    outboundStatusConfig

	access          sync.Mutex
	sessions        map[string]*statusSession
	closed          coreTrafficCounters
	closedSender    senderTotals
	closedRemote    senderTotals
	closedAttempts  uint64
	connectionsMade uint64
	legErrors       [2]statusErrorEvent
	udpCounters     udpTrafficCounters
	udpLegCounters  [2]udpTrafficCounters
	previousUDPLegs [2]statusTraffic

	sampleAccess      sync.Mutex
	lastSample        time.Time
	previous          coreTrafficCounters
	previousUDP       statusTraffic
	previousSessions  map[string]coreTrafficCounters
	peakLogical       statusPeakRate
	peakLeg           [2]statusPeakRate
	peakReplayLocal   int64
	peakReplayRemote  int64
	peakLegBacklog    [2]int64
	peakRemoteBacklog [2]int64

	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

func newOutboundStatus(file string, config outboundStatusConfig) *outboundStatus {
	return &outboundStatus{
		file:             file,
		startedAt:        time.Now(),
		config:           config,
		sessions:         make(map[string]*statusSession),
		previousSessions: make(map[string]coreTrafficCounters),
		stop:             make(chan struct{}),
		done:             make(chan struct{}),
	}
}

func (s *outboundStatus) addSession(id [16]byte, destination string, core *mpCore, phase int32) *statusSession {
	session := &statusSession{
		parent:      s,
		id:          hex.EncodeToString(id[:]),
		destination: destination,
		startedAt:   time.Now(),
		core:        core,
	}
	session.leg1Phase.Store(phase)
	s.access.Lock()
	s.sessions[session.id] = session
	s.connectionsMade++
	s.access.Unlock()
	go func() {
		<-core.released
		s.removeSession(session)
	}()
	return session
}

func (s *outboundStatus) removeSession(session *statusSession) {
	finalSnapshot := session.core.statusSnapshot()
	s.access.Lock()
	if current := s.sessions[session.id]; current == session {
		delete(s.sessions, session.id)
		s.closed.add(finalSnapshot.counters)
		s.closedSender.add(finalSnapshot.localSenderTotals())
		if finalSnapshot.peerSender.status.Sequence > 0 {
			s.closedRemote.add(senderTotalsFromPeer(finalSnapshot.peerSender.status))
		}
		s.closedAttempts += session.leg1Attempts.Load()
	}
	s.access.Unlock()
}

func classifyStatusError(err error) (category string, transient bool, harmless bool) {
	if err == nil {
		return "unknown", false, false
	}
	message := strings.ToLower(err.Error())
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return "peer_closed", true, true
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return "peer_closed", true, false
	}
	if reason, rejected := helloRejectReasonFromError(err); rejected {
		harmless = reason == helloRejectSessionUnavailable || reason == helloRejectLegUnavailable
		return "hello_rejected", harmless, harmless
	}
	if errors.Is(err, errLeg1Stalled) {
		return "replay_timeout", true, false
	}
	if errors.Is(err, context.DeadlineExceeded) || strings.Contains(message, "timeout") {
		return "timeout", true, false
	}
	return "transport_error", false, false
}

func (s *outboundStatus) recordLegError(legID uint8, stage string, destination string, sessionID string, attempt uint64, err error, at time.Time) {
	if s == nil || err == nil || legID > 1 {
		return
	}
	category, transient, harmless := classifyStatusError(err)
	source := closeSourceUnknown
	var sourced *sourcedLegError
	if errors.As(err, &sourced) {
		source = sourced.source
	}
	s.access.Lock()
	count := s.legErrors[legID].count
	if !isEndpointLegError(err) {
		count++
	}
	s.legErrors[legID] = statusErrorEvent{
		message:     err.Error(),
		source:      source.String(),
		category:    category,
		stage:       stage,
		destination: destination,
		sessionID:   sessionID,
		attempt:     attempt,
		count:       count,
		transient:   transient,
		harmless:    harmless,
		at:          at,
	}
	s.access.Unlock()
}

func (s *outboundStatus) countUDPTX(bytes int64) {
	if s != nil && bytes > 0 {
		s.udpCounters.txBytes.Add(uint64(bytes))
	}
}

func (s *outboundStatus) countRecoveryUDP(id byte, rx bool, bytes int64) {
	if s == nil || id > 1 || bytes <= 0 {
		return
	}
	if rx {
		s.countUDPRX(bytes)
		s.udpLegCounters[id].rxBytes.Add(uint64(bytes))
	} else {
		s.countUDPTX(bytes)
		s.udpLegCounters[id].txBytes.Add(uint64(bytes))
	}
}

func (s *outboundStatus) countUDPRX(bytes int64) {
	if s != nil && bytes > 0 {
		s.udpCounters.rxBytes.Add(uint64(bytes))
	}
}

func (s *outboundStatus) udpSnapshot() statusTraffic {
	return statusTraffic{
		TXBytes: s.udpCounters.txBytes.Load(),
		RXBytes: s.udpCounters.rxBytes.Load(),
	}
}

type statusTraffic struct {
	TXBytes uint64 `json:"tx_bytes"`
	RXBytes uint64 `json:"rx_bytes"`
}

type statusRate struct {
	TXBytesPS uint64 `json:"tx_bytes_per_second"`
	RXBytesPS uint64 `json:"rx_bytes_per_second"`
}

type statusPeakRate struct {
	TXBytesPS uint64 `json:"tx_bytes_per_second"`
	RXBytesPS uint64 `json:"rx_bytes_per_second"`
	TXAt      string `json:"tx_at,omitempty"`
	RXAt      string `json:"rx_at,omitempty"`
}

type statusFrames struct {
	TX uint64 `json:"tx"`
	RX uint64 `json:"rx"`
}

type statusActivation struct {
	Reason             string `json:"reason"`
	At                 string `json:"at"`
	CurrentBytes       uint64 `json:"current_bytes,omitempty"`
	ThresholdBytes     uint64 `json:"threshold_bytes,omitempty"`
	WindowBytes        uint64 `json:"window_bytes,omitempty"`
	RateBytesPS        uint64 `json:"rate_bytes_per_second,omitempty"`
	ThresholdBytesPS   uint64 `json:"threshold_bytes_per_second,omitempty"`
	MinRateBytesPS     uint64 `json:"min_rate_bytes_per_second,omitempty"`
	ElapsedMS          int64  `json:"elapsed_ms,omitempty"`
	BacklogBytes       int64  `json:"backlog_bytes,omitempty"`
	QueueBytes         int64  `json:"queue_bytes,omitempty"`
	RequiredDurationMS int64  `json:"required_duration_ms,omitempty"`
}

type statusParameters struct {
	FrameSize          int                       `json:"frame_size"`
	MemoryLimitBytes   int64                     `json:"memory_limit_bytes"`
	HandshakeTimeoutMS int64                     `json:"handshake_timeout_ms"`
	Upload             statusDirectionParameters `json:"upload"`
	Download           statusDirectionParameters `json:"download"`
}
type statusDirectionParameters struct {
	AggregationEnabled          bool    `json:"aggregation_enabled"`
	Leg0TrafficSaving           bool    `json:"leg0_traffic_saving"`
	ActivationOnQueue           bool    `json:"activation_on_queue"`
	ActivationThresholdMbps     uint64  `json:"activation_threshold_mbps"`
	ActivationAfterBytes        uint64  `json:"activation_after_bytes"`
	ActivationAfterBytesMinMbps uint64  `json:"activation_after_bytes_min_mbps"`
	ActivationWindowMS          int64   `json:"activation_window_ms"`
	QueueFrames                 uint32  `json:"queue_frames"`
	QueueBytes                  uint64  `json:"queue_bytes"`
	SendBufferBytes             uint64  `json:"send_buffer_bytes"`
	ReceiveWindowBytes          uint64  `json:"receive_window_bytes"`
	PathStallTimeoutMinMS       int64   `json:"path_stall_timeout_min_ms"`
	EffectiveSendBufferBytes    *uint64 `json:"effective_send_buffer_bytes,omitempty"`
	EffectiveReceiveWindowBytes *uint64 `json:"effective_receive_window_bytes,omitempty"`
}

func directionParameters(p directionPolicy, frame int) statusDirectionParameters {
	p = p.normalized()
	return statusDirectionParameters{
		AggregationEnabled: p.AggregationEnabled, Leg0TrafficSaving: p.Leg0TrafficSaving,
		ActivationOnQueue: p.ActivationOnQueue, ActivationThresholdMbps: p.ThresholdBytesPS * 8 / 1_000_000,
		ActivationAfterBytes: p.ActivationAfterBytes, ActivationAfterBytesMinMbps: p.ActivationAfterBytesMinBytesPS * 8 / 1_000_000,
		ActivationWindowMS: p.ActivationWindow.Milliseconds(), QueueFrames: p.QueueFrames, QueueBytes: uint64(p.QueueFrames) * uint64(frame),
		SendBufferBytes: p.SendBufferBytes, ReceiveWindowBytes: p.ReceiveWindowBytes, PathStallTimeoutMinMS: p.PathStallTimeoutMin.Milliseconds(),
	}
}

type statusMemory struct {
	LimitBytes         int64  `json:"limit_bytes"`
	UsedBytes          int64  `json:"used_bytes"`
	CachedBytes        int64  `json:"cached_bytes"`
	BoosterLimitBytes  int64  `json:"booster_limit_bytes"`
	BoosterResumeBytes int64  `json:"booster_resume_bytes"`
	Automatic          bool   `json:"automatic"`
	Pressure           bool   `json:"pressure"`
	PressureSince      string `json:"pressure_since,omitempty"`
	PressureEvents     uint64 `json:"pressure_events"`
	BackpressureEvents uint64 `json:"backpressure_events"`
	PeakUsedBytes      int64  `json:"peak_used_bytes"`
	PeakCachedBytes    int64  `json:"peak_cached_bytes"`
}

type statusSenderDiagnostics struct {
	Available                bool   `json:"available"`
	Stale                    bool   `json:"stale"`
	UpdatedAt                string `json:"updated_at,omitempty"`
	StaleConnections         int    `json:"stale_connections"`
	SendBufferBytes          int64  `json:"replay_bytes"`
	ReplayPeakBytes          int64  `json:"replay_peak_bytes"`
	FallbackBytes            uint64 `json:"fallback_bytes"`
	FallbackFrames           uint64 `json:"fallback_frames"`
	FallbackEvents           uint64 `json:"fallback_events"`
	Leg1TXBytes              uint64 `json:"leg1_tx_bytes"`
	ReplayTimeouts           uint64 `json:"replay_timeouts"`
	BackpressureEvents       uint64 `json:"backpressure_events"`
	BackpressureDurationMS   uint64 `json:"backpressure_duration_ms"`
	MemoryPressure           bool   `json:"memory_pressure"`
	MemoryUsedBytes          uint64 `json:"memory_used_bytes"`
	MemoryPeakUsedBytes      uint64 `json:"memory_peak_used_bytes"`
	MemoryPressureEvents     uint64 `json:"memory_pressure_events"`
	MemoryBackpressureEvents uint64 `json:"memory_backpressure_events"`
}

type statusLogical struct {
	UploadStates             map[string]int          `json:"upload_states"`
	DownloadStates           map[string]int          `json:"download_states"`
	State                    string                  `json:"state"`
	Connections              int                     `json:"connections"`
	ConnectionsTotal         uint64                  `json:"connections_total"`
	PreferredOnlyConnections int                     `json:"preferred_only_connections"`
	TXAggregatingConnections int                     `json:"tx_aggregating_connections"`
	RXAggregatingConnections int                     `json:"rx_aggregating_connections"`
	BoosterDegraded          int                     `json:"booster_degraded_connections"`
	Current                  statusRate              `json:"current"`
	Peak                     statusPeakRate          `json:"peak"`
	Cumulative               statusTraffic           `json:"cumulative"`
	SendBufferBytes          int64                   `json:"replay_bytes"`
	ReorderBytes             int64                   `json:"reorder_bytes"`
	ReorderFrames            int64                   `json:"reorder_pages"`
	ReorderPeakBytes         int64                   `json:"reorder_peak_bytes"`
	ReorderPeakFrames        int64                   `json:"reorder_peak_pages"`
	LocalSender              statusSenderDiagnostics `json:"local_sender"`
	RemoteSender             statusSenderDiagnostics `json:"remote_sender"`
	LastActivation           *statusActivation       `json:"last_activation,omitempty"`
}

type statusFlow struct {
	SessionID    string        `json:"session_id"`
	Destination  string        `json:"destination"`
	StartedAt    string        `json:"started_at"`
	AgeSeconds   int64         `json:"age_seconds"`
	State        string        `json:"state"`
	Current      statusRate    `json:"current"`
	Cumulative   statusTraffic `json:"cumulative"`
	BacklogBytes int64         `json:"backlog_bytes"`
}

type statusLeg struct {
	RemoteDeliveryRate      uint64         `json:"remote_delivery_bytes_per_second"`
	RemoteDeliveryRTT       uint64         `json:"remote_delivery_rtt_max_ms"`
	RemoteMinimumRTT        uint64         `json:"remote_delivery_rtt_min_ms"`
	RemotePipeline          uint64         `json:"remote_pipeline_bytes"`
	ID                      int            `json:"id"`
	Tag                     string         `json:"tag"`
	Type                    string         `json:"type"`
	Role                    string         `json:"role"`
	State                   string         `json:"state"`
	Connections             int            `json:"connections"`
	CarryingConnections     int            `json:"carrying_connections"`
	StandbyConnections      int            `json:"standby_connections"`
	ConnectingConnections   int            `json:"connecting_connections"`
	RetryingConnections     int            `json:"retrying_connections"`
	Current                 statusRate     `json:"current"`
	Peak                    statusPeakRate `json:"peak"`
	Cumulative              statusTraffic  `json:"cumulative"`
	Frames                  statusFrames   `json:"frames"`
	BacklogBytes            int64          `json:"backlog_bytes"`
	WritingBytes            int64          `json:"writing_bytes"`
	WriteBlockedMS          int64          `json:"write_blocked_ms"`
	PeakBacklogBytes        int64          `json:"peak_backlog_bytes"`
	RemoteBacklogBytes      int64          `json:"remote_backlog_bytes"`
	RemoteWritingBytes      int64          `json:"remote_writing_bytes"`
	RemoteWriteBlockedMS    int64          `json:"remote_write_blocked_ms"`
	RemotePeakBacklogBytes  int64          `json:"remote_peak_backlog_bytes"`
	QueueBytesPerConnection int64          `json:"queue_bytes_per_connection"`
	JoinCount               uint64         `json:"join_count"`
	AttemptCount            uint64         `json:"attempt_count"`
	UDPSelected             bool           `json:"udp_selected"`
	UDPCurrent              statusRate     `json:"udp_current"`
	UDPCumulative           statusTraffic  `json:"udp_cumulative"`
	LastError               string         `json:"last_error,omitempty"`
	LastErrorSource         string         `json:"last_error_source,omitempty"`
	LastErrorAt             string         `json:"last_error_at,omitempty"`
	LastErrorCategory       string         `json:"last_error_category,omitempty"`
	LastErrorStage          string         `json:"last_error_stage,omitempty"`
	LastErrorDestination    string         `json:"last_error_destination,omitempty"`
	LastErrorSessionID      string         `json:"last_error_session_id,omitempty"`
	LastErrorAttempt        uint64         `json:"last_error_attempt,omitempty"`
	ErrorCount              uint64         `json:"error_count"`
	RemoteFailureCount      uint64         `json:"remote_failure_count"`
	RemoteLastFailureStage  string         `json:"remote_last_failure_stage,omitempty"`
	RTTLatestMS             float64        `json:"rtt_latest_ms"`
	RTTAverageMS            float64        `json:"rtt_average_ms"`
	RTTEWMAMS               float64        `json:"rtt_ewma_ms"`
	RTTMinMS                float64        `json:"rtt_min_ms"`
	RTTMaxMS                float64        `json:"rtt_max_ms"`
	RTTJitterMS             float64        `json:"rtt_jitter_ms"`
	RTTSamples              uint64         `json:"rtt_samples"`
	ProbeSent               uint64         `json:"probe_sent"`
	ProbeTimeout            uint64         `json:"probe_timeout"`
	LastErrorTransient      bool           `json:"last_error_transient"`
	LastErrorHarmless       bool           `json:"last_error_harmless"`
	TopFlows                []statusFlow   `json:"top_flows"`
}

type statusNode struct {
	Recovery    *recoveryStatus  `json:"recovery,omitempty"`
	Tag         string           `json:"tag"`
	Type        string           `json:"type"`
	Aggregation string           `json:"aggregation_server"`
	UDPOutbound string           `json:"udp_outbound"`
	TCPFastOpen bool             `json:"tcp_fast_open"`
	Parameters  statusParameters `json:"parameters"`
	Memory      statusMemory     `json:"memory"`
	Logical     statusLogical    `json:"logical"`
	Legs        []statusLeg      `json:"legs"`
}

type statusDocument struct {
	SchemaVersion    int        `json:"schema_version"`
	GeneratedAt      string     `json:"generated_at"`
	ProcessStartedAt string     `json:"process_started_at"`
	Node             statusNode `json:"node"`
}

type sampledSession struct {
	session  *statusSession
	snapshot coreStatusSnapshot
	rates    [2]statusRate
}

func counterRate(current, previous uint64, elapsed time.Duration) uint64 {
	if current < previous || elapsed <= 0 {
		return 0
	}
	return uint64(float64(current-previous) / elapsed.Seconds())
}

func trafficRate(current, previous coreTrafficCounters, elapsed time.Duration) (statusRate, [2]statusRate) {
	logical := statusRate{
		TXBytesPS: counterRate(current.logicalTX, previous.logicalTX, elapsed),
		RXBytesPS: counterRate(current.logicalRX, previous.logicalRX, elapsed),
	}
	var legs [2]statusRate
	for index := range legs {
		legs[index] = statusRate{
			TXBytesPS: counterRate(current.legTX[index], previous.legTX[index], elapsed),
			RXBytesPS: counterRate(current.legRX[index], previous.legRX[index], elapsed),
		}
	}
	return logical, legs
}

func senderRate(current, previous senderTotals, elapsed time.Duration) (uint64, [2]uint64) {
	logical := counterRate(current.logicalTX, previous.logicalTX, elapsed)
	var legs [2]uint64
	for index := range legs {
		legs[index] = counterRate(current.legTX[index], previous.legTX[index], elapsed)
	}
	return logical, legs
}

func updatePeakRate(peak *statusPeakRate, current statusRate, now time.Time) {
	if current.TXBytesPS > peak.TXBytesPS {
		peak.TXBytesPS = current.TXBytesPS
		peak.TXAt = now.Format(time.RFC3339Nano)
	}
	if current.RXBytesPS > peak.RXBytesPS {
		peak.RXBytesPS = current.RXBytesPS
		peak.RXAt = now.Format(time.RFC3339Nano)
	}
}

func durationMilliseconds(value time.Duration) float64 {
	return float64(value) / float64(time.Millisecond)
}

func activationStatus(info activationInfo, at time.Time) *statusActivation {
	if at.IsZero() {
		return nil
	}
	return &statusActivation{
		Reason:             string(info.Reason),
		At:                 at.Format(time.RFC3339Nano),
		CurrentBytes:       info.CurrentBytes,
		ThresholdBytes:     info.ThresholdBytes,
		WindowBytes:        info.WindowBytes,
		RateBytesPS:        info.RateBytesPS,
		ThresholdBytesPS:   info.ThresholdBytesPS,
		MinRateBytesPS:     info.MinRateBytesPS,
		ElapsedMS:          info.Elapsed.Milliseconds(),
		BacklogBytes:       info.BacklogBytes,
		QueueBytes:         info.QueueBytes,
		RequiredDurationMS: info.RequiredDuration.Milliseconds(),
	}
}

func (s *outboundStatus) buildDocument(now time.Time) statusDocument {
	s.sampleAccess.Lock()
	defer s.sampleAccess.Unlock()

	s.access.Lock()
	sessions := make([]*statusSession, 0, len(s.sessions))
	for _, session := range s.sessions {
		sessions = append(sessions, session)
	}
	totals := s.closed
	localSenderTotals := s.closedSender
	remoteSenderTotals := s.closedRemote
	leg1Attempts := s.closedAttempts
	connectionsMade := s.connectionsMade
	legErrors := s.legErrors
	s.access.Unlock()

	snapshots := make([]sampledSession, 0, len(sessions))
	for _, session := range sessions {
		session.core.scheduleProbes(now)
		snapshot := session.core.statusSnapshot()
		totals.add(snapshot.counters)
		localSenderTotals.add(snapshot.localSenderTotals())
		if snapshot.peerSender.status.Sequence > 0 {
			remoteSenderTotals.add(senderTotalsFromPeer(snapshot.peerSender.status))
		}
		snapshots = append(snapshots, sampledSession{session: session, snapshot: snapshot})
	}

	elapsed := now.Sub(s.lastSample)
	firstSample := s.lastSample.IsZero()
	logicalRate, legRates := trafficRate(totals, s.previous, elapsed)
	udpTotals := s.udpSnapshot()
	udpRate := statusRate{
		TXBytesPS: counterRate(udpTotals.TXBytes, s.previousUDP.TXBytes, elapsed),
		RXBytesPS: counterRate(udpTotals.RXBytes, s.previousUDP.RXBytes, elapsed),
	}
	if firstSample {
		logicalRate = statusRate{}
		legRates = [2]statusRate{}
		udpRate = statusRate{}
	}
	logicalRate.TXBytesPS += udpRate.TXBytesPS
	logicalRate.RXBytesPS += udpRate.RXBytesPS

	nextPreviousSessions := make(map[string]coreTrafficCounters, len(snapshots))
	for index := range snapshots {
		item := &snapshots[index]
		previous, loaded := s.previousSessions[item.session.id]
		_, item.rates = trafficRate(item.snapshot.counters, previous, elapsed)
		if !loaded || firstSample {
			item.rates = [2]statusRate{}
		}
		nextPreviousSessions[item.session.id] = item.snapshot.counters
	}
	s.lastSample = now
	s.previous = totals
	s.previousUDP = udpTotals
	s.previousSessions = nextPreviousSessions

	memorySnapshot := s.config.cfg.Memory.snapshot()
	parameters := statusParameters{
		FrameSize: s.config.cfg.FrameSize, MemoryLimitBytes: memorySnapshot.LimitBytes, HandshakeTimeoutMS: s.config.handshakeTimeout.Milliseconds(),
		Upload:   directionParameters(s.config.policy.Upload, s.config.cfg.FrameSize),
		Download: directionParameters(s.config.policy.Download, s.config.cfg.FrameSize),
	}
	localSend, localReceive := uint64(s.config.cfg.SendBufferBytes), uint64(s.config.cfg.ReceiveWindowBytes)
	parameters.Upload.EffectiveSendBufferBytes = &localSend
	parameters.Download.EffectiveReceiveWindowBytes = &localReceive

	memory := statusMemory{
		LimitBytes:         memorySnapshot.LimitBytes,
		UsedBytes:          memorySnapshot.UsedBytes,
		CachedBytes:        memorySnapshot.CachedBytes,
		BoosterLimitBytes:  memorySnapshot.BoosterLimitBytes,
		BoosterResumeBytes: memorySnapshot.BoosterResumeBytes,
		Automatic:          memorySnapshot.Automatic,
		Pressure:           memorySnapshot.Pressure,
		PressureEvents:     memorySnapshot.PressureEvents,
		BackpressureEvents: memorySnapshot.BackpressureEvents,
		PeakUsedBytes:      memorySnapshot.PeakUsedBytes,
		PeakCachedBytes:    memorySnapshot.PeakCachedBytes,
	}
	if !memorySnapshot.PressureSince.IsZero() {
		memory.PressureSince = memorySnapshot.PressureSince.Format(time.RFC3339Nano)
	}
	logical := statusLogical{
		UploadStates: make(map[string]int), DownloadStates: make(map[string]int),
		Connections:      len(snapshots),
		ConnectionsTotal: connectionsMade,
		Current:          logicalRate,
		Cumulative: statusTraffic{
			TXBytes: totals.logicalTX + udpTotals.TXBytes,
			RXBytes: totals.logicalRX + udpTotals.RXBytes,
		},
		LocalSender: statusSenderDiagnostics{
			Available:                true,
			FallbackBytes:            localSenderTotals.fallbackBytes,
			FallbackFrames:           localSenderTotals.fallbackFrames,
			FallbackEvents:           localSenderTotals.fallbackEvents,
			Leg1TXBytes:              localSenderTotals.legTX[1],
			ReplayTimeouts:           localSenderTotals.replayTimeouts,
			BackpressureEvents:       localSenderTotals.backpressureEvents,
			BackpressureDurationMS:   localSenderTotals.backpressureNanos / uint64(time.Millisecond),
			MemoryPressure:           memorySnapshot.Pressure,
			MemoryUsedBytes:          uint64(max(0, memorySnapshot.UsedBytes)),
			MemoryPeakUsedBytes:      uint64(max(0, memorySnapshot.PeakUsedBytes)),
			MemoryPressureEvents:     memorySnapshot.PressureEvents,
			MemoryBackpressureEvents: memorySnapshot.BackpressureEvents,
		},
		RemoteSender: statusSenderDiagnostics{
			Available:              remoteSenderTotals.logicalTX > 0 || remoteSenderTotals.legFailures[0] > 0 || remoteSenderTotals.legFailures[1] > 0,
			FallbackBytes:          remoteSenderTotals.fallbackBytes,
			FallbackFrames:         remoteSenderTotals.fallbackFrames,
			FallbackEvents:         remoteSenderTotals.fallbackEvents,
			Leg1TXBytes:            remoteSenderTotals.legTX[1],
			ReplayTimeouts:         remoteSenderTotals.replayTimeouts,
			BackpressureEvents:     remoteSenderTotals.backpressureEvents,
			BackpressureDurationMS: remoteSenderTotals.backpressureNanos / uint64(time.Millisecond),
		},
	}
	legs := []statusLeg{
		{
			ID: 0, Tag: s.config.legTags[0], Type: s.config.legTypes[0], Role: "preferred",
			Current:                 legRates[0],
			Cumulative:              statusTraffic{TXBytes: totals.legTX[0], RXBytes: totals.legRX[0]},
			Frames:                  statusFrames{TX: totals.legTXF[0], RX: totals.legRXF[0]},
			QueueBytesPerConnection: s.config.cfg.QueueBytes,
			JoinCount:               totals.legJoins[0],
			AttemptCount:            connectionsMade,
			RemoteFailureCount:      remoteSenderTotals.legFailures[0],
			TopFlows:                []statusFlow{},
		},
		{
			ID: 1, Tag: s.config.legTags[1], Type: s.config.legTypes[1], Role: "booster",
			Current:                 legRates[1],
			Cumulative:              statusTraffic{TXBytes: totals.legTX[1], RXBytes: totals.legRX[1]},
			Frames:                  statusFrames{TX: totals.legTXF[1], RX: totals.legRXF[1]},
			QueueBytesPerConnection: s.config.cfg.QueueBytes,
			JoinCount:               totals.legJoins[1],
			AttemptCount:            leg1Attempts,
			RemoteFailureCount:      remoteSenderTotals.legFailures[1],
			TopFlows:                []statusFlow{},
		},
	}
	for index := range legs {
		if s.config.recovery != nil || legs[index].Tag == s.config.udpOutbound {
			current, total := udpRate, udpTotals
			legs[index].UDPSelected = legs[index].Tag == s.config.udpOutbound
			if r := s.config.recovery; r != nil {
				total = statusTraffic{TXBytes: s.udpLegCounters[index].txBytes.Load(), RXBytes: s.udpLegCounters[index].rxBytes.Load()}
				current = statusRate{TXBytesPS: counterRate(total.TXBytes, s.previousUDPLegs[index].TXBytes, elapsed), RXBytesPS: counterRate(total.RXBytes, s.previousUDPLegs[index].RXBytes, elapsed)}
				if firstSample {
					current = statusRate{}
				}
				s.previousUDPLegs[index] = total
				legs[index].UDPSelected = int(r.policy.udp.Load()) == index
			}
			legs[index].UDPCurrent = current
			legs[index].UDPCumulative = total
			legs[index].Current.TXBytesPS += current.TXBytesPS
			legs[index].Current.RXBytesPS += current.RXBytesPS
			legs[index].Cumulative.TXBytes += total.TXBytes
			legs[index].Cumulative.RXBytes += total.RXBytes
		}
		if !legErrors[index].at.IsZero() {
			legs[index].LastError = legErrors[index].message
			legs[index].LastErrorSource = legErrors[index].source
			legs[index].LastErrorAt = legErrors[index].at.Format(time.RFC3339Nano)
			legs[index].LastErrorCategory = legErrors[index].category
			legs[index].LastErrorStage = legErrors[index].stage
			legs[index].LastErrorDestination = legErrors[index].destination
			legs[index].LastErrorSessionID = legErrors[index].sessionID
			legs[index].LastErrorAttempt = legErrors[index].attempt
			legs[index].ErrorCount = legErrors[index].count
			legs[index].LastErrorTransient = legErrors[index].transient
			legs[index].LastErrorHarmless = legErrors[index].harmless
		}
	}
	var latestActivation time.Time
	var latestRemote time.Time
	var latestRemoteFailure [2]time.Time
	for _, item := range snapshots {
		snapshot := item.snapshot
		logical.UploadStates[dataModeName(snapshot.dataMode)]++
		if snapshot.peerSender.status.Sequence > 0 && now.Sub(snapshot.peerSender.receivedAt) <= 3*time.Second {
			logical.DownloadStates[dataModeName(snapshot.peerSender.status.DataMode)]++
		} else {
			logical.DownloadStates["unknown"]++
		}
		logical.SendBufferBytes += snapshot.replayBytes
		logical.LocalSender.SendBufferBytes += snapshot.replayBytes
		s.peakReplayLocal = max(s.peakReplayLocal, snapshot.replayPeak)
		logical.ReorderBytes += snapshot.reorderBytes
		logical.ReorderFrames += snapshot.reorderFrames
		logical.ReorderPeakBytes = max(logical.ReorderPeakBytes, snapshot.reorderPeak)
		logical.ReorderPeakFrames = max(logical.ReorderPeakFrames, snapshot.reorderFPeak)
		remote := snapshot.peerSender
		if remote.status.Sequence > 0 {
			logical.RemoteSender.Available = true
			logical.RemoteSender.SendBufferBytes += int64(remote.status.SendBufferBytes)
			s.peakReplayRemote = max(s.peakReplayRemote, int64(remote.status.ReplayPeakBytes))
			if now.Sub(remote.receivedAt) > 3*time.Second {
				logical.RemoteSender.StaleConnections++
			}
			if remote.receivedAt.After(latestRemote) {
				latestRemote = remote.receivedAt
				remoteSend, remoteReceive := remote.status.SendBufferLimit, remote.status.ReceiveWindowLimit
				parameters.Download.EffectiveSendBufferBytes = &remoteSend
				parameters.Upload.EffectiveReceiveWindowBytes = &remoteReceive
				logical.RemoteSender.UpdatedAt = remote.receivedAt.Format(time.RFC3339Nano)
				logical.RemoteSender.MemoryPressure = remote.status.Flags&senderStatusFlagMemoryPressure != 0
				logical.RemoteSender.MemoryUsedBytes = remote.status.MemoryUsed
				logical.RemoteSender.MemoryPeakUsedBytes = remote.status.MemoryPeakUsed
				logical.RemoteSender.MemoryPressureEvents = remote.status.MemoryPressureEvents
				logical.RemoteSender.MemoryBackpressureEvents = remote.status.MemoryBackpressureEvents
			}
		}
		if snapshot.dataMode == 2 {
			logical.TXAggregatingConnections++
		} else if snapshot.dataMode == 1 {
			logical.PreferredOnlyConnections++
		}
		if remote.status.Sequence > 0 && now.Sub(remote.receivedAt) <= 3*time.Second && remote.status.DataMode == 2 {
			logical.RXAggregatingConnections++
		}
		if snapshot.active && !snapshot.legPresent[1] {
			logical.BoosterDegraded++
		}
		if snapshot.activationAt.After(latestActivation) {
			latestActivation = snapshot.activationAt
			logical.LastActivation = activationStatus(snapshot.activation, snapshot.activationAt)
		}
		for legIndex := range legs {
			leg := &legs[legIndex]
			leg.BacklogBytes += snapshot.legBacklog[legIndex]
			s.peakLegBacklog[legIndex] = max(s.peakLegBacklog[legIndex], snapshot.legPeak[legIndex])
			leg.WritingBytes += snapshot.legWriting[legIndex]
			leg.WriteBlockedMS = max(leg.WriteBlockedMS, snapshot.legWriteBlock[legIndex].Milliseconds())
			if remote.status.Sequence > 0 {
				leg.RemoteDeliveryRate += remote.status.LegDeliveryRate[legIndex]
				leg.RemoteDeliveryRTT = max(leg.RemoteDeliveryRTT, remote.status.LegDeliveryRTT[legIndex]/uint64(time.Millisecond))
				rtt := remote.status.LegMinimumRTT[legIndex] / uint64(time.Millisecond)
				if rtt > 0 && (leg.RemoteMinimumRTT == 0 || rtt < leg.RemoteMinimumRTT) {
					leg.RemoteMinimumRTT = rtt
				}
				leg.RemotePipeline += remote.status.LegPipeline[legIndex]
				leg.RemoteBacklogBytes += int64(remote.status.LegBacklog[legIndex])
				leg.RemoteWritingBytes += int64(remote.status.LegWriting[legIndex])
				leg.RemoteWriteBlockedMS = max(leg.RemoteWriteBlockedMS, int64(remote.status.LegWriteBlockedNanos[legIndex]/uint64(time.Millisecond)))
				s.peakRemoteBacklog[legIndex] = max(s.peakRemoteBacklog[legIndex], int64(remote.status.LegPeakBacklog[legIndex]))
				if int(remote.status.LastFailureLeg) == legIndex && remote.receivedAt.After(latestRemoteFailure[legIndex]) {
					leg.RemoteLastFailureStage = string(senderStatusStage(remote.status.LastFailureStage))
					latestRemoteFailure[legIndex] = remote.receivedAt
				}
			}
			rtt := snapshot.rtt[legIndex]
			if rtt.Samples > 0 {
				leg.RTTLatestMS += durationMilliseconds(rtt.Latest) * float64(rtt.Samples)
				leg.RTTAverageMS += durationMilliseconds(rtt.Total)
				leg.RTTEWMAMS += durationMilliseconds(rtt.EWMA) * float64(rtt.Samples)
				leg.RTTJitterMS += durationMilliseconds(rtt.Jitter) * float64(rtt.Samples)
				if leg.RTTMinMS == 0 || durationMilliseconds(rtt.Minimum) < leg.RTTMinMS {
					leg.RTTMinMS = durationMilliseconds(rtt.Minimum)
				}
				leg.RTTMaxMS = max(leg.RTTMaxMS, durationMilliseconds(rtt.Maximum))
				leg.RTTSamples += rtt.Samples
			}
			leg.ProbeSent += rtt.ProbeSent
			leg.ProbeTimeout += rtt.ProbeTimeout
			flowRate := item.rates[legIndex]
			if snapshot.legPresent[legIndex] {
				leg.Connections++
				if flowRate.TXBytesPS+flowRate.RXBytesPS > 0 {
					leg.CarryingConnections++
				}
			}
			if flowRate.TXBytesPS+flowRate.RXBytesPS == 0 {
				continue
			}
			leg.TopFlows = append(leg.TopFlows, statusFlow{
				SessionID:   item.session.id[:8],
				Destination: item.session.destination,
				StartedAt:   item.session.startedAt.Format(time.RFC3339Nano),
				AgeSeconds:  int64(now.Sub(item.session.startedAt).Seconds()),
				State:       "carrying",
				Current:     flowRate,
				Cumulative: statusTraffic{
					TXBytes: snapshot.counters.legTX[legIndex],
					RXBytes: snapshot.counters.legRX[legIndex],
				},
				BacklogBytes: snapshot.legBacklog[legIndex],
			})
		}
		legs[1].AttemptCount += item.session.leg1Attempts.Load()
		if !snapshot.legPresent[1] {
			switch item.session.leg1Phase.Load() {
			case leg1PhaseRetrying:
				legs[1].RetryingConnections++
			default:
				legs[1].ConnectingConnections++
			}
		}
	}
	logical.RemoteSender.Stale = logical.RemoteSender.StaleConnections > 0
	s.peakReplayLocal = max(s.peakReplayLocal, logical.LocalSender.SendBufferBytes)
	s.peakReplayRemote = max(s.peakReplayRemote, logical.RemoteSender.SendBufferBytes)
	logical.LocalSender.ReplayPeakBytes = s.peakReplayLocal
	logical.RemoteSender.ReplayPeakBytes = s.peakReplayRemote
	updatePeakRate(&s.peakLogical, logical.Current, now)
	logical.Peak = s.peakLogical
	for index := range legs {
		leg := &legs[index]
		updatePeakRate(&s.peakLeg[index], leg.Current, now)
		leg.Peak = s.peakLeg[index]
		s.peakLegBacklog[index] = max(s.peakLegBacklog[index], leg.BacklogBytes)
		s.peakRemoteBacklog[index] = max(s.peakRemoteBacklog[index], leg.RemoteBacklogBytes)
		leg.PeakBacklogBytes = s.peakLegBacklog[index]
		leg.RemotePeakBacklogBytes = s.peakRemoteBacklog[index]
		if leg.RTTSamples > 0 {
			weight := float64(leg.RTTSamples)
			leg.RTTLatestMS /= weight
			leg.RTTAverageMS /= weight
			leg.RTTEWMAMS /= weight
			leg.RTTJitterMS /= weight
		}
	}

	for index := range legs {
		leg := &legs[index]
		leg.StandbyConnections = leg.Connections - leg.CarryingConnections
		sort.Slice(leg.TopFlows, func(left, right int) bool {
			leftRate := leg.TopFlows[left].Current.TXBytesPS + leg.TopFlows[left].Current.RXBytesPS
			rightRate := leg.TopFlows[right].Current.TXBytesPS + leg.TopFlows[right].Current.RXBytesPS
			return leftRate > rightRate
		})
		if len(leg.TopFlows) > statusTopFlowCount {
			leg.TopFlows = leg.TopFlows[:statusTopFlowCount]
		}
		if leg.Current.TXBytesPS+leg.Current.RXBytesPS > 0 {
			leg.State = "carrying"
		} else if len(snapshots) == 0 {
			leg.State = "idle"
		} else if leg.CarryingConnections > 0 {
			leg.State = "carrying"
		} else if leg.Connections > 0 {
			leg.State = "standby"
		} else if leg.RetryingConnections > 0 {
			leg.State = "retrying"
		} else {
			leg.State = "connecting"
		}
	}
	if len(snapshots) == 0 && udpRate.TXBytesPS+udpRate.RXBytesPS > 0 {
		logical.State = "preferred_only"
	} else if len(snapshots) == 0 {
		logical.State = "idle"
	} else if logical.BoosterDegraded > 0 {
		logical.State = "booster_degraded"
	} else if logical.UploadStates["leg1"] > 0 || logical.DownloadStates["leg1"] > 0 {
		logical.State = "traffic_saving"
	} else if logical.UploadStates["leg0_fallback"] > 0 || logical.DownloadStates["leg0_fallback"] > 0 {
		logical.State = "leg0_fallback"
	} else if logical.TXAggregatingConnections > 0 || logical.RXAggregatingConnections > 0 {
		logical.State = "aggregating"
	} else {
		logical.State = "preferred_only"
	}

	if r := s.config.recovery; r != nil {
		for index := range legs {
			legs[index].AttemptCount = r.attempts[index].Load()
		}
		if r.policy.mask.Load() == 0 {
			logical.State = "unavailable"
		} else if !r.policy.allows(0) {
			logical.State = "failover"
		}
	}
	return statusDocument{
		// Schema 4 reports client-owned directional policies and sender modes.
		SchemaVersion:    statusSchemaVersion,
		GeneratedAt:      now.Format(time.RFC3339Nano),
		ProcessStartedAt: s.startedAt.Format(time.RFC3339Nano),
		Node: statusNode{
			Recovery:    s.config.recovery.snapshot(now),
			Tag:         s.config.tag,
			Type:        "multipath",
			Aggregation: s.config.aggregation,
			UDPOutbound: s.config.udpOutbound,
			TCPFastOpen: s.config.tcpFastOpen,
			Parameters:  parameters,
			Memory:      memory,
			Logical:     logical,
			Legs:        legs,
		},
	}
}

func writeStatusDocument(path string, document statusDocument) error {
	content, err := json.Marshal(document)
	if err != nil {
		return err
	}
	directory := filepath.Dir(path)
	if err = os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".multipath-status-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err = temporary.Chmod(0o644); err == nil {
		_, err = temporary.Write(content)
	}
	closeErr := temporary.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}

func (s *outboundStatus) start(ctx context.Context, reportError func(error)) {
	go func() {
		defer close(s.done)
		defer os.Remove(s.file)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		lastError := ""
		write := func(now time.Time) {
			err := writeStatusDocument(s.file, s.buildDocument(now))
			if err == nil {
				lastError = ""
				return
			}
			if message := err.Error(); message != lastError {
				lastError = message
				reportError(err)
			}
		}
		write(time.Now())
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.stop:
				return
			case now := <-ticker.C:
				write(now)
			}
		}
	}()
}

func (s *outboundStatus) close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() { close(s.stop) })
	<-s.done
	if err := os.Remove(s.file); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
