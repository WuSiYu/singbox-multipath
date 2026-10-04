package multipath

import (
	"context"
	"fmt"
	"github.com/sagernet/sing-box/protocol/multipath/stream"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type coreConfig struct {
	Recovery                       *recoveryPolicy
	AggregationEnabled             bool
	Leg0TrafficSaving              bool
	ActivationOnQueue              bool
	FrameSize                      int
	QueueFrames                    int
	QueueBytes                     int64
	ThresholdBytesPS               uint64
	ActivationAfterBytes           uint64
	ActivationAfterBytesMinBytesPS uint64
	ActivationWindow               time.Duration
	ReceiveWindowBytes             int64
	SendBufferBytes                int64
	PathStallTimeoutMin            time.Duration
	Memory                         *memoryBudget
	OnLeg1Active                   func(activationInfo, bool)
	OnLegFailure                   func(uint8, legFailureStage, error)
	OnStatusEvent                  func()
	OnProtocolError                func(error)
	SendStatus                     bool
	HandshakeTimeout               time.Duration
	// LegAbsentTimeout ends a session that has had no attached leg this long.
	LegAbsentTimeout time.Duration
}

type legFailureStage string

const (
	legFailureWriteControl legFailureStage = "write_control"
	legFailureWriteData    legFailureStage = "write_data"
	legFailureHandshake    legFailureStage = "handshake_response"
	legFailureReadData     legFailureStage = "read_data"
	legFailureReplay       legFailureStage = "replay_timeout"
)

type activationReason string

const (
	activationReasonBytes      activationReason = "bytes"
	activationReasonThroughput activationReason = "throughput"
	activationReasonLeg0Queue  activationReason = "leg0_queue"
)

type activationInfo struct {
	Reason           activationReason
	CurrentBytes     uint64
	ThresholdBytes   uint64
	WindowBytes      uint64
	RateBytesPS      uint64
	ThresholdBytesPS uint64
	MinRateBytesPS   uint64
	Elapsed          time.Duration
	BacklogBytes     int64
	QueueBytes       int64
	RequiredDuration time.Duration
}

func (i activationInfo) String() string {
	switch i.Reason {
	case activationReasonBytes:
		if i.MinRateBytesPS > 0 {
			return fmt.Sprintf(
				"reason=%s current_bytes=%d threshold_bytes=%d measured_mbps=%.2f min_mbps=%.2f window=%s",
				i.Reason,
				i.CurrentBytes,
				i.ThresholdBytes,
				float64(i.RateBytesPS)*8/1_000_000,
				float64(i.MinRateBytesPS)*8/1_000_000,
				i.Elapsed.Round(time.Millisecond),
			)
		}
		return fmt.Sprintf("reason=%s current_bytes=%d threshold_bytes=%d", i.Reason, i.CurrentBytes, i.ThresholdBytes)
	case activationReasonThroughput:
		return fmt.Sprintf(
			"reason=%s measured_mbps=%.2f threshold_mbps=%.2f window=%s window_bytes=%d",
			i.Reason,
			float64(i.RateBytesPS)*8/1_000_000,
			float64(i.ThresholdBytesPS)*8/1_000_000,
			i.Elapsed.Round(time.Millisecond),
			i.WindowBytes,
		)
	case activationReasonLeg0Queue:
		ratio := float64(0)
		if i.QueueBytes > 0 {
			ratio = float64(i.BacklogBytes) * 100 / float64(i.QueueBytes)
		}
		return fmt.Sprintf(
			"reason=%s backlog_bytes=%d queue_bytes=%d ratio=%.1f%% duration=%s required_duration=%s",
			i.Reason,
			i.BacklogBytes,
			i.QueueBytes,
			ratio,
			i.Elapsed.Round(time.Millisecond),
			i.RequiredDuration,
		)
	default:
		return fmt.Sprintf("reason=%s", i.Reason)
	}
}

type mpLegCounters struct {
	joins    atomic.Uint64 // Successfully attached transports, independent of TX activation.
	txBytes  atomic.Uint64
	rxBytes  atomic.Uint64
	txFrames atomic.Uint64
	rxFrames atomic.Uint64
}

type legShutdownRequest struct {
	err       error
	frameType byte
	status    *senderStatus
}

type mpLeg struct {
	id                    uint8
	ctx                   context.Context
	cancel                context.CancelFunc
	conn                  net.Conn
	readPreamble          func(net.Conn) error
	send                  chan wireFrame
	queueMu               sync.Mutex
	ready                 atomic.Bool
	control               chan wireFrame
	telemetry             chan struct{}
	telemetryMu           sync.Mutex
	telemetryFrame        wireFrame
	telemetryPending      bool
	shutdown              chan legShutdownRequest
	onClose               func(error)
	done                  chan struct{}
	writerDone            chan struct{}
	readerDone            chan struct{}
	closeOne              sync.Once
	queuedBytes           atomic.Int64
	writingBytes          atomic.Int64
	writeStarted          atomic.Int64
	transportWriteStarted atomic.Int64
	feedback              chan wireFrame
	startupFeedback       *wireFrame
	path                  stream.Path
	busy                  bool
	received              stream.Receipt
	inflight              atomic.Int64
	prepaidFlights        int
	feedbackAt            time.Time // stateMu; last feedback queued on this leg
	peerTerminal          atomic.Bool
}

type mpCore struct {
	finPath         *mpLeg    // stateMu; leg that carried the latest DATA_FIN copy
	finSentAt       time.Time // stateMu
	legsAbsentSince time.Time // stateMu; zero while any leg is attached or joining
	feedbackSeq     uint64    // stateMu; sequence of the feedback this side sends
	peerFeedbackSeq uint64    // stateMu; newest feedback sequence received
	peerPressure    bool      // stateMu; suppress speculative secondary assignments
	cfg             coreConfig
	ctx             context.Context
	cancel          context.CancelFunc
	appConn         *logicalConn
	txPipe          net.Conn
	rxPipe          net.Conn
	legsMu          sync.RWMutex
	legs            map[uint8]*mpLeg
	reserved        map[uint8]bool
	retiring        map[uint8]*mpLeg
	done            chan struct{}
	released        chan struct{}
	closeOne        sync.Once
	txSeq           atomic.Uint64
	ingressBytes    atomic.Uint64
	egressBytes     atomic.Uint64
	legCounters     [2]mpLegCounters
	active          atomic.Bool
	activeCh        chan struct{}
	activateOnce    sync.Once
	activationMu    sync.Mutex
	activation      activationInfo
	activationAt    time.Time
	notifiedLeg1    *mpLeg
	leg1Joins       uint64
	localFIN        atomic.Bool
	remoteFIN       atomic.Bool
	receivedFIN     atomic.Bool
	ackedFIN        atomic.Bool
	localClosing    atomic.Bool
	closeSource     atomic.Uint32
	localReadClosed atomic.Bool
	ackedNext       atomic.Uint64
	rxExpected      atomic.Uint64
	replayMu        sync.Mutex
	replayBytes     int64
	reorderBytes    atomic.Int64
	reorderCount    atomic.Int64
	replayPeak      atomic.Int64
	reorderPeak     atomic.Int64
	reorderFPeak    atomic.Int64
	legPeak         [2]atomic.Int64
	fallbackB       atomic.Uint64
	fallbackF       atomic.Uint64
	fallbackE       atomic.Uint64
	replayTO        atomic.Uint64
	backpressE      atomic.Uint64
	backpressNS     atomic.Uint64
	legFailureMu    sync.Mutex
	legFailures     [2]uint64
	lastFailLeg     uint8
	lastFailStage   legFailureStage
	failureMu       sync.Mutex
	failure         string
	failureAt       time.Time
	memory          *memoryBudget
	sessionBytes    int64
	txReserve       chan []byte
	peerStatusMu    sync.Mutex
	peerStatus      peerSenderStatus
	statusMu        sync.Mutex
	statusSeq       uint64
	lastStatus      senderStatus
	lastStatusAt    time.Time
	probeMu         sync.Mutex
	probeNext       uint64
	probePending    [2]pendingProbe
	probeLast       [2]time.Time
	probeRTT        [2]legRTTSnapshot
	workerMu        sync.Mutex
	workerGroup     sync.WaitGroup
	workerClosed    bool
	stateMu         sync.Mutex
	tx              *stream.Sender
	rx              *stream.Receiver
	headPages       int
	historyGrant    uint64    // stateMu
	historyAt       time.Time // stateMu
	historyGrownAt  time.Time // stateMu
	nextGeneration  uint64
	pumpWake        chan struct{}
	rxWake          chan struct{}
	txWake          chan struct{}
	startedAt       time.Time
	mappings        []dataMapping
	mappingHead     int
	feedbackDirty   bool

	closeProgressAt  time.Time // stateMu; full Close drain only, never CloseWrite
	closeProgressACK uint64
}

type dataMapping struct {
	seq, end            uint64
	path                uint8
	generation, pathEnd uint64
	sentAt              time.Time
	repairedAt          time.Time
}

func (l *mpLeg) Done() <-chan struct{} {
	return l.done
}

func (l *mpLeg) close(err error) {
	l.closeOne.Do(func() {
		l.queueMu.Lock()
		close(l.done)
		l.queueMu.Unlock()
		l.cancel()
		_ = l.conn.Close()
		if l.onClose != nil {
			l.onClose(err)
		}
	})
}

func (l *mpLeg) requestShutdown(err error, frameType byte, status *senderStatus) {
	select {
	case <-l.done:
	case l.shutdown <- legShutdownRequest{err: err, frameType: frameType, status: status}:
	default:
		l.close(err)
	}
}

// blockedFor reports how long the current transport write has been pending.
func (l *mpLeg) blockedFor(now time.Time) time.Duration {
	started := l.transportWriteStarted.Load()
	if started <= 0 {
		return 0
	}
	return max(0, now.Sub(time.Unix(0, started)))
}

// queueFeedback replaces any feedback still waiting for this leg's writer.
func (l *mpLeg) queueFeedback(frame wireFrame, now time.Time) {
	select {
	case <-l.feedback:
	default:
	}
	select {
	case l.feedback <- frame:
		l.feedbackAt = now
	default:
	}
}

func (l *mpLeg) backlogBytes() int64 {
	return max(l.inflight.Load(), l.queuedBytes.Load()+l.writingBytes.Load())
}

func (l *mpLeg) writingSnapshot(now time.Time) (int64, time.Duration) {
	writing := l.writingBytes.Load()
	started := l.writeStarted.Load()
	if writing <= 0 || started <= 0 {
		return writing, 0
	}
	return writing, max(0, now.Sub(time.Unix(0, started)))
}

func (l *mpLeg) queueControl(coreDone <-chan struct{}, frame wireFrame) error {
	select {
	case <-coreDone:
		return errCoreClosed
	case <-l.done:
		return errCoreClosed
	case l.control <- frame:
		return nil
	}
}

func (l *mpLeg) tryQueueControl(frame wireFrame) bool {
	select {
	case <-l.done:
		return false
	case l.control <- frame:
		return true
	default:
		return false
	}
}

func (l *mpLeg) queueLatestTelemetry(frame wireFrame) bool {
	select {
	case <-l.done:
		return false
	default:
	}
	l.telemetryMu.Lock()
	l.telemetryFrame = frame
	l.telemetryPending = true
	l.telemetryMu.Unlock()
	select {
	case l.telemetry <- struct{}{}:
	default:
	}
	return true
}

func (l *mpLeg) takeTelemetry() (wireFrame, bool) {
	l.telemetryMu.Lock()
	frame := l.telemetryFrame
	pending := l.telemetryPending
	l.telemetryPending = false
	l.telemetryMu.Unlock()
	return frame, pending
}
