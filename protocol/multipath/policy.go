package multipath

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
)

// Policies are immutable for a session, including joins and recovery reattaches.
// Zero buffer limits mean automatic on the host actually owning the buffer.
type directionPolicy struct {
	AggregationEnabled             bool
	Leg0TrafficSaving              bool
	ActivationOnQueue              bool
	QueueFrames                    uint32
	ThresholdBytesPS               uint64
	ActivationAfterBytes           uint64
	ActivationAfterBytesMinBytesPS uint64
	ActivationWindow               time.Duration
	SendBufferBytes                uint64
	ReceiveWindowBytes             uint64
	PathStallTimeoutMin            time.Duration
}

type sessionPolicy struct{ Upload, Download directionPolicy }

const directionPolicySize = 61
const sessionPolicySize = directionPolicySize * 2

func policyFromOptions(o option.MultipathDirectionOptions) directionPolicy {
	return directionPolicy{
		AggregationEnabled:             o.AggregationEnabled == nil || *o.AggregationEnabled,
		Leg0TrafficSaving:              o.Leg0TrafficSaving,
		ActivationOnQueue:              o.ActivationOnQueue == nil || *o.ActivationOnQueue,
		QueueFrames:                    o.QueueFrames,
		ThresholdBytesPS:               resolveActivationThreshold(o.ActivationThresholdMbps, o.ActivationAfterBytes.Value()),
		ActivationAfterBytes:           o.ActivationAfterBytes.Value(),
		ActivationAfterBytesMinBytesPS: uint64(o.ActivationAfterBytesMinMbps) * 1_000_000 / 8,
		ActivationWindow:               time.Duration(o.ActivationWindow),
		SendBufferBytes:                o.SendBufferBytes.Value(),
		ReceiveWindowBytes:             o.ReceiveWindowBytes.Value(),
		PathStallTimeoutMin:            time.Duration(o.PathStallTimeoutMin),
	}
}

func (p directionPolicy) normalized() directionPolicy {
	if p.QueueFrames == 0 {
		p.QueueFrames = 256
	}
	if p.ActivationWindow == 0 {
		p.ActivationWindow = defaultActivationWindow
	}
	return p
}

func (p directionPolicy) validate(frameSize int) error {
	p = p.normalized()
	if p.QueueFrames < 8 || p.QueueFrames > 4096 || uint64(p.QueueFrames)*uint64(frameSize) > maxQueueBytes {
		return errors.New("invalid queue_frames or queue_frames * frame_size exceeds 64 MiB")
	}
	if p.ActivationWindow < minActivationWindow || p.ActivationWindow > maxActivationWindow {
		return errors.New("activation_window must be between 20ms and 10s")
	}
	// Rates originate as uint32 Mbps; validate the same range on the wire.
	if p.ThresholdBytesPS > uint64(^uint32(0))*1_000_000/8 || p.ActivationAfterBytesMinBytesPS > uint64(^uint32(0))*1_000_000/8 {
		return errors.New("invalid activation rate")
	}
	for _, size := range []uint64{p.SendBufferBytes, p.ReceiveWindowBytes} {
		if size != 0 && (size < uint64(frameSize) || size > maxReplayBytes) {
			return errors.New("send_buffer_bytes and receive_window_bytes must be between frame_size and 512 MiB, or zero for automatic")
		}
	}
	if p.PathStallTimeoutMin != 0 && (p.PathStallTimeoutMin < 100*time.Millisecond || p.PathStallTimeoutMin > 5*time.Minute) {
		return errors.New("invalid path_stall_timeout_min")
	}
	return nil
}

func (p sessionPolicy) validate(frameSize int) error {
	if frameSize < 1024 || frameSize > maxFramePayload {
		return errors.New("invalid frame_size")
	}
	if err := p.Upload.validate(frameSize); err != nil {
		return fmt.Errorf("upload: %w", err)
	}
	if err := p.Download.validate(frameSize); err != nil {
		return fmt.Errorf("download: %w", err)
	}
	return nil
}

func (p directionPolicy) encode(out []byte) {
	if p.AggregationEnabled {
		out[0] |= 1
	}
	if p.Leg0TrafficSaving {
		out[0] |= 2
	}
	if p.ActivationOnQueue {
		out[0] |= 4
	}
	binary.BigEndian.PutUint32(out[1:5], p.QueueFrames)
	values := [...]uint64{p.ThresholdBytesPS, p.ActivationAfterBytes, p.ActivationAfterBytesMinBytesPS, uint64(p.ActivationWindow), p.SendBufferBytes, p.ReceiveWindowBytes, uint64(p.PathStallTimeoutMin)}
	for i, v := range values {
		binary.BigEndian.PutUint64(out[5+i*8:13+i*8], v)
	}
}

func decodeDirectionPolicy(in []byte) (directionPolicy, error) {
	if in[0]&^byte(7) != 0 {
		return directionPolicy{}, errors.New("invalid multipath policy flags")
	}
	values := [7]uint64{}
	for i := range values {
		values[i] = binary.BigEndian.Uint64(in[5+i*8 : 13+i*8])
	}
	return directionPolicy{
		AggregationEnabled: in[0]&1 != 0, Leg0TrafficSaving: in[0]&2 != 0, ActivationOnQueue: in[0]&4 != 0,
		QueueFrames: binary.BigEndian.Uint32(in[1:5]), ThresholdBytesPS: values[0], ActivationAfterBytes: values[1],
		ActivationAfterBytesMinBytesPS: values[2], ActivationWindow: time.Duration(values[3]),
		SendBufferBytes: values[4], ReceiveWindowBytes: values[5], PathStallTimeoutMin: time.Duration(values[6]),
	}, nil
}

func (p sessionPolicy) encode() [sessionPolicySize]byte {
	var data [sessionPolicySize]byte
	p.Upload.encode(data[:directionPolicySize])
	p.Download.encode(data[directionPolicySize:])
	return data
}

func (p sessionPolicy) digest() [32]byte { data := p.encode(); return sha256.Sum256(data[:]) }

func decodeSessionPolicy(data []byte) (p sessionPolicy, err error) {
	p.Upload, err = decodeDirectionPolicy(data[:directionPolicySize])
	if err == nil {
		p.Download, err = decodeDirectionPolicy(data[directionPolicySize:])
	}
	return
}

func configForPolicy(memory *memoryBudget, frameSize int, tx, rx directionPolicy) (coreConfig, error) {
	tx, rx = tx.normalized(), rx.normalized()
	cfg := coreConfig{
		Memory: memory, FrameSize: frameSize,
		AggregationEnabled: tx.AggregationEnabled, Leg0TrafficSaving: tx.Leg0TrafficSaving,
		ActivationOnQueue: tx.ActivationOnQueue, ThresholdBytesPS: tx.ThresholdBytesPS,
		ActivationAfterBytes: tx.ActivationAfterBytes, ActivationAfterBytesMinBytesPS: tx.ActivationAfterBytesMinBytesPS,
		ActivationWindow: tx.ActivationWindow, QueueFrames: int(tx.QueueFrames), QueueBytes: int64(tx.QueueFrames) * int64(frameSize),
		SendBufferBytes: int64(tx.SendBufferBytes), ReceiveWindowBytes: int64(rx.ReceiveWindowBytes), PathStallTimeoutMin: tx.PathStallTimeoutMin,
	}
	if cfg.SendBufferBytes == 0 {
		cfg.SendBufferBytes = maxReplayBytes
	}
	if cfg.ReceiveWindowBytes == 0 {
		cfg.ReceiveWindowBytes = maxReorderBytes
	}
	if minimumSessionMemory(cfg) > memory.limit {
		return coreConfig{}, errors.New("memory_limit is too small for this multipath session")
	}
	return cfg, nil
}

// Keep deprecated parsing separate from all policy calculations. Each old field
// produces one actionable warning per node, including its complete JSON path.
func warnDeprecated(logger log.ContextLogger, options any, prefix string) {
	value := reflect.ValueOf(options)
	typ := value.Type()
	for n := 0; n < value.NumField(); n++ {
		field := typ.Field(n)
		if field.Anonymous {
			warnDeprecated(logger, value.Field(n).Interface(), prefix)
			continue
		}
		name := strings.Split(field.Tag.Get("json"), ",")[0]
		if !strings.HasPrefix(field.Name, "Deprecated") || value.Field(n).Len() == 0 {
			continue
		}
		replacement := "client upload/download configuration"
		switch name {
		case "chunk_size":
			replacement = "client frame_size"
		case "leg1_replay_bytes":
			replacement = "upload/download.send_buffer_bytes"
		case "leg1_replay_timeout":
			replacement = "upload/download.path_stall_timeout_min"
		case "max_reorder_bytes":
			replacement = "upload/download.receive_window_bytes (receiver of that direction)"
		case "max_reorder_frames":
			replacement = "receive_window_bytes; the frame-count limit has been removed"
		case "bandwidth_mbps":
			replacement = "automatic scheduling (no replacement field)"
		default:
			replacement = "client upload/download." + name
		}
		logger.Warn("multipath obsolete field ", prefix, name, " is ignored; use ", replacement, "; old values are not migrated and new defaults apply when omitted")
	}
}
