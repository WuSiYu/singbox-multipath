package multipath

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"time"
)

const (
	senderStatusSchemaVersion byte = 3
	senderStatusPayloadSize        = 316
	senderStatusHeaderSize         = 3

	senderStatusFlagActive byte = 1 << iota
	senderStatusFlagLeg0Present
	senderStatusFlagLeg1Present
	senderStatusFlagMemoryPressure
)

type senderStatus struct {
	DataMode                 uint64
	SendBufferLimit          uint64
	ReceiveWindowLimit       uint64
	LegDeliveryRate          [2]uint64
	LegDeliveryRTT           [2]uint64
	LegMinimumRTT            [2]uint64
	LegPipeline              [2]uint64
	Sequence                 uint64
	Flags                    byte
	LogicalTX                uint64
	LegTX                    [2]uint64
	LegTXFrames              [2]uint64
	LegBacklog               [2]uint64
	LegWriting               [2]uint64
	LegWriteBlockedNanos     [2]uint64
	LegPeakBacklog           [2]uint64
	SendBufferBytes          uint64
	ReplayPeakBytes          uint64
	FallbackBytes            uint64
	FallbackFrames           uint64
	FallbackEvents           uint64
	ReplayTimeouts           uint64
	BackpressureEvents       uint64
	BackpressureNanos        uint64
	MemoryUsed               uint64
	MemoryPeakUsed           uint64
	MemoryPressureEvents     uint64
	MemoryBackpressureEvents uint64
	LegFailures              [2]uint64
	LastFailureLeg           byte
	LastFailureStage         byte
}

func senderStatusStageCode(stage legFailureStage) byte {
	switch stage {
	case legFailureWriteControl:
		return 1
	case legFailureWriteData:
		return 2
	case legFailureHandshake:
		return 3
	case legFailureReadData:
		return 4
	case legFailureReplay:
		return 5
	default:
		return 0
	}
}

func senderStatusStage(code byte) legFailureStage {
	switch code {
	case 1:
		return legFailureWriteControl
	case 2:
		return legFailureWriteData
	case 3:
		return legFailureHandshake
	case 4:
		return legFailureReadData
	case 5:
		return legFailureReplay
	default:
		return ""
	}
}

func encodeSenderStatus(status senderStatus) [senderStatusPayloadSize]byte {
	var payload [senderStatusPayloadSize]byte
	payload[0] = senderStatusSchemaVersion
	payload[1] = status.Flags
	payload[2] = status.LastFailureLeg
	payload[3] = status.LastFailureStage
	offset := 4
	put := func(value uint64) {
		binary.BigEndian.PutUint64(payload[offset:offset+8], value)
		offset += 8
	}
	put(status.Sequence)
	put(status.LogicalTX)
	for _, values := range [][2]uint64{
		status.LegTX,
		status.LegTXFrames,
		status.LegBacklog,
		status.LegWriting,
		status.LegWriteBlockedNanos,
		status.LegPeakBacklog,
		status.LegDeliveryRate,
		status.LegDeliveryRTT,
		status.LegMinimumRTT,
		status.LegPipeline,
	} {
		put(values[0])
		put(values[1])
	}
	for _, value := range []uint64{
		status.SendBufferBytes,
		status.ReplayPeakBytes,
		status.FallbackBytes,
		status.FallbackFrames,
		status.FallbackEvents,
		status.ReplayTimeouts,
		status.BackpressureEvents,
		status.BackpressureNanos,
		status.MemoryUsed,
		status.MemoryPeakUsed,
		status.MemoryPressureEvents,
		status.MemoryBackpressureEvents,
		status.LegFailures[0],
		status.LegFailures[1],
		status.DataMode, status.SendBufferLimit, status.ReceiveWindowLimit,
	} {
		put(value)
	}
	return payload
}

func decodeSenderStatus(payload []byte) (senderStatus, error) {
	var status senderStatus
	if len(payload) != senderStatusPayloadSize || payload[0] != senderStatusSchemaVersion {
		return status, errors.New("invalid multipath sender status")
	}
	status.Flags = payload[1]
	status.LastFailureLeg = payload[2]
	status.LastFailureStage = payload[3]
	offset := 4
	read := func() uint64 {
		value := binary.BigEndian.Uint64(payload[offset : offset+8])
		offset += 8
		return value
	}
	status.Sequence = read()
	status.LogicalTX = read()
	arrays := []*[2]uint64{
		&status.LegTX,
		&status.LegTXFrames,
		&status.LegBacklog,
		&status.LegWriting,
		&status.LegWriteBlockedNanos,
		&status.LegPeakBacklog,
		&status.LegDeliveryRate,
		&status.LegDeliveryRTT,
		&status.LegMinimumRTT,
		&status.LegPipeline,
	}
	for _, values := range arrays {
		values[0] = read()
		values[1] = read()
	}
	values := []*uint64{
		&status.SendBufferBytes,
		&status.ReplayPeakBytes,
		&status.FallbackBytes,
		&status.FallbackFrames,
		&status.FallbackEvents,
		&status.ReplayTimeouts,
		&status.BackpressureEvents,
		&status.BackpressureNanos,
		&status.MemoryUsed,
		&status.MemoryPeakUsed,
		&status.MemoryPressureEvents,
		&status.MemoryBackpressureEvents,
		&status.LegFailures[0],
		&status.LegFailures[1],
		&status.DataMode, &status.SendBufferLimit, &status.ReceiveWindowLimit,
	}
	for _, value := range values {
		*value = read()
	}
	return status, nil
}

func writeSenderStatus(conn net.Conn, status senderStatus) error {
	payload := encodeSenderStatus(status)
	var header [senderStatusHeaderSize]byte
	header[0] = frameTypeSenderStatus
	binary.BigEndian.PutUint16(header[1:3], senderStatusPayloadSize)
	buffers := net.Buffers{header[:], payload[:]}
	_, err := buffers.WriteTo(conn)
	return err
}

func readSenderStatus(conn net.Conn) (senderStatus, error) {
	var length [2]byte
	if _, err := io.ReadFull(conn, length[:]); err != nil {
		return senderStatus{}, err
	}
	if binary.BigEndian.Uint16(length[:]) != senderStatusPayloadSize {
		return senderStatus{}, errors.New("invalid multipath sender status length")
	}
	var payload [senderStatusPayloadSize]byte
	if _, err := io.ReadFull(conn, payload[:]); err != nil {
		return senderStatus{}, err
	}
	return decodeSenderStatus(payload[:])
}

type peerSenderStatus struct {
	status     senderStatus
	receivedAt time.Time
}

type legRTTSnapshot struct {
	Latest       time.Duration
	Total        time.Duration
	EWMA         time.Duration
	Minimum      time.Duration
	Maximum      time.Duration
	Jitter       time.Duration
	Samples      uint64
	ProbeSent    uint64
	ProbeTimeout uint64
}

type pendingProbe struct {
	id     uint64
	sentAt time.Time
}

func (c *mpCore) buildSenderStatus(now time.Time) senderStatus {
	status := senderStatus{
		LogicalTX:       c.ingressBytes.Load(),
		SendBufferLimit: uint64(c.cfg.SendBufferBytes), ReceiveWindowLimit: uint64(c.cfg.ReceiveWindowBytes),
		SendBufferBytes:    uint64(max(0, c.replayBytesSnapshot())),
		ReplayPeakBytes:    uint64(max(0, c.replayPeak.Load())),
		FallbackBytes:      c.fallbackB.Load(),
		FallbackFrames:     c.fallbackF.Load(),
		FallbackEvents:     c.fallbackE.Load(),
		ReplayTimeouts:     c.replayTO.Load(),
		BackpressureEvents: c.backpressE.Load(),
		BackpressureNanos:  c.backpressNS.Load(),
	}
	if c.active.Load() {
		status.Flags |= senderStatusFlagActive
	}
	c.stateMu.Lock()
	status.DataMode = c.dataModeLocked()
	for index := range status.LegTX {
		status.LegTX[index] = c.legCounters[index].txBytes.Load()
		status.LegTXFrames[index] = c.legCounters[index].txFrames.Load()
		status.LegPeakBacklog[index] = uint64(max(0, c.legPeak[index].Load()))
		leg := c.getLeg(uint8(index))
		if leg == nil {
			continue
		}
		if index == 0 {
			status.Flags |= senderStatusFlagLeg0Present
		} else {
			status.Flags |= senderStatusFlagLeg1Present
		}
		status.LegDeliveryRate[index] = uint64(leg.path.Rate)
		status.LegDeliveryRTT[index] = uint64(leg.path.SRTT)
		status.LegMinimumRTT[index] = uint64(leg.path.MinimumRTT)
		status.LegPipeline[index] = leg.path.Pipeline(min(uint64(c.cfg.QueueBytes), uint64(c.cfg.FrameSize)*4), uint64(c.cfg.SendBufferBytes))
		status.LegBacklog[index] = uint64(max(0, leg.backlogBytes()))
		writing, blocked := leg.writingSnapshot(now)
		status.LegWriting[index] = uint64(max(0, writing))
		status.LegWriteBlockedNanos[index] = uint64(max(0, blocked))
	}
	c.stateMu.Unlock()
	memory := c.memory.snapshot()
	status.MemoryUsed = uint64(max(0, memory.UsedBytes))
	status.MemoryPeakUsed = uint64(max(0, memory.PeakUsedBytes))
	status.MemoryPressureEvents = memory.PressureEvents
	status.MemoryBackpressureEvents = memory.BackpressureEvents
	if memory.Pressure {
		status.Flags |= senderStatusFlagMemoryPressure
	}
	c.legFailureMu.Lock()
	status.LegFailures = c.legFailures
	status.LastFailureLeg = c.lastFailLeg
	status.LastFailureStage = senderStatusStageCode(c.lastFailStage)
	c.legFailureMu.Unlock()
	return status
}

func (c *mpCore) replayBytesSnapshot() int64 {
	c.replayMu.Lock()
	bytes := c.replayBytes
	c.replayMu.Unlock()
	return bytes
}

func (c *mpCore) queueSenderStatus(now time.Time, force bool) bool {
	if !c.cfg.SendStatus {
		return false
	}
	leg0 := c.controlLeg()
	if leg0 == nil {
		return false
	}
	status := c.buildSenderStatus(now)
	c.statusMu.Lock()
	comparable := status
	comparable.Sequence = 0
	previous := c.lastStatus
	previous.Sequence = 0
	if !force && comparable == previous && now.Sub(c.lastStatusAt) < 10*time.Second {
		c.statusMu.Unlock()
		return false
	}
	c.statusSeq++
	status.Sequence = c.statusSeq
	c.lastStatus = status
	c.lastStatusAt = now
	c.statusMu.Unlock()
	return leg0.queueLatestTelemetry(wireFrame{typ: frameTypeSenderStatus, status: status})
}

func (c *mpCore) nextSenderStatus(now time.Time) senderStatus {
	status := c.buildSenderStatus(now)
	c.statusMu.Lock()
	c.statusSeq++
	status.Sequence = c.statusSeq
	c.lastStatus = status
	c.lastStatusAt = now
	c.statusMu.Unlock()
	return status
}

func (c *mpCore) handlePeerSenderStatus(status senderStatus, receivedAt time.Time) {
	c.peerStatusMu.Lock()
	if status.Sequence > c.peerStatus.status.Sequence {
		c.peerStatus = peerSenderStatus{status: status, receivedAt: receivedAt}
	}
	c.peerStatusMu.Unlock()
}

func (c *mpCore) peerSenderStatusSnapshot() peerSenderStatus {
	c.peerStatusMu.Lock()
	status := c.peerStatus
	c.peerStatusMu.Unlock()
	return status
}

func (c *mpCore) scheduleProbes(now time.Time) {
	c.probeMu.Lock()
	defer c.probeMu.Unlock()
	interval := 10 * time.Second
	if c.active.Load() {
		interval = 2 * time.Second
	}
	for index := range c.probePending {
		if index == 1 && !c.active.Load() && c.legCounters[1].txBytes.Load() == 0 && c.legCounters[1].rxBytes.Load() == 0 {
			continue
		}
		pending := c.probePending[index]
		if pending.id != 0 && !pending.sentAt.IsZero() && now.Sub(pending.sentAt) >= 5*time.Second {
			c.probeRTT[index].ProbeTimeout++
			c.probePending[index] = pendingProbe{}
		}
		if c.probePending[index].id != 0 || now.Sub(c.probeLast[index]) < interval {
			continue
		}
		leg := c.getLeg(uint8(index))
		if leg == nil {
			continue
		}
		c.probeNext++
		if c.probeNext == 0 {
			c.probeNext++
		}
		probeID := c.probeNext
		if leg.tryQueueControl(wireFrame{typ: frameTypePing, seq: probeID}) {
			c.probePending[index] = pendingProbe{id: probeID}
			c.probeLast[index] = now
		}
	}
}

func (c *mpCore) markProbeSent(legID uint8, probeID uint64, sentAt time.Time) {
	if legID > 1 {
		return
	}
	c.probeMu.Lock()
	pending := &c.probePending[legID]
	if pending.id == probeID && pending.sentAt.IsZero() {
		pending.sentAt = sentAt
		c.probeRTT[legID].ProbeSent++
	}
	c.probeMu.Unlock()
}

func (c *mpCore) cancelProbe(legID uint8, probeID uint64) {
	if legID > 1 {
		return
	}
	c.probeMu.Lock()
	if c.probePending[legID].id == probeID {
		c.probePending[legID] = pendingProbe{}
	}
	c.probeMu.Unlock()
}

func (c *mpCore) cancelLegProbe(legID uint8) {
	if legID > 1 {
		return
	}
	c.probeMu.Lock()
	c.probePending[legID] = pendingProbe{}
	c.probeMu.Unlock()
}

func (c *mpCore) handlePong(legID uint8, probeID uint64, receivedAt time.Time) {
	if legID > 1 {
		return
	}
	c.probeMu.Lock()
	pending := c.probePending[legID]
	if pending.id != probeID || pending.sentAt.IsZero() {
		c.probeMu.Unlock()
		return
	}
	c.probePending[legID] = pendingProbe{}
	rtt := receivedAt.Sub(pending.sentAt)
	stats := &c.probeRTT[legID]
	if stats.Samples == 0 {
		stats.EWMA = rtt
		stats.Minimum = rtt
		stats.Maximum = rtt
	} else {
		delta := rtt - stats.Latest
		if delta < 0 {
			delta = -delta
		}
		stats.EWMA = (stats.EWMA*7 + rtt) / 8
		stats.Jitter = (stats.Jitter*7 + delta) / 8
		if rtt < stats.Minimum {
			stats.Minimum = rtt
		}
		if rtt > stats.Maximum {
			stats.Maximum = rtt
		}
	}
	stats.Latest = rtt
	stats.Total += rtt
	stats.Samples++
	c.probeMu.Unlock()
}

func (c *mpCore) rttSnapshot() [2]legRTTSnapshot {
	c.probeMu.Lock()
	stats := c.probeRTT
	c.probeMu.Unlock()
	return stats
}
