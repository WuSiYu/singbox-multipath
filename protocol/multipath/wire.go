package multipath

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net"
	"time"

	"github.com/sagernet/sing-box/protocol/multipath/stream"
)

const (
	frameTypeData            byte = 1
	frameTypeFIN             byte = 3
	frameTypeReset           byte = 4
	frameTypeSessionClose    byte = 5
	frameTypeSenderStatus    byte = 6
	frameTypePing            byte = 7
	frameTypePong            byte = 8
	frameTypeWindow          byte = 9
	dataFrameHeaderSize           = 29 // type, DSN, path sequence, incarnation, payload size
	controlFrameHeaderSize        = 9
	flowFixedSize                 = 107 // type, flags, next, limit, seq, 2 path receipts, NACK count
	flowNACKSize                  = 8   // start offset from next (u32), length (u32)
	maxFlowNACKs                  = 4
	maxFlowSize                   = flowFixedSize + maxFlowNACKs*flowNACKSize
	maxFramePayload               = 1 << 20
	maxQueueBytes                 = 64 << 20
	maxReorderBytes               = 512 << 20
	maxReplayBytes                = 512 << 20
	sessionCloseDrainTimeout      = time.Second
	flowFlagPressure         byte = 1
)

var errCoreClosed = errors.New("multipath core closed")

// flowMessage is the receiver's feedback. Next, Limit and the path receipts
// are monotonic and may be merged from any leg in any order; Flags and the
// dropped ranges (NACKs) apply only when Seq is newer than any seen before.
type flowMessage struct {
	Next, Limit uint64
	Seq         uint64
	Paths       [2]stream.Receipt
	Flags       byte
	NACKs       []stream.Range
}

type wireFrame struct {
	typ                      byte
	seq, pathSeq, generation uint64
	data                     []byte
	buffer                   *stream.Buffer
	status                   senderStatus
	flow                     flowMessage
	replay                   bool
	more                     bool // an incremental payload piece; only the last piece counts as a frame
	closeReason              byte
}

func encodeFlow(frame wireFrame) []byte {
	nacks := frame.flow.NACKs[:min(len(frame.flow.NACKs), maxFlowNACKs)]
	data := make([]byte, flowFixedSize+len(nacks)*flowNACKSize)
	data[0], data[1] = frame.typ, frame.flow.Flags
	values := []uint64{frame.flow.Next, frame.flow.Limit, frame.flow.Seq}
	for _, path := range frame.flow.Paths {
		values = append(values, path.Generation, path.Next, path.ReceivedAt, path.FirstNext, path.FirstReceivedAt)
	}
	for i, value := range values {
		binary.BigEndian.PutUint64(data[2+i*8:10+i*8], value)
	}
	data[flowFixedSize-1] = byte(len(nacks))
	for i, r := range nacks {
		offset := flowFixedSize + i*flowNACKSize
		binary.BigEndian.PutUint32(data[offset:offset+4], uint32(r.Start-frame.flow.Next))
		binary.BigEndian.PutUint32(data[offset+4:offset+8], uint32(r.End-r.Start))
	}
	return data
}

func readFlow(conn net.Conn) (flowMessage, error) {
	var data [maxFlowSize - 1]byte
	if _, err := io.ReadFull(conn, data[:flowFixedSize-1]); err != nil {
		return flowMessage{}, err
	}
	message := flowMessage{Flags: data[0]}
	if message.Flags & ^byte(flowFlagPressure) != 0 {
		return message, errors.New("invalid multipath feedback flags")
	}
	values := []*uint64{&message.Next, &message.Limit, &message.Seq}
	for i := range message.Paths {
		p := &message.Paths[i]
		values = append(values, &p.Generation, &p.Next, &p.ReceivedAt, &p.FirstNext, &p.FirstReceivedAt)
	}
	for i, value := range values {
		*value = binary.BigEndian.Uint64(data[1+i*8 : 9+i*8])
	}
	count := int(data[flowFixedSize-2])
	if count > maxFlowNACKs {
		return message, errors.New("invalid multipath feedback NACK count")
	}
	if count == 0 {
		return message, nil
	}
	nacks := data[flowFixedSize-1 : flowFixedSize-1+count*flowNACKSize]
	if _, err := io.ReadFull(conn, nacks); err != nil {
		return message, err
	}
	message.NACKs = make([]stream.Range, count)
	for i := range message.NACKs {
		offset := uint64(binary.BigEndian.Uint32(nacks[i*flowNACKSize : i*flowNACKSize+4]))
		length := uint64(binary.BigEndian.Uint32(nacks[i*flowNACKSize+4 : i*flowNACKSize+8]))
		if length == 0 || message.Next > math.MaxUint64-offset-length {
			return message, errors.New("invalid multipath feedback NACK")
		}
		message.NACKs[i] = stream.Range{Start: message.Next + offset, End: message.Next + offset + length}
	}
	return message, nil
}

func writeWireFrame(conn net.Conn, frame wireFrame) error {
	if initial, ok := conn.(initialFrameWriter); ok {
		if handled, err := initial.writeInitialFrame(frame); handled {
			return err
		}
	}
	if frame.typ == frameTypeSenderStatus {
		return writeSenderStatus(conn, frame.status)
	}
	if frame.typ == frameTypeWindow {
		return writeAll(conn, encodeFlow(frame))
	}
	if frame.typ == frameTypeData {
		if len(frame.data) == 0 || len(frame.data) > maxFramePayload {
			return errors.New("invalid multipath payload size")
		}
		var header [dataFrameHeaderSize]byte
		header[0] = frame.typ
		binary.BigEndian.PutUint64(header[1:9], frame.seq)
		binary.BigEndian.PutUint64(header[9:17], frame.pathSeq)
		binary.BigEndian.PutUint64(header[17:25], frame.generation)
		binary.BigEndian.PutUint32(header[25:29], uint32(len(frame.data)))
		buffers := net.Buffers{header[:], frame.data}
		_, err := buffers.WriteTo(conn)
		return err
	}
	var header [controlFrameHeaderSize]byte
	header[0] = frame.typ
	switch frame.typ {
	case frameTypeFIN, frameTypePing, frameTypePong:
		binary.BigEndian.PutUint64(header[1:], frame.seq)
		return writeAll(conn, header[:])
	case frameTypeSessionClose:
		if frame.closeReason > closeReasonShutdown {
			return errors.New("invalid multipath close reason")
		}
		header[1] = frame.closeReason
		return writeAll(conn, header[:2])
	case frameTypeReset:
		return writeAll(conn, header[:1])
	default:
		return errors.New("unknown multipath frame type")
	}
}

// Each transport reader has a pre-accounted scratch buffer. Even when the
// receive store is full, the reader can parse feedback and consume duplicates.
func readFrame(conn net.Conn, scratch []byte) (wireFrame, error) {
	return readFrameData(conn, scratch, nil)
}

// A mapping describes bytes, not an atomic delivery unit. Publish received
// prefixes before the rest of a large mapping arrives, just as DSS mappings
// may span multiple TCP segments. Test codecs may still request a whole frame.
func readFrameData(conn net.Conn, scratch []byte, receive func(wireFrame) error) (wireFrame, error) {
	var header [dataFrameHeaderSize]byte
	if _, err := io.ReadFull(conn, header[:1]); err != nil {
		return wireFrame{}, err
	}
	frame := wireFrame{typ: header[0]}
	var err error
	switch frame.typ {
	case frameTypeData:
		if _, err = io.ReadFull(conn, header[1:]); err != nil {
			return frame, err
		}
		frame.seq = binary.BigEndian.Uint64(header[1:9])
		frame.pathSeq = binary.BigEndian.Uint64(header[9:17])
		frame.generation = binary.BigEndian.Uint64(header[17:25])
		length := binary.BigEndian.Uint32(header[25:29])
		if length == 0 || uint64(length) > uint64(len(scratch)) || frame.generation == 0 {
			return frame, errors.New("invalid multipath data mapping")
		}
		if uint64(length) > math.MaxUint64-frame.seq || uint64(length) > math.MaxUint64-frame.pathSeq {
			return frame, errors.New("multipath data mapping overflows sequence space")
		}
		if receive != nil {
			remaining := int(length)
			for remaining > 0 {
				n, readErr := conn.Read(scratch[:remaining])
				if n < 0 || n > remaining {
					return frame, errors.New("invalid multipath transport read count")
				}
				if n > 0 {
					frame.data, frame.more = scratch[:n], n < remaining
					if err = receive(frame); err != nil {
						return frame, err
					}
					frame.seq += uint64(n)
					frame.pathSeq += uint64(n)
					remaining -= n
				}
				if readErr != nil && remaining > 0 {
					return frame, readErr
				}
				if n == 0 {
					return frame, io.ErrNoProgress
				}
			}
			frame.data = nil // DATA was already delivered through the callback.
			return frame, nil
		}
		frame.data = scratch[:length]
		_, err = io.ReadFull(conn, frame.data)
	case frameTypeWindow:
		frame.flow, err = readFlow(conn)
	case frameTypeSenderStatus:
		frame.status, err = readSenderStatus(conn)
	case frameTypeFIN, frameTypePing, frameTypePong:
		if _, err = io.ReadFull(conn, header[1:9]); err == nil {
			frame.seq = binary.BigEndian.Uint64(header[1:9])
		}
	case frameTypeSessionClose:
		if _, err = io.ReadFull(conn, header[1:2]); err == nil {
			frame.closeReason = header[1]
			if frame.closeReason > closeReasonShutdown {
				err = errors.New("invalid multipath close reason")
			}
		}
	case frameTypeReset:
	default:
		err = errors.New("unknown multipath frame type")
	}
	return frame, err
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if n < 0 || n > len(data) {
			return io.ErrShortWrite
		}
		data = data[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func wakeFlow(channel chan struct{}) {
	select {
	case channel <- struct{}{}:
	default:
	}
}
