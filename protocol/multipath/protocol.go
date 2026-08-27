package multipath

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"net"
)

var (
	helloMagic    = [4]byte{'S', 'M', 'P', '4'}
	responseMagic = [4]byte{'S', 'M', 'P', 'R'}
)

const (
	helloVersion byte = 4

	helloStatusOK       byte = 0
	helloStatusRejected byte = 1

	helloHeaderSize    = 28
	responseHeaderSize = 10
)

type helloMessage struct {
	Session     [16]byte
	LegID       uint8
	ChunkSize   uint32
	Destination string
}

type helloResponse struct {
	Status       byte
	ChunkSize    uint32
	RejectReason helloRejectReason
}

type helloRejectReason uint32

type helloRejectedError struct {
	reason helloRejectReason
}

func (e *helloRejectedError) Error() string {
	return "multipath hello rejected: " + e.reason.String()
}

func helloRejectReasonFromError(err error) (helloRejectReason, bool) {
	var rejection *helloRejectedError
	if !errors.As(err, &rejection) {
		return helloRejectUnknown, false
	}
	return rejection.reason, true
}

const (
	helloRejectUnknown helloRejectReason = iota
	helloRejectInvalidLegID
	helloRejectInvalidDestination
	helloRejectSessionMismatch
	helloRejectDuplicateControl
	helloRejectLegUnavailable
	helloRejectSessionUnavailable
	helloRejectChunkSizeLimit
)

func (r helloRejectReason) String() string {
	switch r {
	case helloRejectInvalidLegID:
		return "invalid leg id"
	case helloRejectInvalidDestination:
		return "invalid destination"
	case helloRejectSessionMismatch:
		return "session parameters mismatch"
	case helloRejectDuplicateControl:
		return "session already has a control leg"
	case helloRejectLegUnavailable:
		return "leg already attached or joining"
	case helloRejectSessionUnavailable:
		return "control session is not established yet or is already closed"
	case helloRejectChunkSizeLimit:
		return "requested chunk size exceeds server limit"
	default:
		return "unspecified by server"
	}
}

func newSessionID() ([16]byte, error) {
	var id [16]byte
	_, err := rand.Read(id[:])
	return id, err
}

func encodeHelloHeader(message helloMessage) ([helloHeaderSize]byte, error) {
	var header [helloHeaderSize]byte
	if len(message.Destination) == 0 || len(message.Destination) > 65535 {
		return header, errors.New("invalid multipath destination")
	}
	if message.ChunkSize == 0 || message.ChunkSize > maxFramePayload {
		return header, errors.New("invalid multipath chunk size")
	}
	copy(header[0:4], helloMagic[:])
	header[4] = helloVersion
	header[5] = message.LegID
	copy(header[6:22], message.Session[:])
	binary.BigEndian.PutUint32(header[22:26], message.ChunkSize)
	binary.BigEndian.PutUint16(header[26:28], uint16(len(message.Destination)))
	return header, nil
}

func encodeHello(message helloMessage) ([]byte, error) {
	header, err := encodeHelloHeader(message)
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, 0, helloHeaderSize+len(message.Destination))
	encoded = append(encoded, header[:]...)
	encoded = append(encoded, message.Destination...)
	return encoded, nil
}

func writeHello(conn net.Conn, message helloMessage) error {
	header, err := encodeHelloHeader(message)
	if err != nil {
		return err
	}
	buffers := net.Buffers{header[:], []byte(message.Destination)}
	_, err = buffers.WriteTo(conn)
	return err
}

func readHello(conn net.Conn) (helloMessage, error) {
	var message helloMessage
	var header [helloHeaderSize]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return message, err
	}
	if string(header[0:4]) != string(helloMagic[:]) || header[4] != helloVersion {
		return message, errors.New("invalid multipath hello")
	}
	message.LegID = header[5]
	copy(message.Session[:], header[6:22])
	message.ChunkSize = binary.BigEndian.Uint32(header[22:26])
	if message.ChunkSize == 0 || message.ChunkSize > maxFramePayload {
		return message, errors.New("invalid multipath hello chunk size")
	}
	length := int(binary.BigEndian.Uint16(header[26:28]))
	if length <= 0 {
		return message, errors.New("empty multipath destination")
	}
	buffer := make([]byte, length)
	if _, err := io.ReadFull(conn, buffer); err != nil {
		return message, err
	}
	message.Destination = string(buffer)
	return message, nil
}

func writeHelloResponse(conn net.Conn, response helloResponse) error {
	if response.Status == helloStatusOK && (response.ChunkSize == 0 || response.ChunkSize > maxFramePayload) {
		return errors.New("invalid accepted multipath chunk size")
	}
	var header [responseHeaderSize]byte
	copy(header[0:4], responseMagic[:])
	header[4] = helloVersion
	header[5] = response.Status
	value := response.ChunkSize
	if response.Status != helloStatusOK {
		value = uint32(response.RejectReason)
	}
	binary.BigEndian.PutUint32(header[6:10], value)
	return writeAll(conn, header[:])
}

func readHelloResponse(conn net.Conn) (helloResponse, error) {
	var response helloResponse
	var header [responseHeaderSize]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return response, err
	}
	if string(header[0:4]) != string(responseMagic[:]) || header[4] != helloVersion {
		return response, errors.New("invalid multipath hello response")
	}
	response.Status = header[5]
	response.ChunkSize = binary.BigEndian.Uint32(header[6:10])
	if response.Status != helloStatusOK {
		response.RejectReason = helloRejectReason(response.ChunkSize)
		response.ChunkSize = 0
		return response, &helloRejectedError{reason: response.RejectReason}
	}
	if response.ChunkSize == 0 || response.ChunkSize > maxFramePayload {
		return response, errors.New("invalid multipath accepted chunk size")
	}
	return response, nil
}
