package multipath

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"net"
)

var helloMagic = [4]byte{'S', 'M', 'P', '1'}

const helloVersion byte = 1

type helloMessage struct {
	Session     [16]byte
	LegID       uint8
	Destination string
}

func newSessionID() ([16]byte, error) {
	var id [16]byte
	_, err := rand.Read(id[:])
	return id, err
}

func writeHello(conn net.Conn, message helloMessage) error {
	if len(message.Destination) == 0 || len(message.Destination) > 65535 {
		return errors.New("invalid multipath destination")
	}
	var header [24]byte
	copy(header[0:4], helloMagic[:])
	header[4] = helloVersion
	header[5] = message.LegID
	copy(header[6:22], message.Session[:])
	binary.BigEndian.PutUint16(header[22:24], uint16(len(message.Destination)))
	buffers := net.Buffers{header[:], []byte(message.Destination)}
	_, err := buffers.WriteTo(conn)
	return err
}

func readHello(conn net.Conn) (helloMessage, error) {
	var message helloMessage
	var header [24]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return message, err
	}
	if string(header[0:4]) != string(helloMagic[:]) || header[4] != helloVersion {
		return message, errors.New("invalid multipath hello")
	}
	message.LegID = header[5]
	copy(message.Session[:], header[6:22])
	length := int(binary.BigEndian.Uint16(header[22:24]))
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
