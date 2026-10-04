package multipath

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

var (
	helloMagic    = [4]byte{'S', 'M', 'P', 'A'}
	responseMagic = [4]byte{'S', 'M', 'P', 'R'}
)

const (
	helloVersion      byte = 13
	helloFlagStatus   byte = 1 << 0
	helloFlagRecovery byte = 1 << 1
	helloFlagControl  byte = 1 << 2
	helloFlagCreate   byte = 1 << 3
	helloFlagAuth     byte = 1 << 4

	// An authenticated hello appends a Unix time, a random nonce and an
	// HMAC-SHA256 over everything before the MAC, keyed by the shared PSK.
	helloAuthSize = 8 + 16 + sha256.Size
	helloAuthSkew = 2 * time.Minute

	helloStatusOK       byte = 0
	helloStatusRejected byte = 1

	helloHeaderSize    = 55 + sessionPolicySize
	responseHeaderSize = 42
)

type helloMessage struct {
	Session       [16]byte
	LegID         uint8
	RequestStatus bool
	FrameSize     uint32
	Policy        sessionPolicy
	Destination   string
	Group         [16]byte
	Recovery      bool
	Control       bool
	Create        bool
	RecoveryEpoch uint64
	RecoveryMask  byte
	RecoveryUDP   byte
	// PSK authenticates an outgoing hello; it is never sent.
	PSK string
}

// helloAuth is the authentication carried by a received hello.
type helloAuth struct {
	signed []byte
	mac    []byte
	time   time.Time
	nonce  [16]byte
}

type helloResponse struct {
	Status       byte
	PolicyDigest [32]byte
	FrameSize    uint32
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
		return 0, false
	}
	return rejection.reason, true
}

const (
	helloRejectInvalidLegID helloRejectReason = iota + 1
	helloRejectInvalidDestination
	helloRejectSessionMismatch
	helloRejectDuplicateControl
	helloRejectLegUnavailable
	helloRejectSessionUnavailable
	helloRejectPolicy
	helloRejectVersion
	helloRejectAuthentication
)

func (r helloRejectReason) valid() bool {
	return r >= helloRejectInvalidLegID && r <= helloRejectAuthentication
}

// fatal reports a rejection that no retry on any leg can overcome.
func (r helloRejectReason) fatal(confirmed bool) bool {
	switch r {
	case helloRejectLegUnavailable:
		return false
	case helloRejectSessionUnavailable:
		return confirmed
	default:
		return true
	}
}

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
	case helloRejectPolicy:
		return "invalid multipath session policy"
	case helloRejectVersion:
		return "unsupported protocol version (both endpoints must use v13)"
	case helloRejectAuthentication:
		return "authentication failed (check psk on both endpoints)"
	default:
		return "invalid rejection reason"
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
	if message.FrameSize == 0 || message.FrameSize > maxFramePayload {
		return header, errors.New("invalid multipath frame size")
	}
	if message.Control {
		if message.Policy != (sessionPolicy{}) || message.FrameSize != 1 {
			return header, errors.New("invalid control policy")
		}
	} else if err := message.Policy.validate(int(message.FrameSize)); err != nil {
		return header, err
	}
	policy := message.Policy.encode()
	copy(header[55:], policy[:])
	copy(header[0:4], helloMagic[:])
	header[4] = helloVersion
	header[5] = message.LegID
	if message.RequestStatus {
		header[6] |= helloFlagStatus
	}
	if message.Recovery {
		header[6] |= helloFlagRecovery
	}
	if message.Control {
		header[6] |= helloFlagControl
	}
	if message.Create {
		header[6] |= helloFlagCreate
	}
	if message.PSK != "" {
		header[6] |= helloFlagAuth
	}
	copy(header[29:45], message.Group[:])
	binary.BigEndian.PutUint64(header[45:53], message.RecoveryEpoch)
	header[53], header[54] = message.RecoveryMask, message.RecoveryUDP
	copy(header[7:23], message.Session[:])
	binary.BigEndian.PutUint32(header[23:27], message.FrameSize)
	binary.BigEndian.PutUint16(header[27:29], uint16(len(message.Destination)))
	return header, nil
}

func encodeHello(message helloMessage) ([]byte, error) {
	header, err := encodeHelloHeader(message)
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, 0, helloHeaderSize+len(message.Destination)+helloAuthSize)
	encoded = append(encoded, header[:]...)
	encoded = append(encoded, message.Destination...)
	if message.PSK != "" {
		var auth [8 + 16]byte
		binary.BigEndian.PutUint64(auth[:8], uint64(time.Now().Unix()))
		if _, err = rand.Read(auth[8:]); err != nil {
			return nil, err
		}
		encoded = append(encoded, auth[:]...)
		encoded = helloMAC(message.PSK, encoded).Sum(encoded)
	}
	return encoded, nil
}

func helloMAC(psk string, signed []byte) interface{ Sum([]byte) []byte } {
	mac := hmac.New(sha256.New, []byte(psk))
	mac.Write(signed)
	return mac
}

func writeHello(conn net.Conn, message helloMessage) error {
	encoded, err := encodeHello(message)
	if err != nil {
		return err
	}
	return writeAll(conn, encoded)
}

// helloVersionError reports a peer speaking another protocol version.
type helloVersionError struct{ version byte }

func (e *helloVersionError) Error() string {
	return fmt.Sprintf("multipath peer uses protocol v%d; this endpoint requires v%d (upgrade both endpoints)", e.version, helloVersion)
}

func readHello(conn net.Conn) (helloMessage, error) {
	message, _, err := readHelloWithAuth(conn)
	return message, err
}

func readHelloWithAuth(conn net.Conn) (helloMessage, *helloAuth, error) {
	var message helloMessage
	var auth *helloAuth
	var header [helloHeaderSize]byte
	if _, err := io.ReadFull(conn, header[:7]); err != nil {
		return message, nil, err
	}
	if string(header[0:4]) != string(helloMagic[:]) {
		return message, nil, errors.New("invalid multipath hello")
	}
	if header[4] != helloVersion {
		return message, nil, &helloVersionError{version: header[4]}
	}
	if _, err := io.ReadFull(conn, header[7:]); err != nil {
		return message, nil, err
	}
	var policyErr error
	message.Policy, policyErr = decodeSessionPolicy(header[55:])
	if policyErr != nil {
		return message, nil, policyErr
	}
	if header[6]&^(helloFlagStatus|helloFlagRecovery|helloFlagControl|helloFlagCreate|helloFlagAuth) != 0 {
		return message, nil, errors.New("invalid multipath hello flags")
	}
	message.LegID = header[5]
	message.RequestStatus = header[6]&helloFlagStatus != 0
	message.Recovery = header[6]&helloFlagRecovery != 0
	message.Control = header[6]&helloFlagControl != 0
	message.Create = header[6]&helloFlagCreate != 0
	copy(message.Group[:], header[29:45])
	message.RecoveryEpoch = binary.BigEndian.Uint64(header[45:53])
	message.RecoveryMask, message.RecoveryUDP = header[53], header[54]
	if message.RecoveryMask > 3 || message.RecoveryUDP > 1 {
		return message, nil, errors.New("invalid recovery policy")
	}
	if !message.Recovery && (message.Control || message.Group != [16]byte{} || message.RecoveryEpoch != 0 || message.RecoveryMask != 0 || message.RecoveryUDP != 0) {
		return message, nil, errors.New("invalid multipath recovery flags")
	}
	copy(message.Session[:], header[7:23])
	message.FrameSize = binary.BigEndian.Uint32(header[23:27])
	if message.FrameSize == 0 || message.FrameSize > maxFramePayload {
		return message, nil, errors.New("invalid multipath hello frame size")
	}
	if message.Control {
		if message.Policy != (sessionPolicy{}) || message.FrameSize != 1 {
			return message, nil, errors.New("invalid control policy")
		}
	} else if err := message.Policy.validate(int(message.FrameSize)); err != nil {
		return message, nil, err
	}
	length := int(binary.BigEndian.Uint16(header[27:29]))
	if length <= 0 {
		return message, nil, errors.New("empty multipath destination")
	}
	authenticated := header[6]&helloFlagAuth != 0
	size := length
	if authenticated {
		size += helloAuthSize
	}
	buffer := make([]byte, helloHeaderSize+size)
	copy(buffer, header[:])
	if _, err := io.ReadFull(conn, buffer[helloHeaderSize:]); err != nil {
		return message, nil, err
	}
	message.Destination = string(buffer[helloHeaderSize : helloHeaderSize+length])
	if authenticated {
		raw := buffer[helloHeaderSize+length:]
		auth = &helloAuth{signed: buffer[:len(buffer)-sha256.Size], mac: raw[24:], time: time.Unix(int64(binary.BigEndian.Uint64(raw[:8])), 0)}
		copy(auth.nonce[:], raw[8:24])
	}
	return message, auth, nil
}

// verifyHelloAuth checks the MAC, clock skew and replay of an authenticated
// hello against psk. seen records nonces for the skew period.
func verifyHelloAuth(auth *helloAuth, psk string, now time.Time, seen func([16]byte, time.Time) bool) error {
	if auth == nil {
		return errors.New("multipath hello is not authenticated")
	}
	if !hmac.Equal(helloMAC(psk, auth.signed).Sum(nil), auth.mac) {
		return errors.New("multipath hello authentication failed")
	}
	if skew := now.Sub(auth.time); skew > helloAuthSkew || skew < -helloAuthSkew {
		return errors.New("multipath hello timestamp outside the allowed clock skew")
	}
	if !seen(auth.nonce, now) {
		return errors.New("multipath hello replayed")
	}
	return nil
}

func writeHelloResponse(conn net.Conn, response helloResponse) error {
	var value uint32
	switch response.Status {
	case helloStatusOK:
		if response.FrameSize == 0 || response.FrameSize > maxFramePayload {
			return errors.New("invalid accepted multipath frame size")
		}
		value = response.FrameSize
	case helloStatusRejected:
		if !response.RejectReason.valid() {
			return errors.New("invalid multipath hello rejection reason")
		}
		value = uint32(response.RejectReason)
	default:
		return errors.New("invalid multipath hello response status")
	}
	var header [responseHeaderSize]byte
	copy(header[0:4], responseMagic[:])
	header[4] = helloVersion
	header[5] = response.Status
	binary.BigEndian.PutUint32(header[6:10], value)
	copy(header[10:], response.PolicyDigest[:])
	return writeAll(conn, header[:])
}

func readHelloResponse(conn net.Conn) (helloResponse, error) {
	var response helloResponse
	var header [responseHeaderSize]byte
	if _, err := io.ReadFull(conn, header[:7]); err != nil {
		return response, err
	}
	if string(header[0:4]) != string(responseMagic[:]) {
		return response, errors.New("invalid multipath hello response")
	}
	if header[4] != helloVersion {
		return response, &helloVersionError{version: header[4]}
	}
	if _, err := io.ReadFull(conn, header[7:]); err != nil {
		return response, err
	}
	copy(response.PolicyDigest[:], header[10:])
	response.Status = header[5]
	response.FrameSize = binary.BigEndian.Uint32(header[6:10])
	switch response.Status {
	case helloStatusRejected:
		response.RejectReason = helloRejectReason(response.FrameSize)
		response.FrameSize = 0
		if !response.RejectReason.valid() {
			return response, errors.New("invalid multipath hello rejection reason")
		}
		return response, &helloRejectedError{reason: response.RejectReason}
	case helloStatusOK:
	default:
		return response, errors.New("invalid multipath hello response status")
	}
	if response.FrameSize == 0 || response.FrameSize > maxFramePayload {
		return response, errors.New("invalid multipath accepted frame size")
	}
	return response, nil
}
