// Package wal implements a durable, append-only write-ahead log: every
// mutation is framed with a length prefix and CRC32 checksum, fsynced on
// Append, and replayed on startup. It knows nothing about storage.Entry or
// the KV API — it's a generic, storage-format-agnostic log.
package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

// Op identifies the kind of mutation a Record represents.
type Op uint8

const (
	OpSet    Op = 1
	OpDelete Op = 2
)

// ErrCorruptRecord means a record's payload failed its checksum. This is
// the expected shape of a crash that tore a write in the middle of a
// record — replay should stop here, not fail outright.
var ErrCorruptRecord = errors.New("wal: corrupt record")

// Record is one durable mutation.
type Record struct {
	Seq               uint64
	Op                Op
	Timestamp         int64 // unix nano, when the mutation was applied
	ExpiresAtUnixNano int64 // 0 means no expiry
	Key               string
	Consistency       string
	Value             []byte // empty for OpDelete
}

// encode frames r as [4-byte payload length][payload][4-byte CRC32 of payload].
func encode(r Record) []byte {
	payload := make([]byte, 0, 32+len(r.Key)+len(r.Consistency)+len(r.Value))
	payload = binary.BigEndian.AppendUint64(payload, r.Seq)
	payload = append(payload, byte(r.Op))
	payload = binary.BigEndian.AppendUint64(payload, uint64(r.Timestamp))
	payload = binary.BigEndian.AppendUint64(payload, uint64(r.ExpiresAtUnixNano))
	payload = binary.BigEndian.AppendUint16(payload, uint16(len(r.Key)))
	payload = append(payload, r.Key...)
	payload = append(payload, byte(len(r.Consistency)))
	payload = append(payload, r.Consistency...)
	payload = binary.BigEndian.AppendUint32(payload, uint32(len(r.Value)))
	payload = append(payload, r.Value...)

	frame := make([]byte, 0, 4+len(payload)+4)
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(payload)))
	frame = append(frame, payload...)
	frame = binary.BigEndian.AppendUint32(frame, crc32.ChecksumIEEE(payload))
	return frame
}

// decode reads exactly one framed record from r.
//
// io.EOF with nothing read means a clean end of log. io.ErrUnexpectedEOF
// means the frame was truncated mid-write — the classic shape of a crash.
// ErrCorruptRecord means the payload didn't match its checksum. Callers
// (Reader.ReadAll) treat all three as "stop replay here", not as fatal
// errors, since that's exactly the durability guarantee a WAL provides:
// keep everything fsynced before the crash, trust nothing after it.
func decode(r io.Reader) (Record, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return Record{}, err
	}
	payloadLen := binary.BigEndian.Uint32(lenBuf[:])

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return Record{}, io.ErrUnexpectedEOF
	}

	var crcBuf [4]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		return Record{}, io.ErrUnexpectedEOF
	}
	if binary.BigEndian.Uint32(crcBuf[:]) != crc32.ChecksumIEEE(payload) {
		return Record{}, ErrCorruptRecord
	}

	return decodePayload(payload)
}

func decodePayload(payload []byte) (Record, error) {
	var rec Record
	buf := bytes.NewReader(payload)

	if err := binary.Read(buf, binary.BigEndian, &rec.Seq); err != nil {
		return Record{}, ErrCorruptRecord
	}
	opByte, err := buf.ReadByte()
	if err != nil {
		return Record{}, ErrCorruptRecord
	}
	rec.Op = Op(opByte)

	if err := binary.Read(buf, binary.BigEndian, &rec.Timestamp); err != nil {
		return Record{}, ErrCorruptRecord
	}
	if err := binary.Read(buf, binary.BigEndian, &rec.ExpiresAtUnixNano); err != nil {
		return Record{}, ErrCorruptRecord
	}

	var keyLen uint16
	if err := binary.Read(buf, binary.BigEndian, &keyLen); err != nil {
		return Record{}, ErrCorruptRecord
	}
	keyBytes := make([]byte, keyLen)
	if _, err := io.ReadFull(buf, keyBytes); err != nil {
		return Record{}, ErrCorruptRecord
	}
	rec.Key = string(keyBytes)

	consLen, err := buf.ReadByte()
	if err != nil {
		return Record{}, ErrCorruptRecord
	}
	consBytes := make([]byte, consLen)
	if _, err := io.ReadFull(buf, consBytes); err != nil {
		return Record{}, ErrCorruptRecord
	}
	rec.Consistency = string(consBytes)

	var valueLen uint32
	if err := binary.Read(buf, binary.BigEndian, &valueLen); err != nil {
		return Record{}, ErrCorruptRecord
	}
	value := make([]byte, valueLen)
	if _, err := io.ReadFull(buf, value); err != nil {
		return Record{}, ErrCorruptRecord
	}
	rec.Value = value

	return rec, nil
}
