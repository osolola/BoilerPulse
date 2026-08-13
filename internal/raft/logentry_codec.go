package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

// errCorruptLogEntry mirrors internal/storage/wal's ErrCorruptRecord: a
// checksum mismatch on a persisted Raft log entry, treated as the tail end
// of a crash-interrupted write rather than a fatal error.
var errCorruptLogEntry = errors.New("raft: corrupt log entry")

// encodeLogEntry frames e as [4-byte payload length][payload][4-byte CRC32
// of payload], the same technique internal/storage/wal uses for KV
// mutations, applied here to Raft log entries instead.
func encodeLogEntry(e LogEntry) []byte {
	payload := make([]byte, 0, 24+len(e.Command))
	payload = binary.BigEndian.AppendUint64(payload, e.Term)
	payload = binary.BigEndian.AppendUint64(payload, e.Index)
	payload = binary.BigEndian.AppendUint32(payload, uint32(len(e.Command)))
	payload = append(payload, e.Command...)

	frame := make([]byte, 0, 4+len(payload)+4)
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(payload)))
	frame = append(frame, payload...)
	frame = binary.BigEndian.AppendUint32(frame, crc32.ChecksumIEEE(payload))
	return frame
}

// decodeLogEntry reads one framed entry from r, returning the entry and the
// total number of bytes the frame occupied (so callers can track byte
// offsets for later truncation). io.EOF with nothing read means a clean end
// of file; io.ErrUnexpectedEOF means a truncated frame; errCorruptLogEntry
// means a checksum mismatch — all three mean "stop reading here", matching
// wal.decode's crash-safety semantics.
func decodeLogEntry(r io.Reader) (LogEntry, int64, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return LogEntry{}, 0, err
	}
	payloadLen := binary.BigEndian.Uint32(lenBuf[:])

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return LogEntry{}, 0, io.ErrUnexpectedEOF
	}

	var crcBuf [4]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		return LogEntry{}, 0, io.ErrUnexpectedEOF
	}
	if binary.BigEndian.Uint32(crcBuf[:]) != crc32.ChecksumIEEE(payload) {
		return LogEntry{}, 0, errCorruptLogEntry
	}

	entry, err := decodeLogEntryPayload(payload)
	if err != nil {
		return LogEntry{}, 0, err
	}
	return entry, int64(4 + len(payload) + 4), nil
}

func decodeLogEntryPayload(payload []byte) (LogEntry, error) {
	var e LogEntry
	buf := bytes.NewReader(payload)

	if err := binary.Read(buf, binary.BigEndian, &e.Term); err != nil {
		return LogEntry{}, errCorruptLogEntry
	}
	if err := binary.Read(buf, binary.BigEndian, &e.Index); err != nil {
		return LogEntry{}, errCorruptLogEntry
	}
	var cmdLen uint32
	if err := binary.Read(buf, binary.BigEndian, &cmdLen); err != nil {
		return LogEntry{}, errCorruptLogEntry
	}
	cmd := make([]byte, cmdLen)
	if _, err := io.ReadFull(buf, cmd); err != nil {
		return LogEntry{}, errCorruptLogEntry
	}
	e.Command = cmd

	return e, nil
}
