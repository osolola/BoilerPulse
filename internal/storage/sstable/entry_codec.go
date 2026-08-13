package sstable

import (
	"encoding/binary"
	"io"
)

// encodeEntry serializes e as: keyLen+key, tombstone, consLen+consistency,
// expiresAt, version, valueLen+value.
func encodeEntry(e Entry) []byte {
	buf := make([]byte, 0, 32+len(e.Key)+len(e.Consistency)+len(e.Value))
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(e.Key)))
	buf = append(buf, e.Key...)

	tomb := byte(0)
	if e.Tombstone {
		tomb = 1
	}
	buf = append(buf, tomb)

	buf = append(buf, byte(len(e.Consistency)))
	buf = append(buf, e.Consistency...)

	buf = binary.BigEndian.AppendUint64(buf, uint64(e.ExpiresAtUnixNano))
	buf = binary.BigEndian.AppendUint64(buf, e.Version)

	buf = binary.BigEndian.AppendUint32(buf, uint32(len(e.Value)))
	buf = append(buf, e.Value...)

	return buf
}

// decodeEntry reads one entry from r, in the exact field order encodeEntry
// writes them.
func decodeEntry(r io.Reader) (Entry, error) {
	var e Entry

	var keyLen uint16
	if err := binary.Read(r, binary.BigEndian, &keyLen); err != nil {
		return Entry{}, err
	}
	keyBuf := make([]byte, keyLen)
	if _, err := io.ReadFull(r, keyBuf); err != nil {
		return Entry{}, err
	}
	e.Key = string(keyBuf)

	var tombByte [1]byte
	if _, err := io.ReadFull(r, tombByte[:]); err != nil {
		return Entry{}, err
	}
	e.Tombstone = tombByte[0] == 1

	var consLenByte [1]byte
	if _, err := io.ReadFull(r, consLenByte[:]); err != nil {
		return Entry{}, err
	}
	consBuf := make([]byte, consLenByte[0])
	if _, err := io.ReadFull(r, consBuf); err != nil {
		return Entry{}, err
	}
	e.Consistency = string(consBuf)

	if err := binary.Read(r, binary.BigEndian, &e.ExpiresAtUnixNano); err != nil {
		return Entry{}, err
	}
	if err := binary.Read(r, binary.BigEndian, &e.Version); err != nil {
		return Entry{}, err
	}

	var valueLen uint32
	if err := binary.Read(r, binary.BigEndian, &valueLen); err != nil {
		return Entry{}, err
	}
	value := make([]byte, valueLen)
	if _, err := io.ReadFull(r, value); err != nil {
		return Entry{}, err
	}
	e.Value = value

	return e, nil
}
