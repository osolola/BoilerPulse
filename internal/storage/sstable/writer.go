package sstable

import (
	"encoding/binary"
	"os"
)

// WriteSorted writes entries — which callers MUST already have sorted by
// Key ascending — to a new SSTable file at path: a data block, a full key
// index, and a footer. It fsyncs before returning.
func WriteSorted(path string, entries []Entry) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	type indexEntry struct {
		key    string
		offset uint64
	}
	index := make([]indexEntry, 0, len(entries))

	var offset uint64
	for _, e := range entries {
		buf := encodeEntry(e)
		if _, err := f.Write(buf); err != nil {
			return err
		}
		index = append(index, indexEntry{key: e.Key, offset: offset})
		offset += uint64(len(buf))
	}

	indexOffset := offset
	for _, ie := range index {
		buf := make([]byte, 0, 10+len(ie.key))
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(ie.key)))
		buf = append(buf, ie.key...)
		buf = binary.BigEndian.AppendUint64(buf, ie.offset)
		if _, err := f.Write(buf); err != nil {
			return err
		}
		offset += uint64(len(buf))
	}
	indexLen := offset - indexOffset

	footer := make([]byte, 0, footerSize)
	footer = binary.BigEndian.AppendUint64(footer, indexOffset)
	footer = binary.BigEndian.AppendUint64(footer, indexLen)
	footer = binary.BigEndian.AppendUint64(footer, uint64(len(entries)))
	footer = binary.BigEndian.AppendUint32(footer, magic)
	if _, err := f.Write(footer); err != nil {
		return err
	}

	return f.Sync()
}
