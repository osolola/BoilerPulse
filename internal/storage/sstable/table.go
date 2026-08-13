package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// Table is an open, read-only handle on a finalized SSTable file. Its index
// is loaded fully into memory on Open, so a point lookup costs one seek and
// one read against the data block.
type Table struct {
	f           *os.File
	index       map[string]uint64 // key -> data-block byte offset
	indexOffset int64
	count       uint64
}

// Open reads path's footer and index and returns a ready-to-query Table.
func Open(path string) (*Table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if info.Size() < footerSize {
		f.Close()
		return nil, fmt.Errorf("%w: %s: file too small", ErrInvalidFile, path)
	}

	footer := make([]byte, footerSize)
	if _, err := f.ReadAt(footer, info.Size()-footerSize); err != nil {
		f.Close()
		return nil, err
	}
	indexOffset := binary.BigEndian.Uint64(footer[0:8])
	indexLen := binary.BigEndian.Uint64(footer[8:16])
	count := binary.BigEndian.Uint64(footer[16:24])
	gotMagic := binary.BigEndian.Uint32(footer[24:28])
	if gotMagic != magic {
		f.Close()
		return nil, fmt.Errorf("%w: %s: bad magic number", ErrInvalidFile, path)
	}

	indexBuf := make([]byte, indexLen)
	if indexLen > 0 {
		if _, err := f.ReadAt(indexBuf, int64(indexOffset)); err != nil {
			f.Close()
			return nil, err
		}
	}

	index := make(map[string]uint64, count)
	pos := 0
	for pos < len(indexBuf) {
		keyLen := int(binary.BigEndian.Uint16(indexBuf[pos : pos+2]))
		pos += 2
		key := string(indexBuf[pos : pos+keyLen])
		pos += keyLen
		off := binary.BigEndian.Uint64(indexBuf[pos : pos+8])
		pos += 8
		index[key] = off
	}

	return &Table{f: f, index: index, indexOffset: int64(indexOffset), count: count}, nil
}

// Get returns the entry for key, if present in this table.
func (t *Table) Get(key string) (Entry, bool, error) {
	offset, ok := t.index[key]
	if !ok {
		return Entry{}, false, nil
	}
	sr := io.NewSectionReader(t.f, int64(offset), t.indexOffset-int64(offset))
	e, err := decodeEntry(sr)
	if err != nil {
		return Entry{}, false, err
	}
	return e, true, nil
}

// Iterate returns every entry in the table, in sorted key order.
func (t *Table) Iterate() ([]Entry, error) {
	sr := io.NewSectionReader(t.f, 0, t.indexOffset)
	entries := make([]Entry, 0, t.count)
	for {
		e, err := decodeEntry(sr)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// Close closes the underlying file handle.
func (t *Table) Close() error {
	return t.f.Close()
}
