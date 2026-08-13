package wal

import (
	"errors"
	"io"
	"os"
)

// ReadAll replays every valid record in the WAL file at path, in the order
// they were written. A missing file returns (nil, nil) — a brand-new node
// has no WAL yet.
//
// A truncated or checksum-mismatched trailing record stops replay at the
// last valid record instead of returning an error: that's the expected
// shape of a crash that interrupted a write, and dropping only the torn
// tail (never anything before it) is exactly the guarantee a WAL exists to
// provide.
func ReadAll(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var records []Record
	for {
		rec, err := decode(f)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, ErrCorruptRecord) {
				break
			}
			return nil, err
		}
		records = append(records, rec)
	}
	return records, nil
}
