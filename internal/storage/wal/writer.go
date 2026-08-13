package wal

import "os"

// Writer appends records to a durable, append-only log file.
type Writer struct {
	f *os.File
}

// OpenWriter opens (creating if needed) the WAL file at path for appending.
func OpenWriter(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &Writer{f: f}, nil
}

// Append writes r and fsyncs before returning, so a successful Append means
// r survives a crash immediately after.
func (w *Writer) Append(r Record) error {
	if _, err := w.f.Write(encode(r)); err != nil {
		return err
	}
	return w.f.Sync()
}

// Reset truncates the log back to empty. Call this only once the data it
// held has been durably captured elsewhere (e.g. flushed to an SSTable).
func (w *Writer) Reset() error {
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	if _, err := w.f.Seek(0, 0); err != nil {
		return err
	}
	return w.f.Sync()
}

// Close closes the underlying file handle.
func (w *Writer) Close() error {
	return w.f.Close()
}
