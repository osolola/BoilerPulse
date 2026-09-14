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
// r survives a crash immediately after. Equivalent to AppendBatch with a
// single record.
func (w *Writer) Append(r Record) error {
	return w.AppendBatch([]Record{r})
}

// AppendBatch writes every record in order with a single trailing fsync,
// rather than one fsync per record -- group commit. The caller is
// responsible for actually collecting concurrent writers into one batch
// (see internal/storage/lsm.Engine's opLoop); Writer itself has no
// goroutine or queue of its own; it just makes batching-by-the-caller
// cheap by exposing a batch-shaped API instead of forcing one fsync call
// per record.
//
// On a partial failure (record i's Write fails), records before i were
// written to the file but never fsynced -- since Sync is skipped entirely
// in that case, none of them are confirmed durable, and every record in
// the batch (not just record i onward) gets the same error. A crash right
// after would leave whatever landed in the OS page cache to be sorted out
// by replay's existing torn-write detection (see reader.go), exactly as a
// single failed Append always could.
func (w *Writer) AppendBatch(records []Record) error {
	if len(records) == 0 {
		return nil
	}
	for _, r := range records {
		if _, err := w.f.Write(encode(r)); err != nil {
			return err
		}
	}
	return w.f.Sync()
}

// Reset truncates the log back to empty. Call this only once the data it
// held has been durably captured elsewhere (e.g. flushed to an SSTable).
// The caller (Engine.processBatch) is responsible for making sure no
// concurrent AppendBatch call is in flight -- both run from the same
// single opLoop goroutine, so that's automatic in practice, not something
// Writer itself enforces.
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
