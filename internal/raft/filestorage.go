package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// FileStorage is the durable Storage implementation used by cmd/node. The
// log is an append-only, framed-and-checksummed file (mirroring
// internal/storage/wal's technique) with byte offsets tracked per entry so
// TruncateFrom can truncate directly rather than rewriting the file.
// currentTerm/votedFor live in a small separate file, rewritten atomically
// (temp file + fsync + rename + fsync dir) on each change, the same
// protocol internal/storage/lsm uses for SSTable flushes.
var _ Storage = (*FileStorage)(nil)

type FileStorage struct {
	mu        sync.Mutex
	statePath string
	logFile   *os.File
	offsets   []int64 // offsets[i] = byte offset where log entry Index i+1 starts
	loaded    []LogEntry
}

type persistedState struct {
	Term     uint64 `json:"term"`
	VotedFor string `json:"voted_for"`
}

// OpenFileStorage opens (or creates) Raft storage under dir. On open, it
// replays the log file to rebuild the offset index and, per the WAL crash-
// safety pattern, truncates away any torn trailing write left by a crash
// mid-append.
func OpenFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating raft storage dir: %w", err)
	}

	logPath := filepath.Join(dir, "raft-log.bin")
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening raft log: %w", err)
	}

	entries, offsets, validLength, err := replayLogFile(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("replaying raft log: %w", err)
	}
	if err := f.Truncate(validLength); err != nil {
		f.Close()
		return nil, fmt.Errorf("truncating torn raft log tail: %w", err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}

	return &FileStorage{
		statePath: filepath.Join(dir, "raft-state.json"),
		logFile:   f,
		offsets:   offsets,
		loaded:    entries,
	}, nil
}

func replayLogFile(f *os.File) (entries []LogEntry, offsets []int64, validLength int64, err error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, nil, 0, err
	}

	var offset int64
	for {
		entry, frameSize, derr := decodeLogEntry(f)
		if derr != nil {
			if errors.Is(derr, io.EOF) || errors.Is(derr, io.ErrUnexpectedEOF) || errors.Is(derr, errCorruptLogEntry) {
				break
			}
			return nil, nil, 0, derr
		}
		entries = append(entries, entry)
		offsets = append(offsets, offset)
		offset += frameSize
	}
	return entries, offsets, offset, nil
}

func (fs *FileStorage) LoadLog() ([]LogEntry, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	out := make([]LogEntry, len(fs.loaded))
	copy(out, fs.loaded)
	return out, nil
}

func (fs *FileStorage) AppendEntries(entries []LogEntry) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	for _, e := range entries {
		offset, err := fs.logFile.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		if _, err := fs.logFile.Write(encodeLogEntry(e)); err != nil {
			return err
		}
		fs.offsets = append(fs.offsets, offset)
		fs.loaded = append(fs.loaded, e)
	}
	return fs.logFile.Sync()
}

// TruncateFrom discards every persisted entry with Index >= index by
// truncating the log file directly at that entry's recorded byte offset.
func (fs *FileStorage) TruncateFrom(index uint64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if index == 0 || int(index-1) >= len(fs.offsets) {
		return nil
	}
	cutOffset := fs.offsets[index-1]
	if err := fs.logFile.Truncate(cutOffset); err != nil {
		return err
	}
	if _, err := fs.logFile.Seek(cutOffset, io.SeekStart); err != nil {
		return err
	}
	fs.offsets = fs.offsets[:index-1]
	fs.loaded = fs.loaded[:index-1]
	return fs.logFile.Sync()
}

func (fs *FileStorage) SaveTermAndVote(term uint64, votedFor string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	data, err := json.Marshal(persistedState{Term: term, VotedFor: votedFor})
	if err != nil {
		return err
	}

	dir := filepath.Dir(fs.statePath)
	tmpPath := fs.statePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}
	if err := syncFile(tmpPath); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, fs.statePath); err != nil {
		return err
	}
	return syncDir(dir)
}

func (fs *FileStorage) LoadTermAndVote() (uint64, string, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	data, err := os.ReadFile(fs.statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, "", nil
		}
		return 0, "", err
	}
	var ps persistedState
	if err := json.Unmarshal(data, &ps); err != nil {
		return 0, "", fmt.Errorf("parsing raft state file: %w", err)
	}
	return ps.Term, ps.VotedFor, nil
}

// Close closes the log file handle.
func (fs *FileStorage) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.logFile.Close()
}

func syncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
