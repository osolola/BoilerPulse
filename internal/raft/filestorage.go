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
// protocol internal/storage/lsm uses for SSTable flushes. The snapshot
// (state-machine data plus its index/term) lives in a third file, written
// with the same atomic protocol.
var _ Storage = (*FileStorage)(nil)

type FileStorage struct {
	mu           sync.Mutex
	dir          string
	statePath    string
	snapshotPath string
	logPath      string
	logFile      *os.File
	offsets      []int64 // offsets[i] = byte offset where the log entry at position i starts
	loaded       []LogEntry
	// baseIndex is the position-to-index offset: loaded[i].Index ==
	// baseIndex+i+1. 0 for a log that has never been compacted. Kept in
	// sync with the snapshot file's LastIncludedIndex by DiscardLogThrough.
	baseIndex uint64
}

type persistedState struct {
	Term     uint64 `json:"term"`
	VotedFor string `json:"voted_for"`
}

// OpenFileStorage opens (or creates) Raft storage under dir. On open, it
// replays the log file to rebuild the offset index and, per the WAL crash-
// safety pattern, truncates away any torn trailing write left by a crash
// mid-append. If a snapshot exists, baseIndex is initialized from it, since
// the log file on disk only ever holds entries after the snapshot it was
// last compacted against.
func OpenFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating raft storage dir: %w", err)
	}
	if err := cleanupOrphanedTempFiles(dir); err != nil {
		return nil, fmt.Errorf("cleaning up orphaned temp files: %w", err)
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

	fs := &FileStorage{
		dir:          dir,
		statePath:    filepath.Join(dir, "raft-state.json"),
		snapshotPath: filepath.Join(dir, "raft-snapshot.bin"),
		logPath:      logPath,
		logFile:      f,
		offsets:      offsets,
		loaded:       entries,
	}

	if lastIncludedIndex, _, _, ok, err := fs.LoadSnapshot(); err != nil {
		f.Close()
		return nil, fmt.Errorf("loading raft snapshot: %w", err)
	} else if ok {
		fs.baseIndex = lastIncludedIndex
	}

	return fs, nil
}

// cleanupOrphanedTempFiles removes any *.tmp file left behind by a crash
// mid-write (DiscardLogThrough's log rewrite, or SaveSnapshot/
// SaveTermAndVote's atomic replace). Always safe to remove unconditionally:
// by construction, the real file each of these was about to atomically
// replace is still fully intact whenever its .tmp counterpart survives to
// be found here -- that's the entire point of write-temp-then-rename.
// Mirrors internal/storage/lsm's identical cleanup on Open.
func cleanupOrphanedTempFiles(dir string) error {
	for _, name := range []string{"raft-log.bin.tmp", "raft-snapshot.bin.tmp", "raft-state.json.tmp"} {
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
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

// position returns where index sits in offsets/loaded, or false if it's out
// of the currently-retained range (already compacted, or not written yet).
func (fs *FileStorage) position(index uint64) (int, bool) {
	if index <= fs.baseIndex {
		return 0, false
	}
	pos := index - fs.baseIndex - 1
	if pos >= uint64(len(fs.loaded)) {
		return 0, false
	}
	return int(pos), true
}

// TruncateFrom discards every persisted entry with Index >= index by
// truncating the log file directly at that entry's recorded byte offset.
func (fs *FileStorage) TruncateFrom(index uint64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	pos, ok := fs.position(index)
	if !ok {
		return nil // already compacted past index, or nothing there yet
	}
	cutOffset := fs.offsets[pos]
	if err := fs.logFile.Truncate(cutOffset); err != nil {
		return err
	}
	if _, err := fs.logFile.Seek(cutOffset, io.SeekStart); err != nil {
		return err
	}
	fs.offsets = fs.offsets[:pos]
	fs.loaded = fs.loaded[:pos]
	return fs.logFile.Sync()
}

// DiscardLogThrough removes every persisted entry with Index <= index by
// rewriting the log file to contain only what survives -- a full rewrite,
// not an in-place edit, matching this project's other on-disk formats
// (SSTables, WAL segments) which also favor a simple full-rewrite
// compaction over a more surgical one (see docs/storage-engine.md). Log
// compaction is occasional (governed by Options.SnapshotThreshold, at
// minimum hundreds of entries between runs), so the rewrite cost is
// negligible next to the space and startup-replay time it saves.
//
// Written with the same temp-file -> fsync -> rename -> fsync-dir protocol
// used throughout this project (internal/storage/lsm/flush.go,
// FileStorage.SaveTermAndVote): if this crashes before the rename, the temp
// file is simply orphaned and the original log is untouched; the caller is
// only safe to call this after SaveSnapshot has already durably persisted a
// snapshot covering index, so even a fully-lost rewrite attempt leaves
// recoverable state.
func (fs *FileStorage) DiscardLogThrough(index uint64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if index <= fs.baseIndex {
		return nil
	}
	drop := index - fs.baseIndex
	if drop > uint64(len(fs.loaded)) {
		drop = uint64(len(fs.loaded))
	}
	keep := fs.loaded[drop:]

	tmpPath := fs.logPath + ".tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("creating temp raft log: %w", err)
	}
	newOffsets := make([]int64, 0, len(keep))
	var offset int64
	for _, e := range keep {
		frame := encodeLogEntry(e)
		if _, err := tmpFile.Write(frame); err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("writing compacted raft log: %w", err)
		}
		newOffsets = append(newOffsets, offset)
		offset += int64(len(frame))
	}
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("syncing compacted raft log: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("closing compacted raft log: %w", err)
	}

	// Close the old handle before renaming over it -- required on Windows,
	// harmless and irrelevant to correctness on the Unix systems this
	// project actually targets, but cheap to get right either way.
	if err := fs.logFile.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("closing old raft log: %w", err)
	}
	if err := os.Rename(tmpPath, fs.logPath); err != nil {
		return fmt.Errorf("finalizing compacted raft log: %w", err)
	}
	if err := syncDir(fs.dir); err != nil {
		return fmt.Errorf("syncing raft storage dir: %w", err)
	}

	newFile, err := os.OpenFile(fs.logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("reopening compacted raft log: %w", err)
	}

	fs.logFile = newFile
	fs.offsets = newOffsets
	fs.loaded = append([]LogEntry(nil), keep...)
	fs.baseIndex = index
	return nil
}

type persistedSnapshot struct {
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
	Data              []byte `json:"data"`
}

// SaveSnapshot atomically persists a snapshot, replacing any previous one.
// Uses the same write-temp / fsync / rename / fsync-dir protocol as
// SaveTermAndVote.
func (fs *FileStorage) SaveSnapshot(lastIncludedIndex, lastIncludedTerm uint64, data []byte) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	payload, err := json.Marshal(persistedSnapshot{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              data,
	})
	if err != nil {
		return fmt.Errorf("encoding snapshot: %w", err)
	}

	tmpPath := fs.snapshotPath + ".tmp"
	if err := os.WriteFile(tmpPath, payload, 0o644); err != nil {
		return err
	}
	if err := syncFile(tmpPath); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, fs.snapshotPath); err != nil {
		return err
	}
	return syncDir(fs.dir)
}

// LoadSnapshot returns the most recently saved snapshot, if any.
func (fs *FileStorage) LoadSnapshot() (lastIncludedIndex, lastIncludedTerm uint64, data []byte, ok bool, err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	raw, err := os.ReadFile(fs.snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, nil, false, nil
		}
		return 0, 0, nil, false, err
	}
	var ps persistedSnapshot
	if err := json.Unmarshal(raw, &ps); err != nil {
		return 0, 0, nil, false, fmt.Errorf("parsing snapshot file: %w", err)
	}
	return ps.LastIncludedIndex, ps.LastIncludedTerm, ps.Data, true, nil
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
