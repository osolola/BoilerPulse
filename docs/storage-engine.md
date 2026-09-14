# Storage Engine

`internal/storage/lsm.Engine` is the durable `storage.Engine` implementation
used by `cmd/node`. It's a small LSM-tree: a write-ahead log, an in-memory
memtable, and immutable SSTable files on disk, with synchronous flush and
compaction, and group-committed writes.

```
Put/Delete (any number of concurrent callers)
    │
    ▼
 assign seq, enqueue (e.mu held only for this step)
    │
    ▼
 opLoop: drain whatever's queued, ONE WAL.AppendBatch (ONE fsync) for all of it
    │                                                ──────────────► wal.log
    ▼
 apply every op to the memtable, in seq order (e.mu held for this step)
    │
    │  memtable size > threshold?
    ▼
 flush: write SSTable (temp → fsync → rename → fsync dir) → wal.Reset()
    │
    │  sstable count > threshold?
    ▼
 compact: merge all SSTables → drop tombstones/expired → one new SSTable
```

## Packages

- `internal/storage/wal` — the write-ahead log. Format-agnostic: it knows
  about `Record` (seq, op, timestamp, TTL, key, consistency, value), not
  about `storage.Entry`.
- `internal/storage/sstable` — the on-disk sorted-table format: data block +
  full key index + fixed-size footer. Also format-agnostic.
- `internal/storage/lsm` — glues the two together into a `storage.Engine`,
  translating `storage.Entry` to/from `wal.Record` and `sstable.Entry`.

Each is unit-tested independently (`go test ./internal/storage/...`), per
the spec's testing requirements.

## On-disk layout

```
<data_dir>/
  wal.log
  sstables/
    000001.sst
    000002.sst
    ...
```

## WAL record format

Each record is framed as `[4-byte payload length][payload][4-byte CRC32 of
payload]`. The payload holds: seq (uint64), op (1 byte), timestamp (int64,
unix nano), expires-at (int64, unix nano, 0 = none), key (length-prefixed),
consistency (length-prefixed), value (length-prefixed).

`Append` fsyncs before returning — a successful `Append` is a durability
guarantee. On replay, a truncated frame (`io.ErrUnexpectedEOF`) or a
checksum mismatch (`ErrCorruptRecord`) both stop replay at that point rather
than erroring out: that's the expected shape of a crash that interrupted a
write, and the WAL's whole job is to keep everything fsynced before the
crash while trusting nothing after it.

## SSTable format

A data block (sorted entries: key, tombstone flag, consistency, expires-at,
version, value), followed by a full index (key → data-block offset), followed
by a 28-byte footer (index offset, index length, entry count, magic number).
`Open` loads the index into memory; a point lookup costs one seek + read.

## Group commit (concurrent writes share one fsync)

`Put`/`Delete` don't call the WAL directly. Each one briefly holds `e.mu`
just long enough to assign a sequence number and enqueue its record and
memtable mutation onto a channel (`internal/storage/lsm/engine.go`'s
`opCh`) — that brief, fast, in-memory critical section is what guarantees
enqueue order matches sequence order. A single background goroutine
(`opLoop`) drains whatever has accumulated, writes every queued record with
**one** trailing fsync (`wal.Writer.AppendBatch`), and only then applies
every mutation to the memtable, still in that same order, under `e.mu`
again.

The payoff: N concurrent writers no longer mean N serialized fsyncs.
`TestConcurrentPutsCoalesceIntoFewerWALBatches`
(`internal/storage/lsm/groupcommit_test.go`) proves this directly — 50
concurrent `Put`s collapse into a handful of `AppendBatch` calls, not 50.
`TestConcurrentPutsToSameKeyApplyInSeqOrder` guards the correctness
requirement this design introduces: two concurrent writers to the *same*
key must still apply in sequence-assigned order, never the reverse (a
lost update), even though they may land in the same batch or race to
acquire `e.mu` again afterward.

`internal/raft`'s `Node.Propose` uses the identical pattern
(`appendLoop`/`appendBatch` in `apply.go`) for the Raft log itself — see
`docs/raft.md` and `docs/benchmarking.md` for what that fixed (and what it
didn't: the *apply* path, where committed entries reach this engine's
`Put`, is still strictly sequential — see `docs/benchmarking.md`'s "What we
found" for why that's a real, separate, currently-unsolved bottleneck
under sustained heavy write load).

## Crash-safe flush protocol

Flushing the memtable writes to `sstables/tmp-<seq>.sst`, fsyncs it, atomically
renames it to `sstables/<seq>.sst`, fsyncs the `sstables/` directory, and only
*then* truncates the WAL. This ordering is what makes a crash at any point
safe:

- **Crash before rename**: the temp file is orphaned and ignored (cleaned up
  on the next `Open`); the WAL still has every record, so replay rebuilds
  the identical memtable.
- **Crash after rename, before WAL reset**: the new SSTable is durable *and*
  the WAL still has the same records. Replay re-applies them into the fresh
  memtable — harmless, since applying the same Set/Delete twice is
  idempotent.

`TestCrashAfterFlushBeforeWALResetIsHarmlessOnReplay` in
`internal/storage/lsm/engine_test.go` exercises this second case directly.

## Compaction

Compaction is a full merge: every current SSTable's entries are combined
into one map (newer tables overwrite older ones for the same key), tombstones
and already-expired entries are dropped, and the result is written as a
single new SSTable via the same write-temp/rename/fsync protocol as flush.
It's triggered synchronously, inline, once the SSTable count exceeds
`Options.CompactionThreshold` after a flush.

Dropping tombstones is only safe because the merge spans *every* table —
there's no older table left underneath that still needs the tombstone to
shadow a stale value.

## Known simplifications

This milestone favors a simple, verifiably-correct design over a
performance-optimized one:

- **Synchronous flush and compaction.** Both hold the engine's write lock
  for their full duration, blocking concurrent reads and writes. A
  background-flush design (immutable "frozen" memtable served alongside a
  fresh one) would remove that latency spike but adds real concurrency
  complexity — deferred.
- **Full-table compaction**, not leveled/tiered. Every compaction rewrites
  the entire dataset. Fine at this project's scale; a real system would
  partition into levels to avoid full rewrites as data grows.
- **No manifest file.** The next SSTable sequence number and WAL record
  sequence counter are reconstructed on `Open` by scanning filenames and
  iterating every SSTable's contents, rather than being persisted directly.
  Simple and correct at this scale; a manifest would be the natural next
  step for larger datasets.
- **In-memory memtable is a plain map**, not a skip list — per the spec's
  own guidance not to over-engineer this before it's needed.

## Recovery

On `Open`: load and index every finalized SSTable (newest wins on
conflicting keys during reads), replay the WAL on top to reconstruct the
memtable, and continue the sequence counter from the highest value seen.
See `internal/storage/lsm/engine_test.go` for the crash-recovery tests, and
`tests/integration/persistence_test.go` for the same behavior driven over a
real HTTP server. A real `kill -9` + restart was also smoke-tested manually
against the compiled `cmd/node` binary.
