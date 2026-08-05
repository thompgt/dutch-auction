# Recovery

What is on disk, what it means, and what to do when a process dies. This document covers
`auction-wal` only; promotion and fencing of a standby arrive with Phases 3 and 8.

## What a log directory contains

```
<dir>/
  manifest.json                        the auction's config: id, supply, schedule, batch window
  wal-00000000.log                     command log, in segments, oldest first
  wal-00000001.log
  snapshot-00000000000000001234.snap   state as of sequence 1234
```

Only two of these are load-bearing. The **manifest** says which auction this is; without it the
directory cannot be interpreted, because the commands do not carry the supply or the schedule.
The **segments** are the auction — every command that was ever applied, in the order it was
applied. Snapshots are derived data and can all be deleted at any time with no loss beyond
recovery speed.

Nothing in this directory should ever be edited. It is the audit record, and it is the artifact
a regulator would be shown.

## After an unclean shutdown

Nothing to do. `recover()` runs on startup, and the cases below are handled without operator
involvement:

| What the crash left | What happens | Why it is safe |
|---|---|---|
| A partially written record at the end | Discarded, and the file is truncated to the last whole record | Acknowledgement happens after `fsync`, so a torn record was never promised to anyone |
| A snapshot that fails its checksum | Skipped; recovery falls back to an older one or to the whole log | A snapshot is an optimisation; the log alone always reproduces the same state |
| A `.snap.partial` file | Ignored | Snapshots are renamed into place atomically, so a partial file was never a snapshot |
| A segment written but no snapshot since | Replayed in full | Slower start, identical result |

The log line to look for is `recovered auction from disk`, which reports how many commands were
replayed and where the sequencer resumed.

If `replayed` climbs steadily release over release, snapshots are falling behind. That is not an
incident, but recovery time is the thing that quietly degrades until the day it matters.

## When recovery refuses to start

Three errors are deliberate refusals rather than bugs. In each case the safe action is to stop,
because the alternative is serving an auction that looks entirely plausible and is wrong.

**`corrupt record in ... at offset N`** — a record failed its checksum somewhere other than the
end of the final segment, or passed its checksum and would not decode. The first means the file
was damaged after it was written; the second means the log is intact and this build cannot read
it, which is what a format change looks like from the reader's side. Do not truncate the file to
get past it. Preserve the directory, and recover from the standby or from a backup.

**`log jumps from #N to #M`** — a command is missing from the middle. The replayed state would
not be the state that was acknowledged to participants. Same response: preserve and fail over.

**`manifest ... does not match the auction being opened`** — the process was pointed at a
different auction's directory, or at the right directory with the wrong configuration. Check the
auction id and the supply before doing anything else; this one is usually a deployment mistake
rather than data damage.

## Verifying a log by hand

```sh
# every command, in order, as JSON
cargo run -p auction-wal --bin wal-dump -- <dir>       # Phase 5 (audit export)
```

Until that exists, `auction_wal::read_all(dir)` and `auction_wal::recover(dir)` are the two
entry points, and they are the same ones the engine uses — there is no separate "verification"
path that could disagree with the real one.

## What is not yet handled

- **Snapshots are written by whoever calls `snapshot::write`.** Nothing schedules them yet;
  Phase 3 owns that, because it owns the thread that has exclusive access to the state.
- **Segments are never archived or pruned.** They accumulate. For a single auction that is
  bounded and small; a long-lived multi-auction deployment needs a retention policy that moves
  sealed segments to object storage rather than deleting them.
- **`fsync` is trusted.** If the storage device lies about flushing — some consumer SSDs do,
  and so do most virtualised disks with write caching enabled — invariant I6 does not hold, and
  no amount of application code can restore it. Verify write-cache settings on any host that
  runs a primary.
