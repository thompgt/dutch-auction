//! Rebuilding an auction from what is on disk.
//!
//! This is the payoff for everything else in the crate, and the reason there is exactly one
//! function here. Crash recovery and the hot standby are not two mechanisms that happen to
//! resemble each other — they are literally this code, run in two different situations. A bug
//! that only affects failover cannot exist, because failover has no code of its own.
//!
//! The audit export is the same story from a third angle: the log is the record, and this
//! function is the proof that the record reproduces the auction that participants actually
//! traded in.

use std::path::Path;

use auction_core::AuctionState;
use auction_proto::Seq;

use crate::error::{Result, WalError};
use crate::record::LogRecord;
use crate::{snapshot, wal};

/// The outcome of a recovery.
#[derive(Debug)]
pub struct Recovered {
    pub state: AuctionState,
    /// Where the sequencer must resume. Handing out a number at or below this would re-use a
    /// position in the total order, which invariant I4 forbids.
    pub next_seq: Seq,
    /// How many commands were replayed on top of the snapshot. Worth logging: a number that
    /// climbs release over release means snapshots are falling behind, and recovery time is the
    /// thing that quietly degrades until the day it matters.
    pub replayed: usize,
    /// The snapshot recovery started from, if any.
    pub from_snapshot: Option<Seq>,
}

/// Rebuild the auction stored in `dir`.
///
/// Starts from the newest usable snapshot and replays every command after it. With no snapshot,
/// replays the whole log — slower, but identical in result, which is the property that makes
/// snapshots safe to treat as disposable.
pub fn recover(dir: impl AsRef<Path>) -> Result<Recovered> {
    let dir = dir.as_ref();
    let config = wal::read_manifest(dir)?;
    let records = wal::read_all(dir)?;

    let snapshot = snapshot::latest_usable(dir)?;
    let (mut state, from_snapshot) = match snapshot {
        // A snapshot ahead of the log means the log was rolled back or replaced underneath us.
        // Trusting it would mean serving state for commands that no longer exist on disk, so it
        // is discarded rather than reconciled.
        Some(s) if records.last().is_some_and(|r| r.seq < s.seq) => {
            tracing::warn!(
                snapshot = %s.seq,
                log_end = %records.last().unwrap().seq,
                "snapshot is ahead of the log; replaying from the beginning instead"
            );
            (AuctionState::new(config), None)
        }
        Some(s) => {
            let seq = s.seq;
            (s.state, Some(seq))
        }
        None => (AuctionState::new(config), None),
    };

    let resume_after = from_snapshot;
    let mut replayed = 0;
    let mut last = resume_after;

    for record in &records {
        if resume_after.is_some_and(|s| record.seq <= s) {
            continue;
        }
        if let Some(prev) = last {
            if record.seq != prev.next() {
                return Err(WalError::SequenceGap {
                    from: prev,
                    found: record.seq,
                });
            }
        }
        apply(&mut state, record);
        last = Some(record.seq);
        replayed += 1;
    }

    let next_seq = match last {
        Some(seq) => seq.next(),
        None => Seq::START,
    };
    tracing::info!(
        replayed,
        ?from_snapshot,
        %next_seq,
        status = ?state.status(),
        "recovered auction from disk"
    );

    Ok(Recovered {
        state,
        next_seq,
        replayed,
        from_snapshot,
    })
}

/// Apply one logged record. Events are discarded on replay: they were already delivered the
/// first time, and re-emitting them would tell participants their bids filled twice.
fn apply(state: &mut AuctionState, record: &LogRecord) {
    let _ = state.apply(record.seq, record.ts, record.cmd);
}

/// Feed a single record into a state machine that is tracking the primary.
///
/// The hot standby's entire job. Kept as a named function rather than inlined so that the
/// standby and recovery are visibly the same operation.
pub fn follow(state: &mut AuctionState, record: &LogRecord) {
    apply(state, record);
}
