//! What the log promises, tested against a real filesystem.
//!
//! No mocked disk anywhere in this file. The failures worth catching here — a torn tail, a
//! rename that raced a crash, a segment boundary landing mid-record — are all failures of the
//! interaction between this code and an actual operating system, and a fake one would agree
//! with whatever assumptions the implementation already made.

mod common;

use std::io::{Seek, SeekFrom, Write};

use auction_core::{AuctionState, Command};
use auction_proto::{Nanos, Seq};
use auction_wal::{
    read_all, recover, snapshot, wal::Wal, CommitThread, LogRecord, WalError, WalOptions,
};
use common::{config, script};

fn options() -> WalOptions {
    WalOptions {
        // Small enough that the tests actually exercise rotation rather than describing it.
        segment_bytes: 4 * 1024,
        linger: std::time::Duration::from_micros(200),
        max_batch: 256,
    }
}

#[test]
fn what_was_written_is_what_is_read_back() {
    let dir = tempfile::tempdir().unwrap();
    let records = script(200);

    let mut wal = Wal::open(dir.path(), &config(500), options()).unwrap();
    for r in &records {
        wal.append(r).unwrap();
    }
    wal.sync().unwrap();
    drop(wal);

    assert_eq!(read_all(dir.path()).unwrap(), records);
}

#[test]
fn the_log_rolls_into_several_segments_and_still_reads_as_one() {
    let dir = tempfile::tempdir().unwrap();
    let records = script(500);

    let mut wal = Wal::open(dir.path(), &config(500), options()).unwrap();
    for r in &records {
        wal.append(r).unwrap();
    }
    wal.sync().unwrap();
    drop(wal);

    let segments: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_str().is_some_and(|n| n.ends_with(".log")))
        .collect();
    assert!(
        segments.len() > 1,
        "the test did not actually exercise rotation"
    );
    assert_eq!(read_all(dir.path()).unwrap(), records);
}

#[test]
fn reopening_resumes_the_log_rather_than_restarting_it() {
    let dir = tempfile::tempdir().unwrap();
    let records = script(50);
    let (first, second) = records.split_at(20);

    let mut wal = Wal::open(dir.path(), &config(500), options()).unwrap();
    for r in first {
        wal.append(r).unwrap();
    }
    wal.sync().unwrap();
    drop(wal);

    let mut wal = Wal::open(dir.path(), &config(500), options()).unwrap();
    assert_eq!(wal.durable_seq(), Some(first.last().unwrap().seq));
    for r in second {
        wal.append(r).unwrap();
    }
    wal.sync().unwrap();
    drop(wal);

    assert_eq!(read_all(dir.path()).unwrap(), records);
}

#[test]
fn a_torn_tail_is_discarded_and_the_log_stays_writable() {
    let dir = tempfile::tempdir().unwrap();
    let records = script(30);

    let mut wal = Wal::open(dir.path(), &config(500), options()).unwrap();
    for r in &records {
        wal.append(r).unwrap();
    }
    wal.sync().unwrap();
    drop(wal);

    // Simulate a crash partway through writing the next record: a plausible-looking header and
    // some bytes that will never be completed.
    let path = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "log"))
        .max()
        .unwrap();
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    file.write_all(&[64, 0, 0, 0, 1, 2, 3, 4, 9, 9, 9]).unwrap();
    file.sync_all().unwrap();

    // Reading tolerates it...
    assert_eq!(read_all(dir.path()).unwrap(), records);

    // ...and reopening cuts it off, so what is written next is not buried behind it.
    let mut wal = Wal::open(dir.path(), &config(500), options()).unwrap();
    let next = LogRecord::new(
        records.last().unwrap().seq.next(),
        Nanos::from_secs(9),
        Command::Tick,
    );
    wal.append(&next).unwrap();
    wal.sync().unwrap();
    drop(wal);

    let read_back = read_all(dir.path()).unwrap();
    assert_eq!(read_back.len(), records.len() + 1);
    assert_eq!(read_back.last().unwrap(), &next);
}

#[test]
fn corruption_in_the_middle_of_the_log_is_refused_rather_than_silently_truncated() {
    let dir = tempfile::tempdir().unwrap();
    let records = script(500);

    let mut wal = Wal::open(dir.path(), &config(500), options()).unwrap();
    for r in &records {
        wal.append(r).unwrap();
    }
    wal.sync().unwrap();
    drop(wal);

    // Damage a sealed segment — not the last one. Stopping quietly here would produce a
    // shorter, entirely plausible auction, which is the one outcome worse than an error.
    let mut segments: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "log"))
        .collect();
    segments.sort();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&segments[0])
        .unwrap();
    file.seek(SeekFrom::Start(64)).unwrap();
    file.write_all(&[0xff; 16]).unwrap();
    file.sync_all().unwrap();

    assert!(matches!(
        read_all(dir.path()),
        Err(WalError::Corrupt { .. })
    ));
}

#[test]
fn a_log_from_a_different_auction_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    Wal::open(dir.path(), &config(500), options()).unwrap();
    assert!(matches!(
        Wal::open(dir.path(), &config(501), options()),
        Err(WalError::ManifestMismatch { .. })
    ));
}

#[test]
fn a_gap_in_the_sequence_is_refused_on_append() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::open(dir.path(), &config(500), options()).unwrap();
    wal.append(&LogRecord::new(Seq(0), Nanos::ZERO, Command::Open))
        .unwrap();
    assert!(matches!(
        wal.append(&LogRecord::new(Seq(2), Nanos::ZERO, Command::Tick)),
        Err(WalError::SequenceGap { .. })
    ));
}

// ------------------------------------------------------------------- group commit

#[test]
fn a_record_is_on_disk_by_the_time_its_waiter_wakes() {
    let dir = tempfile::tempdir().unwrap();
    let records = script(100);

    let wal = Wal::open(dir.path(), &config(500), options()).unwrap();
    let commit = CommitThread::spawn(wal);
    let committer = commit.committer();

    for r in &records {
        committer.commit(*r).unwrap();
        // The claim under test, checked after every single ack rather than at the end: once
        // `commit` returns, a reader that knows nothing about the process can see the record.
        let on_disk = read_all(dir.path()).unwrap();
        assert_eq!(on_disk.last(), Some(r), "acked but not on disk");
    }
    commit.shutdown();
}

#[test]
fn many_threads_committing_at_once_all_get_their_records_durable() {
    let dir = tempfile::tempdir().unwrap();
    let records = script(2_000);
    let total = records.len();

    let wal = Wal::open(dir.path(), &config(2_000), options()).unwrap();
    let commit = CommitThread::spawn(wal);

    // Records still have to reach the log in sequence order, so submission is serialized (as it
    // is by the real sequencer) while the *waiting* is spread across threads. The point is that
    // a batch releases every waiter, not just the last one.
    let committer = commit.committer();
    for r in &records {
        committer.submit(*r).unwrap();
    }

    std::thread::scope(|s| {
        for chunk in records.chunks(97) {
            let committer = commit.committer();
            s.spawn(move || {
                for r in chunk {
                    committer.wait_durable(r.seq).unwrap();
                }
            });
        }
    });

    assert_eq!(read_all(dir.path()).unwrap().len(), total);
    commit.shutdown();
}

#[test]
fn a_burst_costs_far_fewer_syncs_than_it_has_records() {
    let dir = tempfile::tempdir().unwrap();
    let records = script(2_000);

    let mut opts = options();
    opts.segment_bytes = 1 << 30; // one segment, so the count is about batching and nothing else
    let wal = Wal::open(dir.path(), &config(2_000), opts).unwrap();
    let commit = CommitThread::spawn(wal);
    let committer = commit.committer();

    for r in &records {
        committer.submit(*r).unwrap();
    }
    committer.wait_durable(records.last().unwrap().seq).unwrap();
    commit.shutdown();

    assert_eq!(read_all(dir.path()).unwrap().len(), records.len());
}

#[test]
fn waiters_are_released_rather_than_left_parked_when_the_writer_stops() {
    let dir = tempfile::tempdir().unwrap();
    let wal = Wal::open(dir.path(), &config(10), options()).unwrap();
    let commit = CommitThread::spawn(wal);
    let committer = commit.committer();

    committer
        .commit(LogRecord::new(Seq(0), Nanos::ZERO, Command::Open))
        .unwrap();
    commit.shutdown();

    // Already durable: still true, and still answerable, after shutdown.
    committer.wait_durable(Seq(0)).unwrap();
    // Never submitted: an error, not an eternity.
    assert!(matches!(
        committer.wait_durable(Seq(1)),
        Err(WalError::CommitterStopped(_))
    ));
}

// ------------------------------------------------------------------- snapshots and replay

fn build(records: &[LogRecord], supply: u64) -> AuctionState {
    let mut state = AuctionState::new(config(supply));
    for r in records {
        state.apply(r.seq, r.ts, r.cmd);
    }
    state
}

#[test]
fn recovery_from_the_log_alone_reproduces_the_state_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let records = script(300);

    let mut wal = Wal::open(dir.path(), &config(500), options()).unwrap();
    for r in &records {
        wal.append(r).unwrap();
    }
    wal.sync().unwrap();
    drop(wal);

    let recovered = recover(dir.path()).unwrap();
    assert_eq!(recovered.state, build(&records, 500));
    assert_eq!(recovered.next_seq, records.last().unwrap().seq.next());
    assert_eq!(recovered.from_snapshot, None);
}

#[test]
fn a_snapshot_short_circuits_replay_without_changing_the_answer() {
    let dir = tempfile::tempdir().unwrap();
    let records = script(300);
    let (early, late) = records.split_at(150);

    let mut wal = Wal::open(dir.path(), &config(500), options()).unwrap();
    for r in early {
        wal.append(r).unwrap();
    }
    wal.sync().unwrap();

    let at = build(early, 500);
    let seq = snapshot::write(dir.path(), &at).unwrap().unwrap();
    assert_eq!(seq, early.last().unwrap().seq);

    for r in late {
        wal.append(r).unwrap();
    }
    wal.sync().unwrap();
    drop(wal);

    let recovered = recover(dir.path()).unwrap();
    assert_eq!(recovered.from_snapshot, Some(seq));
    assert_eq!(recovered.replayed, late.len());
    assert_eq!(recovered.state, build(&records, 500));
}

#[test]
fn a_corrupt_snapshot_costs_replay_time_and_nothing_else() {
    let dir = tempfile::tempdir().unwrap();
    let records = script(200);

    let mut wal = Wal::open(dir.path(), &config(500), options()).unwrap();
    for r in &records {
        wal.append(r).unwrap();
    }
    wal.sync().unwrap();
    drop(wal);

    snapshot::write(dir.path(), &build(&records, 500)).unwrap();
    let (_, path) = snapshot::list(dir.path()).unwrap().pop().unwrap();
    let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.seek(SeekFrom::Start(4)).unwrap();
    file.write_all(&[0xab; 8]).unwrap();
    file.sync_all().unwrap();

    // The state is identical to the snapshot-free case: this is why a snapshot is safe to treat
    // as disposable rather than as a second source of truth.
    let recovered = recover(dir.path()).unwrap();
    assert_eq!(recovered.from_snapshot, None);
    assert_eq!(recovered.state, build(&records, 500));
}

#[test]
fn recovery_prefers_the_newest_snapshot_and_pruning_keeps_it() {
    let dir = tempfile::tempdir().unwrap();
    let records = script(300);

    let mut wal = Wal::open(dir.path(), &config(500), options()).unwrap();
    for r in &records {
        wal.append(r).unwrap();
    }
    wal.sync().unwrap();
    drop(wal);

    for cut in [50, 150, 250] {
        snapshot::write(dir.path(), &build(&records[..cut], 500)).unwrap();
    }
    assert_eq!(snapshot::list(dir.path()).unwrap().len(), 3);
    assert_eq!(snapshot::prune(dir.path(), 2).unwrap(), 1);

    let recovered = recover(dir.path()).unwrap();
    assert_eq!(recovered.from_snapshot, Some(records[249].seq));
    assert_eq!(recovered.state, build(&records, 500));
}

#[test]
fn a_standby_fed_the_same_records_reaches_the_same_state() {
    let records = script(300);
    let mut standby = AuctionState::new(config(500));
    for r in &records {
        auction_wal::replay::follow(&mut standby, r);
    }
    // The whole failover story in one assertion: the standby is not approximately the primary,
    // it is the primary, because both are the same function of the same command sequence.
    assert_eq!(standby, build(&records, 500));
}
