//! Invariant I6, tested by killing a real process.
//!
//! Every other test in this crate exercises the code's idea of a crash. This one takes an
//! actual operating-system process that has acknowledged actual bids, destroys it without
//! warning, and checks that recovery honours every acknowledgement it managed to make. The
//! distinction matters because the interesting failure — a record acked before its bytes
//! reached the platter — is invisible to any test that only drops a struct.
//!
//! `wal-crash-victim` prints one line per acknowledgement, flushed after the durability wait
//! returns. Those lines are promises. Recovery must be able to keep all of them.

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};

use auction_proto::Seq;
use auction_wal::{read_all, recover};

/// Kill the victim after it has acked `after` records, then check what survived.
///
/// Returns how many acknowledgements the parent observed, so the test can insist the run
/// actually did some work rather than passing because nothing happened.
fn crash_after(dir: &std::path::Path, after: usize) -> usize {
    let mut child = Command::new(env!("CARGO_BIN_EXE_wal-crash-victim"))
        .arg(dir)
        .arg("100000")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawning the crash victim");

    let stdout = child.stdout.take().expect("piped stdout");
    let mut reader = BufReader::new(stdout);
    let mut acked: Vec<u64> = Vec::new();
    let mut line = String::new();

    while acked.len() < after {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {
                let seq = line
                    .trim()
                    .strip_prefix("acked ")
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or_else(|| panic!("unexpected output from the victim: {line:?}"));
                acked.push(seq);
            }
            Err(e) => panic!("reading the victim's output: {e}"),
        }
    }

    // No cleanup, no flush, no Drop: on Windows this is TerminateProcess and on Unix SIGKILL.
    // Anything the victim was in the middle of writing is left exactly as the crash left it.
    child.kill().expect("killing the crash victim");
    let _ = child.wait();

    // The log must contain every sequence the victim claimed. This is the whole invariant: an
    // acknowledged bid that recovery cannot reproduce is a bid the venue took money for and
    // then forgot.
    let recovered = recover(dir).unwrap_or_else(|e| panic!("recovery failed after a crash: {e}"));
    for seq in &acked {
        assert!(
            recovered.next_seq > Seq(*seq),
            "acked {seq} but recovery resumed at {}: an acknowledged command was lost",
            recovered.next_seq
        );
    }

    // Recovery must also be self-consistent: gapless log, and a state whose position matches.
    let on_disk = read_all(dir).expect("log unreadable after a crash");
    for (i, record) in on_disk.iter().enumerate() {
        assert_eq!(
            record.seq,
            Seq(i as u64),
            "log is not gapless after a crash"
        );
    }
    if let Some(last) = on_disk.last() {
        assert_eq!(recovered.next_seq, last.seq.next());
        assert_eq!(recovered.state.last_seq(), Some(last.seq));
    }
    let state = &recovered.state;
    assert!(
        state.total_filled().0 <= state.config().total_supply.0,
        "I1 violated after recovery: a crash oversold the auction"
    );
    assert_eq!(
        state.total_filled().0 + state.supply_remaining().0,
        state.config().total_supply.0,
        "I1 violated after recovery: the books do not balance"
    );

    acked.len()
}

#[test]
fn an_acknowledged_bid_survives_a_kill() {
    let dir = tempfile::tempdir().unwrap();
    let acked = crash_after(dir.path(), 200);
    assert!(acked >= 200, "the victim only acked {acked} before dying");
}

#[test]
fn repeated_crashes_never_lose_an_acknowledgement() {
    let dir = tempfile::tempdir().unwrap();
    let mut total = 0;

    // Vary the cut point so the kill lands in different places relative to a batch boundary:
    // mid-linger, mid-write, and just after a sync are all different failures, and a fixed
    // count would only ever find one of them.
    for round in 1..=8 {
        let before = read_all(dir.path()).unwrap().len();
        let acked = crash_after(dir.path(), 30 + round * 13);
        let after = read_all(dir.path()).unwrap().len();

        assert!(
            after >= before + acked,
            "round {round}: log grew by {} but {acked} records were acked",
            after - before
        );
        total += acked;
    }
    assert!(total > 500, "the harness did too little work to prove much");
}

#[test]
fn a_crash_leaves_the_log_writable_by_the_next_process() {
    let dir = tempfile::tempdir().unwrap();
    crash_after(dir.path(), 100);
    let first = read_all(dir.path()).unwrap();

    // The restart is the real test of torn-tail handling: if the previous process died partway
    // through a record and the next one appended after the wreckage, everything written from
    // here on would be unreadable — and it would stay silently unreadable until the next crash.
    crash_after(dir.path(), 100);
    let second = read_all(dir.path()).unwrap();

    assert!(second.len() > first.len(), "the restart wrote nothing");
    assert_eq!(
        &second[..first.len()],
        &first[..],
        "the restart rewrote history"
    );
}
