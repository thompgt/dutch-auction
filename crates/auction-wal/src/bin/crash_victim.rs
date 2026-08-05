//! A writer that expects to be killed. Driven by `tests/crash.rs`.
//!
//! Durability that has only been tested by dropping a struct has not been tested. The failure
//! this exists to catch is the one where a record is acknowledged and then lost — and the only
//! honest way to produce that is to acknowledge records in a real process, on a real
//! filesystem, and then destroy the process without warning.
//!
//! The contract with the parent is one line per acknowledgement, flushed immediately:
//!
//! ```text
//! acked <seq>
//! ```
//!
//! Flushed *after* the durability wait returns and never before, so every line the parent
//! manages to read is a promise the system made. Recovery must honour all of them.
//!
//! Usage: `wal-crash-victim <dir> <supply>`

use std::io::Write;

use auction_core::{AuctionConfig, AuctionState, BidKind, Command, PriceSchedule};
use auction_proto::{AuctionId, IdempotencyKey, Nanos, ParticipantId, Price, Qty};
use auction_wal::{recover, wal::Wal, CommitThread, LogRecord, WalOptions};
use uuid::Uuid;

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args.next().expect("usage: wal-crash-victim <dir> <supply>");
    let supply: u64 = args
        .next()
        .expect("usage: wal-crash-victim <dir> <supply>")
        .parse()
        .expect("supply must be a number");

    let config = AuctionConfig::new(
        AuctionId(Uuid::from_u128(0)),
        Qty(supply),
        PriceSchedule::stepped(Price(1000), Price(100), Nanos::from_secs(1), 10),
    );

    let options = WalOptions {
        segment_bytes: 256 * 1024,
        ..WalOptions::default()
    };
    // Open first: it writes the manifest, without which there is nothing to recover from.
    let wal = Wal::open(&dir, &config, options).expect("opening the wal");

    // Resume rather than start fresh: the parent restarts the victim repeatedly, and a writer
    // that could not pick up where the last one died would be testing a much easier problem.
    let recovered = recover(&dir).unwrap_or_else(|e| panic!("recovery failed: {e}"));
    let mut state: AuctionState = recovered.state;
    let mut seq = recovered.next_seq;

    let commit = CommitThread::spawn(wal);
    let committer = commit.committer();

    let stdout = std::io::stdout();
    let mut out = stdout.lock();

    for i in 0u64.. {
        let ts = Nanos::from_millis(state.now().as_millis() + 1 + i % 7);
        let cmd = next_command(&state, seq.0);
        let record = LogRecord::new(seq, ts, cmd);

        // Durable first. Everything below this line is only allowed to happen because the
        // record is on disk (invariant I6).
        committer.commit(record).expect("commit");
        state.apply(record.seq, record.ts, record.cmd);

        writeln!(out, "acked {}", seq.0).expect("write");
        out.flush().expect("flush");

        seq = seq.next();
    }
}

/// A stream of commands that keeps the auction interesting for as long as the parent lets it
/// live: mostly bids, with ticks mixed in so the clock advances and resting bids fire.
fn next_command(state: &AuctionState, n: u64) -> Command {
    let who = ParticipantId(Uuid::from_u128((n % 4) as u128));
    // The first few commands set the auction up; after that it is all traffic.
    if n == 0 {
        return Command::Open;
    }
    if n <= 4 {
        return Command::SetCollateral {
            participant: ParticipantId(Uuid::from_u128((n - 1) as u128)),
            limit: 100_000_000,
        };
    }
    match n % 5 {
        0 => Command::Tick,
        1..=3 => Command::SubmitBid {
            participant: who,
            key: IdempotencyKey(Uuid::from_u128(1 << 96 | n as u128)),
            qty: Qty(1 + n % 3),
            kind: BidKind::Take {
                expected_price: state.price(),
            },
        },
        _ => Command::SubmitBid {
            participant: who,
            key: IdempotencyKey(Uuid::from_u128(1 << 96 | n as u128)),
            qty: Qty(1 + n % 2),
            kind: BidKind::Resting {
                limit: Price(state.price().0 - 20),
            },
        },
    }
}
