//! Shared fixtures for the WAL tests.

#![allow(dead_code)] // each test file uses a different subset

use auction_core::{AuctionConfig, BidKind, Command, PriceSchedule};
use auction_proto::{AuctionId, IdempotencyKey, Nanos, ParticipantId, Price, Qty, Seq};
use auction_wal::LogRecord;
use uuid::Uuid;

pub fn participant(n: u128) -> ParticipantId {
    ParticipantId(Uuid::from_u128(n))
}

pub fn key(n: u128) -> IdempotencyKey {
    IdempotencyKey(Uuid::from_u128(1 << 96 | n))
}

/// The same reference auction the `auction-core` tests use: 1000 down to 100, ten per second.
pub fn config(supply: u64) -> AuctionConfig {
    AuctionConfig::new(
        AuctionId(Uuid::from_u128(0)),
        Qty(supply),
        PriceSchedule::stepped(Price(1000), Price(100), Nanos::from_secs(1), 10),
    )
}

/// A plausible command stream: open, fund everyone, then a mix of takes and resting bids with
/// the clock advancing between them.
pub fn script(commands: usize) -> Vec<LogRecord> {
    let mut records = Vec::with_capacity(commands + 5);
    let mut seq = Seq::START;
    let push = |records: &mut Vec<LogRecord>, seq: &mut Seq, ts: Nanos, cmd: Command| {
        records.push(LogRecord::new(*seq, ts, cmd));
        *seq = seq.next();
    };

    push(&mut records, &mut seq, Nanos::ZERO, Command::Open);
    for who in 0..4u128 {
        push(
            &mut records,
            &mut seq,
            Nanos::ZERO,
            Command::SetCollateral {
                participant: participant(who),
                limit: 1_000_000,
            },
        );
    }
    for i in 0..commands as u64 {
        let ts = Nanos::from_millis(i * 37);
        let who = (i % 4) as u128;
        let cmd = if i % 3 == 0 {
            Command::Tick
        } else if i % 3 == 1 {
            Command::SubmitBid {
                participant: participant(who),
                key: key(i as u128),
                qty: Qty(1 + i % 5),
                kind: BidKind::Take {
                    expected_price: price_at(ts),
                },
            }
        } else {
            Command::SubmitBid {
                participant: participant(who),
                key: key(i as u128),
                qty: Qty(1 + i % 3),
                kind: BidKind::Resting {
                    limit: Price(price_at(ts).0 - 50),
                },
            }
        };
        push(&mut records, &mut seq, ts, cmd);
    }
    records
}

fn price_at(ts: Nanos) -> Price {
    config(1).price_at(ts)
}
