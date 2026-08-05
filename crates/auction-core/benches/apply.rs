//! Benchmarks for the engine's hot path.
//!
//! `docs/slo.md` gives "sequencer enqueue + engine apply" **0.1 ms** of a 10 ms p99 budget —
//! the smallest line in the table, because everything else in the budget is I/O and this is the
//! only part that is pure computation. If `apply` cannot stay far inside 100 µs, no amount of
//! network or storage tuning downstream will save the tail.
//!
//! The interesting case is not a single bid. It is the **thundering herd**: a batch window that
//! accumulated thousands of bids as the clock crossed a round price, all matched in one pass
//! when the window closes. That flush is the largest unit of work the engine ever does
//! synchronously, and it is what the p99.9 is made of.
//!
//! Run with `cargo bench -p auction-core`.

use auction_core::{AuctionConfig, AuctionState, BidKind, Command, PriceSchedule};
use auction_proto::{AuctionId, IdempotencyKey, Nanos, ParticipantId, Price, Qty, Seq};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use uuid::Uuid;

/// A wide window so a whole herd lands in it, and supply deliberately short of demand so the
/// flush exercises the pro-rata path rather than the trivial everyone-fills path.
fn config(supply: u64, window: Nanos) -> AuctionConfig {
    AuctionConfig::new(
        AuctionId(Uuid::from_u128(0)),
        Qty(supply),
        PriceSchedule::stepped(Price(1_000), Price(100), Nanos::from_secs(1), 10),
    )
    .with_batch_window(window)
}

struct Driver {
    state: AuctionState,
    seq: Seq,
}

impl Driver {
    fn new(config: AuctionConfig) -> Self {
        let mut d = Self {
            state: AuctionState::new(config),
            seq: Seq::START,
        };
        d.apply(Nanos::ZERO, Command::Open);
        d
    }

    fn apply(&mut self, ts: Nanos, cmd: Command) {
        let seq = self.seq;
        self.seq = seq.next();
        self.state.apply(seq, ts, cmd);
    }

    fn fund(&mut self, who: u128) {
        self.apply(
            Nanos::ZERO,
            Command::SetCollateral {
                participant: ParticipantId(Uuid::from_u128(who)),
                limit: i64::MAX / 2,
            },
        );
    }

    /// Fill the current window with `n` market takes, all arriving inside it.
    fn stack_window(&mut self, n: u128, ts: Nanos) {
        for i in 0..n {
            self.fund(i);
            self.apply(
                ts,
                Command::SubmitBid {
                    participant: ParticipantId(Uuid::from_u128(i)),
                    key: IdempotencyKey(Uuid::from_u128(1 << 96 | i)),
                    qty: Qty(10),
                    kind: BidKind::Take {
                        expected_price: Price(1_000),
                    },
                },
            );
        }
    }

    fn stack_ladder(&mut self, n: u128, limit: Price) {
        for i in 0..n {
            self.fund(i);
            self.apply(
                Nanos::ZERO,
                Command::SubmitBid {
                    participant: ParticipantId(Uuid::from_u128(i)),
                    key: IdempotencyKey(Uuid::from_u128(1 << 96 | i)),
                    qty: Qty(10),
                    // Spread across price levels so the ladder is a real tree, not one bucket.
                    kind: BidKind::Resting {
                        limit: Price(limit.0 + (i as i64 % 40) * 10),
                    },
                },
            );
        }
    }
}

/// One bid, matched immediately (batching off). The per-bid cost the gateway pays.
fn bench_single_take(c: &mut Criterion) {
    c.bench_function("apply/take_immediate", |b| {
        b.iter_batched(
            || {
                let mut d = Driver::new(config(u64::MAX / 2, Nanos::ZERO));
                d.fund(1);
                d
            },
            |mut d| {
                d.apply(
                    Nanos::ZERO,
                    Command::SubmitBid {
                        participant: ParticipantId(Uuid::from_u128(1)),
                        key: IdempotencyKey(Uuid::from_u128(7)),
                        qty: Qty(1),
                        kind: BidKind::Take {
                            expected_price: Price(1_000),
                        },
                    },
                );
                d
            },
            BatchSize::SmallInput,
        );
    });
}

/// Admitting a bid into an open window — the work that happens on the hot path *before* the
/// window closes, and therefore the latency a participant actually waits on.
fn bench_window_admit(c: &mut Criterion) {
    c.bench_function("apply/queue_into_window", |b| {
        b.iter_batched(
            || {
                let mut d = Driver::new(config(u64::MAX / 2, Nanos::from_secs(1)));
                d.stack_window(1_000, Nanos(1_000));
                d.fund(9_999);
                d
            },
            |mut d| {
                d.apply(
                    Nanos(2_000),
                    Command::SubmitBid {
                        participant: ParticipantId(Uuid::from_u128(9_999)),
                        key: IdempotencyKey(Uuid::from_u128(1 << 100)),
                        qty: Qty(1),
                        kind: BidKind::Take {
                            expected_price: Price(1_000),
                        },
                    },
                );
                d
            },
            BatchSize::SmallInput,
        );
    });
}

/// The herd: closing a window that accumulated N bids against short supply, which sorts,
/// walks price levels, allocates pro-rata at the margin, clears, and reprices every fill.
fn bench_window_flush(c: &mut Criterion) {
    let mut group = c.benchmark_group("apply/flush_window");
    for n in [100u128, 1_000, 5_000] {
        group.bench_function(format!("{n}_bids"), |b| {
            b.iter_batched(
                || {
                    // Supply covers roughly half the herd, so the marginal level is contested.
                    let mut d = Driver::new(config(n as u64 * 5, Nanos::from_secs(1)));
                    d.stack_window(n, Nanos(1_000));
                    d
                },
                |mut d| {
                    d.apply(Nanos::from_millis(1_500), Command::Tick);
                    d
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

/// A clock tick that crosses a price level thousands of resting bids were waiting on — the
/// resting-bid equivalent of the herd, and the one participants cannot influence the timing of.
fn bench_ladder_trigger(c: &mut Criterion) {
    let mut group = c.benchmark_group("apply/trigger_ladder");
    for n in [100u128, 1_000, 5_000] {
        group.bench_function(format!("{n}_resting"), |b| {
            b.iter_batched(
                || {
                    let mut d = Driver::new(config(n as u64 * 5, Nanos::ZERO));
                    d.stack_ladder(n, Price(500));
                    d
                },
                |mut d| {
                    // 60s puts the clock at 400, below every limit on the ladder.
                    d.apply(Nanos::from_secs(60), Command::Tick);
                    d
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_single_take,
    bench_window_admit,
    bench_window_flush,
    bench_ladder_trigger
);
criterion_main!(benches);
