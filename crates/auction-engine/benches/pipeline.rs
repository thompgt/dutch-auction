//! What a bid costs end to end.
//!
//! Everything before this measured a stage: `auction-core` measured matching, `auction-wal`
//! measured the flush. This measures the thing a participant actually experiences — submit to
//! durable acknowledgement, through the real ingress queue, the real state machine, the real
//! `fsync`, and the real ack thread.
//!
//! The number to watch is not the single-bid figure. It is the *ratio* between the two
//! benchmarks, because that ratio is the entire claim the engine's design rests on:
//!
//! - `pipeline/serial` submits one bid, waits for its ack, then submits the next. Every bid pays
//!   a full flush, so this is roughly the WAL's batch-of-one cost and it is the floor for a
//!   participant bidding alone on a quiet auction.
//! - `pipeline/herd` submits N bids and waits for the last one. If the engine blocked on the
//!   disk this would cost N flushes; if the pipeline works it costs a handful, because the flush
//!   for one command overlaps the matching of the next.
//!
//! A herd that costs the same per bid as a lone bidder means the pipelining is not working, and
//! no amount of reading the code will tell you that as quickly as this will.

use std::time::Duration;

use auction_core::{AuctionConfig, BidKind, Command, PriceSchedule};
use auction_engine::{Auction, AuctionHandle, EngineOptions};
use auction_proto::{AuctionId, IdempotencyKey, Nanos, ParticipantId, Price, Qty};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};

/// Effectively unlimited supply and a slow clock: the auction must not clear halfway through a
/// measurement, or the benchmark would be timing rejections.
fn config() -> AuctionConfig {
    AuctionConfig::new(
        AuctionId(uuid::Uuid::from_u128(11)),
        Qty(u64::MAX / 2),
        PriceSchedule::stepped(Price(1_000_000), Price(1), Nanos::from_secs(3600), 1),
    )
}

fn options() -> EngineOptions {
    EngineOptions {
        // Large enough that the benchmark measures the engine rather than the shedding policy.
        queue_depth: 1 << 20,
        // Snapshots are pure overhead here and their cost is measured by their own path.
        snapshot_every: u64::MAX,
        ..EngineOptions::default()
    }
}

fn bid(participant: u128, n: u64) -> Command {
    Command::SubmitBid {
        participant: ParticipantId(uuid::Uuid::from_u128(participant)),
        key: IdempotencyKey(uuid::Uuid::from_u128(1 << 100 | u128::from(n))),
        qty: Qty(1),
        kind: BidKind::Take {
            expected_price: Price(1_000_000),
        },
    }
}

/// A live auction with one very well funded bidder.
fn live(dir: &std::path::Path) -> (Auction, AuctionHandle) {
    let auction = Auction::open(dir, config(), options()).expect("opening the auction");
    let handle = auction.handle();
    for cmd in [
        Command::Open,
        Command::SetCollateral {
            participant: ParticipantId(uuid::Uuid::from_u128(1)),
            limit: i64::MAX / 2,
        },
    ] {
        handle
            .submit(cmd)
            .expect("queued")
            .blocking_recv()
            .expect("acked");
    }
    (auction, handle)
}

/// One bid at a time: the lone bidder's floor, and the cost the pipeline has to beat.
fn bench_serial(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let (auction, handle) = live(dir.path());
    let mut n = 0u64;

    let mut group = c.benchmark_group("pipeline");
    group.throughput(Throughput::Elements(1));
    group.bench_function("serial", |b| {
        b.iter(|| {
            n += 1;
            handle
                .submit(bid(1, n))
                .expect("queued")
                .blocking_recv()
                .expect("acked");
        })
    });
    group.finish();
    drop(auction);
}

/// A burst, timed until the last acknowledgement.
///
/// This is the shape of the real load: everybody bids at once when the clock crosses a level, and
/// what matters is when the *last* of them is told yes, not the average.
fn bench_herd(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline");
    for size in [16usize, 256, 5_000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("herd", size), &size, |b, &size| {
            let dir = tempfile::tempdir().unwrap();
            let (auction, handle) = live(dir.path());
            let mut n = 0u64;

            b.iter_batched(
                || {
                    let first = n;
                    n += size as u64;
                    first
                },
                |first| {
                    // Fire everything without waiting, exactly as a herd of clients would.
                    let waiters: Vec<_> = (0..size as u64)
                        .map(|i| handle.submit(bid(1, first + i)).expect("queued"))
                        .collect();
                    // Then wait for every one of them. Acks arrive in order, so the last to
                    // resolve is the last submitted.
                    for w in waiters {
                        w.blocking_recv().expect("acked");
                    }
                },
                BatchSize::SmallInput,
            );
            drop(auction);
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    // Every iteration here includes at least one real `fsync`, so the measurement is dominated
    // by storage and needs the same patience the WAL benchmarks do.
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .noise_threshold(0.10);
    targets = bench_serial, bench_herd
}
criterion_main!(benches);
