//! What durability costs.
//!
//! The number that matters is not throughput, it is the per-bid wait: how long a participant
//! sits between "the engine accepted this" and "we can tell them so" (invariant I6). The
//! budget in `docs/slo.md` gives that 4 ms of a 10 ms server-side p99, and that budget is a
//! claim about hardware — so it gets measured on hardware rather than asserted.
//!
//! Three things are measured, and the gap between the first two is the entire justification for
//! the group-commit machinery:
//!
//! - `fsync/per_record`: one record, one flush. What a naive implementation pays per bid.
//! - `group_commit/*`: a batch submitted at once, timed until the last record is durable.
//!   Divide by the batch size for the amortized cost.
//! - `append/no_sync`: the encode-and-write path alone, which shows how much of the cost is the
//!   flush and how much is ours.

use std::time::Duration;

use auction_core::{AuctionConfig, BidKind, Command, PriceSchedule};
use auction_proto::{AuctionId, IdempotencyKey, Nanos, ParticipantId, Price, Qty, Seq};
use auction_wal::{wal::Wal, CommitThread, LogRecord, WalOptions};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use uuid::Uuid;

fn config() -> AuctionConfig {
    AuctionConfig::new(
        AuctionId(Uuid::from_u128(0)),
        Qty(u64::MAX / 2),
        PriceSchedule::stepped(Price(1000), Price(100), Nanos::from_secs(1), 10),
    )
}

fn options() -> WalOptions {
    WalOptions {
        segment_bytes: 1 << 30,
        ..WalOptions::default()
    }
}

/// A representative record: a market take, which is what the herd is made of.
fn record(seq: u64) -> LogRecord {
    LogRecord::new(
        Seq(seq),
        Nanos::from_millis(seq),
        Command::SubmitBid {
            participant: ParticipantId(Uuid::from_u128((seq % 1000) as u128)),
            key: IdempotencyKey(Uuid::from_u128(1 << 96 | seq as u128)),
            qty: Qty(5),
            kind: BidKind::Take {
                expected_price: Price(1000),
            },
        },
    )
}

fn bench_fsync(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::open(dir.path(), &config(), options()).unwrap();
    let mut seq = 0u64;

    let mut group = c.benchmark_group("fsync");
    group.throughput(Throughput::Elements(1));
    group.bench_function("per_record", |b| {
        b.iter(|| {
            wal.append(&record(seq)).unwrap();
            wal.sync().unwrap();
            seq += 1;
        })
    });
    group.finish();
}

fn bench_append(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::open(dir.path(), &config(), options()).unwrap();
    let mut seq = 0u64;

    let mut group = c.benchmark_group("append");
    group.throughput(Throughput::Elements(1));
    group.bench_function("no_sync", |b| {
        b.iter(|| {
            wal.append(&record(seq)).unwrap();
            seq += 1;
        })
    });
    group.finish();
}

fn bench_group_commit(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_commit");
    for size in [1usize, 16, 256, 4096] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let dir = tempfile::tempdir().unwrap();
            let wal = Wal::open(dir.path(), &config(), options()).unwrap();
            let commit = CommitThread::spawn(wal);
            let committer = commit.committer();
            let mut seq = 0u64;

            b.iter_batched(
                || {
                    let batch: Vec<LogRecord> = (0..size).map(|i| record(seq + i as u64)).collect();
                    seq += size as u64;
                    batch
                },
                |batch| {
                    let last = batch.last().unwrap().seq;
                    for r in batch {
                        committer.submit(r).unwrap();
                    }
                    committer.wait_durable(last).unwrap();
                },
                BatchSize::SmallInput,
            );
            commit.shutdown();
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    // Durability benchmarks are dominated by the storage device, which is far noisier than
    // in-memory work: a longer measurement and a wider noise threshold keep criterion from
    // reporting drive scheduling as a regression.
    config = Criterion::default()
        .measurement_time(Duration::from_secs(8))
        .noise_threshold(0.10);
    targets = bench_append, bench_fsync, bench_group_commit
}
criterion_main!(benches);
