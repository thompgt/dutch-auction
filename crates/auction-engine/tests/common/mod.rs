//! Shared fixtures for the engine tests.

use auction_core::{AuctionConfig, BidKind, Command, PriceSchedule};
use auction_engine::{Ack, Auction, AuctionHandle, EngineOptions};
use auction_proto::{AuctionId, IdempotencyKey, Nanos, ParticipantId, Price, Qty};

/// A ten-second auction: 1000 down to 100, dropping 100 a second.
///
/// Short enough that a test can watch it clear at the floor, slow enough that a test can bid
/// several times inside one price step without racing the schedule.
pub fn config(supply: u64) -> AuctionConfig {
    AuctionConfig::new(
        AuctionId(uuid::Uuid::from_u128(7)),
        Qty(supply),
        PriceSchedule::stepped(Price(1000), Price(100), Nanos::from_secs(1), 100),
    )
}

/// Options that keep tests fast without changing what is being tested.
pub fn options() -> EngineOptions {
    EngineOptions {
        // Small enough that a test can fill it on purpose to exercise shedding.
        queue_depth: 64,
        snapshot_every: 25,
        ..EngineOptions::default()
    }
}

/// Open an auction and start its clock, returning it with a handle.
pub fn live(dir: &std::path::Path, supply: u64) -> (Auction, AuctionHandle) {
    let auction = Auction::open(dir, config(supply), options()).expect("opening the auction");
    let handle = auction.handle();
    submit(&handle, Command::Open);
    (auction, handle)
}

/// Submit a command and block until it is durable.
pub fn submit(handle: &AuctionHandle, cmd: Command) -> Ack {
    handle
        .submit(cmd)
        .expect("the engine accepted the command")
        .blocking_recv()
        .expect("the command was acknowledged")
}

pub fn participant(n: u128) -> ParticipantId {
    ParticipantId(uuid::Uuid::from_u128(n))
}

pub fn key(n: u128) -> IdempotencyKey {
    IdempotencyKey(uuid::Uuid::from_u128(1 << 100 | n))
}

/// A market take at the price the client believes is in effect.
pub fn take(who: u128, k: u128, qty: u64, at: i64) -> Command {
    Command::SubmitBid {
        participant: participant(who),
        key: key(k),
        qty: Qty(qty),
        kind: BidKind::Take {
            expected_price: Price(at),
        },
    }
}

/// Enough collateral that the credit check never gets in the way of what a test is about.
pub fn fund(handle: &AuctionHandle, who: u128) {
    submit(
        handle,
        Command::SetCollateral {
            participant: participant(who),
            limit: i64::MAX / 2,
        },
    );
}
