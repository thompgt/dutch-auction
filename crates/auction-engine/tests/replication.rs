//! The hot standby, tested against the one claim that matters.
//!
//! Failover is only safe if the standby's state is *identical* to the primary's, not merely
//! similar. `AuctionState` compares structurally, so these tests assert exactly that rather than
//! spot-checking a few fields and hoping the rest followed.

mod common;

use std::sync::Arc;

use auction_core::{AuctionState, Command, Event};
use auction_engine::{Auction, EngineOptions, LocalReplica, Replica, ReplicationMode};
use auction_proto::{Qty, Seq};

use common::*;

fn replicated_options() -> EngineOptions {
    EngineOptions {
        replication: ReplicationMode::Sync,
        ..options()
    }
}

/// The whole point: after a run, the standby is the primary.
#[test]
fn the_standby_reaches_exactly_the_state_the_primary_reached() {
    let dir = tempfile::tempdir().unwrap();
    let replica = LocalReplica::start(AuctionState::new(config(500)));

    let auction = Auction::open_replicated(
        dir.path(),
        config(500),
        replicated_options(),
        replica.clone() as Arc<dyn Replica>,
    )
    .unwrap();
    let handle = auction.handle();

    submit(&handle, Command::Open);
    for who in 1..25u128 {
        fund(&handle, who);
        submit(&handle, take(who, who, 7, 1000));
    }
    submit(&handle, Command::Tick);

    // Stop the primary first, then let the standby drain what it was already sent.
    auction.shutdown();
    replica.stop();

    let primary = auction_wal::recover(dir.path()).unwrap().state;
    assert_eq!(
        *replica.state(),
        primary,
        "the standby diverged from the primary"
    );
    assert!(primary.total_filled() > Qty(0), "the test filled nothing");
}

/// Sync replication means the standby has the command *before* the bidder is told anything.
///
/// This is the entire content of the "zero acked bids lost" promise in `docs/slo.md`: if the
/// primary died the instant after an ack, the standby would already be able to honour it.
#[test]
fn a_synchronously_acked_bid_is_already_on_the_standby() {
    let dir = tempfile::tempdir().unwrap();
    let replica = LocalReplica::start(AuctionState::new(config(500)));

    let auction = Auction::open_replicated(
        dir.path(),
        config(500),
        replicated_options(),
        replica.clone() as Arc<dyn Replica>,
    )
    .unwrap();
    let handle = auction.handle();
    submit(&handle, Command::Open);
    fund(&handle, 1);

    let ack = submit(&handle, take(1, 1, 10, 1000));

    // Checked with no sleep and no retry. If the ack can outrun the standby at all, it will do
    // it here, and the assertion is the only thing standing between that race and a lost bid.
    assert!(
        replica.confirmed() >= Some(ack.seq),
        "acked {} but the standby has only confirmed {:?}",
        ack.seq,
        replica.confirmed()
    );

    auction.shutdown();
    replica.stop();
}

/// A standby joining an auction that is already running catches up from where it starts.
///
/// This is the promotion path in reverse, and it is the reason `follow` takes a state rather
/// than creating one: a replacement standby starts from a snapshot, not from nothing.
#[test]
fn a_standby_can_join_from_a_snapshot_of_a_running_auction() {
    let dir = tempfile::tempdir().unwrap();

    // Phase one: run unreplicated for a while.
    let (auction, handle) = live(dir.path(), 500);
    fund(&handle, 1);
    submit(&handle, take(1, 1, 20, 1000));
    submit(&handle, Command::Tick);
    auction.shutdown();

    // Phase two: a standby joins from the state on disk, and the primary restarts replicated.
    let caught_up = auction_wal::recover(dir.path()).unwrap().state;
    let replica = LocalReplica::start(caught_up);

    let auction = Auction::open_replicated(
        dir.path(),
        config(500),
        replicated_options(),
        replica.clone() as Arc<dyn Replica>,
    )
    .unwrap();
    let handle = auction.handle();
    fund(&handle, 2);
    submit(&handle, take(2, 2, 30, 1000));
    submit(&handle, Command::Tick);

    auction.shutdown();
    replica.stop();

    let primary = auction_wal::recover(dir.path()).unwrap().state;
    assert_eq!(
        *replica.state(),
        primary,
        "a standby that joined mid-auction diverged"
    );
    // And it knows about both halves of the auction, not just the half it watched.
    assert!(replica.state().total_filled() >= Qty(50));
}

/// A standby that never answers must not stop the auction.
///
/// Availability over zero-RPO, deliberately: halting a live auction because a spare machine died
/// is a worse outcome for every participant than the risk being avoided.
#[test]
fn a_dead_standby_degrades_the_promise_but_not_the_auction() {
    /// Accepts everything and confirms nothing, which is what a partitioned standby looks like
    /// from the primary's side.
    struct DeadReplica;
    impl Replica for DeadReplica {
        fn send(&self, _record: &auction_wal::LogRecord) {}
        fn confirmed(&self) -> Option<Seq> {
            None
        }
        fn wait_confirmed(
            &self,
            seq: Seq,
            timeout: std::time::Duration,
        ) -> Result<(), auction_engine::ReplicaLost> {
            std::thread::sleep(timeout);
            Err(auction_engine::ReplicaLost { seq, timeout })
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let options = EngineOptions {
        replication: ReplicationMode::Sync,
        // Short enough to keep the test quick; the production default is seconds, so a garbage
        // collection pause on the secondary does not cost the zero-loss promise.
        replica_timeout: std::time::Duration::from_millis(100),
        ..options()
    };

    let auction =
        Auction::open_replicated(dir.path(), config(500), options, Arc::new(DeadReplica)).unwrap();
    let handle = auction.handle();

    submit(&handle, Command::Open);
    fund(&handle, 1);
    let ack = submit(&handle, take(1, 1, 10, 1000));
    assert!(ack.events.iter().any(|e| matches!(e, Event::Queued { .. })));

    // Once degraded, the wait is not paid again: the next commands come back promptly rather
    // than each spending the timeout.
    let started = std::time::Instant::now();
    for n in 2..12u128 {
        fund(&handle, n);
        submit(&handle, take(n, n, 1, 1000));
    }
    assert!(
        started.elapsed() < std::time::Duration::from_millis(500),
        "the auction kept paying the replica timeout after declaring it lost: {:?}",
        started.elapsed()
    );

    auction.shutdown();
}
