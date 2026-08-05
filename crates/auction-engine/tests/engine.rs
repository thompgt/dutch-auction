//! What the engine promises, tested through the whole pipeline.
//!
//! These tests drive a real engine thread, a real log, and a real `fsync`. That is deliberate:
//! every property here is a claim about the *seam* between the state machine and the disk, and
//! a test that mocked either side would be testing the mock.

mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};

use auction_core::{BidKind, Command, Event, Outcome};
use auction_engine::{Auction, Shed};
use auction_proto::{Price, Qty, RejectReason, Seq, Status};

use common::*;

/// I6, the load-bearing one: an acknowledgement means the command is on the platter.
///
/// Checked by reading the log from a *separate* handle after the ack returns, rather than by
/// asking the engine what it thinks it wrote — the point is that the bytes are there for a
/// process that has no shared memory with this one.
#[test]
fn an_acknowledged_command_is_already_in_the_log() {
    let dir = tempfile::tempdir().unwrap();
    let (auction, handle) = live(dir.path(), 1_000);
    fund(&handle, 1);

    let ack = submit(&handle, take(1, 1, 10, 1000));

    let records = auction_wal::read_all(dir.path()).expect("reading the log");
    assert!(
        records.iter().any(|r| r.seq == ack.seq),
        "acked {} but the log ends at {:?}",
        ack.seq,
        records.last().map(|r| r.seq)
    );
    drop(auction);
}

/// The engine ticks itself.
///
/// One bid arrives, and then the auction goes completely silent. Nothing else is submitted, so
/// nothing but the engine's own deadline can close the batch window — and the fill has to arrive
/// anyway. This is the property `docs/slo.md` requires: the flush is charged to the timer, not
/// to whichever participant happens to bid next.
#[test]
fn an_open_window_closes_on_its_own_boundary_with_no_further_traffic() {
    let dir = tempfile::tempdir().unwrap();
    let (auction, handle) = live(dir.path(), 1_000);
    fund(&handle, 1);
    let mut events = auction.subscribe();

    let ack = submit(&handle, take(1, 1, 10, 1000));
    assert!(
        ack.events.iter().any(|e| matches!(e, Event::Queued { .. })),
        "a batched bid should be queued, not filled on arrival: {:?}",
        ack.events
    );

    // Nothing else is submitted from here on.
    let filled = loop {
        let sequenced = events
            .blocking_recv()
            .expect("the event stream stayed open");
        if let Event::Filled(fill) = sequenced.event {
            break fill;
        }
    };
    assert_eq!(filled.qty, Qty(10));
    // The fill is attributed to the command that produced the allocation, which is the bid
    // itself — the tick that closed the window has a later sequence number but does not own
    // anyone's units.
    assert!(filled.seq >= ack.seq);
    drop(auction);
}

/// The clock starts when the auction opens, not when the process does.
///
/// An engine that anchored elapsed time to its own start-up would apply the first bid of a
/// long-scheduled auction far down the schedule — for a descending clock, at the floor.
#[test]
fn the_price_clock_starts_at_the_open_command_not_at_process_start() {
    let dir = tempfile::tempdir().unwrap();
    let auction = Auction::open(dir.path(), config(1_000), options()).unwrap();
    let handle = auction.handle();

    // The engine has been up for a while before anyone opens the auction.
    std::thread::sleep(std::time::Duration::from_millis(1_200));
    let opened = submit(&handle, Command::Open);

    match opened.events.first() {
        Some(Event::Opened { start_price, .. }) => assert_eq!(
            *start_price,
            Price(1000),
            "the auction opened below its start price"
        ),
        other => panic!("expected an Opened event, got {other:?}"),
    }

    // And a bid immediately after the open is priced at the start price, not 1.2 steps down it.
    fund(&handle, 1);
    let ack = submit(&handle, take(1, 1, 1, 1000));
    assert!(
        !ack.events.iter().any(|e| matches!(
            e,
            Event::Rejected {
                reason: RejectReason::PriceMoved { .. },
                ..
            }
        )),
        "the clock had already run before the auction opened: {:?}",
        ack.events
    );
    drop(auction);
}

/// I10: overload is answered, not absorbed.
///
/// The queue is filled with commands nobody is draining, and the next submission must come back
/// `Busy` from the calling thread rather than blocking. A shed command is also *not in the log* —
/// which is the entire strength of the promise, since a command in the log can still fill.
#[test]
fn an_overloaded_ingress_sheds_and_the_shed_command_never_reaches_the_log() {
    let dir = tempfile::tempdir().unwrap();
    let auction = Auction::open(dir.path(), config(1_000), options()).unwrap();
    let handle = auction.handle();

    // Fill the queue faster than the engine can drain it. Some will be applied as we go, so
    // this loop keeps pushing until a submission is actually refused.
    let mut shed = None;
    for i in 0..100_000u128 {
        match handle.submit(take(1, i, 1, 1000)) {
            Ok(_rx) => {}
            Err(Shed::Busy(reason)) => {
                shed = Some((i, reason));
                break;
            }
            Err(e) => panic!("unexpected refusal: {e}"),
        }
    }
    let (shed_at, reason) = shed.expect("the bounded queue never filled");
    assert_eq!(reason, RejectReason::Busy);

    drop(auction);

    // The shed bid's key must appear nowhere in the audit record.
    let records = auction_wal::read_all(dir.path()).unwrap();
    let shed_key = key(shed_at);
    assert!(
        !records.iter().any(|r| matches!(
            r.cmd,
            Command::SubmitBid { key, .. } if key == shed_key
        )),
        "a bid rejected as Busy reached the log"
    );
}

/// I1, through the whole stack: many threads, one supply, no oversell.
///
/// The single-writer design makes this structurally true rather than probabilistically true, so
/// the test is really checking that nothing in the plumbing reintroduced a second writer.
#[test]
fn a_herd_of_bidders_cannot_oversell_the_auction() {
    const SUPPLY: u64 = 500;
    const BIDDERS: u128 = 16;
    const PER_BIDDER: u128 = 40;

    let dir = tempfile::tempdir().unwrap();
    let (auction, handle) = live(dir.path(), SUPPLY);
    for who in 0..BIDDERS {
        fund(&handle, who);
    }

    let filled = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(BIDDERS as usize));
    let mut threads = Vec::new();

    for who in 0..BIDDERS {
        let handle = handle.clone();
        let filled = Arc::clone(&filled);
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            // Everyone starts at once: this is the thundering herd, not a trickle.
            barrier.wait();
            for n in 0..PER_BIDDER {
                let k = who * 1_000 + n;
                // 10 units each, 16 x 40 x 10 = 6,400 units chasing 500.
                let Ok(rx) = handle.submit(take(who, k, 10, 1000)) else {
                    continue; // shed under load, which is a legitimate answer
                };
                let Ok(ack) = rx.blocking_recv() else { return };
                for event in &ack.events {
                    if let Event::Filled(fill) = event {
                        filled.fetch_add(fill.qty.0, Ordering::Relaxed);
                    }
                }
            }
        }));
    }
    for t in threads {
        t.join().unwrap();
    }

    // Let the last window close and the auction clear.
    std::thread::sleep(std::time::Duration::from_millis(50));
    drop(auction);

    // Recover from disk and ask the state itself, so the assertion covers the log too.
    let recovered = auction_wal::recover(dir.path()).unwrap();
    assert!(
        recovered.state.total_filled() <= Qty(SUPPLY),
        "oversold: {} of {SUPPLY}",
        recovered.state.total_filled()
    );
    assert!(
        filled.load(Ordering::Relaxed) <= SUPPLY,
        "acknowledged more fills than there was supply"
    );

    // Sequence numbers are gapless and start at the beginning (I4).
    let records = auction_wal::read_all(dir.path()).unwrap();
    for (i, record) in records.iter().enumerate() {
        assert_eq!(record.seq, Seq(i as u64), "gap in the audit record");
    }
}

/// The auction ends itself when the clock reaches the floor, with nobody bidding.
///
/// Without an engine-generated tick this can only happen when the next command arrives, which
/// for an auction that nobody wants is never.
#[test]
fn the_auction_clears_at_the_floor_without_any_traffic() {
    let dir = tempfile::tempdir().unwrap();
    // 1000 -> 900 in one 200 ms step, so the floor arrives quickly.
    let config = auction_core::AuctionConfig::new(
        auction_proto::AuctionId(uuid::Uuid::from_u128(9)),
        Qty(100),
        auction_core::PriceSchedule::stepped(
            Price(1000),
            Price(900),
            auction_proto::Nanos::from_millis(200),
            100,
        ),
    );
    let auction = Auction::open(dir.path(), config, options()).unwrap();
    let handle = auction.handle();
    let mut events = auction.subscribe();
    submit(&handle, Command::Open);

    let cleared = loop {
        let sequenced = events
            .blocking_recv()
            .expect("the event stream stayed open");
        if let Event::Cleared { price, unsold, .. } = sequenced.event {
            break (price, unsold);
        }
    };
    assert_eq!(cleared.0, Price(900), "cleared away from the floor");
    assert_eq!(cleared.1, Qty(100), "all supply should be unsold");
    drop(auction);
}

/// Restarting is recovery, and recovery resumes the auction rather than restarting it.
#[test]
fn an_auction_restarts_where_it_stopped() {
    let dir = tempfile::tempdir().unwrap();

    let (auction, handle) = live(dir.path(), 1_000);
    fund(&handle, 1);
    let before = submit(&handle, take(1, 1, 25, 1000));
    // Enough traffic to force at least one snapshot, so recovery uses that path too.
    for n in 2..60u128 {
        fund(&handle, n);
        submit(&handle, take(n, n, 1, 1000));
    }
    let last = submit(&handle, Command::Tick);
    auction.shutdown();

    let reopened = Auction::open(dir.path(), config(1_000), options()).unwrap();
    let handle = reopened.handle();

    // The sequence continues rather than restarting: reusing a position in the total order
    // would make the audit record ambiguous (I4).
    let next = submit(&handle, Command::Tick);
    assert!(
        next.seq > last.seq,
        "restarted at {} which is not past {}",
        next.seq,
        last.seq
    );

    // And the auction remembers the fill it acknowledged before the restart (I6, I7).
    let recovered = auction_wal::recover(dir.path()).unwrap();
    assert!(recovered.state.total_filled() >= Qty(25));
    match recovered.state.outcome(key(1)) {
        Some(Outcome::Filled { qty, .. }) => assert_eq!(qty, Qty(25)),
        other => panic!("the pre-restart fill was lost: {other:?}"),
    }
    assert!(before.seq < next.seq);
    drop(reopened);
}

/// A resting bid is triggered by the clock, with no client message involved.
///
/// This is the mechanism's answer to being outraced, so it has to work when the participant is
/// disconnected, asleep, or on the other side of the planet.
#[test]
fn a_resting_bid_is_triggered_by_the_engines_own_clock() {
    let dir = tempfile::tempdir().unwrap();
    let (auction, handle) = live(dir.path(), 1_000);
    fund(&handle, 1);
    let mut events = auction.subscribe();

    // Two steps below the opening price: the clock reaches 800 after 2 seconds.
    let rested = submit(
        &handle,
        Command::SubmitBid {
            participant: participant(1),
            key: key(1),
            qty: Qty(10),
            kind: BidKind::Resting { limit: Price(800) },
        },
    );
    assert!(
        rested
            .events
            .iter()
            .any(|e| matches!(e, Event::Rested { .. })),
        "expected the bid to rest: {:?}",
        rested.events
    );

    // Nothing else is submitted. The fill can only come from the engine's own tick.
    let fill = loop {
        let sequenced = events
            .blocking_recv()
            .expect("the event stream stayed open");
        if let Event::Filled(fill) = sequenced.event {
            break fill;
        }
    };
    assert_eq!(fill.qty, Qty(10));
    assert_eq!(
        fill.price,
        Price(800),
        "a resting bid fills at the level it was waiting for"
    );
    drop(auction);
}

/// Shutting down must not wait for the auction's next deadline.
///
/// Deadlines are unbounded — a schedule whose floor is a century out is perfectly legal — and the
/// engine parks until the next one. `Auction::drop` sets the stop flag and then *joins* that
/// thread, so an unclamped park turns dropping an idle auction into a deadlock lasting as long as
/// the schedule does. The whole existing suite missed this because its test schedule bottoms out
/// after nine seconds, which merely made every shutdown slow instead of infinite.
#[test]
fn shutting_down_an_idle_auction_does_not_wait_for_its_next_deadline() {
    let dir = tempfile::tempdir().unwrap();
    // A floor the clock reaches in about a century.
    let config = auction_core::AuctionConfig::new(
        auction_proto::AuctionId(uuid::Uuid::from_u128(13)),
        Qty(1_000),
        auction_core::PriceSchedule::stepped(
            Price(1_000_000),
            Price(1),
            auction_proto::Nanos::from_secs(3_600),
            1,
        ),
    );
    let auction = Auction::open(dir.path(), config, options()).unwrap();
    let handle = auction.handle();
    submit(&handle, Command::Open);

    // Let the engine settle into its park before pulling the rug out.
    std::thread::sleep(std::time::Duration::from_millis(50));

    let started = std::time::Instant::now();
    auction.shutdown();
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "shutdown waited {:?} for a deadline a century away",
        started.elapsed()
    );
}

/// An idle auction must not write commands into the audit record for having nothing to do.
///
/// A tick per batch window would be a thousand `fsync`s a second and a log that grows without
/// anything happening — and the audit record would no longer be a record of the auction, it
/// would be a record of the clock.
#[test]
fn an_idle_auction_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let auction = Auction::open(dir.path(), config(1_000), options()).unwrap();
    let handle = auction.handle();
    submit(&handle, Command::Open);

    let after_open = auction_wal::read_all(dir.path()).unwrap().len();
    // Many batch windows' worth of doing nothing.
    std::thread::sleep(std::time::Duration::from_millis(300));
    let after_idling = auction_wal::read_all(dir.path()).unwrap().len();

    assert_eq!(
        after_open,
        after_idling,
        "an idle auction wrote {} commands in 300 ms",
        after_idling - after_open
    );
    assert_eq!(
        auction_wal::recover(dir.path()).unwrap().state.status(),
        Status::Live
    );
    drop(auction);
}
