//! Worked examples of the mechanism.
//!
//! Where `properties.rs` asserts that the invariants hold for *any* command sequence, these
//! tests pin down what the auction actually does for specific ones — they are the executable
//! form of `docs/auction-rules.md`, and the place to look when arguing about behavior.

mod common;

use auction_core::{Command, Event, Outcome};
use auction_proto::{Nanos, Price, Qty, RejectReason, Status};
use common::{key, participant, Harness};

const NO_BATCHING: Nanos = Nanos::ZERO;
const ONE_MS: Nanos = Nanos::from_millis(1);

/// The headline property of a uniform-price auction: bidding early is not punished.
#[test]
fn clearing_reprices_earlier_fills_down_to_the_clearing_price() {
    let mut h = Harness::open(10, NO_BATCHING);
    h.fund(1, 100_000);
    h.fund(2, 100_000);

    // Alice takes 4 at the opening price of 1000.
    h.take(Nanos::ZERO, 1, 1, 4);
    assert_eq!(h.state.fills()[0].price, Price(1000));
    h.assert_invariants();

    // Fifty seconds later the clock is at 500 and Bob takes the last 6, exhausting supply.
    h.take(Nanos::from_secs(50), 2, 2, 6);

    assert_eq!(h.state.status(), Status::Cleared);
    assert_eq!(h.state.clearing_price(), Some(Price(500)));
    // Alice's fill was repriced from 1000 down to 500 — she is not punished for going first.
    assert!(h.events.iter().any(|e| matches!(
        e,
        Event::Repriced {
            from: Price(1000),
            to: Price(500),
            ..
        }
    )));
    for f in h.state.fills() {
        assert_eq!(f.price, Price(500));
    }
    h.assert_invariants();
}

#[test]
fn a_bid_larger_than_the_remaining_supply_fills_partially_and_clears() {
    let mut h = Harness::open(10, NO_BATCHING);
    h.fund(1, 100_000);

    h.take(Nanos::ZERO, 1, 1, 25);

    assert_eq!(h.filled_by(1), 10);
    assert_eq!(h.state.status(), Status::Cleared);
    assert_eq!(
        h.state.outcome(key(1)),
        Some(Outcome::Filled {
            qty: Qty(10),
            price: Price(1000)
        })
    );
    h.assert_invariants();
}

#[test]
fn reaching_the_floor_clears_with_the_remaining_supply_unsold() {
    let mut h = Harness::open(10, NO_BATCHING);
    h.fund(1, 100_000);
    h.take(Nanos::ZERO, 1, 1, 3);

    // The clock bottoms out at 100 after 90 seconds.
    h.tick(Nanos::from_secs(90));

    assert_eq!(h.state.status(), Status::Cleared);
    assert_eq!(h.state.clearing_price(), Some(Price(100)));
    assert_eq!(h.state.supply_remaining(), Qty(7));
    // The early fill still gets the floor price.
    assert_eq!(h.state.fills()[0].price, Price(100));
    assert!(h
        .events
        .iter()
        .any(|e| matches!(e, Event::Cleared { unsold: Qty(7), .. })));
    h.assert_invariants();
}

/// A participant is entitled to be wrong about the price and to find out for free.
#[test]
fn a_take_outside_the_tolerance_band_is_rejected_with_the_true_price() {
    let mut h = Harness::open(10, NO_BATCHING);
    h.fund(1, 100_000);

    // Tolerance defaults to one step (10). Believing the price is 985 when it is 1000 is a
    // 15-unit disagreement.
    h.take_expecting(Nanos::ZERO, 1, 1, 5, Price(985));
    assert_eq!(
        h.rejection_for(1),
        Some(RejectReason::PriceMoved {
            server_price: Price(1000)
        })
    );
    assert_eq!(h.filled_by(1), 0);

    // One step of disagreement is forgiven — otherwise every step boundary rejects honest bids.
    h.take_expecting(Nanos::ZERO, 1, 2, 5, Price(990));
    assert_eq!(h.filled_by(1), 5);
    h.assert_invariants();
}

#[test]
fn a_resting_bid_fires_when_the_clock_reaches_it_and_not_before() {
    let mut h = Harness::open(10, NO_BATCHING);
    h.fund(1, 100_000);

    h.rest(Nanos::ZERO, 1, 1, 6, 900);
    assert_eq!(
        h.state.outcome(key(1)),
        Some(Outcome::Resting { qty: Qty(6) })
    );

    // At 5s the clock is 950 — still above the limit.
    h.tick(Nanos::from_secs(5));
    assert_eq!(h.filled_by(1), 0);

    // At 10s it reaches 900 exactly.
    h.tick(Nanos::from_secs(10));
    assert_eq!(h.filled_by(1), 6);
    assert_eq!(h.state.fills()[0].price, Price(900));
    h.assert_invariants();
}

/// Willingness to pay more wins, regardless of who committed first.
#[test]
fn a_higher_resting_limit_outranks_an_earlier_lower_one() {
    let mut h = Harness::open(5, ONE_MS);
    h.fund(1, 100_000);
    h.fund(2, 100_000);

    h.rest(Nanos::ZERO, 1, 1, 5, 800); // earlier, but cheaper
    h.rest(Nanos::ZERO, 2, 2, 5, 900); // later, but dearer

    h.tick(Nanos::from_secs(20)); // clock is 800; both are now satisfiable
    h.tick(Nanos::from_secs(21)); // close the window

    assert_eq!(h.filled_by(2), 5, "the higher limit should take the supply");
    assert_eq!(h.filled_by(1), 0);
    assert_eq!(h.rejection_for(1), Some(RejectReason::SupplyExhausted));
    h.assert_invariants();
}

/// Within a batch window, sub-millisecond advantage buys nothing: everyone at the same price
/// shares pro-rata by requested quantity.
#[test]
fn a_window_that_is_oversubscribed_allocates_pro_rata() {
    let mut h = Harness::open(10, ONE_MS);
    for who in 1..=3 {
        h.fund(who, 100_000);
    }

    // Three bids for 5 units each, arriving microseconds apart inside the same window.
    h.take(Nanos(10_000), 1, 1, 5);
    h.take(Nanos(20_000), 2, 2, 5);
    h.take(Nanos(30_000), 3, 3, 5);
    assert_eq!(h.filled_by(1), 0, "nothing matches until the window closes");

    h.tick(Nanos::from_millis(2));

    // 10 units over 15 demanded: floor(10*5/15) = 3 each, and the odd unit goes to the first
    // in priority order rather than being lost.
    assert_eq!(
        (h.filled_by(1), h.filled_by(2), h.filled_by(3)),
        (4, 3, 3),
        "pro-rata allocation with the rounding remainder handed out in priority order"
    );
    assert_eq!(h.state.status(), Status::Cleared);
    h.assert_invariants();
}

/// Everyone in a window pays the window's price, which is the lowest price anyone in it could
/// have seen — batching for fairness never fills a participant worse than they bid.
#[test]
fn a_window_spanning_a_step_boundary_fills_everyone_at_the_lower_price() {
    // A 2-second window straddles the 1s step, where the price drops 1000 -> 990.
    let mut h = Harness::open(10, Nanos::from_secs(2));
    h.fund(1, 100_000);

    h.take(Nanos::ZERO, 1, 1, 4); // arrived believing 1000
    h.tick(Nanos::from_secs(3)); // close the window

    assert_eq!(
        h.state.fills()[0].price,
        Price(990),
        "the window price is the price at its last instant, never worse than on arrival"
    );
    h.assert_invariants();
}

#[test]
fn a_replayed_idempotency_key_returns_the_original_outcome_and_fills_nothing() {
    let mut h = Harness::open(10, NO_BATCHING);
    h.fund(1, 100_000);

    h.take(Nanos::ZERO, 1, 1, 4);
    assert_eq!(h.filled_by(1), 4);

    // The client times out and retries the same key.
    let events = h.take(Nanos::from_secs(1), 1, 1, 4);

    assert_eq!(h.filled_by(1), 4, "I7 violated: the retry filled again");
    assert_eq!(
        events.as_slice(),
        &[Event::Duplicate {
            key: key(1),
            outcome: Outcome::Filled {
                qty: Qty(4),
                price: Price(1000)
            }
        }]
    );
    h.assert_invariants();
}

#[test]
fn a_bid_beyond_a_participants_collateral_is_refused() {
    let mut h = Harness::open(100, NO_BATCHING);
    h.fund(1, 5_000); // enough for 5 units at 1000, not 6

    h.take(Nanos::ZERO, 1, 1, 6);
    assert_eq!(
        h.rejection_for(1),
        Some(RejectReason::InsufficientCollateral)
    );

    h.take(Nanos::ZERO, 1, 2, 5);
    assert_eq!(h.filled_by(1), 5);
    assert_eq!(h.state.collateral_of(participant(1)).available(), 0);

    // Fully committed, so even one more unit is refused.
    h.take(Nanos::ZERO, 1, 3, 1);
    assert_eq!(
        h.rejection_for(3),
        Some(RejectReason::InsufficientCollateral)
    );
    h.assert_invariants();
}

/// Repricing releases collateral, because the clock only ever descends.
#[test]
fn clearing_frees_collateral_rather_than_consuming_more() {
    let mut h = Harness::open(10, NO_BATCHING);
    h.fund(1, 10_000);
    h.fund(2, 10_000);

    h.take(Nanos::ZERO, 1, 1, 5); // 5 @ 1000 = 5000 committed
    assert_eq!(h.state.collateral_of(participant(1)).committed, 5_000);

    h.take(Nanos::from_secs(50), 2, 2, 5); // clears at 500
    assert_eq!(
        h.state.collateral_of(participant(1)).committed,
        2_500,
        "repricing 1000 -> 500 must halve what Alice owes"
    );
    h.assert_invariants();
}

#[test]
fn a_cancelled_auction_answers_everyone_and_returns_their_collateral() {
    let mut h = Harness::open(10, ONE_MS);
    h.fund(1, 100_000);
    h.fund(2, 100_000);

    h.rest(Nanos::ZERO, 1, 1, 5, 500);
    h.take(Nanos(1_000), 2, 2, 5);

    h.apply(Nanos(2_000), Command::Cancel);

    assert_eq!(h.state.status(), Status::Cancelled);
    // I10: nothing is silently dropped, not even on the way out.
    assert_eq!(h.rejection_for(1), Some(RejectReason::NotLive));
    assert_eq!(h.rejection_for(2), Some(RejectReason::NotLive));
    assert_eq!(h.state.collateral_of(participant(1)).committed, 0);
    assert_eq!(h.state.collateral_of(participant(2)).committed, 0);
}

#[test]
fn a_resting_bid_can_be_withdrawn_before_the_clock_reaches_it() {
    let mut h = Harness::open(10, NO_BATCHING);
    h.fund(1, 100_000);

    h.rest(Nanos::ZERO, 1, 1, 5, 500);
    assert_eq!(h.state.collateral_of(participant(1)).committed, 2_500);

    h.apply(
        Nanos::from_secs(1),
        Command::CancelResting {
            participant: participant(1),
            key: key(1),
        },
    );
    assert_eq!(h.state.outcome(key(1)), Some(Outcome::Cancelled));
    assert_eq!(h.state.collateral_of(participant(1)).committed, 0);

    // The clock passes the limit and nothing fires.
    h.tick(Nanos::from_secs(50));
    assert_eq!(h.filled_by(1), 0);

    // Withdrawing it again is answered, not silently ignored.
    let events = h.apply(
        Nanos::from_secs(51),
        Command::CancelResting {
            participant: participant(1),
            key: key(1),
        },
    );
    assert_eq!(
        events.as_slice(),
        &[Event::Duplicate {
            key: key(1),
            outcome: Outcome::Cancelled
        }]
    );
}

#[test]
fn a_resting_bid_above_the_clock_is_refused_rather_than_filled_as_a_take() {
    let mut h = Harness::open(10, NO_BATCHING);
    h.fund(1, 100_000);

    // At 10s the clock is 900; a limit of 950 is already in the past.
    h.rest(Nanos::from_secs(10), 1, 1, 5, 950);
    assert_eq!(
        h.rejection_for(1),
        Some(RejectReason::LimitAboveClock {
            server_price: Price(900)
        })
    );
    assert_eq!(h.filled_by(1), 0);
}

#[test]
fn bids_are_refused_before_the_auction_opens_and_after_it_ends() {
    use auction_core::{AuctionConfig, AuctionState, BidKind};
    use auction_proto::{AuctionId, IdempotencyKey, ParticipantId, Seq};

    let config = AuctionConfig::new(
        AuctionId(uuid::Uuid::from_u128(0)),
        Qty(10),
        common::schedule(),
    );
    let mut state = AuctionState::new(config);
    let events = state.apply(
        Seq::START,
        Nanos::ZERO,
        Command::SubmitBid {
            participant: ParticipantId(uuid::Uuid::from_u128(1)),
            key: IdempotencyKey(uuid::Uuid::from_u128(2)),
            qty: Qty(1),
            kind: BidKind::Take {
                expected_price: Price(1000),
            },
        },
    );
    assert!(matches!(
        events.as_slice(),
        [Event::Rejected {
            reason: RejectReason::NotLive,
            ..
        }]
    ));
}

#[test]
fn a_zero_quantity_bid_is_refused() {
    let mut h = Harness::open(10, NO_BATCHING);
    h.fund(1, 100_000);
    h.take(Nanos::ZERO, 1, 1, 0);
    assert_eq!(h.rejection_for(1), Some(RejectReason::ZeroQuantity));
}

#[test]
#[should_panic(expected = "I4")]
fn a_gap_in_the_sequence_is_fatal_rather_than_silently_accepted() {
    let mut h = Harness::open(10, NO_BATCHING);
    // Skip a sequence number, which would mean a command was lost from the audit record.
    h.state
        .apply(auction_proto::Seq(99), Nanos::ZERO, Command::Tick);
}
