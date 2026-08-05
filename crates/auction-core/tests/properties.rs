//! The property suite: `docs/invariants.md`, mechanically checked.
//!
//! `scenarios.rs` says what the auction does for command sequences a human chose. This file
//! says what it must never do for command sequences nobody chose — which is where the
//! interesting failures live, because the adversary in a financial venue does not restrict
//! itself to the cases the author imagined.
//!
//! Every generated script asserts I1, I2, I3, I9 and I10 after *each* command, not only at the
//! end: an invariant that holds only at rest is not an invariant, it is a coincidence.

mod common;

use auction_core::{AuctionState, Command, Event};
use auction_proto::{Nanos, Price, Qty, RejectReason, Status};
use common::{key, participant, Harness};
use proptest::prelude::*;

/// Four participants is enough for contention without making failures unreadable.
const PARTICIPANTS: u128 = 4;
const COLLATERAL: i64 = 50_000;

#[derive(Debug, Clone, Copy)]
enum Op {
    /// A market take, with a deliberate error in the client's idea of the price.
    Take { who: u128, qty: u64, price_error: i64 },
    /// A resting bid `below` minor units under the current clock.
    Rest { who: u128, qty: u64, below: i64 },
    /// Withdraw the bid submitted by op number `which` — which may not exist.
    Withdraw { who: u128, which: u128 },
    /// Advance the clock.
    Tick,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (0..PARTICIPANTS, 1u64..20, -30i64..30)
            .prop_map(|(who, qty, price_error)| Op::Take { who, qty, price_error }),
        3 => (0..PARTICIPANTS, 1u64..20, 0i64..500)
            .prop_map(|(who, qty, below)| Op::Rest { who, qty, below }),
        1 => (0..PARTICIPANTS, 0u128..40).prop_map(|(who, which)| Op::Withdraw { who, which }),
        4 => Just(Op::Tick),
    ]
}

/// A script of `(milliseconds since the previous command, command)`.
///
/// Gaps run to three seconds so a long script walks the clock past its 90-second floor, and
/// short gaps put several commands inside one batch window.
fn script() -> impl Strategy<Value = Vec<(u64, Op)>> {
    prop::collection::vec((0u64..3_000, op()), 1..40)
}

/// Run a script and assert the always-true invariants after every single command.
fn run(script: &[(u64, Op)], supply: u64, batch_window: Nanos) -> Harness {
    let mut h = Harness::open(supply, batch_window);
    for who in 0..PARTICIPANTS {
        h.fund(who, COLLATERAL);
    }

    let mut ts = Nanos::ZERO;
    for (i, (gap_ms, op)) in script.iter().enumerate() {
        ts = Nanos(ts.0 + gap_ms * 1_000_000);
        let k = i as u128;
        let clock = h.state.config().price_at(ts);

        let events = match *op {
            Op::Take {
                who,
                qty,
                price_error,
            } => h.take_expecting(ts, who, k, qty, Price(clock.0.saturating_add(price_error))),
            Op::Rest { who, qty, below } => {
                let limit = clock.0.saturating_sub(below);
                h.rest(ts, who, k, qty, limit)
            }
            Op::Withdraw { who, which } => h.apply(
                ts,
                Command::CancelResting {
                    participant: participant(who),
                    key: key(which),
                },
            ),
            Op::Tick => h.tick(ts),
        };

        // I10 — every bid gets an answer. A silently dropped command is indistinguishable from
        // a lost one, and the participant has no way to tell whether they are exposed.
        if matches!(op, Op::Take { .. } | Op::Rest { .. } | Op::Withdraw { .. }) {
            assert!(
                !events.is_empty(),
                "I10 violated: op {i} ({op:?}) produced no outcome"
            );
        }

        // I1, I2, I3, I9.
        h.assert_invariants();
    }
    h
}

proptest! {
    /// The core sweep: I1, I2, I3, I9, I10 after every command, with batching both on and off.
    #[test]
    fn invariants_hold_under_arbitrary_command_sequences(
        script in script(),
        supply in 1u64..200,
        batched in any::<bool>(),
    ) {
        let window = if batched { Nanos::from_millis(1) } else { Nanos::ZERO };
        run(&script, supply, window);
    }

    /// I5 — replay is byte-identical.
    ///
    /// The load-bearing property of the whole architecture: crash recovery, the hot standby,
    /// and the audit export are the same mechanism, and all three break together if this fails.
    #[test]
    fn replaying_a_script_reproduces_identical_state(
        script in script(),
        supply in 1u64..200,
        batched in any::<bool>(),
    ) {
        let window = if batched { Nanos::from_millis(1) } else { Nanos::ZERO };
        let first = run(&script, supply, window).state;
        let second = run(&script, supply, window).state;

        prop_assert_eq!(&first, &second);
        // Compare the serialized form too: `PartialEq` could agree where the bytes a snapshot
        // would actually persist do not.
        prop_assert_eq!(
            serde_json::to_string(&first).unwrap(),
            serde_json::to_string(&second).unwrap()
        );
    }

    /// I7 — a replayed idempotency key never executes twice, whatever the original outcome was.
    #[test]
    fn a_repeated_key_never_executes_twice(
        script in script(),
        supply in 1u64..200,
        repeat in 0usize..40,
    ) {
        let mut h = run(&script, supply, Nanos::from_millis(1));
        let repeat = repeat.min(script.len().saturating_sub(1));

        let before_fills = h.state.fills().to_vec();
        let before_supply = h.state.supply_remaining();
        let original = h.state.outcome(key(repeat as u128));

        let ts = Nanos(h.state.now().0 + 1);
        let events = h.take_expecting(ts, 0, repeat as u128, 5, h.state.config().price_at(ts));

        if let Some(original) = original {
            prop_assert_eq!(
                events.as_slice(),
                &[Event::Duplicate { key: key(repeat as u128), outcome: original }],
                "I7 violated: a known key did something other than report its original outcome"
            );
            prop_assert_eq!(h.state.fills(), before_fills.as_slice());
            prop_assert_eq!(h.state.supply_remaining(), before_supply);
        }
    }

    /// Supply is never stranded: if anyone was turned away for want of supply, there is none
    /// left. A matching bug that skipped a bid would show up here as unsold units alongside a
    /// rejection that blamed exhaustion.
    #[test]
    fn nobody_is_refused_for_exhaustion_while_supply_remains(
        script in script(),
        supply in 1u64..200,
        batched in any::<bool>(),
    ) {
        let window = if batched { Nanos::from_millis(1) } else { Nanos::ZERO };
        let h = run(&script, supply, window);

        let refused_for_exhaustion = h.events.iter().any(|e| {
            matches!(e, Event::Rejected { reason: RejectReason::SupplyExhausted, .. })
        });
        if refused_for_exhaustion {
            prop_assert_eq!(h.state.supply_remaining(), Qty::ZERO);
            prop_assert_eq!(h.state.status(), Status::Cleared);
        }
    }

    /// Collateral is fully returned once the auction is over: every reservation is either
    /// converted into a fill or released, never left dangling.
    #[test]
    fn a_finished_auction_leaves_no_dangling_reservations(
        script in script(),
        supply in 1u64..200,
    ) {
        let mut h = run(&script, supply, Nanos::from_millis(1));
        // Walk the clock past the floor so the auction is guaranteed to have finished.
        h.tick(Nanos::from_secs(200));
        prop_assert!(h.state.status().is_terminal());

        for who in 0..PARTICIPANTS {
            let committed = h.state.collateral_of(participant(who)).committed;
            let owed: i64 = h.state.fills()
                .iter()
                .filter(|f| f.participant == participant(who))
                .map(|f| f.consideration().unwrap())
                .sum();
            prop_assert_eq!(
                committed, owed,
                "participant {} has {} committed but owes {}", who, committed, owed
            );
        }
    }
}

/// I4, stated positively: sequence numbers advance by exactly one, forever.
#[test]
fn sequence_numbers_are_gapless_across_a_long_run() {
    let mut state = AuctionState::new(auction_core::AuctionConfig::new(
        auction_proto::AuctionId(uuid::Uuid::from_u128(0)),
        Qty(1_000),
        common::schedule(),
    ));
    let mut seq = auction_proto::Seq::START;
    state.apply(seq, Nanos::ZERO, Command::Open);
    for i in 1..10_000u64 {
        seq = seq.next();
        state.apply(seq, Nanos(i * 1_000), Command::Tick);
    }
    assert_eq!(seq.0, 9_999);
}
