//! Shared test harness.
//!
//! Identifiers are minted from small integers rather than randomly, so a failing case is
//! reproducible from its seed alone and a failure message names "participant 1" instead of a
//! UUID nobody can hold in their head.

#![allow(dead_code)] // each test file uses a different subset

use auction_core::{AuctionConfig, AuctionState, Command, Event, PriceSchedule};
use auction_proto::{
    IdempotencyKey, Nanos, ParticipantId, Price, Qty, RejectReason, Seq, Status,
};
use uuid::Uuid;

pub fn participant(n: u128) -> ParticipantId {
    ParticipantId(Uuid::from_u128(n))
}

pub fn key(n: u128) -> IdempotencyKey {
    // Offset so a key and a participant id are never accidentally interchangeable in a test.
    IdempotencyKey(Uuid::from_u128(1 << 96 | n))
}

/// The reference schedule for tests: 1000 down to 100, dropping 10 every second, so the floor
/// is reached at 90s and every price in between is a round number.
pub fn schedule() -> PriceSchedule {
    PriceSchedule::stepped(Price(1000), Price(100), Nanos::from_secs(1), 10)
}

/// Drives an [`AuctionState`], assigning sequence numbers and collecting the event stream.
pub struct Harness {
    pub state: AuctionState,
    pub events: Vec<Event>,
    next_seq: Seq,
}

impl Harness {
    /// A live auction with `supply` units and the reference schedule.
    pub fn open(supply: u64, batch_window: Nanos) -> Self {
        let config = AuctionConfig::new(
            auction_proto::AuctionId(Uuid::from_u128(0)),
            Qty(supply),
            schedule(),
        )
        .with_batch_window(batch_window);
        let mut h = Self {
            state: AuctionState::new(config),
            events: Vec::new(),
            next_seq: Seq::START,
        };
        h.apply(Nanos::ZERO, Command::Open);
        h
    }

    /// Apply a command at `ts`, recording the events it produced and returning them.
    pub fn apply(&mut self, ts: Nanos, cmd: Command) -> Vec<Event> {
        let seq = self.next_seq;
        self.next_seq = seq.next();
        let produced = self.state.apply(seq, ts, cmd).to_vec();
        self.events.extend(produced.iter().copied());
        produced
    }

    pub fn fund(&mut self, who: u128, limit: i64) {
        self.apply(
            Nanos::ZERO,
            Command::SetCollateral {
                participant: participant(who),
                limit,
            },
        );
    }

    /// A market take, priced at whatever the clock actually says (so the tolerance band passes).
    pub fn take(&mut self, ts: Nanos, who: u128, k: u128, qty: u64) -> Vec<Event> {
        let expected_price = self.state.config().price_at(ts);
        self.take_expecting(ts, who, k, qty, expected_price)
    }

    pub fn take_expecting(
        &mut self,
        ts: Nanos,
        who: u128,
        k: u128,
        qty: u64,
        expected_price: Price,
    ) -> Vec<Event> {
        self.apply(
            ts,
            Command::SubmitBid {
                participant: participant(who),
                key: key(k),
                qty: Qty(qty),
                kind: auction_core::BidKind::Take { expected_price },
            },
        )
    }

    pub fn rest(&mut self, ts: Nanos, who: u128, k: u128, qty: u64, limit: i64) -> Vec<Event> {
        self.apply(
            ts,
            Command::SubmitBid {
                participant: participant(who),
                key: key(k),
                qty: Qty(qty),
                kind: auction_core::BidKind::Resting {
                    limit: Price(limit),
                },
            },
        )
    }

    pub fn tick(&mut self, ts: Nanos) -> Vec<Event> {
        self.apply(ts, Command::Tick)
    }

    // ------------------------------------------------------------ assertions

    /// Units allocated to one participant, summed across fills.
    pub fn filled_by(&self, who: u128) -> u64 {
        let p = participant(who);
        self.state
            .fills()
            .iter()
            .filter(|f| f.participant == p)
            .map(|f| f.qty.0)
            .sum()
    }

    pub fn rejection_for(&self, k: u128) -> Option<RejectReason> {
        let k = key(k);
        self.events.iter().rev().find_map(|e| match e {
            Event::Rejected { key, reason } if *key == k => Some(*reason),
            _ => None,
        })
    }

    /// Check the invariants that must hold after *every* command, not just at clearing.
    pub fn assert_invariants(&self) {
        let s = &self.state;
        let filled = s.total_filled();

        // I1 — supply is never oversold, and the books balance.
        assert!(
            filled.0 <= s.config().total_supply.0,
            "I1 violated: filled {filled} exceeds supply {}",
            s.config().total_supply
        );
        assert_eq!(
            filled.0 + s.supply_remaining().0,
            s.config().total_supply.0,
            "I1 violated: filled + remaining != total supply"
        );

        // I3 — any clearing price is one the clock could actually have produced.
        if let Some(p) = s.clearing_price() {
            assert!(
                s.config().floor_price() <= p && p <= s.config().start_price(),
                "I3 violated: clearing price {p} is outside the published bounds"
            );
        }

        // I2 — a cleared auction has exactly one price, and it is the clearing price.
        if s.status() == Status::Cleared {
            let clearing = s.clearing_price().expect("cleared without a clearing price");
            for f in s.fills() {
                assert_eq!(f.price, clearing, "I2 violated: fill {} kept its own price", f.id);
            }
        }

        // I9 — nobody owes more than they posted.
        let mut owed: std::collections::BTreeMap<ParticipantId, i64> = Default::default();
        for f in s.fills() {
            *owed.entry(f.participant).or_default() +=
                f.consideration().expect("fill consideration overflowed");
        }
        for (p, amount) in owed {
            let limit = s.collateral_of(p).limit;
            assert!(
                amount <= limit,
                "I9 violated: {p} owes {amount} against a limit of {limit}"
            );
        }
    }
}
