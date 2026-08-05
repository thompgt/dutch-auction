//! Events: everything the state machine says happened.
//!
//! Events are derived, not stored — the WAL holds commands, because commands are smaller and
//! events are recomputable from them. Events exist to be broadcast to participants and to be
//! projected into Postgres.
//!
//! Every command produces at least one event (invariant I10). There is no silent drop: a bid is
//! filled, queued, rested, or rejected with a machine-readable reason, always.

use auction_proto::{IdempotencyKey, ParticipantId, Price, Qty, RejectReason, Seq};

/// A unique handle for one allocation of units.
///
/// Distinct from [`Seq`] because a single resting bid can fill across several windows as supply
/// frees up, producing several fills from one command.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct FillId(pub u64);

impl std::fmt::Display for FillId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "fill_{}", self.0)
    }
}

/// Units allocated to a participant.
///
/// `price` is the clock price at allocation time until the auction clears, at which point every
/// fill is repriced to the uniform clearing price (invariant I2). Because the clock only
/// descends, that repricing can only ever reduce what a participant owes — which is precisely
/// why the collateral pre-check can be a cheap comparison rather than a reservation protocol
/// (invariant I9).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Fill {
    pub id: FillId,
    /// The command that produced this allocation.
    pub seq: Seq,
    pub participant: ParticipantId,
    pub key: IdempotencyKey,
    pub qty: Qty,
    pub price: Price,
}

impl Fill {
    /// What this fill is worth at its current price. `None` on overflow, which the engine
    /// treats as a nonsense order rather than a panic.
    pub fn consideration(&self) -> Option<i64> {
        self.price.checked_mul_qty(self.qty)
    }
}

/// What became of a submitted idempotency key.
///
/// Stored per key so that a retry returns the *original* answer instead of executing again
/// (invariant I7). A key admitted into an open batch window is [`Outcome::Queued`] until the
/// window closes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// Admitted into a batch window; matching has not run yet.
    Queued { qty: Qty },
    /// Sitting in the price ladder waiting for the clock.
    Resting { qty: Qty },
    /// Allocated. `qty` may be less than requested — partial fills are normal.
    Filled { qty: Qty, price: Price },
    /// Refused, with the reason.
    Rejected { reason: RejectReason },
    /// Withdrawn by the participant before it filled.
    Cancelled,
}

/// Something that happened, in sequence order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Event {
    /// The clock started.
    Opened { start_price: Price, supply: Qty },
    /// A participant's collateral ceiling changed.
    CollateralSet {
        participant: ParticipantId,
        limit: i64,
    },
    /// A bid was admitted into the current batch window. It has not been matched yet;
    /// the [`Event::Filled`] or [`Event::Rejected`] follows when the window closes.
    Queued {
        key: IdempotencyKey,
        participant: ParticipantId,
        qty: Qty,
        /// The price this bid will compete at.
        price: Price,
    },
    /// A resting bid entered the ladder. Not broadcast to other participants — resting bids are
    /// the sealed component of the mechanism.
    Rested {
        key: IdempotencyKey,
        participant: ParticipantId,
        qty: Qty,
        limit: Price,
    },
    /// A resting bid was withdrawn.
    RestingCancelled {
        key: IdempotencyKey,
        participant: ParticipantId,
        qty: Qty,
    },
    /// Units were allocated.
    Filled(Fill),
    /// A command was refused.
    Rejected {
        key: IdempotencyKey,
        reason: RejectReason,
    },
    /// An idempotency key was submitted again. Carries the original outcome; nothing executed.
    Duplicate {
        key: IdempotencyKey,
        outcome: Outcome,
    },
    /// A fill was repriced down to the clearing price. Emitted at clearing, per fill, so the
    /// record is never in a state where fills disagree about price.
    Repriced { id: FillId, from: Price, to: Price },
    /// Supply was exhausted, or the clock reached the floor. The auction is over.
    Cleared {
        price: Price,
        filled: Qty,
        unsold: Qty,
    },
    /// An operator cancelled the auction. No settlement instructions are produced.
    Cancelled,
}

/// The event batch one `apply` returns.
///
/// Four is the common case (a fill, maybe a clearing, maybe a couple of repricings); anything
/// larger spills to the heap, which only happens on the once-per-auction clearing path.
pub type Events = smallvec::SmallVec<[Event; 4]>;
