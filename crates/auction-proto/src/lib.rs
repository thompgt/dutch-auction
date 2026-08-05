//! Shared domain primitives for the auction venue.
//!
//! Every crate in the workspace speaks these types, so they are defined once here
//! rather than duplicated across the engine, the gateway, and the projector.
//!
//! Two conventions are load-bearing and enforced by the types themselves:
//!
//! 1. **Money is integral.** [`Price`] wraps an `i64` of minor units. Floating point never
//!    appears in the price path — it would break replay determinism, and replay determinism
//!    is what crash recovery, the hot standby, and the audit trail are all built on.
//! 2. **Time is integral nanoseconds since auction open.** [`Nanos`] is a monotonic offset,
//!    not a wall clock, so a replay on a different machine at a different date produces
//!    identical state.

pub mod ids;
pub mod money;
pub mod time;

pub use ids::{AuctionId, IdempotencyKey, ParticipantId, Seq};
pub use money::{Price, Qty};
pub use time::Nanos;

/// Why the engine refused a command.
///
/// Every rejection is explicit and machine-readable — see invariant I10. In particular
/// [`RejectReason::Busy`] is a *promise* that the bid did not execute, which is what lets the
/// gateway shed load under overload instead of queueing and going slow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectReason {
    /// The auction is not accepting bids (pending, cleared, or cancelled).
    NotLive,
    /// The client's `expected_price` was outside the tolerance band. Carries the true price
    /// back to the client so it can retry against reality rather than guess.
    PriceMoved { server_price: Price },
    /// No units remain.
    SupplyExhausted,
    /// The participant's collateral does not cover the requested quantity at the current price.
    InsufficientCollateral,
    /// Quantity was zero.
    ZeroQuantity,
    /// The participant exceeded their bid rate limit.
    RateLimited,
    /// The system shed this bid under overload. It did *not* execute.
    Busy,
    /// A resting bid's limit price is above the current clock price, which would fill it
    /// immediately at a worse price than requested — almost always a client bug.
    LimitAboveClock { server_price: Price },
    /// The auction ended with this bid still resting: the clock never reached its limit.
    /// Not an error, but the participant is owed an explicit answer (invariant I10).
    Unfilled,
    /// No live bid exists under this key for this participant — already filled, already
    /// cancelled, or never submitted.
    UnknownBid,
}

impl std::fmt::Display for RejectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotLive => write!(f, "auction is not live"),
            Self::PriceMoved { server_price } => {
                write!(f, "price moved; server price is {server_price}")
            }
            Self::SupplyExhausted => write!(f, "supply exhausted"),
            Self::InsufficientCollateral => write!(f, "insufficient collateral"),
            Self::ZeroQuantity => write!(f, "quantity must be positive"),
            Self::RateLimited => write!(f, "rate limited"),
            Self::Busy => write!(f, "system busy; bid was not executed"),
            Self::LimitAboveClock { server_price } => {
                write!(f, "limit price is above the clock price of {server_price}")
            }
            Self::Unfilled => write!(f, "auction ended before the clock reached this bid"),
            Self::UnknownBid => write!(f, "no live bid under this key"),
        }
    }
}

/// Lifecycle of an auction. See `docs/auction-rules.md` §1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// Created but not yet open for bids.
    Pending,
    /// The clock is running and bids are accepted.
    Live,
    /// Supply was exhausted or the floor was reached. All fills carry the clearing price.
    Cleared,
    /// Cancelled by an operator. No settlement instructions are produced.
    Cancelled,
}

impl Status {
    /// Whether the auction accepts bids in this state.
    pub fn accepts_bids(self) -> bool {
        matches!(self, Self::Live)
    }

    /// Whether this is an end state.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Cleared | Self::Cancelled)
    }
}
