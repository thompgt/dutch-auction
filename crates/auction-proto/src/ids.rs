//! Identifiers.

use std::fmt;
use uuid::Uuid;

macro_rules! uuid_id {
    ($(#[$m:meta])* $name:ident, $prefix:literal) => {
        $(#[$m])*
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash,
            serde::Serialize, serde::Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub Uuid);

        impl $name {
            /// Generate a fresh identifier.
            ///
            /// Never call this inside `auction-core`: randomness in the state machine breaks
            /// replay determinism (invariant I5). Identifiers are minted at the edge and
            /// arrive on commands as data.
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!($prefix, "_{}"), self.0)
            }
        }
    };
}

uuid_id!(
    /// Identifies one auction.
    AuctionId,
    "auc"
);

uuid_id!(
    /// Identifies one bidding participant.
    ParticipantId,
    "par"
);

/// A client-supplied key that makes bid submission safely retryable.
///
/// Clients retry — on timeout, on reconnect, on user impatience. Replaying a key returns the
/// *original* outcome rather than filling a second time (invariant I7).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct IdempotencyKey(pub Uuid);

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "idem_{}", self.0)
    }
}

/// A position in an auction's total order.
///
/// Assigned by the sequencer, gapless and strictly increasing per auction (invariant I4). The
/// sequence of commands indexed by `Seq` *is* the audit record, and it doubles as the batch
/// tiebreak so that ties resolve deterministically rather than by arrival order.
#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(transparent)]
pub struct Seq(pub u64);

impl Seq {
    pub const START: Seq = Seq(0);

    /// The next position. Panics on overflow, which at one bid per nanosecond would take
    /// roughly 584 years — if it ever fires, something is very wrong and stopping is correct.
    pub fn next(self) -> Seq {
        Seq(self.0.checked_add(1).expect("sequence number overflow"))
    }
}

impl fmt::Display for Seq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequence_is_gapless_and_increasing() {
        let mut s = Seq::START;
        for expected in 1..=100 {
            s = s.next();
            assert_eq!(s.0, expected);
        }
    }

    #[test]
    fn ids_are_distinct() {
        assert_ne!(AuctionId::new(), AuctionId::new());
    }
}
