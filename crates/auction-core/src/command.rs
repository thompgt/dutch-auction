//! Commands: everything that can happen to an auction.
//!
//! A command is pure data. It carries no clock reading and no randomness — the sequencer stamps
//! it with a [`Seq`](auction_proto::Seq) and an elapsed timestamp on the way in, and those
//! stamps travel with it into the WAL. Replaying the same commands in the same order therefore
//! reproduces the same state on any machine at any time (invariant I5).
//!
//! Note that [`Command::Tick`] is a command like any other. The clock does not get a side door
//! into the state machine; price advances and resting-bid triggers pass through the same total
//! order as user bids (invariant I8).
//!
//! These enums are externally tagged — the serde default — rather than carrying a `tag = "..."`
//! discriminant field. An internally tagged enum can only be *deserialized* from a
//! self-describing format, which would quietly rule out every compact binary encoding the log
//! might use. A prettier JSON shape is not worth constraining the format the audit record is
//! written in.

use auction_proto::{IdempotencyKey, ParticipantId, Price, Qty};

/// Something to be applied to an auction, in sequence order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Command {
    /// Start the clock. The timestamp of this command defines `elapsed == 0`.
    Open,
    /// Set a participant's collateral ceiling, in minor units.
    ///
    /// This flows through the log rather than being read from a ledger at bid time, because a
    /// replay has to see the same limits in the same order to reach the same state.
    SetCollateral {
        participant: ParticipantId,
        limit: i64,
    },
    /// Submit a bid. See [`BidKind`].
    SubmitBid {
        participant: ParticipantId,
        /// Makes the submission safely retryable — a replayed key returns the original outcome
        /// rather than filling twice (invariant I7).
        key: IdempotencyKey,
        qty: Qty,
        kind: BidKind,
    },
    /// Withdraw a resting bid that has not yet filled, identified by its submission key.
    CancelResting {
        participant: ParticipantId,
        key: IdempotencyKey,
    },
    /// Advance the clock to this command's timestamp: close any elapsed batch window and
    /// trigger resting bids the price has now reached.
    Tick,
    /// Operator cancellation. Produces no settlement instructions.
    Cancel,
}

/// The two ways to bid (`docs/auction-rules.md` §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BidKind {
    /// "Give me up to Q units at whatever the clock says now."
    ///
    /// `expected_price` is the price the client believed was in effect. If it disagrees with
    /// the server beyond the auction's tolerance the bid is rejected *with the true price*,
    /// never silently filled at a worse one.
    Take { expected_price: Price },
    /// "Take up to Q units when the clock reaches `limit` or below."
    ///
    /// Held in the price ladder and triggered by the clock, not by a client message — which is
    /// what makes it the defense against being outraced. Invisible to other participants.
    Resting { limit: Price },
}

impl BidKind {
    /// The price this bid competes at when it is matched.
    ///
    /// For a take that is the prevailing clock price; for a resting bid it is the limit the
    /// participant committed to, which is what gives an earlier, higher commitment priority.
    pub fn priority_price(&self, clock: Price) -> Price {
        match *self {
            BidKind::Take { .. } => clock,
            BidKind::Resting { limit } => limit,
        }
    }

    pub fn is_resting(&self) -> bool {
        matches!(self, BidKind::Resting { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_resting_bid_competes_at_its_limit_not_the_clock() {
        let clock = Price(900);
        assert_eq!(
            BidKind::Take {
                expected_price: Price(900)
            }
            .priority_price(clock),
            Price(900)
        );
        assert_eq!(
            BidKind::Resting { limit: Price(950) }.priority_price(clock),
            Price(950)
        );
    }
}
