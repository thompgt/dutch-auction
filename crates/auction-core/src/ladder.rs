//! The resting-bid ladder.
//!
//! Resting bids are committed in advance and triggered by the clock rather than by a client
//! message, so at any instant the engine needs one question answered fast: *which bids does the
//! current price now satisfy?* A `BTreeMap` keyed by limit price answers it with a range scan
//! and, unlike a hash map, iterates in a deterministic order — which invariant I5 requires,
//! since ladder order decides who fills first.
//!
//! Within a price level, order is arrival order (`VecDeque`, push back / pop front). Ties at a
//! price are therefore broken by sequence number, never by anything address- or hash-dependent.

use std::collections::{BTreeMap, VecDeque};

use auction_proto::{IdempotencyKey, ParticipantId, Price, Qty, Seq};

/// A bid waiting for the clock to reach its limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RestingBid {
    /// The submitting command's position in the total order. Doubles as the tiebreak.
    pub seq: Seq,
    pub participant: ParticipantId,
    pub key: IdempotencyKey,
    /// Units still wanted. A partial fill reduces this and the bid stays on the ladder.
    pub qty: Qty,
    pub limit: Price,
}

/// Resting bids indexed by limit price, highest first when drained.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PriceLadder {
    levels: BTreeMap<Price, VecDeque<RestingBid>>,
    /// Maintained alongside the map so callers can ask "is anything resting?" without walking.
    total_qty: Qty,
}

impl PriceLadder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.levels.is_empty()
    }

    /// Total units resting across all levels.
    pub fn total_qty(&self) -> Qty {
        self.total_qty
    }

    /// Number of resting bids. Linear in the number of levels; used by tests and metrics, not
    /// on the hot path.
    pub fn len(&self) -> usize {
        self.levels.values().map(VecDeque::len).sum()
    }

    /// The highest limit resting on the ladder, which — because the clock only descends — is the
    /// next one it will reach.
    ///
    /// This is how the engine knows when to wake up. Without it the only way to discover that a
    /// resting bid became triggerable would be to tick continuously and check, which would write
    /// a command a millisecond into the audit record for the privilege of finding nothing to do.
    pub fn next_trigger(&self) -> Option<Price> {
        self.levels.keys().next_back().copied()
    }

    /// Place a bid on the ladder.
    pub fn insert(&mut self, bid: RestingBid) {
        self.total_qty = self.total_qty.saturating_add(bid.qty);
        self.levels.entry(bid.limit).or_default().push_back(bid);
    }

    /// Remove and return every bid whose limit the clock has reached, i.e. `limit >= price`.
    ///
    /// Returned highest limit first, and within a level in arrival order — the same priority the
    /// batch window then applies, so a drained bid keeps its place among bids that arrived by
    /// message.
    pub fn drain_triggered(&mut self, price: Price) -> Vec<RestingBid> {
        // Everything at or above `price` is now satisfiable; the rest of the map stays put.
        let triggered = self.levels.split_off(&price);
        let mut out = Vec::new();
        // `BTreeMap` iterates ascending, so reverse for highest-limit-first priority.
        for (_, level) in triggered.into_iter().rev() {
            for bid in level {
                self.total_qty = self.total_qty.saturating_sub(bid.qty);
                out.push(bid);
            }
        }
        out
    }

    /// Remove every resting bid, highest limit first.
    ///
    /// Used at clearing and at cancellation, where bids the clock never reached still need an
    /// outcome and still have collateral reserved against them (invariants I9, I10).
    pub fn drain_all(&mut self) -> Vec<RestingBid> {
        self.total_qty = Qty::ZERO;
        std::mem::take(&mut self.levels)
            .into_iter()
            .rev()
            .flat_map(|(_, level)| level.into_iter())
            .collect()
    }

    /// Withdraw a bid by its submission key, returning the units freed.
    ///
    /// Linear in ladder size. Cancellation is a rare, human-speed operation, and paying for a
    /// secondary index on every insert to speed it up would tax the path that actually matters.
    pub fn remove(
        &mut self,
        participant: ParticipantId,
        key: IdempotencyKey,
    ) -> Option<RestingBid> {
        let mut found = None;
        for level in self.levels.values_mut() {
            if let Some(pos) = level
                .iter()
                .position(|b| b.key == key && b.participant == participant)
            {
                found = level.remove(pos);
                break;
            }
        }
        if let Some(bid) = found {
            self.total_qty = self.total_qty.saturating_sub(bid.qty);
            self.levels.retain(|_, level| !level.is_empty());
        }
        found
    }

    /// Put a partially-filled bid back at the front of its level, so it keeps the priority it
    /// earned rather than going to the back of the queue behind bids that arrived later.
    pub fn requeue(&mut self, bid: RestingBid) {
        if bid.qty.is_zero() {
            return;
        }
        self.total_qty = self.total_qty.saturating_add(bid.qty);
        self.levels.entry(bid.limit).or_default().push_front(bid);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bid(seq: u64, limit: i64, qty: u64) -> RestingBid {
        RestingBid {
            seq: Seq(seq),
            participant: ParticipantId::new(),
            key: IdempotencyKey(uuid::Uuid::from_u128(seq.into())),
            qty: Qty(qty),
            limit: Price(limit),
        }
    }

    #[test]
    fn only_bids_the_clock_has_reached_are_triggered() {
        let mut l = PriceLadder::new();
        l.insert(bid(1, 900, 10));
        l.insert(bid(2, 800, 10));
        l.insert(bid(3, 700, 10));

        let fired = l.drain_triggered(Price(800));
        assert_eq!(fired.len(), 2);
        // Highest limit first.
        assert_eq!(fired[0].limit, Price(900));
        assert_eq!(fired[1].limit, Price(800));
        // The 700 bid is untouched and its quantity is still accounted for.
        assert_eq!(l.len(), 1);
        assert_eq!(l.total_qty(), Qty(10));
    }

    #[test]
    fn ties_at_a_price_resolve_in_arrival_order() {
        let mut l = PriceLadder::new();
        l.insert(bid(5, 900, 1));
        l.insert(bid(6, 900, 1));
        l.insert(bid(7, 900, 1));

        let fired = l.drain_triggered(Price(900));
        assert_eq!(
            fired.iter().map(|b| b.seq.0).collect::<Vec<_>>(),
            vec![5, 6, 7]
        );
    }

    #[test]
    fn a_partial_fill_keeps_its_place_at_the_front() {
        let mut l = PriceLadder::new();
        l.insert(bid(1, 900, 10));
        let mut early = bid(0, 900, 4);
        early.qty = Qty(4);
        l.requeue(early);

        let fired = l.drain_triggered(Price(900));
        assert_eq!(fired[0].seq, Seq(0));
        assert_eq!(fired[0].qty, Qty(4));
    }

    #[test]
    fn cancelling_frees_the_units_and_prunes_the_level() {
        let mut l = PriceLadder::new();
        let b = bid(1, 900, 10);
        l.insert(b);
        assert_eq!(l.total_qty(), Qty(10));

        assert!(l.remove(b.participant, b.key).is_some());
        assert_eq!(l.total_qty(), Qty::ZERO);
        assert!(l.is_empty());
        // A second cancel is a no-op, not a panic.
        assert!(l.remove(b.participant, b.key).is_none());
    }

    #[test]
    fn a_cancel_from_the_wrong_participant_does_nothing() {
        let mut l = PriceLadder::new();
        let b = bid(1, 900, 10);
        l.insert(b);
        assert!(l.remove(ParticipantId::new(), b.key).is_none());
        assert_eq!(l.len(), 1);
    }
}
