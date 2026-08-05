//! Per-auction configuration.
//!
//! Everything the state machine needs to know that is *not* derived from the command stream.
//! It is fixed at creation and never mutated, which is what lets a replay start from the same
//! config and reach the same state (invariant I5).

use auction_proto::{AuctionId, Nanos, Price, Qty};

use crate::schedule::PriceSchedule;

/// The immutable terms of one auction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AuctionConfig {
    pub auction_id: AuctionId,
    /// Total units offered. Fixed; the engine never creates supply.
    pub total_supply: Qty,
    /// The published price clock.
    pub schedule: PriceSchedule,
    /// Width of the fairness batch window. `Nanos::ZERO` disables batching and matches every
    /// bid on arrival — faster, but it turns the auction into a latency race
    /// (`docs/auction-rules.md` §5). Default 1 ms.
    pub batch_window: Nanos,
    /// How far a market take's `expected_price` may differ from the true clock price before
    /// the bid is rejected. Zero means exact agreement is required.
    pub price_tolerance: u64,
}

impl AuctionConfig {
    /// A config with the defaults the rules document specifies: batching on at 1 ms, and a
    /// tolerance of one step (or one second of decay, for a linear clock).
    pub fn new(auction_id: AuctionId, total_supply: Qty, schedule: PriceSchedule) -> Self {
        Self {
            auction_id,
            total_supply,
            schedule,
            batch_window: Nanos::from_millis(1),
            price_tolerance: default_tolerance(&schedule),
        }
    }

    pub fn with_batch_window(mut self, width: Nanos) -> Self {
        self.batch_window = width;
        self
    }

    pub fn with_price_tolerance(mut self, tolerance: u64) -> Self {
        self.price_tolerance = tolerance;
        self
    }

    pub fn start_price(&self) -> Price {
        self.schedule.start_price
    }

    pub fn floor_price(&self) -> Price {
        self.schedule.floor_price
    }

    /// The clock price at `elapsed`.
    pub fn price_at(&self, elapsed: Nanos) -> Price {
        self.schedule.price_at(elapsed)
    }
}

/// One step's worth of decay: the smallest band that never rejects a client whose only sin was
/// being a step behind. A tighter band would reject honest bidders on every step boundary.
fn default_tolerance(schedule: &PriceSchedule) -> u64 {
    use crate::schedule::ScheduleKind::*;
    match schedule.kind {
        Stepped { step_size, .. } => step_size.unsigned_abs(),
        Linear { rate_per_sec } => rate_per_sec.unsigned_abs(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schedule::PriceSchedule;

    #[test]
    fn defaults_match_the_rules_document() {
        let c = AuctionConfig::new(
            AuctionId::new(),
            Qty(100),
            PriceSchedule::stepped(Price(1000), Price(100), Nanos::from_secs(1), 10),
        );
        assert_eq!(c.batch_window, Nanos::from_millis(1));
        assert_eq!(c.price_tolerance, 10);
    }
}
