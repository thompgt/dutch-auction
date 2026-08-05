//! The price clock.
//!
//! Price is a pure function of elapsed time. That purity is what lets clients evaluate the
//! same function locally and animate the price themselves, so the server never has to push a
//! price tick — it broadcasts only real state changes. A hundred thousand watchers cost
//! essentially nothing in fan-out, and no client ever displays a price that is stale by a
//! network hop.
//!
//! See `docs/auction-rules.md` §2.

use auction_proto::{Nanos, Price};

/// How price decays over time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScheduleKind {
    /// Price holds flat for `step_duration`, then drops by `step_size`. **The default.**
    ///
    /// A continuously sliding price invites participants to shave microseconds off their
    /// reaction time for a fractionally better price. A stepped clock means everyone inside a
    /// step faces exactly the same price, so the only thing left to race for is supply — which
    /// the batch window then defuses.
    Stepped {
        step_duration: Nanos,
        step_size: i64,
    },
    /// Price falls continuously at `rate_per_sec` minor units per second.
    ///
    /// Available for cases where price granularity matters more than fairness optics.
    Linear { rate_per_sec: i64 },
}

/// A complete, published price schedule.
///
/// Published and signed when the auction opens, so every participant can compute the price
/// independently and verify the server agrees.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PriceSchedule {
    pub start_price: Price,
    pub floor_price: Price,
    pub kind: ScheduleKind,
}

impl PriceSchedule {
    /// A stepped schedule. Panics if `floor > start`, which is a malformed auction.
    pub fn stepped(
        start_price: Price,
        floor_price: Price,
        step_duration: Nanos,
        step_size: i64,
    ) -> Self {
        assert!(floor_price <= start_price, "floor price exceeds start price");
        assert!(step_size >= 0, "step size must be non-negative");
        Self {
            start_price,
            floor_price,
            kind: ScheduleKind::Stepped {
                step_duration,
                step_size,
            },
        }
    }

    /// A linear schedule. Panics if `floor > start`, which is a malformed auction.
    pub fn linear(start_price: Price, floor_price: Price, rate_per_sec: i64) -> Self {
        assert!(floor_price <= start_price, "floor price exceeds start price");
        assert!(rate_per_sec >= 0, "decay rate must be non-negative");
        Self {
            start_price,
            floor_price,
            kind: ScheduleKind::Linear { rate_per_sec },
        }
    }

    /// The price at `elapsed` nanoseconds after the auction opened.
    ///
    /// Always within `[floor_price, start_price]` (invariant I3), and always integral —
    /// integer division truncates toward zero, and since all inputs here are non-negative that
    /// is a floor, which is the conventional and monotonic choice for a descending clock.
    pub fn price_at(&self, elapsed: Nanos) -> Price {
        let drop = match self.kind {
            ScheduleKind::Stepped {
                step_duration,
                step_size,
            } => {
                if step_duration.0 == 0 {
                    // A zero-length step would drop infinitely fast; treat it as an immediate
                    // jump to the floor rather than dividing by zero.
                    return self.floor_price;
                }
                let steps = elapsed.0 / step_duration.0;
                saturating_drop(steps, step_size)
            }
            ScheduleKind::Linear { rate_per_sec } => {
                // Scale before dividing so sub-second precision is not lost. Both operands are
                // non-negative, so this cannot overflow into a negative drop.
                let ns = i128::from(elapsed.0);
                let rate = i128::from(rate_per_sec);
                let dropped = ns.saturating_mul(rate) / 1_000_000_000;
                i64::try_from(dropped).unwrap_or(i64::MAX)
            }
        };

        Price(self.start_price.0.saturating_sub(drop)).clamp_to(self.floor_price, self.start_price)
    }

    /// The earliest instant at which the price is at or below `target`.
    ///
    /// Used to schedule the clock tick that triggers resting bids, so the engine can sleep
    /// until a bid is actually due instead of polling. Returns `None` if the schedule never
    /// reaches `target` (i.e. `target` is below the floor).
    pub fn time_to_reach(&self, target: Price) -> Option<Nanos> {
        if target > self.start_price {
            return Some(Nanos::ZERO); // already at or below it
        }
        if target < self.floor_price {
            return None; // the clock never gets there
        }
        let drop_needed = self.start_price.0.saturating_sub(target.0);

        match self.kind {
            ScheduleKind::Stepped {
                step_duration,
                step_size,
            } => {
                if step_size == 0 {
                    // Price never moves; reachable only if it already satisfies the target.
                    return (drop_needed == 0).then_some(Nanos::ZERO);
                }
                // Ceiling division: we need the step that first meets or passes the target.
                let steps = ceil_div(i128::from(drop_needed), i128::from(step_size));
                Some(Nanos(
                    u64::try_from(steps)
                        .unwrap_or(u64::MAX)
                        .saturating_mul(step_duration.0.max(1)),
                ))
            }
            ScheduleKind::Linear { rate_per_sec } => {
                if rate_per_sec == 0 {
                    return (drop_needed == 0).then_some(Nanos::ZERO);
                }
                let ns = ceil_div(
                    i128::from(drop_needed).saturating_mul(1_000_000_000),
                    i128::from(rate_per_sec),
                );
                Some(Nanos(u64::try_from(ns).unwrap_or(u64::MAX)))
            }
        }
    }

    /// Whether the clock has bottomed out at the floor by `elapsed`.
    pub fn at_floor(&self, elapsed: Nanos) -> bool {
        self.price_at(elapsed) == self.floor_price
    }
}

/// Ceiling division for non-negative operands.
///
/// `i128::div_ceil` is still unstable, and rounding *up* is what makes `time_to_reach` return
/// the first instant at which the price has actually met the target rather than the last
/// instant at which it had not.
fn ceil_div(numerator: i128, denominator: i128) -> i128 {
    debug_assert!(numerator >= 0 && denominator > 0);
    (numerator + denominator - 1) / denominator
}

/// `steps * step_size`, saturating. A very long-running auction should bottom out at the floor,
/// not wrap around to a negative drop and send the price back up.
fn saturating_drop(steps: u64, step_size: i64) -> i64 {
    i64::try_from(steps)
        .unwrap_or(i64::MAX)
        .saturating_mul(step_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stepped() -> PriceSchedule {
        // 1000 -> 100, dropping 10 every second.
        PriceSchedule::stepped(Price(1000), Price(100), Nanos::from_secs(1), 10)
    }

    fn linear() -> PriceSchedule {
        // 1000 -> 100, dropping 10 per second continuously.
        PriceSchedule::linear(Price(1000), Price(100), 10)
    }

    #[test]
    fn stepped_price_holds_flat_within_a_step() {
        let s = stepped();
        assert_eq!(s.price_at(Nanos::ZERO), Price(1000));
        assert_eq!(s.price_at(Nanos::from_millis(999)), Price(1000));
        // The step boundary is exact.
        assert_eq!(s.price_at(Nanos::from_secs(1)), Price(990));
        assert_eq!(s.price_at(Nanos::from_millis(1999)), Price(990));
        assert_eq!(s.price_at(Nanos::from_secs(2)), Price(980));
    }

    #[test]
    fn linear_price_falls_continuously_with_subsecond_precision() {
        let s = linear();
        assert_eq!(s.price_at(Nanos::ZERO), Price(1000));
        assert_eq!(s.price_at(Nanos::from_millis(100)), Price(999));
        assert_eq!(s.price_at(Nanos::from_secs(1)), Price(990));
        assert_eq!(s.price_at(Nanos::from_secs(10)), Price(900));
    }

    #[test]
    fn price_never_falls_below_the_floor() {
        for s in [stepped(), linear()] {
            assert_eq!(s.price_at(Nanos::from_secs(90)), Price(100));
            assert_eq!(s.price_at(Nanos::from_secs(100_000)), Price(100));
            // Far enough out to overflow a naive multiplication.
            assert_eq!(s.price_at(Nanos(u64::MAX)), Price(100));
        }
    }

    #[test]
    fn price_is_monotonically_non_increasing() {
        for s in [stepped(), linear()] {
            let mut prev = Price(i64::MAX);
            for t in (0..120).map(Nanos::from_secs) {
                let p = s.price_at(t);
                assert!(p <= prev, "price rose at {t}: {prev} -> {p}");
                prev = p;
            }
        }
    }

    #[test]
    fn time_to_reach_lands_at_or_below_the_target() {
        for s in [stepped(), linear()] {
            for target in [1000, 995, 990, 500, 101, 100].map(Price) {
                let t = s.time_to_reach(target).expect("target is above the floor");
                assert!(
                    s.price_at(t) <= target,
                    "at {t} price is {} which is above target {target}",
                    s.price_at(t)
                );
            }
        }
    }

    #[test]
    fn time_to_reach_is_the_earliest_such_instant() {
        let s = stepped();
        // 990 is first reached at exactly 1s, not before.
        let t = s.time_to_reach(Price(990)).unwrap();
        assert_eq!(t, Nanos::from_secs(1));
        assert!(s.price_at(Nanos(t.0 - 1)) > Price(990));
    }

    #[test]
    fn a_target_below_the_floor_is_never_reached() {
        for s in [stepped(), linear()] {
            assert_eq!(s.time_to_reach(Price(99)), None);
        }
    }

    #[test]
    fn a_flat_schedule_never_moves() {
        let s = PriceSchedule::linear(Price(500), Price(100), 0);
        assert_eq!(s.price_at(Nanos::from_secs(10_000)), Price(500));
        assert_eq!(s.time_to_reach(Price(499)), None);
        assert_eq!(s.time_to_reach(Price(500)), Some(Nanos::ZERO));
    }

    #[test]
    fn at_floor_reports_the_end_of_the_clock() {
        let s = stepped();
        assert!(!s.at_floor(Nanos::from_secs(10)));
        assert!(s.at_floor(Nanos::from_secs(90)));
    }
}
