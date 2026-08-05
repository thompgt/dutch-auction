//! Integral money and quantity types.
//!
//! `Price` is minor units (cents, or the instrument's smallest increment) as `i64`. There is
//! deliberately no `From<f64>` and no `Mul<f64>`: if a float can never enter, replay can never
//! diverge, and invariant I5 holds by construction rather than by discipline.

use std::fmt;

/// A price in minor units.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct Price(pub i64);

impl Price {
    pub const ZERO: Price = Price(0);

    /// Total consideration for `qty` units at this price.
    ///
    /// Returns `None` on overflow rather than wrapping or panicking — an overflow here is a
    /// nonsense order, and the caller rejects it as such instead of the engine dying mid-auction.
    pub fn checked_mul_qty(self, qty: Qty) -> Option<i64> {
        i64::try_from(qty.0).ok().and_then(|q| self.0.checked_mul(q))
    }

    /// Clamp into `[floor, start]`. Used by the price clock, which must never emit a price
    /// outside the auction's published bounds (invariant I3).
    pub fn clamp_to(self, floor: Price, start: Price) -> Price {
        Price(self.0.clamp(floor.0, start.0))
    }

    /// Absolute difference, saturating. Used for the client price-tolerance band.
    pub fn abs_diff(self, other: Price) -> u64 {
        self.0.abs_diff(other.0)
    }
}

impl fmt::Display for Price {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A quantity of units. Units are indivisible, so this is a whole count.
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
pub struct Qty(pub u64);

impl Qty {
    pub const ZERO: Qty = Qty(0);

    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    /// The fillable amount when `self` is requested against `available` remaining.
    pub fn min_with(self, available: Qty) -> Qty {
        Qty(self.0.min(available.0))
    }

    /// Saturating subtraction. Saturating rather than wrapping so that a logic error shows up
    /// as a stuck-at-zero supply the tests catch, never as an oversell (invariant I1).
    pub fn saturating_sub(self, other: Qty) -> Qty {
        Qty(self.0.saturating_sub(other.0))
    }

    pub fn saturating_add(self, other: Qty) -> Qty {
        Qty(self.0.saturating_add(other.0))
    }
}

impl fmt::Display for Qty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consideration_rejects_overflow_instead_of_wrapping() {
        assert_eq!(Price(i64::MAX).checked_mul_qty(Qty(2)), None);
        assert_eq!(Price(100).checked_mul_qty(Qty(3)), Some(300));
    }

    #[test]
    fn fill_never_exceeds_available() {
        assert_eq!(Qty(500).min_with(Qty(10)), Qty(10));
        assert_eq!(Qty(5).min_with(Qty(10)), Qty(5));
    }

    #[test]
    fn oversubtraction_floors_at_zero_rather_than_wrapping() {
        assert_eq!(Qty(3).saturating_sub(Qty(9)), Qty::ZERO);
    }

    #[test]
    fn clock_price_stays_within_published_bounds() {
        assert_eq!(Price(5).clamp_to(Price(10), Price(100)), Price(10));
        assert_eq!(Price(500).clamp_to(Price(10), Price(100)), Price(100));
        assert_eq!(Price(50).clamp_to(Price(10), Price(100)), Price(50));
    }
}
