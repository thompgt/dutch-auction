//! Monotonic auction time.
//!
//! [`Nanos`] is nanoseconds **since the auction opened**, not a wall clock. The engine never
//! reads the system clock: time arrives as data on a command (see `Command::ClockTick`), which
//! is what makes a replay on a different machine, on a different day, produce identical state
//! (invariant I5).

use std::fmt;

/// Nanoseconds elapsed since the auction opened.
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
pub struct Nanos(pub u64);

impl Nanos {
    pub const ZERO: Nanos = Nanos(0);

    pub const fn from_millis(ms: u64) -> Nanos {
        Nanos(ms.saturating_mul(1_000_000))
    }

    pub const fn from_secs(s: u64) -> Nanos {
        Nanos(s.saturating_mul(1_000_000_000))
    }

    pub fn as_millis(self) -> u64 {
        self.0 / 1_000_000
    }

    pub fn as_secs(self) -> u64 {
        self.0 / 1_000_000_000
    }

    pub fn saturating_sub(self, other: Nanos) -> Nanos {
        Nanos(self.0.saturating_sub(other.0))
    }

    /// Which batch window this instant falls into, given a window width.
    ///
    /// Bids sharing a window index are matched together rather than in arrival order — this is
    /// the mechanism that makes sub-millisecond advantage worthless (`docs/auction-rules.md` §5).
    /// A zero width disables batching, putting every bid in its own window.
    pub fn window_index(self, width: Nanos) -> u64 {
        if width.0 == 0 {
            self.0
        } else {
            self.0 / width.0
        }
    }
}

impl fmt::Display for Nanos {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}ns", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bids_within_one_window_share_an_index() {
        let width = Nanos::from_millis(1);
        assert_eq!(Nanos(0).window_index(width), 0);
        assert_eq!(Nanos(999_999).window_index(width), 0);
        // One nanosecond later is a different window — the boundary is exact.
        assert_eq!(Nanos(1_000_000).window_index(width), 1);
    }

    #[test]
    fn zero_width_disables_batching() {
        assert_eq!(Nanos(7).window_index(Nanos::ZERO), 7);
        assert_eq!(Nanos(8).window_index(Nanos::ZERO), 8);
    }
}
