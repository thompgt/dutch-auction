//! Where auction time comes from.
//!
//! `auction-core` never reads a clock — time arrives as data on every command, which is what
//! makes replay deterministic (invariant I5). Somebody has to actually read a clock, though, and
//! this is the only place in the system that does. Keeping it to one small type means the
//! non-deterministic part of the engine is a dozen lines that can be reasoned about in one
//! sitting, rather than a habit spread across the crate.
//!
//! # Auction time is elapsed time, not wall time
//!
//! [`Nanos`] counts from the moment the auction opened. That choice pays off at recovery: a
//! restarted process resumes auction time at the last timestamp it applied, so an outage
//! **pauses the price clock** instead of letting it descend through levels nobody could bid at.
//! A participant who was disconnected for those ten seconds did not miss ten seconds of price
//! decay, because there weren't any — the alternative would hand the auction's supply to whoever
//! happened to still be connected when the venue came back.
//!
//! The cost is that a live auction's price is no longer a pure function of the published start
//! *wall* time. That is the right trade for a venue whose fairness argument is that nobody can
//! be advantaged by being closer to the machine, but it is a real consequence and belongs in the
//! participant-facing rules, not just here.

use std::time::Instant;

use auction_proto::Nanos;

/// The mapping between this machine's monotonic clock and auction time.
///
/// `Instant` rather than `SystemTime` deliberately: auction time must never run backwards, and a
/// wall clock can — an NTP correction mid-auction would rewrite the order of events in the audit
/// record, which is the one thing the record exists not to do.
#[derive(Debug, Clone, Copy)]
pub struct Clock {
    /// The instant that corresponds to `Nanos::ZERO`.
    origin: Instant,
}

impl Clock {
    /// Start an auction clock at this instant. Elapsed time begins at zero.
    pub fn start() -> Clock {
        Clock {
            origin: Instant::now(),
        }
    }

    /// Resume a clock for an auction that is already `elapsed` old.
    ///
    /// Used after recovery and on a standby promotion: auction time continues from the last
    /// applied timestamp rather than from zero or from wall time. See the module note on why the
    /// downtime is not counted.
    pub fn resume(elapsed: Nanos) -> Clock {
        Clock {
            origin: Instant::now() - std::time::Duration::from_nanos(elapsed.0),
        }
    }

    /// Auction time right now. This is the only clock read in the system.
    pub fn now(&self) -> Nanos {
        Nanos(self.origin.elapsed().as_nanos().min(u64::MAX as u128) as u64)
    }
}

// There is deliberately no `instant_at(elapsed) -> Instant`. It reads like the obvious companion
// to `now()`, and it is a trap: auction deadlines are unbounded — a schedule whose floor is a
// century out is perfectly legal — and `Instant + Duration` panics on overflow rather than
// saturating. All deadline arithmetic therefore stays in `Nanos`, where the worst case is a
// number too large to be interesting rather than a panic in the one thread that owns the auction.

/// The next instant the engine has work to do on its own, with nothing arriving from outside.
///
/// The engine is otherwise entirely reactive, so this is what stops an open batch window from
/// sitting unmatched because the herd went quiet one microsecond after the last bid landed.
///
/// `None` means there is genuinely nothing pending and the engine may block indefinitely — an
/// idle auction must cost nothing, and a timer that fires a thousand times a second to discover
/// there is no work would put a thousand pointless commands a second into the audit record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Deadline(pub Nanos);

/// When the batch window containing `now` closes.
///
/// A zero width means batching is off and every bid matches on arrival, so there is no window to
/// close and no deadline to keep.
pub fn window_close(now: Nanos, width: Nanos) -> Option<Deadline> {
    if width.0 == 0 {
        return None;
    }
    let index = now.window_index(width);
    Some(Deadline(Nanos(
        index.saturating_add(1).saturating_mul(width.0),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_resumed_clock_continues_where_the_last_command_left_off() {
        let clock = Clock::resume(Nanos::from_secs(30));
        let now = clock.now();
        assert!(
            now >= Nanos::from_secs(30) && now < Nanos::from_secs(31),
            "resumed clock reported {now}"
        );
    }

    #[test]
    fn the_window_deadline_is_the_end_of_the_current_window_not_a_width_from_now() {
        let width = Nanos::from_millis(1);
        // Two thirds of the way through window 4: the deadline is the end of window 4, so the
        // window's total width is honoured rather than extended by every late arrival.
        let now = Nanos(4_666_666);
        assert_eq!(window_close(now, width), Some(Deadline(Nanos(5_000_000))));
        // Exactly on a boundary belongs to the window that is starting, not the one that ended.
        assert_eq!(
            window_close(Nanos(5_000_000), width),
            Some(Deadline(Nanos(6_000_000)))
        );
    }

    #[test]
    fn batching_off_means_no_window_deadline() {
        assert_eq!(window_close(Nanos(7), Nanos::ZERO), None);
    }
}
