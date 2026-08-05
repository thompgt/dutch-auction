//! The deterministic auction state machine.
//!
//! This crate is the heart of the system and deliberately the most boring one in the
//! workspace. It contains **no I/O, no clock reads, no randomness, and no async**. Time and
//! identity arrive as data on commands; the only thing this crate does is:
//!
//! ```text
//! apply(seq, ts, cmd) -> [event]
//! ```
//!
//! That signature is the whole architecture. Because it is deterministic, replaying the same
//! commands reproduces the same state — which is simultaneously crash recovery, the hot
//! standby, and the audit trail (see `docs/invariants.md`, I5).
//!
//! Phase 1 fills this in. Currently scaffolded.

#![forbid(unsafe_code)]

pub mod schedule;

pub use schedule::{PriceSchedule, ScheduleKind};
