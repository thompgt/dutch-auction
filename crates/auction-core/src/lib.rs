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
//! The pieces, in the order they matter:
//!
//! - [`schedule`] — the price clock, a pure function of elapsed time
//! - [`command`] / [`event`] — the alphabet the machine reads and writes
//! - [`ladder`] — resting bids indexed by limit price, triggered by the clock
//! - [`state`] — [`AuctionState::apply`], batch-window matching, clearing and repricing

#![forbid(unsafe_code)]

pub mod command;
pub mod config;
pub mod event;
pub mod ladder;
pub mod schedule;
pub mod state;

pub use command::{BidKind, Command};
pub use config::AuctionConfig;
pub use event::{Event, Events, Fill, FillId, Outcome};
pub use ladder::{PriceLadder, RestingBid};
pub use schedule::{PriceSchedule, ScheduleKind};
pub use state::{AuctionState, Collateral};
