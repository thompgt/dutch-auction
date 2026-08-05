//! Durability: an append-only log of *commands*, group commit, snapshots, and replay.
//!
//! Commands are logged rather than events because commands are smaller and events are
//! recomputable from them — the state machine is deterministic, so the command log is a
//! complete description of everything that happened.
//!
//! Group commit is where most of the latency budget goes (4 ms of 10 — see `docs/slo.md`) and
//! it is the price of invariant I6: no bid is acknowledged before it is durable. A writer
//! thread collects commands arriving within a short linger window, issues one `fsync`, then
//! releases every waiter at once, so a burst of a thousand bids costs one disk flush rather
//! than a thousand.
//!
//! # The shape of the thing
//!
//! ```text
//! submit ──> [ commit thread ] ──append──> wal-00000001.log
//!                   │                             │
//!                   └── fsync ──> watermark ──> waiters wake, bids are acked
//!
//! recover(dir) = newest usable snapshot + every command after it
//! ```
//!
//! # What this crate refuses to do
//!
//! It never acknowledges anything. [`Committer::wait_durable`] returning is the *fact* a caller
//! may act on; deciding what to tell the participant belongs to the gateway. Keeping that line
//! sharp is what makes invariant I6 checkable — there is exactly one place where "durable"
//! becomes true, and it is after an `fsync` returns.
//!
//! It also never invents a sequence number. Ordering is assigned upstream by the sequencer and
//! is only recorded here; a log that renumbered anything would no longer be the audit record.

#![forbid(unsafe_code)]

pub mod commit;
pub mod error;
pub mod record;
pub mod replay;
mod segment;
pub mod snapshot;
pub mod wal;

pub use commit::{CommitThread, Committer};
pub use error::{Result, WalError};
pub use record::LogRecord;
pub use replay::{recover, Recovered};
pub use snapshot::Snapshot;
pub use wal::{read_all, read_manifest, Wal, WalOptions};
