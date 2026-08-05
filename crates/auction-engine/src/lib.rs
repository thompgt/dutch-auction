//! The sequencer, the per-auction writer thread, the clock, and replication.
//!
//! One OS thread owns each auction's state. Every command — user bids *and* clock ticks — enters
//! through a bounded channel whose ordering **is** the auction's ordering (invariant I8). Because
//! there is exactly one writer, oversell is structurally impossible and no lock or database
//! transaction appears anywhere on the hot path.
//!
//! The channel is bounded on purpose. An unbounded queue converts overload into unbounded
//! latency, which is the failure mode this system exists to avoid. When it fills, the engine
//! sheds with an explicit [`Shed::Busy`] — a promise that the bid did not execute — rather than
//! accepting work it cannot serve in time.
//!
//! ```text
//!  submit ─> [ ingress: bounded, sheds when full ]
//!                        │
//!                        v
//!            [ engine thread ]  seq, timestamp, apply       <- the only writer
//!                        │
//!            ┌───────────┴───────────┐
//!            v                       v
//!    [ wal commit thread ]    [ ack thread ] ─> reply + broadcast
//!         fsync, watermark ────────┘
//! ```
//!
//! Three threads and one direction of travel. The engine never blocks on the disk, the ack
//! thread never touches the state, and nothing is acknowledged before an `fsync` returns.
//!
//! - [`clock`] — the only clock read in the system
//! - [`ingress`] — the bounded door, and load shedding
//! - [`engine`] — the writer thread, the tick scheduler, and snapshots
//! - [`replication`] — the hot standby, which runs no code of its own

#![forbid(unsafe_code)]

pub mod clock;
pub mod engine;
pub mod ingress;
pub mod replication;

pub use clock::Clock;
pub use engine::{Auction, EngineOptions, Sequenced};
pub use ingress::{Ack, AuctionHandle, Shed};
pub use replication::{LocalReplica, Replica, ReplicaLost, ReplicationMode};
