//! Durability: an append-only log of *commands*, group commit, snapshots, and replay.
//!
//! Commands are logged rather than events because commands are smaller and events are
//! recomputable from them — the state machine is deterministic, so the command log is a
//! complete description of everything that happened.
//!
//! Group commit is where most of the latency budget goes (4 ms of 10 — see `docs/slo.md`) and
//! it is the price of invariant I6: no bid is acknowledged before it is durable. A writer task
//! collects commands arriving within roughly a millisecond, issues one `fsync`, then releases
//! every waiter at once, so a burst of a thousand bids costs one disk flush rather than a
//! thousand.
//!
//! Phase 2 fills this in. Currently scaffolded.

#![forbid(unsafe_code)]
