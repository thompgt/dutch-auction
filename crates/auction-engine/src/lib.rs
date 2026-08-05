//! The sequencer, the per-auction writer thread, the clock, and replication.
//!
//! One pinned OS thread owns each auction's state. Every command — user bids *and* clock
//! ticks — enters through a bounded channel whose ordering **is** the auction's ordering
//! (invariant I8). Because there is exactly one writer, oversell is structurally impossible
//! and no lock or database transaction appears anywhere on the hot path.
//!
//! The channel is bounded on purpose. An unbounded queue converts overload into unbounded
//! latency, which is the failure mode this system exists to avoid. When it fills, the engine
//! sheds with an explicit `Busy` rejection — a promise that the bid did not execute — rather
//! than accepting work it cannot serve in time.
//!
//! Phase 3 fills this in. Currently scaffolded.

#![forbid(unsafe_code)]
