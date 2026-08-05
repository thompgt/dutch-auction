//! Projections: the committed event stream becomes Postgres read models, settlement
//! instructions, and the audit export.
//!
//! Everything here is deliberately **off** the hot path and asynchronous. The database is a
//! consumer of the log, never a participant in bid acceptance — the moment a bid has to wait
//! on Postgres, the latency budget is gone.
//!
//! Because this is a projection of a totally-ordered log, it is idempotent by construction:
//! track the last applied sequence number and skip anything at or below it. A crashed
//! projector simply resumes.
//!
//! This crate also produces the audit export — the full command log plus derived events —
//! which is the regulator-facing artifact and the main reason the event-sourced design earns
//! its complexity.
//!
//! Phase 5 fills this in. Currently scaffolded.

#![forbid(unsafe_code)]
