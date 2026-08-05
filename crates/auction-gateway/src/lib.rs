//! The network edge.
//!
//! WebSocket is the primary bid transport: the connection is already warm when the clock
//! crosses a price, so a bid costs one frame rather than a TCP handshake plus a TLS handshake.
//! That alone removes tens of milliseconds versus REST. HTTP handles everything that is not
//! latency-sensitive.
//!
//! Hot-path work, in order, with the whole budget being 10 ms (`docs/slo.md`):
//!
//! 1. parse the frame
//! 2. session lookup in an in-memory map populated at connect — **never** a database read
//! 3. per-participant token-bucket rate limit
//! 4. in-memory collateral check
//! 5. send to the sequencer
//! 6. await the durability ack
//! 7. respond
//!
//! Ingress timestamps are captured at first-byte-read on the socket, not here, so queueing
//! inside our own process can never reorder participants.
//!
//! Phase 4 fills this in. Currently scaffolded.

#![forbid(unsafe_code)]
