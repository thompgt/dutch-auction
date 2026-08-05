//! Load generator.
//!
//! The scenario that matters is not steady-state throughput — it is the **thundering herd**:
//! thousands of warm WebSocket connections all firing bids inside a ~10 ms window as the clock
//! crosses a round price. That is what a real Dutch auction does at every step boundary, and
//! it is the only load profile that exercises the behavior the SLO is written about.
//!
//! Latency is recorded in an HDR histogram and reported as p50 / p99 / p99.9. Averages are
//! never reported: an average hides exactly the correlated spike this system exists to control.
//!
//! Client-observed and server-observed latency are measured separately so network time can be
//! subtracted from service time rather than guessed at.
//!
//! Phase 7 fills this in. Currently scaffolded.

fn main() {
    println!("auction-load: scaffolded; the herd scenario lands in Phase 7");
}
