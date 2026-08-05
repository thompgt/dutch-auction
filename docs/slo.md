# Service Level Objectives

The latency contract. These numbers are the acceptance criterion for every phase — a change
that regresses them is not finished, regardless of what else it does.

## The requirement

**Bid acceptance: p99 ≤ 50 ms end-to-end**, measured from the client sending a bid to the client
receiving a durable acknowledgment.

Roughly 40 ms of that is client network and is not ours to control. So the number we actually
engineer, test, and gate on is the **server-side** figure, measured at the ingress socket:

| Percentile | Server-side budget |
|---|---|
| p50 | ≤ 3 ms |
| p99 | ≤ 10 ms |
| p99.9 | ≤ 25 ms |
| max (under herd load) | ≤ 100 ms |

## Where the 10 ms goes

| Stage | Budget | Notes |
|---|---|---|
| TLS termination + WS frame parse | 2.0 ms | Connection is already warm; no per-bid handshake |
| Auth | 0.5 ms | In-memory session cache populated at connect — zero DB reads per bid |
| Credit / collateral check | 0.2 ms | In-memory ledger, single comparison (see invariant I9) |
| Sequencer enqueue + engine apply | 0.1 ms | Bounded channel send + pure state machine |
| **WAL group-commit fsync** | **4.0 ms** | The dominant cost, and the price of invariant I6 |
| Sync replication to hot standby | 1.5 ms | Toggleable to async; async removes this line |
| Response serialize + write | 1.0 ms | |
| Headroom | 0.7 ms | |
| **Total** | **10.0 ms** | |

Each boundary is instrumented as a Prometheus histogram, so a regression shows up as a specific
line in this table rather than as a vague slowdown.

### Measured: the engine-apply line

`cargo bench -p auction-core --bench apply`, release profile. The two rows a bid actually waits
on are comfortable against the 0.1 ms budget:

| Operation | Measured | Budget |
|---|---|---|
| Match one bid, batching off | ~2.1 µs | 100 µs |
| Admit one bid into an open window | ~3.5 µs | 100 µs |
| **Close a 5,000-bid window** | **~2.6 ms** | — |
| **Trigger 5,000 resting bids** | **~3.5 ms** | — |

The last two rows are the finding. Closing a herd-sized window is one synchronous burst that
sorts, walks price levels, allocates pro-rata, clears, and reprices every fill. Amortized it is
cheap — ~0.5 µs per participant — but it is not *charged* amortized: whichever command happens
to arrive first past the window boundary pays the whole 2.6 ms, and every command queued behind
it waits too. That is 26% of the p99 budget landing on one arbitrary participant.

The architecture already contains the answer, and Phase 3 has to actually deliver it: **the
engine's own clock tick must reach the sequencer before user traffic does at every window
boundary.** Ticks are commands (invariant I8), so if the tick wins the race the flush cost is
charged to the timer rather than to a participant's bid, and the herd's acks all leave together
right behind it. If it loses the race the cost is charged to a customer. This is a scheduling
requirement on the sequencer thread, not a matching-engine optimization, and the load harness in
Phase 7 must measure the flush-triggering command separately to prove which one is paying.

### Measured: the durability line

`cargo bench -p auction-wal --bench commit`, release profile, on an NVMe SSD. This is the 4 ms
line, the largest single item in the budget:

| Operation | Measured | Per record |
|---|---|---|
| Bare `fsync`, one record | 1.36 ms | 1.36 ms |
| Append without syncing | 0.40 µs | 0.40 µs |
| Group commit, batch of 1 | 1.99 ms | 1.99 ms |
| Group commit, batch of 16 | 1.93 ms | 121 µs |
| Group commit, batch of 256 | 2.06 ms | 8 µs |
| **Group commit, batch of 4,096** | **3.43 ms** | **0.84 µs** |

The shape is the argument for the whole mechanism: a batch of 4,096 costs about 1.7× a batch of
one. Durability is a fixed toll on the disk, not a per-bid cost, and group commit is what turns
16,000 bids per second from impossible into the same flush everyone was already waiting for.
Even the worst case — a herd-sized batch — lands at 3.43 ms against a 4 ms budget.

The batch-of-one figure is the honest floor: 1.36 ms of `fsync` plus the 500 µs linger the bid
spends waiting for company that never arrives. A lone bid on an idle auction pays for batching
and gets nothing back. That is the right trade — the auction's hard moment is the herd, not the
quiet — but it is a real cost and it is why the linger is 500 µs rather than the millisecond the
architecture sketch assumed.

Two bugs were found by taking that measurement rather than asserting it, and both were invisible
to every correctness test in the crate:

1. **The linger was parking on a timer.** A 500 µs park rounds up to the OS timer granularity —
   ~15.6 ms on stock Windows — so a single-record commit cost 13.6 ms, ten times a bare `fsync`.
   Batches of 16 and 256 cost *exactly the same* 13 ms, and identical cost across a 16× change in
   batch size is not a disk, it is a timer.
2. **Then the spin that replaced it was too greedy.** A bare `spin_loop` on `try_recv` hammers
   the same channel head the producers are pushing into, so the writer spent its linger stealing
   the cache line from the threads it was waiting on. Small batches got faster; the herd-sized
   batch went from 4.2 ms to 9.9 ms. Backing off — spin briefly, then yield — fixed both, and is
   what the table above measures.

A 500 µs batching window that actually waits 13 ms fails no test. It just quietly spends the
entire p99 budget on a sleep.

## Percentiles, not averages

All latency is reported as p50 / p99 / p99.9. Averages are not reported anywhere in the
dashboards or the CI output. An average hides precisely the behavior this system exists to
control: the correlated spike when everyone bids at once.

## The load profile that matters

Steady-state throughput is not the failure mode. The gate is a **thundering herd**: N warm
WebSocket connections all firing bids inside a ~10 ms window as the clock crosses a round
price. That is what a real Dutch auction does at every step boundary.

| Scenario | Target |
|---|---|
| 10,000 concurrent connections, idle | < 5% CPU, stable memory |
| 5,000 bids inside a 10 ms window | p99 ≤ 10 ms, zero oversell, zero lost acks |
| Sustained 20,000 bids/sec | p99 ≤ 10 ms |
| Overload (2× capacity) | Explicit `Busy` rejections, p99 holds for accepted bids |

The overload row is the important one: under excess load the system must **shed and stay fast**,
not queue and go slow. A bid rejected with `Busy` is a promise it did not execute (invariant
I10); a bid sitting in an unbounded queue is a lie about what is happening.

## Correctness gates, checked alongside latency

Latency numbers are meaningless if the auction is wrong, so every load run also asserts:

- zero oversell (I1)
- every acked bid present after the run (I6)
- one clearing price across all fills (I2)
- gapless sequence numbers (I4)

## Availability

| Metric | Target |
|---|---|
| Auction availability during a live auction | 99.99% |
| Failover (primary loss → standby serving) | ≤ 5 s, no lost acked bids |
| Data loss on primary failure | Zero acked bids (sync replication) |

## Enforcement

The herd scenario runs in CI at smoke scale on every PR and at full scale nightly. CI fails on a
p99 regression beyond 20% of the budget line, so drift is caught as it is introduced rather than
discovered during an auction.
