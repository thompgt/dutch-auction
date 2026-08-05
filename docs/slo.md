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
