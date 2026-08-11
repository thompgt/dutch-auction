# dutch-auction

[![CI](https://github.com/thompgt/dutch-auction/actions/workflows/ci.yml/badge.svg)](https://github.com/thompgt/dutch-auction/actions/workflows/ci.yml)

A production-grade **multi-unit descending-price (Dutch) auction** venue, built for tail latency.

Fixed supply, a price that decays on a published schedule, bidders take quantity at the
prevailing price, and every winner settles at a single **uniform clearing price** — the price
at which supply was exhausted.

The whole system is organized around one hard requirement: when the clock crosses a price
level and thousands of participants race for the remaining supply, bid acceptance must be
strictly ordered, must never oversell, must be auditable, and must acknowledge within
**p99 of 50 ms end-to-end** (server-side budget: **p99 ≤ 10 ms**, see [docs/slo.md](docs/slo.md)).

## Design in one paragraph

A **single-writer, event-sourced, in-memory matching engine**. Oversell and ordering are the
same problem — concurrent mutation of `remaining_supply` — so one thread owns each auction's
state and every bid is funneled through a sequencer that assigns a monotonic sequence number
and an authoritative timestamp. Correctness becomes trivial and the hot path becomes a straight
line with no database in it. Because the engine is a deterministic state machine
(`apply(seq, ts, cmd) -> [event]`), the same property buys crash recovery, a hot standby, and a
regulator-grade audit trail for free. Postgres is a downstream projection, never on the hot path.

## Documentation

| Doc | What it covers |
|---|---|
| [docs/auction-rules.md](docs/auction-rules.md) | Exact auction mechanics: the clock, bid types, clearing, repricing, fairness |
| [docs/invariants.md](docs/invariants.md) | Properties the engine must never violate — the source of the property-test suite |
| [docs/slo.md](docs/slo.md) | The latency budget and the acceptance criteria that gate every phase |
| [docs/recovery.md](docs/recovery.md) | What is on disk, and what to do when a process dies |

## Skills this exercises

| Area | Where it shows up |
|---|---|
| **Market microstructure / investment banking** | Uniform-price multi-unit auction in the Treasury/IPO mould: a descending clock, sealed resting limit bids, pro-rata allocation at the marginal price level, and retroactive repricing of every fill to the single clearing price. The mechanism design is the hard part — batching bids into fair windows so sub-millisecond colocation buys nothing, and integral minor units throughout because a float in a price is a settlement break waiting to happen. |
| **Rust** | The whole backend. `#![forbid(unsafe_code)]`, newtypes over primitives for money and time, ownership used to make the single-writer rule a compile-time fact rather than a convention, and `proptest` asserting the documented invariants against randomized command sequences. |
| **Concurrency** | A sequencer whose channel order *is* the auction's order; group commit where a thousand waiters are released by one `fsync` and one condition-variable notify; bounded queues that shed load rather than converting overload into unbounded latency. |
| **Storage and crash recovery** | An append-only segmented command log with checksummed framing, torn-tail truncation on open, atomic snapshot writes, and a single `recover()` path shared by crash recovery and the hot standby — so failover has no code of its own to get wrong. Tested by killing a real process mid-commit. |
| **Performance engineering** | A latency budget with a line per stage, benchmarked against the real numbers; percentiles rather than averages; and a thundering-herd load profile as the acceptance gate, because steady-state throughput is not the failure mode. |
| **Next.js / TypeScript** | *(Phase 6)* The price clock is a pure function of elapsed time, so clients compute it locally and the server broadcasts only genuine state changes — a 100k-viewer auction costs almost nothing in fan-out. Bid submission is optimistic-with-reconciliation, with rejection reasons as first-class UI states. |

## Layout

```
crates/
  auction-proto/      shared wire types — single source of truth
  auction-core/       pure deterministic state machine (no I/O, no clock, no async)
  auction-wal/        append-only log, group commit, snapshots, replay
  auction-engine/     core + wal + sequencer + replication; one thread per auction
  auction-gateway/    (stub) planned: WebSocket + HTTP edge, auth, rate limit, risk pre-checks
  auction-projector/  (stub) planned: event stream -> Postgres read models, settlement, audit
docs/                 the specification: rules, invariants, latency budget, recovery runbook
load/                 (stub) planned: thundering-herd load generator + latency analysis
```

Every directory above exists. The three marked `(stub)` are a doc comment and a `Cargo.toml`
apiece, filled in by Phases 4, 5 and 7 respectively — the Status table below is the authority on
what is actually built. The frontend (`web/`, Phase 6) and the deployment manifests (`deploy/`,
Phase 8) are not in the tree at all yet.

## Status

| Phase | State |
|---|---|
| 0 — repository, spec, invariants | done |
| 1 — `auction-core` state machine | done |
| 2 — `auction-wal` durability and replay | done |
| 3 — `auction-engine` sequencer, threading, replication | done |
| 4 — `auction-gateway` network edge | next |
| 5 — `auction-projector` + Postgres | |
| 6 — frontend | |
| 7 — observability and load testing | |
| 8 — HA, hardening, delivery | |

`auction-core` implements the full mechanism: the price clock, both bid types, the resting-bid
ladder, batch-window matching with pro-rata allocation at the marginal price, and uniform
clearing with retroactive repricing. Invariants I1, I2, I3, I5, I7, I9 and I10 are asserted by
the property suite; I4 is asserted by the engine itself on every apply.

`auction-wal` makes that state survive a power cut. It logs commands rather than events —
events are recomputable from a deterministic machine, so logging them would mean paying for
derived data twice — and batches appends into a single `fsync` that releases every waiter at
once. `recover()` is the only path back from disk, which is what makes crash recovery and the
hot standby the same mechanism rather than two that resemble each other. Invariant I6 is tested
by killing a real process mid-commit and checking that recovery honours every acknowledgement
it managed to make.

`auction-engine` is the single writer, and the thing worth pointing at is what it does with its
own clock. Closing a herd-sized batch window costs ~2.6 ms, and whoever's command triggers it
pays all of it — so the engine checks its own deadline *before* taking anything from the ingress
queue, and the command it applies at a window boundary is its own `Tick`. The flush is charged to
the timer instead of to a participant, and the herd's acknowledgements leave together right
behind it. Only three things can ever come due — a window boundary, a resting bid's limit, the
floor — and each is cleared by the tick it schedules, so an idle auction has no deadline at all
and writes nothing at all. The engine never waits for an `fsync` either: acknowledgement happens
on its own thread, so the flush for one command overlaps the matching of the next while still
landing strictly after the disk (I6). The standby is fed by the same path and applies records
through the same `follow()` recovery uses, which is why failover has no code of its own.

## Development

Requires a Rust toolchain (stable); `rust-toolchain.toml` pins the channel and the components.
Node 22+ becomes a requirement in Phase 6, when the frontend arrives.

```sh
cargo test --workspace                            # unit, scenario, and property tests
cargo bench -p auction-core --bench apply         # matching latency, see docs/slo.md
cargo bench -p auction-wal  --bench commit        # durability latency, see docs/slo.md
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all
```

### CI

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs the same commands on every push to
`main` and every pull request: `cargo fmt --check`, clippy with warnings denied, the whole test
suite — scenarios, properties, and the crash suite that kills a real process mid-commit — and a
compile-only pass over the benchmarks. The benchmarks are not *run* there, because a number
measured on a shared runner is a measurement of the runner; `docs/slo.md` records what they say
on real hardware.

### Windows

CI runs on Linux (`x86_64-unknown-linux-gnu`), so `rust-toolchain.toml` intentionally pins only
the channel, not a host triple. On Windows that resolves to whichever host rustup defaults to.
Without Visual Studio's C++ workload there is no `link.exe`, and the build fails at the link
step — use the GNU host instead, which links with mingw-w64 gcc:

```powershell
rustup set default-host x86_64-pc-windows-gnu
$env:Path = "C:\msys64\mingw64\bin;$env:Path"   # or wherever mingw-w64 gcc lives
```

Build from PowerShell, not Git Bash: Git Bash puts coreutils `link` ahead of MSVC's `link.exe`
on `PATH`, and the resulting error (`link: extra operand`) points nowhere useful.
