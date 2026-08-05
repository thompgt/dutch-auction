# dutch-auction

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

## Layout

```
crates/
  auction-proto/      shared wire types — single source of truth
  auction-core/       pure deterministic state machine (no I/O, no clock, no async)
  auction-wal/        append-only log, group commit, snapshots, replay
  auction-engine/     core + wal + sequencer + replication; one thread per auction
  auction-gateway/    axum: WebSocket + HTTP, auth, rate limit, risk pre-checks
  auction-projector/  event stream -> Postgres read models, settlement, audit export
web/                  Next.js + TypeScript frontend
load/                 thundering-herd load generator + latency analysis
deploy/               Docker, compose, k8s manifests, Grafana dashboards
```

## Status

| Phase | State |
|---|---|
| 0 — repository, spec, invariants | done |
| 1 — `auction-core` state machine | done |
| 2 — `auction-wal` durability and replay | done |
| 3 — `auction-engine` sequencer, threading, replication | next |
| 4 — `auction-gateway` network edge | |
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

## Development

Requires a Rust toolchain (stable) and Node 22+.

```sh
cargo test --workspace                            # unit, scenario, and property tests
cargo bench -p auction-core --bench apply         # matching latency, see docs/slo.md
cargo bench -p auction-wal  --bench commit        # durability latency, see docs/slo.md
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all
```

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
