# Invariants

Properties the system must never violate. Each one is written to be **mechanically checkable**,
because each becomes a property test (`proptest`) in `crates/auction-core` or a harness assertion
in `crates/auction-wal` / `crates/auction-engine`.

These are stated before the code exists on purpose. An invariant discovered after the fact tends
to be a description of what the implementation happens to do; an invariant written first is a
constraint the implementation has to satisfy.

Notation: `S` is an `AuctionState`, `cmds` an arbitrary sequence of well-formed commands.

---

## I1 — Supply is never oversold

```
∀ S:  sum(f.qty for f in S.fills) <= S.total_supply
```

Holds after *every* command, not just at clearing. `supply_remaining` is `u64`, so an oversell
would have to underflow — the type makes the violation loud rather than silent, and the property
test asserts it anyway.

**Test:** apply randomized command sequences, including bids far exceeding supply and
concurrent-window bursts; assert after each apply.

## I2 — A cleared auction has one price

```
S.status == Cleared  ⟹  ∀ f ∈ S.fills: f.price == S.clearing_price
```

No fill may retain its original clock price after clearing. This is the invariant that makes
early bidding safe (see auction-rules §1), so it is the one most worth testing adversarially.

**Test:** run auctions to clearing from randomized bid streams; assert the fill price set has
cardinality ≤ 1 and equals `clearing_price`.

## I3 — The clearing price is reachable and correct

```
S.status == Cleared  ⟹  S.floor_price <= S.clearing_price <= S.start_price
                     ∧  S.clearing_price == price(t_exhaustion)
```

The clearing price must be a price the clock actually produced, not an artifact of matching.

## I4 — Sequence numbers are gapless and strictly monotonic

```
∀ consecutive applies:  seq_{n+1} == seq_n + 1
```

Per auction. A gap means a lost command; a repeat means a double-apply. Both are corruption of
the audit record, which is the artifact the whole event-sourced design exists to produce.

## I5 — Replay is byte-identical

```
replay(snapshot_at(k), wal[k..n]) == state_at(n)
```

Determinism is the load-bearing property of the architecture: it is what makes crash recovery,
the hot standby, and the audit export the *same mechanism*. If replay diverges, all three are
broken simultaneously.

The usual culprits, all banned in `auction-core`: floating point, `SystemTime`/`Instant`,
`HashMap` iteration order, and address-dependent ordering.

**Test:** apply N random commands twice from a fresh state; assert identical serialized state.
Separately, snapshot at a random k, replay the tail, and compare against the live state.

## I6 — Nothing is acknowledged before it is durable

```
ack(bid) happens-after fsync(wal_entry(bid))
```

A participant who receives a fill confirmation must never lose that fill to a crash. This is the
invariant that costs the most latency (§ the group-commit line in the budget) and it is not
negotiable for a financial venue.

**Test:** fault-injection harness SIGKILLs the process at randomized points mid-commit; on
recovery, assert every bid the client observed as acked is present in the recovered state.

## I7 — Idempotency keys are honored exactly once

```
apply(cmd with key k) twice  ⟹  second apply produces no new fill
                              ∧  returns the original outcome
```

Clients retry — on timeout, on reconnect, on user impatience. A retry that double-fills is
indistinguishable from a bug that double-fills.

## I8 — Total order includes the clock

Clock ticks and resting-bid triggers pass through the same sequencer as user bids. There is
exactly one total order over all events in an auction, and it is the WAL.

**Test:** assert no event exists whose sequence number is unordered with respect to a clock tick
in the same auction.

## I9 — Collateral is never exceeded

```
∀ participant p:  sum(f.qty * f.price for f in fills(p)) <= collateral(p)
```

Evaluated at the clock price at fill time. Because clearing reprices *downward* (I2, and the
clock only descends), a participant who was within collateral at fill time remains within it
after clearing — repricing can only reduce what they owe. Worth stating explicitly, since it is
the reason the pre-trade check can be a cheap in-memory comparison rather than a reservation
protocol.

## I10 — Rejections are total and informative

Every command produces an outcome: a fill, a partial fill, or a rejection carrying a machine-
readable reason. No command is silently dropped, including under load shedding — a shed bid is
rejected with `Busy`, which is a promise that it did *not* execute.

---

## Where each invariant is enforced

| Invariant | Enforced in | Verified by |
|---|---|---|
| I1, I2, I3, I7, I9, I10 | `auction-core` | `proptest` suite |
| I4, I5 | `auction-core` + `auction-wal` | replay + snapshot tests |
| I6 | `auction-wal` + `auction-gateway` | fault-injection harness |
| I8 | `auction-engine` | sequencer ordering test |
