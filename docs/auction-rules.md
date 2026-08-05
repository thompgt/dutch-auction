# Auction Rules

The normative description of the auction mechanism. The engine in `crates/auction-core`
implements exactly this and nothing more; anything not stated here is not a rule.

## 1. Shape of an auction

An auction offers a fixed **supply** of identical, indivisible units. Price starts high and
decays on a published schedule. Participants take quantity at the prevailing price. When
cumulative demand meets supply, the auction **clears**: the clock stops and *every* fill —
including fills that occurred earlier at higher clock prices — settles at one **uniform
clearing price**.

Repricing earlier fills down to the clearing price is what makes early participation safe.
Without it, bidding early is strictly punished and every rational participant waits, which
collapses the mechanism. This is the same reason Treasury auctions are uniform-price.

### Lifecycle

```
Pending ──open──> Live ──supply exhausted──> Cleared
                   │
                   ├──floor price reached, supply remains──> Cleared (partial)
                   └──operator cancel──> Cancelled
```

An auction that reaches its floor price with supply remaining still clears: the clearing price
is the floor, and unsold supply is simply unsold. An auction with zero fills clears at the floor
with no settlement instructions.

## 2. The price clock

Price is a **pure function** of `(start_price, floor_price, schedule, elapsed)`:

```
price(elapsed) = clamp(schedule.evaluate(start_price, elapsed), floor_price, start_price)
```

`schedule.evaluate` is deterministic and side-effect free. v1 ships two schedules:

- **Linear** — `start - rate * elapsed`
- **Stepped** — price holds flat for `step_duration`, then drops by `step_size`

Stepped is the default. A continuously sliding price invites participants to shave
microseconds off their reaction time for a fractionally better price; a stepped clock means
everyone within a step faces exactly the same price, and the only race is for supply, which
the batch window (§5) then defuses. Linear exists for cases where price granularity matters
more than fairness optics.

### Prices are integers

All prices and amounts are `i64` in **minor units** (cents, or the instrument's smallest
tradable increment). Floating point never appears in the price path. This is non-negotiable:
floats break determinism across platforms, which breaks replay, which breaks recovery, the
hot standby, and the audit trail all at once.

### The schedule is published, so clients compute price locally

The full schedule is published and signed when the auction opens. Clients evaluate the same
pure function locally and animate the price themselves. The server therefore **never pushes a
price tick** — it broadcasts only genuine state changes (supply remaining, fills, clearing).
A hundred thousand watchers cost approximately nothing in fan-out, and the price a client
displays is never stale by a network hop.

Clients estimate their offset from server time with an NTP-style probe over the same
WebSocket. Every bid carries `expected_price` — the price the client believed was in effect.

## 3. Bid types

### Market take

> "Give me up to Q units at whatever the clock says now."

Accepted if `|expected_price - server_price| <= tolerance` (per-auction config, default one
step). Outside the band the bid is **rejected with the true price**, never silently filled at a
worse one. A participant is entitled to be wrong about the price and to find out for free.

### Resting limit bid

> "Take up to Q units when the clock reaches P or below."

Committed in advance, held in a price-indexed ladder inside the engine, and triggered **by the
clock**, not by a client message. Resting bids are not visible to other participants — this is
the sealed component of the mechanism.

Resting bids are the main defense a participant without colocation has against being outraced:
they execute at the instant the clock crosses their price, with zero network latency in the
path. The UI should present them as the default way to bid.

## 4. Fills, partial fills, and clearing

A bid for Q units against R units remaining fills `min(Q, R)`. Partial fills are normal and
are reported as such; the unfilled remainder of a market take is discarded, and the unfilled
remainder of a resting bid stays resting at its price.

When R reaches zero the auction clears. The **clearing price** is the clock price at which the
final unit was allocated. Every fill in the auction is then repriced to that price and a
`Repriced` event is emitted per fill. Repricing is engine-emitted at clearing time, not a batch
job — the auction is never in a state where the fills on record disagree about price.

Settlement instructions produced at clearing state, per participant: units allocated, clearing
price, total consideration, and the collateral to release.

## 5. Fairness under contention

Pure arrival-order matching turns a Dutch auction into a latency arms race. The engine
therefore supports a **batch window** (per-auction config, **default 1 ms, on**).

Bids arriving within the same window are collected and matched together, ordered by
`(price, sequence_number)` with a deterministic tiebreak derived from the sequence number —
not by raw arrival order. Sub-millisecond advantage inside a window is worth nothing, while
honest participants lose at most one window of latency.

If demand within a window exceeds remaining supply, allocation is **pro-rata by requested
quantity**, with the remainder distributed by the deterministic tiebreak. Pro-rata is what
keeps a window from becoming a lottery that rewards spamming large quantities.

Two supporting rules:

- **Ingress timestamps are taken at first-byte-read on the socket**, not at engine entry, so
  queueing inside our own process can never reorder participants.
- **The clock is a command.** Price advances and resting-bid triggers enter the *same*
  sequencer as user bids, so there is exactly one total order over everything that happened.
  There is no separate timer path that could interleave ambiguously.

## 6. What the engine refuses

- A bid from a participant with insufficient collateral for `qty * current_price`
- A bid against an auction that is not `Live`
- A duplicate idempotency key (returns the *original* outcome, never a second fill)
- A quantity of zero or a negative quantity
- Any command that would drive `supply_remaining` below zero — structurally impossible under
  the single-writer design, and asserted anyway
