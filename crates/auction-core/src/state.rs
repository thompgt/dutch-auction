//! The auction state machine.
//!
//! One `apply(seq, ts, cmd) -> [event]` and nothing else. No I/O, no clock read, no allocation
//! the hot path can avoid, no randomness, no floating point. That austerity is not aesthetic:
//! it is what makes crash recovery, the hot standby, and the audit export the *same mechanism*
//! (invariant I5). Everything that could differ between two machines arrives as data on the
//! command.
//!
//! `ts` is nanoseconds since the auction opened, stamped by the sequencer. The state machine
//! never asks what time it is.

use std::collections::BTreeMap;

use auction_proto::{IdempotencyKey, Nanos, ParticipantId, Price, Qty, RejectReason, Seq, Status};

use crate::command::{BidKind, Command};
use crate::config::AuctionConfig;
use crate::event::{Event, Events, Fill, FillId, Outcome};
use crate::ladder::{PriceLadder, RestingBid};

/// A participant's spending ceiling and what is currently committed against it.
///
/// `committed` moves with reservations, not just fills: a bid is reserved against at admission
/// and released if it does not fill, so a participant can never have more outstanding exposure
/// than they posted (invariant I9).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Collateral {
    pub limit: i64,
    pub committed: i64,
}

impl Collateral {
    pub fn available(&self) -> i64 {
        self.limit.saturating_sub(self.committed)
    }
}

/// A bid admitted into the current batch window, awaiting matching.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct PendingBid {
    seq: Seq,
    participant: ParticipantId,
    key: IdempotencyKey,
    qty: Qty,
    /// The price this bid competes at: the clock for a take, the limit for a resting bid.
    /// Ordering only — everyone in a window fills at the *window* price.
    priority: Price,
    /// The price collateral was reserved at, so the release is exact.
    reserve_price: Price,
    /// `Some(limit)` if this came off the ladder, so an unfilled remainder can go back.
    resting_limit: Option<Price>,
}

/// The batch window currently accumulating bids.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct OpenWindow {
    index: u64,
    /// The single price every bid in this window fills at.
    price: Price,
    bids: Vec<PendingBid>,
}

/// Everything one auction knows about itself.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AuctionState {
    config: AuctionConfig,
    status: Status,
    supply_remaining: Qty,
    clearing_price: Option<Price>,
    fills: Vec<Fill>,
    next_fill_id: u64,
    resting: PriceLadder,
    window: Option<OpenWindow>,
    /// What happened to each idempotency key (invariant I7).
    outcomes: BTreeMap<IdempotencyKey, Outcome>,
    collateral: BTreeMap<ParticipantId, Collateral>,
    last_seq: Option<Seq>,
    now: Nanos,
}

impl AuctionState {
    pub fn new(config: AuctionConfig) -> Self {
        Self {
            supply_remaining: config.total_supply,
            config,
            status: Status::Pending,
            clearing_price: None,
            fills: Vec::new(),
            next_fill_id: 0,
            resting: PriceLadder::new(),
            window: None,
            outcomes: BTreeMap::new(),
            collateral: BTreeMap::new(),
            last_seq: None,
            now: Nanos::ZERO,
        }
    }

    // ---------------------------------------------------------------- accessors

    pub fn config(&self) -> &AuctionConfig {
        &self.config
    }
    pub fn status(&self) -> Status {
        self.status
    }
    pub fn supply_remaining(&self) -> Qty {
        self.supply_remaining
    }
    pub fn clearing_price(&self) -> Option<Price> {
        self.clearing_price
    }
    pub fn fills(&self) -> &[Fill] {
        &self.fills
    }
    pub fn resting(&self) -> &PriceLadder {
        &self.resting
    }
    pub fn now(&self) -> Nanos {
        self.now
    }
    /// The last sequence number applied, or `None` if nothing has been applied yet.
    ///
    /// Recovery needs this to know where in the log to resume: a snapshot is only useful if it
    /// says which command it corresponds to.
    pub fn last_seq(&self) -> Option<Seq> {
        self.last_seq
    }
    /// The batch window currently accumulating bids, as `(index, bids admitted)`.
    ///
    /// The engine needs this to know whether it owes the auction a tick. An open window must be
    /// matched at its own boundary even if not one further command ever arrives — otherwise the
    /// last bids of a quiet moment sit unmatched until somebody else happens to bid, which is a
    /// fairness bug wearing a latency bug's clothes.
    pub fn open_window(&self) -> Option<(u64, usize)> {
        self.window.as_ref().map(|w| (w.index, w.bids.len()))
    }

    pub fn outcome(&self, key: IdempotencyKey) -> Option<Outcome> {
        self.outcomes.get(&key).copied()
    }
    pub fn collateral_of(&self, participant: ParticipantId) -> Collateral {
        self.collateral
            .get(&participant)
            .copied()
            .unwrap_or_default()
    }
    /// Units allocated so far. Never exceeds `total_supply` (invariant I1).
    pub fn total_filled(&self) -> Qty {
        self.fills
            .iter()
            .fold(Qty::ZERO, |acc, f| acc.saturating_add(f.qty))
    }
    /// The clock price right now.
    pub fn price(&self) -> Price {
        self.clearing_price
            .unwrap_or_else(|| self.config.price_at(self.now))
    }

    // ---------------------------------------------------------------- apply

    /// Apply one command. This is the only way state changes.
    ///
    /// Panics if `seq` is not exactly one past the previous one: a gap means a lost command and
    /// a repeat means a double-apply, and both corrupt the audit record the whole design exists
    /// to produce (invariant I4). Failing loudly here beats serving a wrong auction.
    pub fn apply(&mut self, seq: Seq, ts: Nanos, cmd: Command) -> Events {
        if let Some(last) = self.last_seq {
            assert_eq!(
                seq,
                last.next(),
                "I4: sequence must be gapless and strictly monotonic"
            );
        }
        self.last_seq = Some(seq);
        // Time cannot run backwards. Clamping rather than asserting keeps a slightly out-of-order
        // ingress stamp from killing the auction; the sequence number is the real ordering.
        self.now = Nanos(ts.0.max(self.now.0));
        let ts = self.now;

        let mut ev = Events::new();
        // A window whose time has passed settles before anything newer is considered.
        self.close_elapsed_window(ts, &mut ev);

        match cmd {
            Command::Open => self.on_open(&mut ev),
            Command::SetCollateral { participant, limit } => {
                self.collateral.entry(participant).or_default().limit = limit;
                ev.push(Event::CollateralSet { participant, limit });
            }
            Command::SubmitBid {
                participant,
                key,
                qty,
                kind,
            } => self.on_bid(seq, ts, participant, key, qty, kind, &mut ev),
            Command::CancelResting { participant, key } => {
                self.on_cancel_resting(participant, key, &mut ev)
            }
            Command::Tick => self.on_tick(ts, &mut ev),
            Command::Cancel => self.on_cancel(&mut ev),
        }
        ev
    }

    fn on_open(&mut self, ev: &mut Events) {
        if self.status != Status::Pending {
            return;
        }
        self.status = Status::Live;
        ev.push(Event::Opened {
            start_price: self.config.start_price(),
            supply: self.config.total_supply,
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn on_bid(
        &mut self,
        seq: Seq,
        ts: Nanos,
        participant: ParticipantId,
        key: IdempotencyKey,
        qty: Qty,
        kind: BidKind,
        ev: &mut Events,
    ) {
        // A retry returns the original answer and executes nothing (invariant I7).
        if let Some(outcome) = self.outcomes.get(&key).copied() {
            ev.push(Event::Duplicate { key, outcome });
            return;
        }
        if qty.is_zero() {
            self.reject(key, RejectReason::ZeroQuantity, ev);
            return;
        }
        if !self.status.accepts_bids() {
            self.reject(key, RejectReason::NotLive, ev);
            return;
        }

        let clock = self.config.price_at(ts);
        match kind {
            BidKind::Take { expected_price } => {
                // The participant is entitled to be wrong about the price and find out for free,
                // rather than be filled at a price they never agreed to.
                if clock.abs_diff(expected_price) > self.config.price_tolerance {
                    self.reject(
                        key,
                        RejectReason::PriceMoved {
                            server_price: clock,
                        },
                        ev,
                    );
                    return;
                }
                let (index, price) = self.window_bounds(ts);
                if !self.reserve(participant, price, qty) {
                    self.reject(key, RejectReason::InsufficientCollateral, ev);
                    return;
                }
                self.push_pending(
                    index,
                    price,
                    PendingBid {
                        seq,
                        participant,
                        key,
                        qty,
                        priority: price,
                        reserve_price: price,
                        resting_limit: None,
                    },
                );
                self.set_outcome(key, Outcome::Queued { qty });
                ev.push(Event::Queued {
                    key,
                    participant,
                    qty,
                    price,
                });
                // With batching off every bid is its own window, so it matches immediately.
                if self.config.batch_window == Nanos::ZERO {
                    self.flush_window(ev);
                }
            }
            BidKind::Resting { limit } => {
                if limit > clock {
                    // The clock is already below this limit, so it is a market take wearing a
                    // disguise — and one that would skip the tolerance check. Say so instead.
                    self.reject(
                        key,
                        RejectReason::LimitAboveClock {
                            server_price: clock,
                        },
                        ev,
                    );
                    return;
                }
                if !self.reserve(participant, limit, qty) {
                    self.reject(key, RejectReason::InsufficientCollateral, ev);
                    return;
                }
                self.resting.insert(RestingBid {
                    seq,
                    participant,
                    key,
                    qty,
                    limit,
                });
                self.set_outcome(key, Outcome::Resting { qty });
                ev.push(Event::Rested {
                    key,
                    participant,
                    qty,
                    limit,
                });
            }
        }
    }

    fn on_cancel_resting(
        &mut self,
        participant: ParticipantId,
        key: IdempotencyKey,
        ev: &mut Events,
    ) {
        match self.resting.remove(participant, key) {
            Some(bid) => {
                release(&mut self.collateral, participant, bid.limit, bid.qty);
                self.set_outcome(key, Outcome::Cancelled);
                ev.push(Event::RestingCancelled {
                    key,
                    participant,
                    qty: bid.qty,
                });
            }
            None => match self.outcomes.get(&key).copied() {
                // Already filled or already cancelled: tell them what actually happened.
                Some(outcome) => ev.push(Event::Duplicate { key, outcome }),
                None => ev.push(Event::Rejected {
                    key,
                    reason: RejectReason::UnknownBid,
                }),
            },
        }
    }

    fn on_tick(&mut self, ts: Nanos, ev: &mut Events) {
        if !self.status.accepts_bids() {
            return;
        }
        let clock = self.config.price_at(ts);
        let (index, window_price) = self.window_bounds(ts);

        // Resting bids the clock has now reached join this window alongside bids that arrived by
        // message — one matching pass, one priority order, no separate timer path (invariant I8).
        for bid in self.resting.drain_triggered(clock) {
            self.push_pending(
                index,
                window_price,
                PendingBid {
                    seq: bid.seq,
                    participant: bid.participant,
                    key: bid.key,
                    qty: bid.qty,
                    priority: bid.limit,
                    reserve_price: bid.limit,
                    resting_limit: Some(bid.limit),
                },
            );
        }

        if self.config.batch_window == Nanos::ZERO {
            self.flush_window(ev);
        }

        // The clock has bottomed out: settle whatever is in flight and close the auction at the
        // floor, with whatever supply is left simply unsold (`docs/auction-rules.md` §1).
        if self.status.accepts_bids() && self.config.schedule.at_floor(ts) {
            self.flush_window(ev);
            if self.status.accepts_bids() {
                self.clear(self.config.floor_price(), ev);
            }
        }
    }

    fn on_cancel(&mut self, ev: &mut Events) {
        if self.status.is_terminal() {
            return;
        }
        self.status = Status::Cancelled;
        // Everyone in flight is owed an answer, and their collateral back.
        if let Some(w) = self.window.take() {
            for b in w.bids {
                release(&mut self.collateral, b.participant, b.reserve_price, b.qty);
                self.reject(b.key, RejectReason::NotLive, ev);
            }
        }
        for b in self.resting.drain_all() {
            release(&mut self.collateral, b.participant, b.limit, b.qty);
            self.reject(b.key, RejectReason::NotLive, ev);
        }
        ev.push(Event::Cancelled);
    }

    // ---------------------------------------------------------------- windows

    /// The window index `ts` falls in, and the price every bid in it fills at.
    ///
    /// The window price is the clock price at the window's *last* nanosecond. Since the clock
    /// only descends, that is the lowest price anyone in the window could have seen on arrival —
    /// so batching for fairness never fills a participant worse than the price they bid against.
    fn window_bounds(&self, ts: Nanos) -> (u64, Price) {
        let width = self.config.batch_window;
        if width == Nanos::ZERO {
            return (ts.0, self.config.price_at(ts));
        }
        let index = ts.0 / width.0;
        let last = Nanos(index.saturating_mul(width.0).saturating_add(width.0 - 1));
        (index, self.config.price_at(last))
    }

    fn push_pending(&mut self, index: u64, price: Price, bid: PendingBid) {
        let w = self.window.get_or_insert_with(|| OpenWindow {
            index,
            price,
            bids: Vec::new(),
        });
        w.bids.push(bid);
    }

    /// Settle the open window if `ts` has moved past it.
    fn close_elapsed_window(&mut self, ts: Nanos, ev: &mut Events) {
        let Some(open) = self.window.as_ref() else {
            return;
        };
        if self.window_bounds(ts).0 > open.index {
            self.flush_window(ev);
        }
    }

    /// Match everything in the open window against remaining supply.
    ///
    /// Priority is `(price descending, seq ascending)`. Bids above the marginal price fill in
    /// full; bids *at* the marginal price share what is left **pro-rata by requested quantity**,
    /// which is what stops a window from becoming a lottery that rewards spamming size
    /// (`docs/auction-rules.md` §5).
    fn flush_window(&mut self, ev: &mut Events) {
        let Some(mut w) = self.window.take() else {
            return;
        };
        if !self.status.accepts_bids() {
            for b in w.bids {
                release(&mut self.collateral, b.participant, b.reserve_price, b.qty);
                self.reject(b.key, RejectReason::NotLive, ev);
            }
            return;
        }

        w.bids
            .sort_by(|a, b| b.priority.cmp(&a.priority).then(a.seq.cmp(&b.seq)));

        let n = w.bids.len();
        let mut allocs = vec![0u64; n];
        let mut exhausted = false;
        let mut i = 0;
        while i < n {
            let level_price = w.bids[i].priority;
            let mut j = i;
            while j < n && w.bids[j].priority == level_price {
                j += 1;
            }
            if !self.supply_remaining.is_zero() {
                let level = allocate(&w.bids[i..j], self.supply_remaining.0);
                let taken: u64 = level.iter().sum();
                allocs[i..j].copy_from_slice(&level);
                self.supply_remaining = self.supply_remaining.saturating_sub(Qty(taken));
                exhausted = self.supply_remaining.is_zero();
            }
            i = j;
        }

        for (b, alloc) in w.bids.into_iter().zip(allocs) {
            // The reservation was taken at the bid's own price; settle it and re-commit at the
            // price actually paid.
            release(&mut self.collateral, b.participant, b.reserve_price, b.qty);
            let alloc = Qty(alloc);
            if alloc.is_zero() {
                self.reject(b.key, RejectReason::SupplyExhausted, ev);
                continue;
            }
            commit(&mut self.collateral, b.participant, w.price, alloc);

            let fill = Fill {
                id: FillId(self.next_fill_id),
                seq: b.seq,
                participant: b.participant,
                key: b.key,
                qty: alloc,
                price: w.price,
            };
            self.next_fill_id += 1;
            self.fills.push(fill);
            self.set_outcome(
                b.key,
                Outcome::Filled {
                    qty: alloc,
                    price: w.price,
                },
            );
            ev.push(Event::Filled(fill));

            // A take's unfilled remainder is discarded; a resting bid's stays on the ladder at
            // the priority it already earned (`docs/auction-rules.md` §4).
            let remainder = b.qty.saturating_sub(alloc);
            if let (Some(limit), false) = (b.resting_limit, remainder.is_zero() || exhausted) {
                // Re-commit rather than re-reserve: this is strictly less than what was just
                // released, so it cannot fail, and a checked call here could silently drop a
                // live bid.
                commit(&mut self.collateral, b.participant, limit, remainder);
                self.resting.requeue(RestingBid {
                    seq: b.seq,
                    participant: b.participant,
                    key: b.key,
                    qty: remainder,
                    limit,
                });
            }
        }

        if exhausted {
            // Supply met demand at this window's price, so this is the clearing price — a price
            // the clock genuinely produced, not an artifact of matching (invariant I3).
            self.clear(w.price, ev);
        }
    }

    // ---------------------------------------------------------------- clearing

    /// End the auction at `price` and reprice every fill to it.
    ///
    /// Repricing happens here, synchronously, rather than in a settlement batch: the record is
    /// never in a state where two fills disagree about price (invariant I2).
    fn clear(&mut self, price: Price, ev: &mut Events) {
        debug_assert!(!self.status.is_terminal(), "clearing a finished auction");
        self.status = Status::Cleared;
        self.clearing_price = Some(price);

        for fill in &mut self.fills {
            // The outcomes map is what a retry is answered from, so it has to be repriced
            // alongside the fill it mirrors. Leaving it behind would have every duplicate-key
            // reply quote the pre-clearing price, which is exactly the disagreement invariant I2
            // exists to rule out.
            if let Some(
                Outcome::Filled {
                    price: quoted_price,
                    ..
                }
                | Outcome::FilledThenCancelled {
                    price: quoted_price,
                    ..
                },
            ) = self.outcomes.get_mut(&fill.key)
            {
                *quoted_price = price;
            }

            if fill.price == price {
                continue;
            }
            let from = fill.price;
            // The clock only descends, so this always *reduces* what the participant owes —
            // which is why the pre-trade check could be a cheap comparison (invariant I9).
            let freed = Price(from.0.saturating_sub(price.0));
            release(&mut self.collateral, fill.participant, freed, fill.qty);
            fill.price = price;
            ev.push(Event::Repriced {
                id: fill.id,
                from,
                to: price,
            });
        }

        // Bids the clock never reached are owed an explicit answer and their collateral back.
        for b in self.resting.drain_all() {
            release(&mut self.collateral, b.participant, b.limit, b.qty);
            self.reject(b.key, RejectReason::Unfilled, ev);
        }

        ev.push(Event::Cleared {
            price,
            filled: self.total_filled(),
            unsold: self.supply_remaining,
        });
    }

    // ---------------------------------------------------------------- collateral

    /// Reserve `price * qty` against a participant's ceiling. `false` if they cannot cover it.
    fn reserve(&mut self, participant: ParticipantId, price: Price, qty: Qty) -> bool {
        let Some(cost) = price.checked_mul_qty(qty) else {
            return false; // a nonsense order, not an engine failure
        };
        let c = self.collateral.entry(participant).or_default();
        if cost > c.available() {
            return false;
        }
        c.committed = c.committed.saturating_add(cost);
        true
    }

    fn reject(&mut self, key: IdempotencyKey, reason: RejectReason, ev: &mut Events) {
        self.set_outcome(key, Outcome::Rejected { reason });
        ev.push(Event::Rejected { key, reason });
    }

    /// Record what became of `key`, never regressing a fill.
    ///
    /// The outcomes map is the idempotency record (invariant I7): it is the only answer a retry
    /// will ever get, so once units have been allocated against a key nothing may erase them.
    /// Without this, a resting bid that filled and was then withdrawn — or whose remainder was
    /// turned away when supply ran out — would report only the last thing that happened to it,
    /// and a client retrying would be told "cancelled" for a bid that settled.
    ///
    /// Today the ladder only ever holds bids that have not filled at all, so the merging arms
    /// are unreachable. They are here because the property they enforce is one the outcomes map
    /// must have whatever the matching path does next, not one that happens to hold.
    fn set_outcome(&mut self, key: IdempotencyKey, outcome: Outcome) {
        use Outcome::{Cancelled, Filled, FilledThenCancelled, Rejected};

        let merged = match (self.outcomes.get(&key).copied(), outcome) {
            // Fills accumulate: one bid can be allocated units in more than one window.
            (
                Some(Filled { qty: prior, .. }),
                Filled {
                    qty: extra,
                    price: at,
                },
            ) => Filled {
                qty: prior.saturating_add(extra),
                price: at,
            },
            // A withdrawal settles the remainder; it cannot undo what already filled.
            (Some(Filled { qty, price }), Cancelled) => FilledThenCancelled { qty, price },
            // Nor can an unfilled remainder being turned away.
            (Some(prior @ Filled { .. }), Rejected { .. }) => prior,
            (Some(prior @ FilledThenCancelled { .. }), _) => prior,
            (_, fresh) => fresh,
        };
        self.outcomes.insert(key, merged);
    }
}

fn commit(
    map: &mut BTreeMap<ParticipantId, Collateral>,
    participant: ParticipantId,
    price: Price,
    qty: Qty,
) {
    let amount = price.checked_mul_qty(qty).unwrap_or(i64::MAX);
    let c = map.entry(participant).or_default();
    c.committed = c.committed.saturating_add(amount);
}

/// Give back a reservation.
///
/// Unlike [`commit`], an overflowing amount here cannot be clamped to `i64::MAX`: subtracting it
/// would zero the participant's committed balance and so free the collateral backing every other
/// live bid they hold — an invariant I9 violation dressed up as arithmetic safety. It is
/// unreachable today because `reserve` refuses any bid whose cost overflows, so nothing that
/// large is ever committed in the first place. The assertion is there so that stays true: any
/// future path that releases at a price it did not reserve at fails a test rather than silently
/// handing out collateral.
fn release(
    map: &mut BTreeMap<ParticipantId, Collateral>,
    participant: ParticipantId,
    price: Price,
    qty: Qty,
) {
    let amount = price.checked_mul_qty(qty);
    debug_assert!(
        amount.is_some(),
        "I9: releasing {qty} at {price} overflows, which would zero {participant}'s balance"
    );
    let c = map.entry(participant).or_default();
    // In release builds, releasing nothing strands the reservation. That is the conservative
    // direction: collateral stays committed, which refuses bids, rather than being handed back
    // twice, which lets a participant exceed the ceiling they posted.
    c.committed = c.committed.saturating_sub(amount.unwrap_or(0));
}

/// Allocate `remaining` units across one price level.
///
/// Full fills if the level fits; otherwise pro-rata by requested quantity, with the rounding
/// remainder handed out one unit at a time in priority order. `u128` throughout because
/// `remaining * qty` overflows `u64` long before either factor is unreasonable.
fn allocate(level: &[PendingBid], remaining: u64) -> Vec<u64> {
    let demand: u128 = level.iter().map(|b| u128::from(b.qty.0)).sum();
    if demand <= u128::from(remaining) {
        return level.iter().map(|b| b.qty.0).collect();
    }

    let r = u128::from(remaining);
    let mut alloc: Vec<u64> = level
        .iter()
        .map(|b| (r * u128::from(b.qty.0) / demand) as u64)
        .collect();

    // Flooring leaves fewer than `level.len()` units over, and because this branch only runs
    // when demand exceeds supply, every bid's floored share is strictly below its own request —
    // so nobody is saturated and one pass in priority order places all of them. Re-scanning the
    // level per unit, as this used to, is quadratic in the level size on the window-flush path
    // the SLO budgets 2.6 ms for.
    let mut left = remaining - alloc.iter().sum::<u64>();
    for (a, b) in alloc.iter_mut().zip(level) {
        if left == 0 {
            break;
        }
        if *a < b.qty.0 {
            *a += 1;
            left -= 1;
        }
    }
    // Anything still here would be supply the auction refuses to sell rather than oversells, so
    // release builds carry on; the assertion is what stops the reasoning above from rotting.
    debug_assert_eq!(left, 0, "pro-rata remainder was not fully distributed");
    alloc
}
