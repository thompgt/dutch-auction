//! The single writer.
//!
//! One thread owns one auction's state and is the only code in the system permitted to mutate
//! it. Every command — bids, cancels, operator actions, and the engine's own clock ticks — is
//! pulled from one channel, stamped with the next sequence number, applied, and logged. Oversell
//! and ordering are the same problem, concurrent mutation of `supply_remaining`, and one writer
//! dissolves both: there is no lock to forget to take because there is nothing to take it
//! against.
//!
//! # The pipeline
//!
//! ```text
//! ingress ──> [ engine thread ]──apply──> state        (single writer, no locks)
//!                    │
//!                    ├──submit──> [ wal commit thread ] ──fsync──> watermark
//!                    │                                                │
//!                    └──pending──> [ ack thread ] ──wait_durable──────┘──> reply + broadcast
//! ```
//!
//! The engine thread never waits for an `fsync`. If it did, the 4 ms durability budget would be
//! charged serially to every command and the auction would run at 250 bids a second. Instead it
//! hands each applied command to an ack thread and moves on, so the disk flush for command *n*
//! overlaps the matching of command *n+1*. The acknowledgement still happens strictly after the
//! flush (invariant I6) — it just happens on a different thread.
//!
//! # Who pays for closing a window
//!
//! `docs/slo.md` makes this a requirement rather than an optimisation: closing a herd-sized batch
//! window costs about 2.6 ms, and whoever's command triggers it pays that in full. So the engine
//! ticks *itself*. Before taking anything from the ingress queue it checks whether the current
//! window's boundary has passed, and if it has, the command it applies is its own `Tick`. The
//! flush is charged to the timer, and the herd's acknowledgements leave together right behind
//! it. A participant's bid can no longer lose that race, because the race no longer exists.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use auction_core::{AuctionConfig, AuctionState, Command, Event, Events};
use auction_proto::{Nanos, Price, Qty, Seq, Status};
use auction_wal::{CommitThread, Committer, LogRecord, Wal, WalError, WalOptions};
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TryRecvError};
use crossbeam_utils::Backoff;
use parking_lot::RwLock;
use tokio::sync::{broadcast, oneshot};

use crate::clock::{window_close, Clock, Deadline};
use crate::ingress::{Ack, AuctionHandle, Submission};
use crate::replication::{Replica, ReplicationMode};

/// One event, with its place in the total order.
///
/// The sequence number travels with the event because a subscriber that reconnects needs to know
/// what it missed, and "the event after seq N" is the only description of that which does not
/// depend on the subscriber and the server agreeing about time.
#[derive(Debug, Clone, Copy)]
pub struct Sequenced {
    pub seq: Seq,
    pub event: Event,
}

/// The longest the engine will park before looking at the world again.
///
/// Bounds how long a shutdown waits, and how stale the engine's idea of "nothing is due" may be.
/// Ten times a second is far below anything the latency budget notices and far above anything an
/// idle auction can be accused of spending.
const MAX_PARK: Duration = Duration::from_millis(100);

/// A cheap, read-mostly summary of an auction, for the network edge.
///
/// The engine thread owns the state and will not share it — that is the single-writer rule, and
/// handing a lock on `AuctionState` to a thousand websockets would dismantle it. So the engine
/// publishes this instead: a handful of `Copy` scalars behind an `RwLock`.
///
/// It is snapshotted by the engine thread but *written by the ack thread*, once the command it
/// reflects is durable. Anything else would let a gateway show a `Cleared` a crash could still
/// erase — the same broken promise as acknowledging early, told to a wider audience (invariant
/// I6).
///
/// The write is uncontended and costs tens of nanoseconds against a 0.1 ms budget line, and it is
/// skipped entirely for commands that produced no events — which is most ticks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct View {
    pub status: Status,
    pub supply_remaining: Qty,
    /// The clock price, or the clearing price once the auction has settled.
    pub price: Price,
    pub clearing_price: Option<Price>,
    /// Auction time as of the last applied command.
    pub elapsed: Nanos,
    /// The last sequence applied. A client that has this number knows exactly what it has seen.
    pub seq: Option<Seq>,
}

impl View {
    fn of(state: &AuctionState) -> View {
        View {
            status: state.status(),
            supply_remaining: state.supply_remaining(),
            price: state.price(),
            clearing_price: state.clearing_price(),
            elapsed: state.now(),
            seq: state.last_seq(),
        }
    }
}

/// How the engine is tuned. The defaults are the ones `docs/slo.md` budgets for.
#[derive(Debug, Clone, Copy)]
pub struct EngineOptions {
    /// Ingress slots. Full means [`crate::Shed::Busy`].
    ///
    /// Sized as a latency budget, not a memory budget: at a measured drain rate of roughly a
    /// bid per 3 µs, 16k slots is about 50 ms of work — past the p99 and into the territory
    /// where shedding is the kinder answer.
    pub queue_depth: usize,
    /// Passed through to the log.
    pub wal: WalOptions,
    /// Take a snapshot roughly every this many commands. Snapshots only affect recovery time,
    /// never correctness, so the engine is free to defer one until it is not busy.
    pub snapshot_every: u64,
    /// How long before a deadline the engine stops parking and starts spinning.
    ///
    /// A timed park rounds up to the operating system's timer granularity — ~15.6 ms on stock
    /// Windows — so parking until a window boundary 500 µs away would miss it by an order of
    /// magnitude more than the window is wide. Inside this threshold the engine spins with
    /// backoff instead; outside it, it parks, because burning a core to wait out a whole price
    /// step buys nothing.
    ///
    /// **This costs a core.** Whenever a deadline is this close, one thread is spinning flat
    /// out. The default is a quarter of the default batch window, so an auction with an open
    /// window spins for the last 250 µs of it and parks through the rest; set it *above* the
    /// batch window and the park branch stops existing entirely, which burns a full core for as
    /// long as any window is open. That is a defensible trade on dedicated hardware and a
    /// surprising one everywhere else, so it should be chosen rather than inherited.
    ///
    /// On a host whose timers are coarse enough that a 750 µs park overshoots the boundary,
    /// raise this — the trade is CPU for punctuality, and this is the dial.
    pub spin_ahead: Duration,
    /// Whether an acknowledgement waits for the standby. Ignored when there is no replica.
    pub replication: ReplicationMode,
    /// How long a synchronous acknowledgement waits for the standby before declaring it lost and
    /// degrading to async. See the note in [`crate::replication`] on why this is not infinite.
    pub replica_timeout: Duration,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            queue_depth: 16_384,
            wal: WalOptions::default(),
            snapshot_every: 50_000,
            // A quarter of the default 1 ms batch window. Deliberately below it: at the previous
            // 2 ms an open window's boundary was always inside the threshold, so the engine never
            // parked at all and spun a core for every window's entire life.
            spin_ahead: Duration::from_micros(250),
            replication: ReplicationMode::Async,
            // Generous against the 1.5 ms budget: this is not the latency target, it is the
            // point at which a standby is declared gone. Being trigger-happy here would give up
            // the zero-loss promise over a garbage collection pause on the secondary.
            replica_timeout: Duration::from_secs(2),
        }
    }
}

/// A running auction: one engine thread, one commit thread, one ack thread.
pub struct Auction {
    handle: AuctionHandle,
    events: broadcast::Sender<Sequenced>,
    view: Arc<RwLock<View>>,
    stop: Arc<AtomicBool>,
    engine: Option<JoinHandle<()>>,
    acks: Option<JoinHandle<()>>,
    commit: Option<CommitThread>,
}

impl Auction {
    /// Open the auction whose log lives in `dir`, recovering it if there is one.
    ///
    /// There is deliberately no separate "start fresh" and "recover" entry point. A fresh start
    /// is recovery from an empty log, which means the recovery path is exercised on every single
    /// start rather than only during an incident.
    pub fn open(
        dir: impl AsRef<std::path::Path>,
        config: AuctionConfig,
        options: EngineOptions,
    ) -> Result<Auction, WalError> {
        Auction::open_inner(dir, config, options, None)
    }

    /// Open with a hot standby following along.
    ///
    /// With [`ReplicationMode::Sync`] this is what makes "zero acked bids lost on primary
    /// failure" true rather than aspirational: no acknowledgement leaves until the standby has
    /// applied the command.
    pub fn open_replicated(
        dir: impl AsRef<std::path::Path>,
        config: AuctionConfig,
        options: EngineOptions,
        replica: Arc<dyn Replica>,
    ) -> Result<Auction, WalError> {
        Auction::open_inner(dir, config, options, Some(replica))
    }

    fn open_inner(
        dir: impl AsRef<std::path::Path>,
        config: AuctionConfig,
        options: EngineOptions,
        replica: Option<Arc<dyn Replica>>,
    ) -> Result<Auction, WalError> {
        let dir = dir.as_ref().to_path_buf();

        let (state, next_seq) = match auction_wal::recover(&dir) {
            Ok(r) => {
                if r.state.config() != &config {
                    return Err(WalError::ManifestMismatch { path: dir.clone() });
                }
                (r.state, r.next_seq)
            }
            Err(WalError::NoManifest { .. }) => (AuctionState::new(config), Seq::START),
            Err(e) => return Err(e),
        };

        // Auction time resumes where the last applied command left it, so an outage pauses the
        // price clock rather than descending through levels nobody could bid at.
        let clock = Clock::resume(state.now());

        let wal = Wal::open(&dir, &config, options.wal)?;
        let commit = CommitThread::spawn(wal);
        let committer = commit.committer();

        let (tx, rx) = crossbeam_channel::bounded(options.queue_depth);
        let (ack_tx, ack_rx) = crossbeam_channel::unbounded();
        let (snap_tx, snap_rx) = crossbeam_channel::bounded(1);
        // Capacity here bounds how far a slow subscriber may lag before it is disconnected and
        // told to resynchronise. Blocking the engine on a slow websocket is not on the table.
        let (events, _) = broadcast::channel(16_384);
        let stop = Arc::new(AtomicBool::new(false));
        let view = Arc::new(RwLock::new(View::of(&state)));

        let acks = std::thread::Builder::new()
            .name("auction-ack".into())
            .spawn({
                let committer = committer.clone();
                let events = events.clone();
                let replica = replica.clone();
                let view = Arc::clone(&view);
                move || run_acks(ack_rx, committer, events, view, replica, options)
            })
            .expect("spawning the ack thread");

        std::thread::Builder::new()
            .name("auction-snapshot".into())
            .spawn({
                let dir = dir.clone();
                move || run_snapshots(snap_rx, dir)
            })
            .expect("spawning the snapshot thread");

        let engine = std::thread::Builder::new()
            .name("auction-engine".into())
            .spawn({
                let stop = Arc::clone(&stop);
                move || {
                    run(Engine {
                        state,
                        next_seq,
                        clock,
                        rx,
                        committer,
                        ack_tx,
                        snap_tx,
                        stop,
                        options,
                        replica,
                        last_snapshot: next_seq,
                        snapshotted_terminal: false,
                    })
                }
            })
            .expect("spawning the engine thread");

        Ok(Auction {
            handle: AuctionHandle { tx },
            events,
            view,
            stop,
            engine: Some(engine),
            acks: Some(acks),
            commit: Some(commit),
        })
    }

    /// A handle for submitting commands. Clone it per connection.
    pub fn handle(&self) -> AuctionHandle {
        self.handle.clone()
    }

    /// Subscribe to the event stream. A subscriber sees every event from now on, in sequence
    /// order; it never sees one before the command that caused it is durable.
    pub fn subscribe(&self) -> broadcast::Receiver<Sequenced> {
        self.events.subscribe()
    }

    /// A snapshot of the auction, cheap enough to call on every connect.
    ///
    /// Consistent as of some applied command, never a torn mixture of two — which is what makes
    /// it safe to send to a client as the base state its subsequent events apply to.
    pub fn view(&self) -> View {
        *self.view.read()
    }

    /// Stop the auction, draining what is already queued.
    ///
    /// Ordered deliberately: the engine stops applying first, then the log is flushed, then the
    /// ack thread is allowed to finish. Stopping them in any other order would leave commands
    /// applied but unlogged, which is precisely the state recovery cannot reproduce.
    pub fn shutdown(mut self) {
        self.stop_all();
    }

    fn stop_all(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(engine) = self.engine.take() {
            let _ = engine.join();
        }
        if let Some(commit) = self.commit.take() {
            commit.shutdown();
        }
        if let Some(acks) = self.acks.take() {
            let _ = acks.join();
        }
    }
}

impl Drop for Auction {
    fn drop(&mut self) {
        self.stop_all();
    }
}

/// Everything the engine thread owns. Owned, not shared — that is the single-writer rule stated
/// in the type system rather than in a comment.
struct Engine {
    state: AuctionState,
    next_seq: Seq,
    clock: Clock,
    rx: Receiver<Submission>,
    committer: Committer,
    ack_tx: Sender<Pending>,
    snap_tx: Sender<(Seq, AuctionState)>,
    stop: Arc<AtomicBool>,
    options: EngineOptions,
    replica: Option<Arc<dyn Replica>>,
    last_snapshot: Seq,
    /// Whether the once-only snapshot of the settled auction has been handed off.
    snapshotted_terminal: bool,
}

/// A command applied but not yet durable, waiting on the ack thread.
struct Pending {
    seq: Seq,
    events: Events,
    reply: Option<oneshot::Sender<Ack>>,
    /// The state as of this command, if it changed anything — published by the ack thread once
    /// this sequence is durable. `None` when the command produced no events, which is most ticks.
    view: Option<View>,
}

/// What the engine does next.
enum Next {
    /// Something arrived from outside.
    Work(Submission),
    /// A deadline passed with nothing arriving: the engine owes the auction a tick.
    Tick,
    /// Shut down.
    Stop,
}

fn run(mut e: Engine) {
    loop {
        let submission = match next(&e) {
            Next::Work(s) => s,
            Next::Tick => Submission {
                cmd: Command::Tick,
                reply: None,
            },
            Next::Stop => break,
        };

        let seq = e.next_seq;
        e.next_seq = seq.next();

        // `Command::Open` is what defines elapsed zero. The price clock starts when the auction
        // opens, not when the process did — an engine started an hour before its scheduled open
        // would otherwise stamp its first bid an hour down the schedule, which for a descending
        // clock means opening at the floor and selling the whole book at the reserve price.
        if matches!(submission.cmd, Command::Open) && e.state.status() == Status::Pending {
            e.clock = Clock::resume(e.state.now());
        }

        // The one clock read per command. Everything downstream — the state machine, the log,
        // the replica — sees this number as data and never asks for its own.
        let ts = e.clock.now();

        let events = e.state.apply(seq, ts, submission.cmd);

        // Log after applying, not before. `apply` panics on a sequence violation (invariant I4),
        // and a command that could not be applied must not be in the record as though it was.
        let record = LogRecord::new(seq, ts, submission.cmd);

        // The standby gets the record at the same moment the disk does, so the round trip
        // overlaps the `fsync` rather than queueing behind it. Both are gates on the ack; only a
        // fool pays for them one after the other.
        if let Some(replica) = &e.replica {
            replica.send(&record);
        }

        if let Err(err) = e.committer.submit(record) {
            // The log is the only proof of what was acknowledged. Continuing to match without
            // it would mean filling bids that no recovery could reproduce.
            tracing::error!(error = %err, %seq, "commit thread is gone; stopping the engine");
            break;
        }

        metrics::counter!("auction_commands_total").increment(1);

        // Snapshot the view here but do not publish it here. The ack path waits for the `fsync`
        // before telling anyone anything (invariant I6), and the view is read by exactly the same
        // audience — a gateway showing `Cleared` for a command a crash would erase is the same
        // broken promise as acknowledging it. So the view travels with the command and the ack
        // thread publishes it, in sequence order, on the durable side of the disk.
        //
        // Only when something actually changed: most ticks find an empty window and produce
        // nothing, and republishing an identical view would pay for the quiet case to make the
        // busy one no faster.
        let view = (!events.is_empty()).then(|| View::of(&e.state));

        if e.ack_tx
            .send(Pending {
                seq,
                events,
                reply: submission.reply,
                view,
            })
            .is_err()
        {
            tracing::error!(%seq, "ack thread is gone; stopping the engine");
            break;
        }

        maybe_snapshot(&mut e, seq);

        // Nothing further can change the auction, so snapshot once and let a restart of a settled
        // auction be instant. *Once*: `is_terminal` stays true forever, so cloning per command
        // here would deep-copy every fill and both maps on the engine thread for every late bid —
        // an expensive thing to do on demand, and trivially weaponisable after a clear.
        //
        // Late arrivals are still served either way: they get a `NotLive` rejection rather than
        // silence (invariant I10).
        if e.state.status().is_terminal() && !e.snapshotted_terminal {
            // Advance only on success, exactly as `maybe_snapshot` does. A full queue means one
            // is still being written, and the log covers the gap regardless.
            if e.snap_tx.try_send((seq, e.state.clone())).is_ok() {
                e.last_snapshot = seq;
                e.snapshotted_terminal = true;
            }
        }
    }

    tracing::info!(
        next_seq = %e.next_seq,
        status = ?e.state.status(),
        "engine thread stopped"
    );
}

/// Block until there is a command to apply, a deadline to honour, or a reason to stop.
fn next(e: &Engine) -> Next {
    let backoff = Backoff::new();
    loop {
        if e.stop.load(Ordering::Acquire) {
            // Drain first: these commands were accepted before the stop and their submitters are
            // waiting on them. Shedding them here would break the promise `submit` already made.
            return match e.rx.try_recv() {
                Ok(s) => Next::Work(s),
                Err(_) => Next::Stop,
            };
        }

        match e.rx.try_recv() {
            Ok(s) => return Next::Work(s),
            Err(TryRecvError::Disconnected) => return Next::Stop,
            Err(TryRecvError::Empty) => {}
        }

        let Some(Deadline(due)) = deadline(&e.state) else {
            // Nothing is pending, so there is nothing to wake up for. Park on the channel with a
            // coarse timeout, which exists only to notice a shutdown — an idle auction must cost
            // no CPU and must not write a command a millisecond into the audit record just to
            // discover it had nothing to do.
            match e.rx.recv_timeout(MAX_PARK) {
                Ok(s) => return Next::Work(s),
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => return Next::Stop,
            }
        };

        let now = e.clock.now();
        if now >= due {
            return Next::Tick;
        }

        let remaining = Duration::from_nanos(due.0 - now.0);
        if remaining <= e.options.spin_ahead {
            // Close enough that a park would overshoot the deadline by more than the deadline is
            // far away. Spin, backing off so the poll does not steal the channel's cache line
            // from the threads trying to push into it.
            backoff.snooze();
        } else {
            // Far enough that timer granularity is noise. Park, waking `spin_ahead` early so the
            // spin above covers the imprecise part — but never for longer than `MAX_PARK`.
            //
            // The clamp is not tidiness. Deadlines are unbounded: an auction whose floor is a
            // year out computes a wake-up a year out, and a thread parked for a year notices
            // neither a shutdown nor anything else. `Auction::drop` sets the stop flag and then
            // *joins* this thread, so an unclamped park turns dropping an idle auction into a
            // deadlock that lasts as long as the schedule does. Waking ten times a second to
            // look at an atomic costs nothing measurable and bounds shutdown to `MAX_PARK`.
            let park = (remaining - e.options.spin_ahead).min(MAX_PARK);
            match e.rx.recv_timeout(park) {
                Ok(s) => return Next::Work(s),
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => return Next::Stop,
            }
        }
    }
}

/// The next instant the engine has work to do with nothing arriving from outside.
///
/// Three things can come due, and nothing else: an open batch window has to be matched at its
/// boundary, a resting bid has to be triggered when the clock reaches its limit, and the auction
/// has to clear when the clock reaches the floor. If none of them applies, the engine has
/// genuinely nothing to do and should sleep until somebody bids.
fn deadline(state: &AuctionState) -> Option<Deadline> {
    // Only a live auction has deadlines. A pending one is waiting on `Open` and a terminal one
    // is finished — and each source below is cleared *by* the tick it schedules, so scheduling
    // one against a state that ignores ticks would spin the engine writing ticks forever.
    if !state.status().accepts_bids() {
        return None;
    }
    let config = state.config();
    let now = state.now();

    let window = state
        .open_window()
        .and_then(|_| window_close(now, config.batch_window));

    // The clock only descends, so the highest resting limit is the next one it reaches.
    let trigger = state
        .resting()
        .next_trigger()
        .and_then(|limit| config.schedule.time_to_reach(limit));

    // An auction at the floor is over, and something has to notice. Without this a live auction
    // with no resting bids and no open window would sit at the floor forever, waiting for a bid
    // to arrive and tell it that it had ended.
    let floor = config.schedule.time_to_reach(config.floor_price());

    [window.map(|d| d.0), trigger, floor]
        .into_iter()
        .flatten()
        // A deadline in the past is due now, not skipped.
        .map(|t| Nanos(t.0.max(now.0)))
        .min()
        .map(Deadline)
}

/// Snapshot if enough has happened, preferring a moment when the engine is not busy.
///
/// Cloning the state costs real time on the hot path, and a snapshot is pure optimisation — it
/// can always wait for a gap. The hard ceiling stops a permanently busy auction from never
/// snapshotting at all, because "never snapshots" turns into "takes an hour to recover" exactly
/// when recovery is needed.
fn maybe_snapshot(e: &mut Engine, seq: Seq) {
    let since = seq.0.saturating_sub(e.last_snapshot.0);
    let every = e.options.snapshot_every;
    let due = since >= every && e.rx.is_empty();
    let overdue = since >= every.saturating_mul(4);
    if !(due || overdue) {
        return;
    }
    // A full snapshot queue means one is still being written; skipping is correct, since the
    // next command will offer another one and the log covers the gap either way.
    if e.snap_tx.try_send((seq, e.state.clone())).is_ok() {
        e.last_snapshot = seq;
    }
}

/// Turn durability into acknowledgements, in order.
///
/// One `wait_durable` releases the whole batch: the watermark is monotonic, so once the head of
/// the queue is durable every earlier record already is, and the rest of the batch drains
/// without touching the disk again.
fn run_acks(
    rx: Receiver<Pending>,
    committer: Committer,
    events: broadcast::Sender<Sequenced>,
    view: Arc<RwLock<View>>,
    replica: Option<Arc<dyn Replica>>,
    options: EngineOptions,
) {
    // Set once the standby has missed its deadline. From here on the promise in `docs/slo.md`
    // that a primary failure loses no acked bid no longer holds, which is why it is an error log
    // and a metric rather than a quiet fallback.
    let mut replica_lost = false;

    while let Ok(pending) = rx.recv() {
        if let Err(e) = committer.wait_durable(pending.seq) {
            // The log stopped. Every waiter from here on is dropped rather than acknowledged:
            // dropping the reply channel tells the caller "unknown", and unknown is the truth.
            // Sending an ack would be claiming a durability that does not exist (invariant I6).
            tracing::error!(error = %e, seq = %pending.seq, "not durable; refusing to acknowledge");
            return;
        }

        // Durable here, replicated below. Both gates were opened at the same instant by the
        // engine thread, so this wait is whatever the standby still owes beyond the `fsync` —
        // usually nothing.
        if let (Some(replica), ReplicationMode::Sync, false) =
            (&replica, options.replication, replica_lost)
        {
            if let Err(e) = replica.wait_confirmed(pending.seq, options.replica_timeout) {
                // Availability over zero-RPO, deliberately and loudly. Halting a live auction
                // because a spare machine died is a worse outcome for every participant than
                // the risk being avoided — but the risk is now real, so it is an incident.
                tracing::error!(
                    error = %e,
                    seq = %pending.seq,
                    "standby did not confirm; degrading to async replication -- acked bids can \
                     now be lost if this primary fails"
                );
                metrics::counter!("auction_replica_lost_total").increment(1);
                replica_lost = true;
            }
        }

        // Durable, and replicated if that was asked for. Only now may anyone be shown this
        // state — the view is published from here, in sequence order, for the same reason the
        // reply below is.
        if let Some(published) = pending.view {
            *view.write() = published;
        }

        // Broadcast before replying. The public record of what happened should not lag the
        // private one, and a subscriber that sees a fill before the bidder does is harmless
        // whereas the reverse invites a client to act on an event nobody else has seen.
        for event in &pending.events {
            let _ = events.send(Sequenced {
                seq: pending.seq,
                event: *event,
            });
        }
        if let Some(reply) = pending.reply {
            // A dropped receiver is normal: the participant disconnected. The command is durable
            // and the auction is unaffected — there is simply nobody left to tell.
            let _ = reply.send(Ack {
                seq: pending.seq,
                events: pending.events,
            });
        }
    }
}

/// Write snapshots off the hot path.
fn run_snapshots(rx: Receiver<(Seq, AuctionState)>, dir: std::path::PathBuf) {
    while let Ok((seq, state)) = rx.recv() {
        match auction_wal::snapshot::write(&dir, &state) {
            Ok(_) => {
                // Keep a couple of generations: recovery falls back to an older snapshot if the
                // newest one fails its checksum, and one spare is the difference between a slow
                // start and a full replay.
                if let Err(e) = auction_wal::snapshot::prune(&dir, 2) {
                    tracing::warn!(error = %e, "could not prune old snapshots");
                }
            }
            // A snapshot is an optimisation. Failing to write one costs recovery time and
            // nothing else, so it is logged and the auction carries on.
            Err(e) => tracing::warn!(error = %e, %seq, "snapshot failed"),
        }
    }
}
