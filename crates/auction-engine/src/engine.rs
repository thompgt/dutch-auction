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
use auction_proto::{Nanos, Seq, Status};
use auction_wal::{CommitThread, Committer, LogRecord, Wal, WalError, WalOptions};
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TryRecvError};
use crossbeam_utils::Backoff;
use tokio::sync::{broadcast, oneshot};

use crate::clock::{window_close, Clock, Deadline};
use crate::ingress::{Ack, AuctionHandle, Submission};

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
    pub spin_ahead: Duration,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            queue_depth: 16_384,
            wal: WalOptions::default(),
            snapshot_every: 50_000,
            spin_ahead: Duration::from_millis(2),
        }
    }
}

/// A running auction: one engine thread, one commit thread, one ack thread.
pub struct Auction {
    handle: AuctionHandle,
    events: broadcast::Sender<Sequenced>,
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

        let acks = std::thread::Builder::new()
            .name("auction-ack".into())
            .spawn({
                let committer = committer.clone();
                let events = events.clone();
                move || run_acks(ack_rx, committer, events)
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
                        last_snapshot: next_seq,
                    })
                }
            })
            .expect("spawning the engine thread");

        Ok(Auction {
            handle: AuctionHandle { tx },
            events,
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
    last_snapshot: Seq,
}

/// A command applied but not yet durable, waiting on the ack thread.
struct Pending {
    seq: Seq,
    events: Events,
    reply: Option<oneshot::Sender<Ack>>,
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
        if let Err(err) = e.committer.submit(LogRecord::new(seq, ts, submission.cmd)) {
            // The log is the only proof of what was acknowledged. Continuing to match without
            // it would mean filling bids that no recovery could reproduce.
            tracing::error!(error = %err, %seq, "commit thread is gone; stopping the engine");
            break;
        }

        metrics::counter!("auction_commands_total").increment(1);
        if e.ack_tx
            .send(Pending {
                seq,
                events,
                reply: submission.reply,
            })
            .is_err()
        {
            tracing::error!(%seq, "ack thread is gone; stopping the engine");
            break;
        }

        maybe_snapshot(&mut e, seq);

        if e.state.status().is_terminal() {
            // Nothing further can change the auction. Snapshot unconditionally so that a
            // restart of a settled auction is instant, and keep serving: late arrivals still
            // get a `NotLive` rejection rather than silence (invariant I10).
            let _ = e.snap_tx.try_send((seq, e.state.clone()));
            e.last_snapshot = seq;
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
            match e.rx.recv_timeout(Duration::from_millis(100)) {
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
            // spin above covers the imprecise part.
            let wake = e.clock.instant_at(due) - e.options.spin_ahead;
            match e.rx.recv_deadline(wake) {
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
fn run_acks(rx: Receiver<Pending>, committer: Committer, events: broadcast::Sender<Sequenced>) {
    while let Ok(pending) = rx.recv() {
        if let Err(e) = committer.wait_durable(pending.seq) {
            // The log stopped. Every waiter from here on is dropped rather than acknowledged:
            // dropping the reply channel tells the caller "unknown", and unknown is the truth.
            // Sending an ack would be claiming a durability that does not exist (invariant I6).
            tracing::error!(error = %e, seq = %pending.seq, "not durable; refusing to acknowledge");
            return;
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
