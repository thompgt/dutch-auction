//! Group commit: many appends, one `fsync`.
//!
//! An `fsync` to NVMe costs on the order of a hundred microseconds and barely notices whether
//! it is flushing one record or a thousand. Paying it per bid would put a hard ceiling of a few
//! thousand bids per second on the engine and would make the thundering herd — the one moment
//! this system exists to survive — the moment it falls over. So a writer thread collects
//! everything that arrives within a short linger window, writes it all, syncs once, and
//! releases every waiter together.
//!
//! The cost is honest and worth naming: a bid that arrives just as a batch closes waits for the
//! next one. That is the `linger` in [`crate::WalOptions`], and it is why the default is 500 µs
//! rather than the 1 ms the architecture sketch assumed — the batch has to fit inside the 4 ms
//! the SLO gives durability, sync replication has to fit alongside it, and a linger that eats
//! its own budget helps nobody.
//!
//! # How waiting works
//!
//! There is no per-bid channel or future. The committer publishes one number — the highest
//! durable sequence — and waiters block on a condition variable until that number reaches
//! theirs. Sequence numbers are monotonic, so "my record is durable" is exactly "the published
//! number is at least mine", and a batch of a thousand waiters is woken by a single notify.

use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use auction_proto::Seq;
use crossbeam_channel::{Receiver, Sender, TryRecvError};
use crossbeam_utils::Backoff;
use parking_lot::{Condvar, Mutex};

use crate::error::{Result, WalError};
use crate::record::LogRecord;
use crate::wal::{Wal, WalOptions};

#[derive(Debug)]
enum State {
    /// The writer is running; this is the highest sequence proven durable so far.
    Live(Option<Seq>),
    /// The writer has stopped. `durable` is final and will never advance again.
    Closed {
        durable: Option<Seq>,
        reason: String,
    },
}

/// The published durability watermark, and the parking lot for everyone waiting on it.
#[derive(Debug)]
struct Watermark {
    state: Mutex<State>,
    changed: Condvar,
}

impl Watermark {
    fn new(durable: Option<Seq>) -> Watermark {
        Watermark {
            state: Mutex::new(State::Live(durable)),
            changed: Condvar::new(),
        }
    }

    /// Advance the durability mark to `seq`, if that is forward.
    ///
    /// [`Watermark::wait_for`] reads "durable >= mine" as "my record survived the `fsync`", which
    /// is only sound for a number that never goes down — a mark that moved backwards would park
    /// waiters who had already been released by a value they were promised was final. Nothing
    /// today publishes out of order, since one writer thread syncs in sequence order; comparing
    /// makes that a property of the type rather than of its only caller. A `close` still wins,
    /// because a stopped writer is not a lower watermark, it is a different state.
    fn publish(&self, seq: Option<Seq>) {
        let mut state = self.state.lock();
        match &*state {
            // Already at or past it, or stopped for good. Either way the mark does not move.
            State::Live(durable) if *durable >= seq => return,
            State::Closed { .. } => return,
            State::Live(_) => {}
        }
        *state = State::Live(seq);
        self.changed.notify_all();
    }

    /// Stop the watermark permanently and wake everyone parked on it.
    ///
    /// Waiters at or below `durable` still succeed — their records really are on disk, and the
    /// writer stopping afterwards does not un-write them. Everyone above it gets an error,
    /// because a caller blocked forever cannot tell a slow disk from a dead writer, and only
    /// one of those is safe to keep waiting on.
    fn close(&self, reason: String) {
        let mut state = self.state.lock();
        let durable = match &*state {
            State::Live(d) => *d,
            State::Closed { durable, .. } => *durable,
        };
        *state = State::Closed { durable, reason };
        self.changed.notify_all();
    }

    fn wait_for(&self, target: Seq) -> Result<()> {
        let mut state = self.state.lock();
        loop {
            match &*state {
                State::Live(Some(durable)) if *durable >= target => return Ok(()),
                State::Live(_) => self.changed.wait(&mut state),
                State::Closed { durable, .. } if *durable >= Some(target) => return Ok(()),
                State::Closed { reason, .. } => {
                    return Err(WalError::CommitterStopped(reason.clone()))
                }
            }
        }
    }

    fn durable(&self) -> Option<Seq> {
        match &*self.state.lock() {
            State::Live(d) | State::Closed { durable: d, .. } => *d,
        }
    }
}

/// What the writer thread consumes.
enum Msg {
    Record(LogRecord),
    /// Stop after draining. An explicit message rather than a channel disconnect, because
    /// `Committer` is clonable and there is no way to prove every clone has been dropped.
    Stop,
}

/// A handle for submitting records and waiting for their durability.
///
/// Cheap to clone and safe to share: the ordering that matters was already fixed by the
/// sequencer upstream, so this type only has to preserve it, not create it.
#[derive(Clone)]
pub struct Committer {
    tx: Sender<Msg>,
    watermark: Arc<Watermark>,
}

impl Committer {
    /// Queue a record for the next batch. Returns as soon as it is queued — the record is
    /// **not** durable yet, and nothing may be acknowledged on the strength of this call.
    ///
    /// Blocks if the queue is full, which is the point: the caller has already applied this
    /// command and cannot drop it, so the only honest response to a disk that cannot keep up is
    /// to stop accepting new work until it can. The stall propagates to ingress, which sheds as
    /// `Busy`. Load is refused at the door or not at all.
    pub fn submit(&self, record: LogRecord) -> Result<()> {
        if self.tx.is_full() {
            metrics::counter!("auction_wal_submit_queue_full_total").increment(1);
        }
        self.tx
            .send(Msg::Record(record))
            .map_err(|_| WalError::CommitterStopped("the commit thread is gone".into()))
    }

    /// How many records are queued for the writer and not yet durable.
    ///
    /// The disk-side twin of the ingress backlog: rising depth here is the engine outrunning
    /// storage, and it is the number to alarm on before the queue is full and the engine stalls.
    pub fn queue_depth(&self) -> usize {
        self.tx.len()
    }

    /// Block until `seq` has survived an `fsync`. This is the line invariant I6 draws: an
    /// acknowledgement sent before this returns is a promise the system cannot keep.
    pub fn wait_durable(&self, seq: Seq) -> Result<()> {
        self.watermark.wait_for(seq)
    }

    /// Submit and wait — the whole hot path, for callers with nothing else to do meanwhile.
    pub fn commit(&self, record: LogRecord) -> Result<()> {
        let seq = record.seq;
        self.submit(record)?;
        self.wait_durable(seq)
    }

    /// The current watermark, without blocking. For metrics, not for correctness decisions.
    pub fn durable_seq(&self) -> Option<Seq> {
        self.watermark.durable()
    }
}

/// A running group-commit writer. Dropping it stops the thread once the queue drains.
pub struct CommitThread {
    committer: Committer,
    handle: Option<JoinHandle<()>>,
}

impl CommitThread {
    /// Take ownership of `wal` and start batching.
    pub fn spawn(wal: Wal) -> CommitThread {
        let options = *wal.options();
        // Bounded: see `WalOptions::submit_queue_depth`. An unbounded queue here turns a slow
        // disk into unbounded memory rather than into backpressure the ingress door can act on.
        let (tx, rx) = crossbeam_channel::bounded(options.submit_queue_depth);
        let watermark = Arc::new(Watermark::new(wal.durable_seq()));

        let thread_watermark = Arc::clone(&watermark);
        let handle = std::thread::Builder::new()
            .name("wal-commit".into())
            .spawn(move || run(wal, rx, thread_watermark, options))
            .expect("spawning the wal commit thread");

        CommitThread {
            committer: Committer { tx, watermark },
            handle: Some(handle),
        }
    }

    pub fn committer(&self) -> Committer {
        self.committer.clone()
    }

    /// Stop the writer, flushing whatever is already queued.
    pub fn shutdown(mut self) {
        self.stop();
    }

    fn stop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = self.committer.tx.send(Msg::Stop);
            let _ = handle.join();
        }
    }
}

impl Drop for CommitThread {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The writer loop.
fn run(mut wal: Wal, rx: Receiver<Msg>, watermark: Arc<Watermark>, options: WalOptions) {
    let mut batch: Vec<LogRecord> = Vec::with_capacity(options.max_batch);

    'writer: loop {
        // Block until there is something to do. An idle auction costs nothing: no timer, no
        // spinning, no syscall.
        batch.clear();
        match rx.recv() {
            Ok(Msg::Record(record)) => batch.push(record),
            Ok(Msg::Stop) | Err(_) => break,
        }

        // Linger, gathering whatever else shows up. The deadline is absolute rather than
        // per-message so a steady trickle cannot extend the window indefinitely — the tail is
        // bounded by `linger`, not by the arrival pattern.
        //
        // The linger is spun, not slept, and that is not a micro-optimisation. A timed park
        // rounds up to the operating system's timer granularity — ~15.6 ms on stock Windows,
        // and a millisecond or more on many Linux configurations — so asking to wait 500 µs
        // buys a 13 ms wait, which is the entire p99 budget spent on a sleep that was supposed
        // to be sub-millisecond. Measured: `group_commit/1` went from 13.6 ms to well under
        // one, and batches of 16 and 256 were paying exactly the same 13 ms, which is the
        // giveaway that the cost was a timer rather than the disk.
        //
        // But the spin has to back off, and the benchmark caught that too: a bare `spin_loop`
        // took the herd-sized batch from 4.2 ms to 9.9 ms. Hammering `try_recv` writes to the
        // same channel head the producers are pushing into, so the writer spends its linger
        // stealing the cache line from the threads it is waiting on — it slows down the very
        // work it wants to arrive. `Backoff` spins a few times, then yields the core, which
        // has no timer granularity to round up and leaves the producers alone.
        //
        // The cost is one core busy for at most `linger` per batch, and only while there is
        // work in flight — the outer `recv()` above is a genuine block, so an idle auction
        // still spins on nothing. For a venue whose product is the tail, that is the right side
        // of the trade.
        let deadline = Instant::now() + options.linger;
        let backoff = Backoff::new();
        let mut stopping = false;
        while batch.len() < options.max_batch {
            match rx.try_recv() {
                Ok(Msg::Record(record)) => {
                    batch.push(record);
                    // Records are flowing; the next empty poll should start its backoff from
                    // scratch rather than inheriting the patience of an earlier lull.
                    backoff.reset();
                }
                // Stop *after* this batch: these records were submitted before the stop and
                // their submitters may already be waiting on them.
                Ok(Msg::Stop) => {
                    stopping = true;
                    break;
                }
                Err(TryRecvError::Disconnected) => break,
                Err(TryRecvError::Empty) => {
                    if Instant::now() >= deadline {
                        break;
                    }
                    backoff.snooze();
                }
            }
        }

        if let Err(e) = write_batch(&mut wal, &batch, &watermark) {
            // A failed write is not survivable: the log is the only record of what was
            // acknowledged, so continuing without it would mean acking bids that no recovery
            // could reproduce. Poison every waiter and stop.
            tracing::error!(error = %e, "wal write failed; stopping the commit thread");
            watermark.close(e.to_string());
            return;
        }
        if stopping {
            break 'writer;
        }
    }

    // Drain and sync whatever was queued before the stop arrived.
    let leftovers: Vec<LogRecord> = rx
        .try_iter()
        .filter_map(|m| match m {
            Msg::Record(r) => Some(r),
            Msg::Stop => None,
        })
        .collect();
    if !leftovers.is_empty() {
        if let Err(e) = write_batch(&mut wal, &leftovers, &watermark) {
            watermark.close(e.to_string());
            return;
        }
    }
    tracing::debug!(durable = ?wal.durable_seq(), "commit thread stopped cleanly");

    // Release anyone still waiting rather than leaving them parked on a watermark that will
    // never move again. A caller blocked forever cannot distinguish a slow disk from a dead
    // writer, and only one of those is safe to keep waiting on.
    watermark.close("the commit thread has shut down".into());
}

fn write_batch(wal: &mut Wal, batch: &[LogRecord], watermark: &Watermark) -> Result<()> {
    for record in batch {
        wal.append(record)?;
    }
    let durable = wal.sync()?;
    // Publish only after the sync returns. Everything about I6 is in the order of these two
    // lines.
    watermark.publish(durable);
    tracing::trace!(records = batch.len(), ?durable, "group commit");
    Ok(())
}

/// How long an `fsync` actually takes on this machine's storage.
///
/// Not a test of correctness — a measurement, because the durability budget in `docs/slo.md` is
/// a claim about hardware and a claim about hardware should be checked on the hardware.
pub fn measure_sync_cost(wal: &mut Wal, samples: usize) -> Result<Duration> {
    let start = Instant::now();
    for _ in 0..samples {
        wal.sync()?;
    }
    Ok(start.elapsed() / samples.max(1) as u32)
}
