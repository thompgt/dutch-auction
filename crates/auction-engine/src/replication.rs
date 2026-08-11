//! Keeping a second machine ready to take over.
//!
//! The standby has no code of its own. It receives the primary's log records and feeds them to
//! [`auction_wal::follow`], which is the same function crash recovery calls — so "the standby is
//! correct" is not a separate claim needing separate tests, it is the determinism of the state
//! machine (invariant I5) applied to a live stream instead of to a file. A bug that only shows
//! up during failover cannot exist, because failover runs no code that has not already run
//! millions of times during normal operation.
//!
//! # What sync replication actually buys
//!
//! `docs/slo.md` budgets 1.5 ms for it and promises **zero acked bids lost on primary failure**.
//! That promise is only worth something if the acknowledgement genuinely waits for the standby,
//! so [`ReplicationMode::Sync`] gates the ack on the standby's watermark exactly as invariant I6
//! gates it on the `fsync`. Both are the same shape: publish a number, wake the waiters.
//!
//! # And what happens when the standby dies
//!
//! This is the part that is easy to get wrong by being principled about it. Waiting forever for
//! a standby that is never coming back converts the loss of a *secondary* into the loss of the
//! auction — participants stop being able to bid because a spare machine failed. So a sync
//! replica that misses its deadline is **declared lost**: the primary logs loudly, marks itself
//! degraded, and carries on acknowledging on durability alone.
//!
//! That is a deliberate choice of availability over zero-RPO, and it is a real weakening of the
//! promise above: from the moment of that log line, a primary failure can lose acked bids. It is
//! an operational emergency and `docs/recovery.md` treats it as one. The alternative — halting a
//! live auction mid-clock — hands every participant a worse outcome than the risk being avoided.

use std::sync::Arc;
use std::time::Duration;

use auction_core::AuctionState;
use auction_proto::Seq;
use crossbeam_channel::{Receiver, Sender};
use parking_lot::{Condvar, Mutex};

use auction_wal::LogRecord;

/// How much the primary waits for the standby before acknowledging a bid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReplicationMode {
    /// Records are streamed but acknowledgement does not wait for them. Costs nothing on the hot
    /// path; a primary failure can lose bids the standby had not yet received.
    #[default]
    Async,
    /// Acknowledgement waits for the standby to confirm. Costs the round trip — 1.5 ms of the
    /// budget — and is what makes "zero acked bids lost" true.
    Sync,
}

/// The standby was not there when the primary needed it.
#[derive(Debug, Clone, thiserror::Error)]
#[error("replica did not confirm {seq} within {timeout:?}")]
pub struct ReplicaLost {
    pub seq: Seq,
    pub timeout: Duration,
}

/// Somewhere the primary's log records are being applied.
///
/// A trait because the transport is not this crate's business: an in-process channel here, a TCP
/// stream in Phase 8, and — usefully — a test double in between. What *is* this crate's business
/// is the watermark, because that is what an acknowledgement is allowed to depend on.
pub trait Replica: Send + Sync + 'static {
    /// Hand over a record. Must not block: the engine thread calls this, and an engine that can
    /// be stalled by a slow network is an engine whose latency is somebody else's to control.
    fn send(&self, record: &LogRecord);

    /// The highest sequence the replica has confirmed. For metrics; `None` means nothing yet.
    fn confirmed(&self) -> Option<Seq>;

    /// Block until the replica confirms `seq`, or give up.
    fn wait_confirmed(&self, seq: Seq, timeout: Duration) -> Result<(), ReplicaLost>;
}

/// A published sequence number and the people waiting for it to reach theirs.
///
/// The same mechanism the WAL uses for durability, for the same reason: sequence numbers are
/// monotonic, so "my record is replicated" is exactly "the published number is at least mine",
/// and one notify releases a whole batch of waiters.
#[derive(Debug, Default)]
struct Watermark {
    seq: Mutex<Option<Seq>>,
    changed: Condvar,
}

impl Watermark {
    /// Advance the watermark to `seq`, if that is forward.
    ///
    /// [`Watermark::wait_for`] reads "confirmed >= mine" as "my record is replicated", which is
    /// only true of a number that never goes down. A replica that delivered out of order — a
    /// reconnecting stream replaying, a future transport with more than one connection — would
    /// otherwise lower the mark and let an acknowledgement claim a confirmation it does not
    /// have. Comparing here makes that impossible rather than merely unlikely.
    fn publish(&self, seq: Seq) {
        let mut current = self.seq.lock();
        if current.is_some_and(|confirmed| confirmed >= seq) {
            return;
        }
        *current = Some(seq);
        self.changed.notify_all();
    }

    fn get(&self) -> Option<Seq> {
        *self.seq.lock()
    }

    fn wait_for(&self, target: Seq, timeout: Duration) -> Result<(), ReplicaLost> {
        let deadline = std::time::Instant::now() + timeout;
        let mut seq = self.seq.lock();
        while !matches!(*seq, Some(confirmed) if confirmed >= target) {
            if self.changed.wait_until(&mut seq, deadline).timed_out() {
                return Err(ReplicaLost {
                    seq: target,
                    timeout,
                });
            }
        }
        Ok(())
    }
}

/// A standby running in this process, fed by a channel.
///
/// Not a toy. It is the reference implementation the networked one has to match, and it is what
/// makes "the standby reaches the same state as the primary" a test rather than an assurance.
pub struct LocalReplica {
    tx: Sender<LogRecord>,
    watermark: Arc<Watermark>,
    state: Arc<Mutex<AuctionState>>,
    /// Set by [`LocalReplica::stop`]. A flag rather than dropping the sender, because the
    /// replica itself holds one for the lifetime of the handle — a channel disconnect would
    /// only arrive once every clone anywhere had gone, which is not a thing shutdown can wait
    /// for.
    stopping: Arc<std::sync::atomic::AtomicBool>,
    handle: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl LocalReplica {
    /// Start following, beginning from `state` — which for a standby joining a running auction
    /// is the state a snapshot or a full replay produced, not an empty one.
    pub fn start(state: AuctionState) -> Arc<LocalReplica> {
        let (tx, rx) = crossbeam_channel::unbounded();
        let watermark = Arc::new(Watermark::default());
        let state = Arc::new(Mutex::new(state));
        let stopping = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let handle = std::thread::Builder::new()
            .name("auction-standby".into())
            .spawn({
                let watermark = Arc::clone(&watermark);
                let state = Arc::clone(&state);
                let stopping = Arc::clone(&stopping);
                move || follow(rx, state, watermark, stopping)
            })
            .expect("spawning the standby thread");

        Arc::new(LocalReplica {
            tx,
            watermark,
            state,
            stopping,
            handle: Mutex::new(Some(handle)),
        })
    }

    /// The standby's state, for the one question worth asking of it: does it match the primary?
    ///
    /// The lock exists for observation. A networked standby owns its state outright and takes no
    /// lock at all, which is the whole point of the single-writer design surviving the failover.
    pub fn state(&self) -> parking_lot::MutexGuard<'_, AuctionState> {
        self.state.lock()
    }

    /// Stop following once the queue is empty, and wait for the thread.
    ///
    /// Drains first: records already in flight were sent by a primary that may have acked them,
    /// and a standby that stopped short of what was acknowledged is not a standby. Call it after
    /// the primary has stopped, or it will simply keep following whatever still arrives.
    pub fn stop(&self) {
        self.stopping
            .store(true, std::sync::atomic::Ordering::Release);
        let handle = self.handle.lock().take();
        if let Some(handle) = handle {
            let _ = handle.join();
        }
    }
}

impl Replica for LocalReplica {
    fn send(&self, record: &LogRecord) {
        // Unbounded and therefore non-blocking. Backpressure from a standby must never reach the
        // primary's hot path — a slow secondary should cost memory and eventually its own
        // membership, not the auction's latency.
        let _ = self.tx.send(*record);
    }

    fn confirmed(&self) -> Option<Seq> {
        self.watermark.get()
    }

    fn wait_confirmed(&self, seq: Seq, timeout: Duration) -> Result<(), ReplicaLost> {
        self.watermark.wait_for(seq, timeout)
    }
}

/// The standby's entire job.
fn follow(
    rx: Receiver<LogRecord>,
    state: Arc<Mutex<AuctionState>>,
    watermark: Arc<Watermark>,
    stopping: Arc<std::sync::atomic::AtomicBool>,
) {
    loop {
        let record = match rx.recv_timeout(Duration::from_millis(20)) {
            Ok(record) => record,
            // An empty queue is the only safe moment to stop: everything sent has been applied.
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                if stopping.load(std::sync::atomic::Ordering::Acquire) {
                    break;
                }
                continue;
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        };
        {
            let mut state = state.lock();
            auction_wal::replay::follow(&mut state, &record);
        }
        // Confirm only after applying. Confirming on receipt would mean acknowledging a bid the
        // standby could still fail to apply, which is the failure mode sync replication exists
        // to rule out.
        watermark.publish(record.seq);
    }
    tracing::info!(confirmed = ?watermark.get(), "standby stopped following");
}

#[cfg(test)]
mod tests {
    use super::*;
    use auction_core::{AuctionConfig, Command, PriceSchedule};
    use auction_proto::{AuctionId, Nanos, Price, Qty};

    fn config() -> AuctionConfig {
        AuctionConfig::new(
            AuctionId(uuid::Uuid::from_u128(3)),
            Qty(100),
            PriceSchedule::stepped(Price(1000), Price(100), Nanos::from_secs(1), 10),
        )
    }

    #[test]
    fn a_standby_confirms_only_what_it_has_applied() {
        let replica = LocalReplica::start(AuctionState::new(config()));
        let record = LogRecord::new(Seq(0), Nanos::ZERO, Command::Open);

        replica.send(&record);
        replica
            .wait_confirmed(Seq(0), Duration::from_secs(5))
            .expect("the standby confirmed");

        assert_eq!(replica.confirmed(), Some(Seq(0)));
        // Confirmed means applied, not merely received.
        assert_eq!(replica.state().status(), auction_proto::Status::Live);
    }

    #[test]
    fn a_silent_standby_is_declared_lost_rather_than_waited_on_forever() {
        let replica = LocalReplica::start(AuctionState::new(config()));
        // Nothing was ever sent, so nothing can be confirmed.
        let err = replica
            .wait_confirmed(Seq(0), Duration::from_millis(50))
            .expect_err("waiting on a record that was never sent must time out");
        assert_eq!(err.seq, Seq(0));
    }
}
