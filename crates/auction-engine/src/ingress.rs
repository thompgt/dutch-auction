//! The door into the engine, and the bouncer on it.
//!
//! Every command reaches the auction through one **bounded** channel. Bounded is the whole
//! point. An unbounded queue does not make a system able to absorb more load; it converts
//! overload from a visible rejection into an invisible latency blow-out, and hands the
//! participant an acknowledgement that arrives after the price they bid at is gone. The queue
//! depth is therefore a latency budget expressed in slots: at the measured drain rate, a full
//! queue is exactly as much work as the engine can finish inside the p99.
//!
//! When it is full the answer is [`RejectReason::Busy`], returned from the calling thread
//! without the command ever entering the log. That is a promise with teeth: a `Busy` bid is not
//! in the WAL, was never applied, and cannot fill. A client that retries is safe, and a client
//! that does not has lost nothing (invariant I10).

use auction_core::{Command, Events};
use auction_proto::{RejectReason, Seq};
use crossbeam_channel::{Sender, TrySendError};
use tokio::sync::oneshot;

/// What the engine sends back once a command is durable.
///
/// Note what has already happened by the time a caller holds one of these: the command was
/// sequenced, applied, written, and `fsync`ed. There is no separate "is it safe to tell the
/// participant" question left to get wrong (invariant I6).
#[derive(Debug, Clone)]
pub struct Ack {
    /// This command's position in the total order. Also its audit-record identifier.
    pub seq: Seq,
    /// Everything the state machine said happened, in order.
    pub events: Events,
}

/// Why a command never made it to the engine.
///
/// Distinct from [`auction_core::Event::Rejected`]: that is the auction refusing a bid it
/// considered, this is the venue refusing to consider it at all. Both are answers — invariant
/// I10 does not allow silence — but only one of them is in the log.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Shed {
    /// The ingress queue was full. The command did not execute.
    #[error("{0}")]
    Busy(RejectReason),
    /// The engine thread is gone: it panicked, or the auction was shut down.
    #[error("the auction engine has stopped: {0}")]
    Stopped(String),
}

impl Shed {
    fn busy() -> Shed {
        Shed::Busy(RejectReason::Busy)
    }
}

/// A command travelling to the engine, and the return address for its answer.
pub(crate) struct Submission {
    pub(crate) cmd: Command,
    /// `None` for commands nobody is waiting on — the engine's own clock ticks. A tick still
    /// costs an `fsync`, but nothing blocks on hearing about it.
    pub(crate) reply: Option<oneshot::Sender<Ack>>,
}

/// A cheap, clonable handle for submitting commands to one auction.
///
/// Clone it per connection. Everything about ordering was decided by the channel, so a handle
/// carries no state and no lock, and two threads sharing one cannot produce an order the audit
/// record disagrees with.
#[derive(Clone)]
pub struct AuctionHandle {
    pub(crate) tx: Sender<Submission>,
}

impl AuctionHandle {
    /// Offer a command to the engine.
    ///
    /// Returns as soon as the command is *queued*, handing back a receiver that resolves when it
    /// is durable. Nothing may be told to a participant before that receiver resolves.
    ///
    /// This never blocks. Blocking here would defeat the bound: a caller parked on a full queue
    /// is a caller queueing, just somewhere the metrics cannot see it.
    pub fn submit(&self, cmd: Command) -> Result<oneshot::Receiver<Ack>, Shed> {
        let (reply, rx) = oneshot::channel();
        let submission = Submission {
            cmd,
            reply: Some(reply),
        };
        match self.tx.try_send(submission) {
            Ok(()) => Ok(rx),
            Err(TrySendError::Full(_)) => {
                metrics::counter!("auction_ingress_shed_total").increment(1);
                Err(Shed::busy())
            }
            Err(TrySendError::Disconnected(_)) => {
                Err(Shed::Stopped("the ingress channel is closed".into()))
            }
        }
    }

    /// Submit a command nobody is waiting on.
    ///
    /// For operator actions issued by a caller that has nothing to block on, and for the tick a
    /// timer injects from outside the engine thread.
    pub fn submit_detached(&self, cmd: Command) -> Result<(), Shed> {
        match self.tx.try_send(Submission { cmd, reply: None }) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(Shed::busy()),
            Err(TrySendError::Disconnected(_)) => {
                Err(Shed::Stopped("the ingress channel is closed".into()))
            }
        }
    }

    /// How many commands are queued and not yet applied.
    ///
    /// The single most useful number about a live auction: it is the engine's backlog measured
    /// in work rather than in time, and it starts climbing before latency does.
    pub fn depth(&self) -> usize {
        self.tx.len()
    }

    /// Total slots. `depth() / capacity()` is the load-shed alarm.
    pub fn capacity(&self) -> usize {
        self.tx.capacity().unwrap_or(usize::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd() -> Command {
        Command::Tick
    }

    #[test]
    fn a_full_queue_sheds_rather_than_blocking() {
        // Nothing is draining this channel, which is exactly the overload case.
        let (tx, _rx) = crossbeam_channel::bounded(2);
        let handle = AuctionHandle { tx };

        assert!(handle.submit(cmd()).is_ok());
        assert!(handle.submit(cmd()).is_ok());
        assert_eq!(handle.depth(), 2);

        // The third does not wait for a slot, it is told no.
        assert_eq!(handle.submit(cmd()).unwrap_err(), Shed::busy());
        assert_eq!(handle.depth(), 2, "a shed command must not be queued");
    }

    #[test]
    fn a_dead_engine_is_reported_as_stopped_not_as_busy() {
        let (tx, rx) = crossbeam_channel::bounded(1);
        let handle = AuctionHandle { tx };
        drop(rx);

        match handle.submit(cmd()) {
            Err(Shed::Stopped(_)) => {}
            other => panic!("expected Stopped, got {other:?}"),
        }
    }
}
