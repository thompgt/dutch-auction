//! Failures the log can produce.
//!
//! The distinction that matters here is between a **torn tail** and **corruption**. A torn tail
//! is the normal, expected consequence of a crash: the process died partway through writing a
//! record that was never acknowledged, so discarding it loses nothing anyone was promised.
//! Corruption is a record that fails its checksum in the middle of the log, which means the
//! audit record is not what it says it is — and for a financial venue that is a stop-the-world
//! condition, not something to recover past.

use std::path::PathBuf;

use auction_proto::Seq;

pub type Result<T> = std::result::Result<T, WalError>;

#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("io error on {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("could not encode record: {0}")]
    Encode(#[source] bincode::Error),

    /// A record failed its checksum somewhere other than the very end of the log. Everything
    /// after it is unreadable, and the log no longer proves what it claims to prove.
    #[error("corrupt record in {path} at offset {offset}: {detail}")]
    Corrupt {
        path: PathBuf,
        offset: u64,
        detail: String,
    },

    /// The log's sequence numbers are not gapless (invariant I4). A missing command means the
    /// replayed state is not the state that was acknowledged to participants.
    #[error("log jumps from {from} to {found}: a command is missing")]
    SequenceGap { from: Seq, found: Seq },

    /// The directory holds a log for a different auction, or a different configuration of the
    /// same one. Replaying commands against the wrong supply or schedule silently produces a
    /// plausible, wrong auction, so it is refused.
    #[error("manifest in {path} does not match the auction being opened")]
    ManifestMismatch { path: PathBuf },

    #[error("no manifest in {path}: not an auction log directory")]
    NoManifest { path: PathBuf },

    /// The group-commit thread died. Every waiter is released with this rather than blocking
    /// forever, because a bid that can never be acked must be rejected, not abandoned.
    #[error("the commit thread has stopped: {0}")]
    CommitterStopped(String),
}

impl WalError {
    pub(crate) fn io(path: impl Into<PathBuf>, source: std::io::Error) -> WalError {
        WalError::Io {
            path: path.into(),
            source,
        }
    }
}
