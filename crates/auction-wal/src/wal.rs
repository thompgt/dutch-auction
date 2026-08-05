//! The log itself: append, sync, and read back.
//!
//! [`Wal`] is deliberately single-threaded and blocking. Concurrency belongs one layer up in
//! [`crate::commit`], where batching many appends into one `fsync` is the entire point; pushing
//! locks down into the file handle would buy nothing and would make the durability story harder
//! to reason about, which is not a trade worth making for the one invariant (I6) that the whole
//! system's credibility rests on.
//!
//! The split between `append` and `sync` is the load-bearing part of the API. `append` puts
//! bytes in the operating system's hands; `sync` is the moment the data survives losing power.
//! Nothing may be acknowledged to a participant between those two calls, and keeping them
//! separate is what lets one `sync` discharge a thousand `append`s.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use auction_core::AuctionConfig;
use auction_proto::Seq;

use crate::error::{Result, WalError};
use crate::record::{encode_into, LogRecord};
use crate::segment::{read_segment, segment_path, segments};

const MANIFEST: &str = "manifest.json";

/// Tuning knobs. The defaults are the ones `docs/slo.md` budgets for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalOptions {
    /// Roll to a new segment once the active one reaches this size.
    pub segment_bytes: u64,
    /// How long the committer waits, gathering more commands, before flushing a batch.
    pub linger: std::time::Duration,
    /// Upper bound on batch size, so a sustained flood still gets flushed promptly instead of
    /// growing a batch forever and turning a throughput problem into a latency problem.
    pub max_batch: usize,
}

impl Default for WalOptions {
    fn default() -> Self {
        Self {
            segment_bytes: 64 * 1024 * 1024,
            linger: std::time::Duration::from_micros(500),
            max_batch: 4096,
        }
    }
}

/// An append-only log of commands for one auction.
pub struct Wal {
    dir: PathBuf,
    options: WalOptions,
    active: BufWriter<File>,
    active_path: PathBuf,
    active_index: u32,
    active_bytes: u64,
    /// Written, but not necessarily on the platter.
    appended: Option<Seq>,
    /// Proven to survive power loss. Nothing above this may be acknowledged (invariant I6).
    durable: Option<Seq>,
    scratch: Vec<u8>,
}

impl Wal {
    /// Open the log in `dir`, creating it if absent.
    ///
    /// Refuses to open a directory whose manifest describes a different auction: replaying
    /// commands against the wrong supply or schedule produces an auction that looks entirely
    /// plausible and is entirely wrong, which is worse than failing to start.
    pub fn open(dir: impl AsRef<Path>, config: &AuctionConfig, options: WalOptions) -> Result<Wal> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir).map_err(|e| WalError::io(&dir, e))?;

        match read_manifest(&dir) {
            Ok(existing) if &existing == config => {}
            Ok(_) => {
                return Err(WalError::ManifestMismatch {
                    path: dir.join(MANIFEST),
                })
            }
            Err(WalError::NoManifest { .. }) => write_manifest(&dir, config)?,
            Err(e) => return Err(e),
        }

        // Resume the highest-numbered segment rather than starting a new one, so a restart
        // does not leave a trail of near-empty files.
        let existing = segments(&dir)?;
        let active_index = existing.last().map(|(i, _)| *i).unwrap_or(0);
        let active_path = segment_path(&dir, active_index);

        let durable = last_seq_in(&dir)?;

        // Cut off a torn tail before appending anything.
        //
        // Recovery reads a segment until the first record that does not check out and stops
        // there, so leaving the torn bytes in place and writing after them would bury every
        // subsequent command behind unreadable garbage — turning a crash that lost nothing into
        // one that silently loses everything written after the restart.
        truncate_torn_tail(&active_path)?;

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&active_path)
            .map_err(|e| WalError::io(&active_path, e))?;
        let active_bytes = file
            .metadata()
            .map_err(|e| WalError::io(&active_path, e))?
            .len();

        Ok(Wal {
            dir,
            options,
            active: BufWriter::with_capacity(256 * 1024, file),
            active_path,
            active_index,
            active_bytes,
            appended: durable,
            durable,
            scratch: Vec::with_capacity(4096),
        })
    }

    /// Hand a record to the operating system. **Not** durable until [`Wal::sync`] returns.
    pub fn append(&mut self, record: &LogRecord) -> Result<()> {
        if let Some(prev) = self.appended {
            if record.seq != prev.next() {
                return Err(WalError::SequenceGap {
                    from: prev,
                    found: record.seq,
                });
            }
        }

        self.scratch.clear();
        encode_into(record, &mut self.scratch)?;

        // Roll before writing, never mid-record: a record that spanned two files would have to
        // be reassembled during replay, and reassembly is exactly the kind of code that is only
        // ever exercised during an incident.
        if self.active_bytes > 0
            && self.active_bytes + self.scratch.len() as u64 > self.options.segment_bytes
        {
            self.rotate()?;
        }

        self.active
            .write_all(&self.scratch)
            .map_err(|e| WalError::io(&self.active_path, e))?;
        self.active_bytes += self.scratch.len() as u64;
        self.appended = Some(record.seq);
        Ok(())
    }

    /// Flush and `fsync`. On return, everything appended so far survives power loss.
    ///
    /// Uses `sync_data` rather than `sync_all`: the file's length is data as far as the
    /// filesystem is concerned, and the metadata we would additionally be paying for — access
    /// times — is not something recovery consults.
    pub fn sync(&mut self) -> Result<Option<Seq>> {
        self.active
            .flush()
            .map_err(|e| WalError::io(&self.active_path, e))?;
        self.active
            .get_ref()
            .sync_data()
            .map_err(|e| WalError::io(&self.active_path, e))?;
        self.durable = self.appended;
        Ok(self.durable)
    }

    /// The highest sequence number known to have survived a power loss.
    pub fn durable_seq(&self) -> Option<Seq> {
        self.durable
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn options(&self) -> &WalOptions {
        &self.options
    }

    fn rotate(&mut self) -> Result<()> {
        self.active
            .flush()
            .map_err(|e| WalError::io(&self.active_path, e))?;
        self.active
            .get_ref()
            .sync_data()
            .map_err(|e| WalError::io(&self.active_path, e))?;

        self.active_index += 1;
        self.active_path = segment_path(&self.dir, self.active_index);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.active_path)
            .map_err(|e| WalError::io(&self.active_path, e))?;
        self.active = BufWriter::with_capacity(256 * 1024, file);
        self.active_bytes = 0;
        tracing::info!(segment = self.active_index, "rolled to a new wal segment");
        Ok(())
    }
}

/// Every record in the log, in order.
///
/// A torn record at the very end of the final segment is discarded: it was never acknowledged,
/// because acknowledgement happens after `fsync`, so nobody was told it happened. The same
/// damage anywhere earlier is corruption and is reported as such.
pub fn read_all(dir: impl AsRef<Path>) -> Result<Vec<LogRecord>> {
    let dir = dir.as_ref();
    let segments = segments(dir)?;
    let mut all: Vec<LogRecord> = Vec::new();

    for (i, (_, path)) in segments.iter().enumerate() {
        let contents = read_segment(path)?;
        let is_last = i + 1 == segments.len();
        if contents.trailing_bytes > 0 && !is_last {
            return Err(WalError::Corrupt {
                path: path.clone(),
                offset: 0,
                detail: format!(
                    "{} unreadable trailing bytes in a sealed segment",
                    contents.trailing_bytes
                ),
            });
        }
        if contents.trailing_bytes > 0 {
            tracing::warn!(
                bytes = contents.trailing_bytes,
                segment = ?path,
                "discarded a torn tail: these bytes were never acknowledged"
            );
        }
        for record in contents.records {
            if let Some(prev) = all.last() {
                if record.seq != prev.seq.next() {
                    return Err(WalError::SequenceGap {
                        from: prev.seq,
                        found: record.seq,
                    });
                }
            }
            all.push(record);
        }
    }
    Ok(all)
}

/// Shorten `path` to its last complete record, if a crash left a partial one behind.
fn truncate_torn_tail(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let contents = read_segment(path)?;
    if contents.trailing_bytes == 0 {
        return Ok(());
    }
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|e| WalError::io(path, e))?;
    let len = file.metadata().map_err(|e| WalError::io(path, e))?.len();
    let valid = len - contents.trailing_bytes as u64;
    file.set_len(valid).map_err(|e| WalError::io(path, e))?;
    file.sync_all().map_err(|e| WalError::io(path, e))?;
    tracing::warn!(
        discarded = contents.trailing_bytes,
        segment = ?path,
        "truncated a torn tail left by an unclean shutdown"
    );
    Ok(())
}

fn last_seq_in(dir: &Path) -> Result<Option<Seq>> {
    Ok(read_all(dir)?.last().map(|r| r.seq))
}

/// The auction this log belongs to.
///
/// Stored as JSON rather than bincode: it is written once, read once, and the one artifact in
/// the directory a human might have to read during an incident.
pub fn read_manifest(dir: impl AsRef<Path>) -> Result<AuctionConfig> {
    let path = dir.as_ref().join(MANIFEST);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(WalError::NoManifest {
                path: dir.as_ref().to_path_buf(),
            })
        }
        Err(e) => return Err(WalError::io(&path, e)),
    };
    serde_json::from_slice(&bytes).map_err(|e| WalError::Corrupt {
        path: path.clone(),
        offset: 0,
        detail: e.to_string(),
    })
}

fn write_manifest(dir: &Path, config: &AuctionConfig) -> Result<()> {
    let path = dir.join(MANIFEST);
    let bytes = serde_json::to_vec_pretty(config).map_err(|e| WalError::Corrupt {
        path: path.clone(),
        offset: 0,
        detail: e.to_string(),
    })?;
    let mut file = File::create(&path).map_err(|e| WalError::io(&path, e))?;
    file.write_all(&bytes).map_err(|e| WalError::io(&path, e))?;
    file.sync_all().map_err(|e| WalError::io(&path, e))?;
    Ok(())
}
