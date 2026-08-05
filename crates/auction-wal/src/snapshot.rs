//! Snapshots: a state, and the sequence number it corresponds to.
//!
//! A snapshot is purely an optimisation — the log alone is always sufficient to rebuild the
//! auction. That framing matters, because it means a snapshot that fails its checksum is not an
//! incident: recovery falls back to an older one, or to the log from the beginning, and reaches
//! exactly the same state either way. Determinism (invariant I5) is what makes a corrupt
//! snapshot a performance problem rather than a data-loss one.
//!
//! The sequence number in the filename is not decoration. A snapshot without one is unusable:
//! there would be no way to know which commands it already contains, and replaying too few or
//! too many produces a state nobody can distinguish from the correct one until settlement.
//!
//! # Writing without a window of loss
//!
//! Written to a temporary file, `fsync`ed, then renamed into place. A rename is atomic, so a
//! crash at any point leaves either the old snapshot or the new one, never a half-written file
//! wearing a name that promises it is complete.

use std::io::Write;
use std::path::{Path, PathBuf};

use auction_core::AuctionState;
use auction_proto::Seq;

use crate::error::{Result, WalError};

const PREFIX: &str = "snapshot-";
const SUFFIX: &str = ".snap";
const DIGITS: usize = 20;

/// A state plus its position in the log.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Snapshot {
    /// The last command applied to `state`. Replay resumes at `seq + 1`.
    pub seq: Seq,
    pub state: AuctionState,
}

fn snapshot_path(dir: &Path, seq: Seq) -> PathBuf {
    dir.join(format!("{PREFIX}{:0DIGITS$}{SUFFIX}", seq.0))
}

fn snapshot_seq(name: &str) -> Option<Seq> {
    let digits = name.strip_prefix(PREFIX)?.strip_suffix(SUFFIX)?;
    digits.parse().ok().map(Seq)
}

/// Write a snapshot of `state` at its last applied sequence.
///
/// Returns `None` for a state that has had nothing applied to it — there is nothing to
/// snapshot, and a snapshot with no sequence number could not be resumed from.
pub fn write(dir: impl AsRef<Path>, state: &AuctionState) -> Result<Option<Seq>> {
    let Some(seq) = state.last_seq() else {
        return Ok(None);
    };
    let dir = dir.as_ref();
    std::fs::create_dir_all(dir).map_err(|e| WalError::io(dir, e))?;

    let snapshot = Snapshot {
        seq,
        state: state.clone(),
    };
    let mut body = bincode::serialize(&snapshot).map_err(WalError::Encode)?;
    let crc = crc32fast::hash(&body);
    body.extend_from_slice(&crc.to_le_bytes());

    let final_path = snapshot_path(dir, seq);
    let temp_path = final_path.with_extension("snap.partial");
    {
        let mut file =
            std::fs::File::create(&temp_path).map_err(|e| WalError::io(&temp_path, e))?;
        file.write_all(&body)
            .map_err(|e| WalError::io(&temp_path, e))?;
        file.sync_all().map_err(|e| WalError::io(&temp_path, e))?;
    }
    std::fs::rename(&temp_path, &final_path).map_err(|e| WalError::io(&final_path, e))?;
    tracing::info!(%seq, "wrote snapshot");
    Ok(Some(seq))
}

/// Read one snapshot file, verifying its checksum.
pub fn read(path: impl AsRef<Path>) -> Result<Snapshot> {
    let path = path.as_ref();
    let bytes = std::fs::read(path).map_err(|e| WalError::io(path, e))?;
    if bytes.len() < 4 {
        return Err(WalError::Corrupt {
            path: path.to_path_buf(),
            offset: 0,
            detail: "shorter than its checksum".into(),
        });
    }
    let split = bytes.len() - 4;
    let (body, tail) = bytes.split_at(split);
    let expected = u32::from_le_bytes(tail.try_into().unwrap());
    if crc32fast::hash(body) != expected {
        return Err(WalError::Corrupt {
            path: path.to_path_buf(),
            offset: 0,
            detail: "checksum mismatch".into(),
        });
    }
    bincode::deserialize(body).map_err(|e| WalError::Corrupt {
        path: path.to_path_buf(),
        offset: 0,
        detail: e.to_string(),
    })
}

/// Snapshots in the directory, newest first.
pub fn list(dir: impl AsRef<Path>) -> Result<Vec<(Seq, PathBuf)>> {
    let dir = dir.as_ref();
    let mut found = Vec::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(found),
        Err(e) => return Err(WalError::io(dir, e)),
    };
    for entry in entries {
        let entry = entry.map_err(|e| WalError::io(dir, e))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(seq) = snapshot_seq(name) {
            found.push((seq, entry.path()));
        }
    }
    found.sort_unstable_by_key(|(seq, _)| std::cmp::Reverse(*seq));
    Ok(found)
}

/// The newest snapshot that actually loads.
///
/// Skips past any that do not, rather than failing: a bad snapshot costs replay time and
/// nothing else, so refusing to start over one would be trading a real outage for an imaginary
/// risk.
pub fn latest_usable(dir: impl AsRef<Path>) -> Result<Option<Snapshot>> {
    for (seq, path) in list(dir)? {
        match read(&path) {
            Ok(snapshot) => return Ok(Some(snapshot)),
            Err(e) => tracing::warn!(%seq, error = %e, "skipping an unreadable snapshot"),
        }
    }
    Ok(None)
}

/// Delete all but the newest `keep` snapshots.
///
/// Only snapshots are removed. Log segments are the audit record and are never deleted here,
/// whatever their age.
pub fn prune(dir: impl AsRef<Path>, keep: usize) -> Result<usize> {
    let all = list(&dir)?;
    let mut removed = 0;
    for (_, path) in all.into_iter().skip(keep) {
        std::fs::remove_file(&path).map_err(|e| WalError::io(&path, e))?;
        removed += 1;
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_names_sort_newest_first_by_sequence() {
        let dir = Path::new("x");
        let name = snapshot_path(dir, Seq(42));
        let name = name.file_name().unwrap().to_str().unwrap();
        assert_eq!(name, "snapshot-00000000000000000042.snap");
        assert_eq!(snapshot_seq(name), Some(Seq(42)));
        // Lexical order matches numeric order, so listing is sorting.
        let a = snapshot_path(dir, Seq(9));
        let b = snapshot_path(dir, Seq(10));
        assert!(a < b);
    }

    #[test]
    fn a_partial_file_is_not_mistaken_for_a_snapshot() {
        assert_eq!(
            snapshot_seq("snapshot-00000000000000000042.snap.partial"),
            None
        );
    }
}
