//! Segment files: naming, rotation, and reading one back.
//!
//! The log is split into fixed-size segments rather than one growing file so that old regions
//! become immutable and archivable the moment they are sealed. A snapshot at sequence N makes
//! every segment ending before N redundant for recovery, but they are still the audit record,
//! so they are moved rather than truncated — and moving a whole file is a rename, not a copy.
//!
//! Segments are named `wal-00000001.log`. Zero-padded because lexical order is then the same as
//! numeric order, which means a directory listing, a glob, and a sort all agree without anyone
//! having to remember to parse the number first.

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::error::{Result, WalError};
use crate::record::{decode, Decoded, LogRecord};

const PREFIX: &str = "wal-";
const SUFFIX: &str = ".log";
const DIGITS: usize = 8;

pub(crate) fn segment_path(dir: &Path, index: u32) -> PathBuf {
    dir.join(format!("{PREFIX}{index:0DIGITS$}{SUFFIX}"))
}

fn segment_index(name: &str) -> Option<u32> {
    let digits = name.strip_prefix(PREFIX)?.strip_suffix(SUFFIX)?;
    if digits.len() != DIGITS {
        return None;
    }
    digits.parse().ok()
}

/// Every segment in the directory, in sequence order.
pub(crate) fn segments(dir: &Path) -> Result<Vec<(u32, PathBuf)>> {
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
        if let Some(index) = segment_index(name) {
            found.push((index, entry.path()));
        }
    }
    found.sort_unstable_by_key(|(index, _)| *index);
    Ok(found)
}

/// The records in one segment, plus how many trailing bytes could not be read.
///
/// Trailing bytes are returned rather than judged. In the final segment they are a torn write
/// from a crash and are correctly discarded; anywhere else they mean the log is not what it
/// claims to be, and only the caller knows which segment this was.
pub(crate) struct SegmentContents {
    pub records: Vec<LogRecord>,
    pub trailing_bytes: usize,
}

pub(crate) fn read_segment(path: &Path) -> Result<SegmentContents> {
    let mut file = File::open(path).map_err(|e| WalError::io(path, e))?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)
        .map_err(|e| WalError::io(path, e))?;

    let mut records = Vec::new();
    let mut at = 0usize;
    while at < buf.len() {
        match decode(&buf[at..]) {
            Decoded::Record { record, consumed } => {
                records.push(record);
                at += consumed;
            }
            Decoded::Incomplete => break,
        }
    }
    Ok(SegmentContents {
        records,
        trailing_bytes: buf.len() - at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_names_sort_lexically_into_numeric_order() {
        let dir = Path::new("x");
        let a = segment_path(dir, 2).file_name().unwrap().to_owned();
        let b = segment_path(dir, 10).file_name().unwrap().to_owned();
        assert!(a < b, "{a:?} should sort before {b:?}");
        assert_eq!(a.to_str().unwrap(), "wal-00000002.log");
    }

    #[test]
    fn only_well_formed_segment_names_are_recognised() {
        assert_eq!(segment_index("wal-00000007.log"), Some(7));
        assert_eq!(segment_index("wal-7.log"), None);
        assert_eq!(segment_index("snapshot-00000007.snap"), None);
        assert_eq!(segment_index("wal-0000000x.log"), None);
    }
}
