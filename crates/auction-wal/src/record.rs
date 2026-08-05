//! What a log entry is, and how it is framed on disk.
//!
//! A record is a command plus the two stamps the sequencer assigned it: its position in the
//! total order and the auction-relative time it arrived. Those three things are exactly the
//! arguments to `AuctionState::apply`, which is the point — the log is a recording of the calls
//! that were made, so replaying it is not a reconstruction, it is the same computation again.
//!
//! Events are deliberately *not* logged. They are recomputable from commands by a deterministic
//! machine (invariant I5), and writing them would mean paying for the derived data twice: once
//! in fsync latency on the hot path, and again in every future schema change.
//!
//! # Framing
//!
//! ```text
//! [u32 len][u32 crc32(payload)][payload: bincode(LogRecord)]
//! ```
//!
//! Little-endian, fixed 8-byte header. The checksum covers the payload only; a header that
//! survives but a payload that does not is caught by the CRC, and a header that does not
//! survive is caught by the length bound. Both cases read as a torn tail rather than as data.

use auction_core::Command;
use auction_proto::{Nanos, Seq};

use crate::error::{Result, WalError};

/// A single entry: one call to `apply`, recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LogRecord {
    pub seq: Seq,
    pub ts: Nanos,
    pub cmd: Command,
}

impl LogRecord {
    pub fn new(seq: Seq, ts: Nanos, cmd: Command) -> Self {
        Self { seq, ts, cmd }
    }
}

/// Size of the fixed frame header, in bytes.
pub(crate) const HEADER_LEN: usize = 8;

/// Refuses to allocate for an absurd length read out of a torn header. Commands are tens of
/// bytes; a megabyte is four orders of magnitude of headroom and still bounds the damage a
/// garbage length can do.
pub(crate) const MAX_RECORD_LEN: u32 = 1 << 20;

/// Append the framed encoding of `record` to `out`.
///
/// Takes a buffer rather than returning one so the commit path can reuse a single allocation
/// across a whole batch — this runs inside the latency budget.
pub(crate) fn encode_into(record: &LogRecord, out: &mut Vec<u8>) -> Result<()> {
    let start = out.len();
    out.extend_from_slice(&[0u8; HEADER_LEN]);
    bincode::serialize_into(&mut *out, record).map_err(WalError::Encode)?;

    let payload_len = out.len() - start - HEADER_LEN;
    let crc = crc32fast::hash(&out[start + HEADER_LEN..]);

    let len = u32::try_from(payload_len).map_err(|_| {
        WalError::Encode(Box::new(bincode::ErrorKind::Custom(format!(
            "record of {payload_len} bytes exceeds the frame length field"
        ))))
    })?;
    out[start..start + 4].copy_from_slice(&len.to_le_bytes());
    out[start + 4..start + HEADER_LEN].copy_from_slice(&crc.to_le_bytes());
    Ok(())
}

/// What a decode attempt found.
pub(crate) enum Decoded {
    Record {
        record: LogRecord,
        consumed: usize,
    },
    /// Not enough bytes, or the bytes present do not check out. Either way the caller cannot
    /// read past this point; whether that is benign depends on where in the log it happened,
    /// which is the reader's judgement to make, not this function's.
    Incomplete,
    /// The checksum passed and the payload still would not decode.
    ///
    /// This is categorically different from a torn write: the bytes on disk are exactly the
    /// bytes that were written, so the log is intact and it is our *reading* of it that is
    /// wrong — a format change, most likely. Treating it as an end-of-log would silently
    /// discard real, acknowledged commands, so it is never benign, wherever it appears.
    Malformed(String),
}

/// Decode one framed record from the front of `buf`.
pub(crate) fn decode(buf: &[u8]) -> Decoded {
    if buf.len() < HEADER_LEN {
        return Decoded::Incomplete;
    }
    let len = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    let crc = u32::from_le_bytes(buf[4..HEADER_LEN].try_into().unwrap());

    if len == 0 || len > MAX_RECORD_LEN {
        return Decoded::Incomplete;
    }
    let end = HEADER_LEN + len as usize;
    if buf.len() < end {
        return Decoded::Incomplete;
    }
    let payload = &buf[HEADER_LEN..end];
    if crc32fast::hash(payload) != crc {
        return Decoded::Incomplete;
    }
    match bincode::deserialize::<LogRecord>(payload) {
        Ok(record) => Decoded::Record {
            record,
            consumed: end,
        },
        Err(e) => Decoded::Malformed(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use auction_proto::ParticipantId;
    use uuid::Uuid;

    fn describe(d: &Decoded) -> String {
        match d {
            Decoded::Record { .. } => "a record".into(),
            Decoded::Incomplete => "incomplete".into(),
            Decoded::Malformed(why) => format!("malformed: {why}"),
        }
    }

    fn sample(seq: u64) -> LogRecord {
        LogRecord::new(
            Seq(seq),
            Nanos::from_millis(seq),
            Command::SetCollateral {
                participant: ParticipantId(Uuid::from_u128(seq as u128)),
                limit: 10_000,
            },
        )
    }

    #[test]
    fn a_record_survives_a_round_trip() {
        let mut buf = Vec::new();
        encode_into(&sample(7), &mut buf).unwrap();
        match decode(&buf) {
            Decoded::Record { record, consumed } => {
                assert_eq!(record, sample(7));
                assert_eq!(consumed, buf.len());
            }
            other => panic!("a whole record did not decode: {}", describe(&other)),
        }
    }

    #[test]
    fn records_pack_back_to_back() {
        let mut buf = Vec::new();
        for i in 0..5 {
            encode_into(&sample(i), &mut buf).unwrap();
        }
        let mut at = 0;
        for i in 0..5 {
            match decode(&buf[at..]) {
                Decoded::Record { record, consumed } => {
                    assert_eq!(record, sample(i));
                    at += consumed;
                }
                other => panic!("record {i} did not decode: {}", describe(&other)),
            }
        }
        assert_eq!(at, buf.len());
    }

    #[test]
    fn a_truncated_record_reads_as_incomplete_at_every_cut() {
        let mut buf = Vec::new();
        encode_into(&sample(1), &mut buf).unwrap();
        // Every prefix short of the whole thing must refuse to yield a record — a crash can
        // land the cut anywhere, including inside the header.
        for cut in 0..buf.len() {
            assert!(
                matches!(decode(&buf[..cut]), Decoded::Incomplete),
                "a {cut}-byte prefix decoded as a whole record"
            );
        }
    }

    #[test]
    fn a_flipped_bit_in_the_payload_is_caught() {
        let mut buf = Vec::new();
        encode_into(&sample(3), &mut buf).unwrap();
        buf[HEADER_LEN] ^= 0b1000_0000;
        assert!(matches!(decode(&buf), Decoded::Incomplete));
    }

    #[test]
    fn intact_bytes_we_cannot_read_are_not_mistaken_for_an_end_of_log() {
        // A well-formed frame whose payload is not a record: what a format change looks like
        // from the reader's side. Reporting this as a torn tail would throw away every command
        // after it, which is the failure mode the checksum exists to make impossible.
        let payload = b"this is not a log record";
        let mut buf = Vec::new();
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
        buf.extend_from_slice(payload);
        assert!(matches!(decode(&buf), Decoded::Malformed(_)));
    }

    #[test]
    fn a_garbage_length_does_not_become_a_giant_allocation() {
        let mut buf = vec![0u8; HEADER_LEN];
        buf[0..4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(decode(&buf), Decoded::Incomplete));
    }
}
