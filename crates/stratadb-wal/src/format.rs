//! WAL on-disk format v1. Normative description: docs/storage-format/wal-v1.md.
//!
//! Segment file `wal-<base_seq:020>.log`:
//!
//! ```text
//! header (32 bytes):
//!   0..8   magic          b"STRWAL\r\n"
//!   8..10  format_version u16 LE (= 1)
//!   10..12 flags          u16 LE (= 0; non-zero rejected)
//!   12..20 base_seq       u64 LE  (sequence number of the first record)
//!   20..28 created_ms     i64 LE  (Unix ms; informational)
//!   28..32 header_crc     u32 LE  CRC-32C of bytes 0..28
//! frames, back to back, no padding:
//!   0..4   crc            u32 LE  CRC-32C(payload ‖ bytes 4..17 of the frame header)
//!   4..8   payload_len    u32 LE  (<= MAX_RECORD_BYTES)
//!   8      record_type    u8
//!   9..17  seq            u64 LE  (strictly +1 per frame within a segment)
//!   17..   payload
//! ```

use stratadb_common::checksum::{crc32c, crc32c_append};
use stratadb_common::{Error, Result};

pub const MAGIC: [u8; 8] = *b"STRWAL\r\n";
pub const FORMAT_VERSION: u16 = 1;
pub const HEADER_LEN: usize = 32;
pub const FRAME_HEADER_LEN: usize = 17;
/// Absolute maximum payload size accepted by the v1 reader (format limit).
pub const MAX_RECORD_BYTES: u32 = 256 * 1024 * 1024;

/// Record payload types. Unknown types with a valid checksum are reported as
/// `Unsupported` (written by a newer version), never skipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordType {
    /// A batch of canonical events (schema crate `encode_batch`), committed atomically.
    EventBatch = 1,
    /// An event batch carrying an idempotency key header (storage crate
    /// `idempotency::encode_record`). Added in StrataDB 0.0.1; readers that
    /// predate it report `Unsupported` rather than skipping it.
    IdempotentEventBatch = 2,
}

impl RecordType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(RecordType::EventBatch),
            2 => Some(RecordType::IdempotentEventBatch),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeader {
    pub base_seq: u64,
    pub created_ms: i64,
}

pub fn encode_header(h: &SegmentHeader) -> [u8; HEADER_LEN] {
    let mut b = [0u8; HEADER_LEN];
    b[0..8].copy_from_slice(&MAGIC);
    b[8..10].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    b[10..12].copy_from_slice(&0u16.to_le_bytes());
    b[12..20].copy_from_slice(&h.base_seq.to_le_bytes());
    b[20..28].copy_from_slice(&h.created_ms.to_le_bytes());
    let crc = crc32c(&b[0..28]);
    b[28..32].copy_from_slice(&crc.to_le_bytes());
    b
}

fn le_u16(b: &[u8]) -> u16 {
    u16::from_le_bytes([b[0], b[1]])
}
fn le_u32(b: &[u8]) -> u32 {
    u32::from_le_bytes([b[0], b[1], b[2], b[3]])
}
fn le_u64(b: &[u8]) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[..8]);
    u64::from_le_bytes(a)
}

pub fn decode_header(b: &[u8], location: &str) -> Result<SegmentHeader> {
    if b.len() < HEADER_LEN {
        return Err(Error::corruption(location, "file shorter than WAL header"));
    }
    if b[0..8] != MAGIC {
        return Err(Error::corruption(location, "bad WAL magic"));
    }
    let crc = le_u32(&b[28..32]);
    if crc32c(&b[0..28]) != crc {
        return Err(Error::corruption(location, "WAL header checksum mismatch"));
    }
    let version = le_u16(&b[8..10]);
    if version != FORMAT_VERSION {
        return Err(Error::FormatVersion {
            what: "WAL",
            found: u32::from(version),
            supported: FORMAT_VERSION.to_string(),
        });
    }
    if le_u16(&b[10..12]) != 0 {
        return Err(Error::unsupported(format!(
            "{location}: unknown WAL header flags"
        )));
    }
    let created = le_u64(&b[20..28]);
    Ok(SegmentHeader {
        base_seq: le_u64(&b[12..20]),
        created_ms: i64::from_le_bytes(created.to_le_bytes()),
    })
}

/// Build a complete frame. `payload_crc` must be `crc32c(payload)`; it is
/// accepted separately so callers can compute it outside a lock.
pub fn encode_frame_into(
    frame: &mut [u8],
    record_type: RecordType,
    seq: u64,
    payload_len: u32,
    payload_crc: u32,
) {
    frame[4..8].copy_from_slice(&payload_len.to_le_bytes());
    frame[8] = record_type as u8;
    frame[9..17].copy_from_slice(&seq.to_le_bytes());
    let crc = crc32c_append(payload_crc, &frame[4..FRAME_HEADER_LEN]);
    frame[0..4].copy_from_slice(&crc.to_le_bytes());
}

/// A decoded frame borrowing its payload.
#[derive(Debug)]
pub struct Frame<'a> {
    pub record_type: u8,
    pub seq: u64,
    pub payload: &'a [u8],
}

impl Frame<'_> {
    pub fn encoded_len(&self) -> usize {
        FRAME_HEADER_LEN + self.payload.len()
    }
}

/// Why bytes at a position are not a valid frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameError {
    /// Fewer bytes than a header, or than the declared payload.
    Truncated,
    /// Declared length exceeds the format maximum.
    BadLength,
    /// Checksum mismatch.
    BadChecksum,
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            FrameError::Truncated => "truncated frame",
            FrameError::BadLength => "frame length exceeds format maximum",
            FrameError::BadChecksum => "frame checksum mismatch",
        })
    }
}

/// Parse one frame at the start of `b`.
pub fn parse_frame(b: &[u8]) -> std::result::Result<Frame<'_>, FrameError> {
    if b.len() < FRAME_HEADER_LEN {
        return Err(FrameError::Truncated);
    }
    let len = le_u32(&b[4..8]);
    if len > MAX_RECORD_BYTES {
        return Err(FrameError::BadLength);
    }
    let end = FRAME_HEADER_LEN + len as usize;
    if b.len() < end {
        return Err(FrameError::Truncated);
    }
    let payload = &b[FRAME_HEADER_LEN..end];
    let crc = crc32c_append(crc32c(payload), &b[4..FRAME_HEADER_LEN]);
    if crc != le_u32(&b[0..4]) {
        return Err(FrameError::BadChecksum);
    }
    Ok(Frame {
        record_type: b[8],
        seq: le_u64(&b[9..17]),
        payload,
    })
}

/// Search `b` for any checksum-valid frame with `seq >= min_seq`.
/// Used to distinguish a torn tail (nothing valid follows) from mid-log
/// corruption (valid data follows the damaged region).
pub fn find_later_valid_frame(b: &[u8], min_seq: u64) -> Option<usize> {
    if b.len() < FRAME_HEADER_LEN {
        return None;
    }
    for off in 0..=(b.len() - FRAME_HEADER_LEN) {
        let h = &b[off..];
        let seq = le_u64(&h[9..17]);
        // Cheap plausibility filter before computing a checksum.
        if seq < min_seq || seq - min_seq > (1 << 32) {
            continue;
        }
        if let Ok(f) = parse_frame(h) {
            if f.seq >= min_seq {
                return Some(off);
            }
        }
    }
    None
}

/// File name for a segment starting at `base_seq`.
pub fn segment_file_name(base_seq: u64) -> String {
    format!("wal-{base_seq:020}.log")
}

/// Parse `wal-<20 digits>.log`.
pub fn parse_segment_file_name(name: &str) -> Option<u64> {
    let digits = name.strip_prefix("wal-")?.strip_suffix(".log")?;
    if digits.len() != 20 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(seq: u64, payload: &[u8]) -> Vec<u8> {
        let mut f = vec![0u8; FRAME_HEADER_LEN];
        f.extend_from_slice(payload);
        encode_frame_into(
            &mut f,
            RecordType::EventBatch,
            seq,
            payload.len() as u32,
            crc32c(payload),
        );
        f
    }

    #[test]
    fn header_roundtrip_and_corruption() {
        let h = SegmentHeader {
            base_seq: 42,
            created_ms: 7,
        };
        let mut b = encode_header(&h);
        assert_eq!(decode_header(&b, "x").unwrap(), h);
        b[13] ^= 1;
        assert!(decode_header(&b, "x").is_err());
    }

    #[test]
    fn frame_roundtrip_and_every_bitflip_detected() {
        let f = frame(9, b"hello wal");
        let p = parse_frame(&f).unwrap();
        assert_eq!((p.seq, p.payload), (9, &b"hello wal"[..]));
        for i in 0..f.len() {
            for bit in 0..8 {
                let mut g = f.clone();
                g[i] ^= 1 << bit;
                assert!(
                    parse_frame(&g).is_err(),
                    "flip at byte {i} bit {bit} undetected"
                );
            }
        }
        for cut in 0..f.len() {
            assert_eq!(parse_frame(&f[..cut]).unwrap_err(), FrameError::Truncated);
        }
    }

    #[test]
    fn finds_later_frames() {
        let mut b = vec![0xAAu8; 10];
        b.extend(frame(5, b"x"));
        assert_eq!(find_later_valid_frame(&b, 5), Some(10));
        assert_eq!(find_later_valid_frame(&b, 6), None);
    }

    #[test]
    fn file_names() {
        assert_eq!(parse_segment_file_name(&segment_file_name(17)), Some(17));
        assert_eq!(parse_segment_file_name("wal-1.log"), None);
        assert_eq!(
            parse_segment_file_name("wal-0000000000000000001x.log"),
            None
        );
    }
}
