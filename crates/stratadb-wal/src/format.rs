//! WAL on-disk format. Normative descriptions: docs/storage-format/wal-v1.md
//! (read-only since 0.0.1) and docs/storage-format/wal-v2.md (written).
//!
//! v2 differs from v1 only in the frame header, which gains `synced_through`
//! (see [`classify_invalid_tail`]):
//!
//! ```text
//! v2 frame:
//!   0..4   crc            u32 LE  CRC-32C(payload ‖ bytes 4..25 of the frame header)
//!   4..8   payload_len    u32 LE
//!   8      record_type    u8
//!   9..17  seq            u64 LE
//!   17..25 synced_through u64 LE  every seq <= this was fsynced before this
//!                                 frame was written (< seq)
//!   25..   payload
//! ```
//!
//! v1 layout:
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
/// Version written by this build. Readers accept [`MIN_READ_VERSION`]..=this.
pub const FORMAT_VERSION: u16 = 2;
pub const MIN_READ_VERSION: u16 = 1;
pub const HEADER_LEN: usize = 32;
/// Frame header length of the version written by this build.
pub const FRAME_HEADER_LEN: usize = 25;

/// Frame header length for a segment format version.
pub fn frame_header_len(version: u16) -> usize {
    if version >= 2 {
        25
    } else {
        17
    }
}
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
    /// Format version of the segment (decides the frame layout).
    pub version: u16,
}

pub fn encode_header(h: &SegmentHeader) -> [u8; HEADER_LEN] {
    let mut b = [0u8; HEADER_LEN];
    b[0..8].copy_from_slice(&MAGIC);
    b[8..10].copy_from_slice(&h.version.to_le_bytes());
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
    if !(MIN_READ_VERSION..=FORMAT_VERSION).contains(&version) {
        return Err(Error::FormatVersion {
            what: "WAL",
            found: u32::from(version),
            supported: format!("{MIN_READ_VERSION}..={FORMAT_VERSION}"),
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
        version,
    })
}

/// Build a complete v2 frame in `frame` (`FRAME_HEADER_LEN` header bytes
/// followed by the payload). `payload_crc` must be `crc32c(payload)`; it is
/// accepted separately so callers can compute it outside a lock.
/// `synced_through` must be < `seq`: the highest sequence number already
/// fsynced when this frame is written.
pub fn encode_frame_into(
    frame: &mut [u8],
    record_type: RecordType,
    seq: u64,
    synced_through: u64,
    payload_len: u32,
    payload_crc: u32,
) {
    frame[4..8].copy_from_slice(&payload_len.to_le_bytes());
    frame[8] = record_type as u8;
    frame[9..17].copy_from_slice(&seq.to_le_bytes());
    frame[17..25].copy_from_slice(&synced_through.to_le_bytes());
    let crc = crc32c_append(payload_crc, &frame[4..FRAME_HEADER_LEN]);
    frame[0..4].copy_from_slice(&crc.to_le_bytes());
}

/// Build a complete v1 frame (tests and compatibility fixtures only).
pub fn encode_frame_v1(record_type: RecordType, seq: u64, payload: &[u8]) -> Vec<u8> {
    let mut f = vec![0u8; 17];
    f.extend_from_slice(payload);
    f[4..8].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    f[8] = record_type as u8;
    f[9..17].copy_from_slice(&seq.to_le_bytes());
    let crc = crc32c_append(crc32c(payload), &f[4..17]);
    f[0..4].copy_from_slice(&crc.to_le_bytes());
    f
}

/// A decoded frame borrowing its payload.
#[derive(Debug)]
pub struct Frame<'a> {
    pub record_type: u8,
    pub seq: u64,
    /// v2: every seq <= this was durable when the frame was written. `None`
    /// for v1 frames (unknown).
    pub synced_through: Option<u64>,
    pub payload: &'a [u8],
    header_len: usize,
}

impl Frame<'_> {
    pub fn encoded_len(&self) -> usize {
        self.header_len + self.payload.len()
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

/// Parse one frame of a `version` segment at the start of `b`.
pub fn parse_frame(b: &[u8], version: u16) -> std::result::Result<Frame<'_>, FrameError> {
    let hl = frame_header_len(version);
    if b.len() < hl {
        return Err(FrameError::Truncated);
    }
    let len = le_u32(&b[4..8]);
    if len > MAX_RECORD_BYTES {
        return Err(FrameError::BadLength);
    }
    let end = hl + len as usize;
    if b.len() < end {
        return Err(FrameError::Truncated);
    }
    let payload = &b[hl..end];
    let crc = crc32c_append(crc32c(payload), &b[4..hl]);
    if crc != le_u32(&b[0..4]) {
        return Err(FrameError::BadChecksum);
    }
    Ok(Frame {
        record_type: b[8],
        seq: le_u64(&b[9..17]),
        synced_through: (version >= 2).then(|| le_u64(&b[17..25])),
        payload,
        header_len: hl,
    })
}

/// Every checksum-valid frame with `seq >= min_seq` found at any offset of
/// `b`: (offset, seq, synced_through).
fn later_valid_frames(b: &[u8], min_seq: u64, version: u16) -> Vec<(usize, u64, Option<u64>)> {
    let hl = frame_header_len(version);
    let mut out = Vec::new();
    if b.len() < hl {
        return out;
    }
    for off in 0..=(b.len() - hl) {
        let h = &b[off..];
        let seq = le_u64(&h[9..17]);
        // Cheap plausibility filter before computing a checksum.
        if seq < min_seq || seq - min_seq > (1 << 32) {
            continue;
        }
        if let Ok(f) = parse_frame(h, version) {
            out.push((off, f.seq, f.synced_through));
        }
    }
    out
}

/// Search `b` for any checksum-valid frame with `seq >= min_seq`.
pub fn find_later_valid_frame(b: &[u8], min_seq: u64, version: u16) -> Option<usize> {
    later_valid_frames(b, min_seq, version).first().map(|f| f.0)
}

/// What follows an invalid frame in the final WAL segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidTail {
    /// No valid frame follows: a torn or lost tail. Safe to truncate.
    Torn,
    /// Valid frames follow, but none of them was written after the damaged
    /// record was fsynced (v2 `synced_through < expected_seq`), so every byte
    /// from the damaged record on was un-synced when power was lost — writes
    /// that reached the disk out of order. Safe to truncate: nothing in it was
    /// acknowledged under `sync`/`group_commit`.
    UnsyncedReordered,
    /// A later valid frame proves the damaged record had been fsynced (v2), or
    /// the segment is v1 and cannot tell: possible damage to acknowledged data.
    /// Recovery fails closed.
    PossiblyDurableDamage,
}

/// Classify the bytes `rest` that follow an invalid frame where a frame with
/// sequence number `expected_seq` was expected, in a `version` segment.
///
/// Soundness: an fsync makes *all* previously written bytes of the file
/// durable. If a later frame records `synced_through >= expected_seq`, the
/// damaged record had been fsynced before that frame was written, so power
/// loss cannot explain the damage.
pub fn classify_invalid_tail(rest: &[u8], expected_seq: u64, version: u16) -> InvalidTail {
    let later = later_valid_frames(rest, expected_seq, version);
    if later.is_empty() {
        return InvalidTail::Torn;
    }
    if version < 2 {
        return InvalidTail::PossiblyDurableDamage;
    }
    if later
        .iter()
        .any(|(_, _, st)| st.is_none_or(|st| st >= expected_seq))
    {
        InvalidTail::PossiblyDurableDamage
    } else {
        InvalidTail::UnsyncedReordered
    }
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

    fn frame(seq: u64, synced_through: u64, payload: &[u8]) -> Vec<u8> {
        let mut f = vec![0u8; FRAME_HEADER_LEN];
        f.extend_from_slice(payload);
        encode_frame_into(
            &mut f,
            RecordType::EventBatch,
            seq,
            synced_through,
            payload.len() as u32,
            crc32c(payload),
        );
        f
    }

    #[test]
    fn header_roundtrip_versions_and_corruption() {
        for version in [1, 2] {
            let h = SegmentHeader {
                base_seq: 42,
                created_ms: 7,
                version,
            };
            let mut b = encode_header(&h);
            assert_eq!(decode_header(&b, "x").unwrap(), h);
            b[13] ^= 1;
            assert!(decode_header(&b, "x").is_err());
        }
        for version in [0, 3] {
            let b = encode_header(&SegmentHeader {
                base_seq: 1,
                created_ms: 0,
                version,
            });
            assert!(matches!(
                decode_header(&b, "x"),
                Err(Error::FormatVersion { .. })
            ));
        }
    }

    #[test]
    fn frames_roundtrip_and_every_bitflip_detected() {
        let v2 = frame(9, 7, b"hello wal");
        let v1 = encode_frame_v1(RecordType::EventBatch, 9, b"hello wal");
        for (f, version, st) in [(v2, 2u16, Some(7)), (v1, 1, None)] {
            let p = parse_frame(&f, version).unwrap();
            assert_eq!(
                (p.seq, p.payload, p.synced_through),
                (9, &b"hello wal"[..], st)
            );
            assert_eq!(p.encoded_len(), f.len());
            for i in 0..f.len() {
                for bit in 0..8 {
                    let mut g = f.clone();
                    g[i] ^= 1 << bit;
                    assert!(
                        parse_frame(&g, version).is_err(),
                        "v{version}: flip at byte {i} bit {bit} undetected"
                    );
                }
            }
            for cut in 0..f.len() {
                assert_eq!(
                    parse_frame(&f[..cut], version).unwrap_err(),
                    FrameError::Truncated
                );
            }
        }
    }

    /// Golden bytes: any change to the v2 frame layout must fail here.
    #[test]
    fn v2_frame_golden_bytes() {
        let f = frame(0x0102, 0x01, b"ab");
        let hex: String = f.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            hex,
            format!("{}0200000001020100000000000001000000000000006162", {
                let crc = le_u32(&f[0..4]);
                hex_le(crc)
            })
        );
        // And the CRC is the documented one.
        let crc = crc32c_append(crc32c(b"ab"), &f[4..25]);
        assert_eq!(le_u32(&f[0..4]), crc);
    }

    fn hex_le(v: u32) -> String {
        v.to_le_bytes().iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn classifies_invalid_tails() {
        let garbage = vec![0xAAu8; 40];
        assert_eq!(classify_invalid_tail(&garbage, 5, 2), InvalidTail::Torn);
        // Frame 5 lost, frame 6 (written before 5 was synced) survived.
        let mut b = vec![0u8; 30];
        b.extend(frame(6, 4, b"x"));
        assert_eq!(
            classify_invalid_tail(&b, 5, 2),
            InvalidTail::UnsyncedReordered
        );
        assert_eq!(find_later_valid_frame(&b, 5, 2), Some(30));
        assert_eq!(find_later_valid_frame(&b, 7, 2), None);
        // Frame 7 was written after 5 was synced: 5 was durable → fail closed.
        b.extend(frame(7, 5, b"y"));
        assert_eq!(
            classify_invalid_tail(&b, 5, 2),
            InvalidTail::PossiblyDurableDamage
        );
        // v1 cannot tell.
        let mut c = vec![0u8; 30];
        c.extend(encode_frame_v1(RecordType::EventBatch, 6, b"x"));
        assert_eq!(
            classify_invalid_tail(&c, 5, 1),
            InvalidTail::PossiblyDurableDamage
        );
        assert_eq!(classify_invalid_tail(&garbage, 5, 1), InvalidTail::Torn);
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
