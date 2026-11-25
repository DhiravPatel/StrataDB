//! Keyset pagination cursors for row queries (spec §2.3, ADR-0023).
//!
//! A cursor is an opaque, URL-safe token that records the position of the last
//! row returned: the values of the `ORDER BY` keys plus the row identity
//! `(seq, idx)` — the WAL sequence number of its batch and its index within the
//! batch, which is unique per tenant and preserved by flush, compaction,
//! retention rewrites, upgrades and restore. The next page returns rows that
//! sort strictly after that position in the total order
//! `(order keys…, seq ASC, idx ASC)`, so pages never overlap.
//!
//! The cursor also pins `as_of_seq`, the largest sequence number visible to the
//! first page's snapshot. Later pages ignore rows with a larger sequence
//! number, so writes acknowledged after the first page never appear.
//!
//! Wire format v1 (then base64url without padding):
//! `version u8 | fingerprint u32 | as_of_seq uvarint | returned uvarint |
//!  nkeys uvarint | (present u8 [value])* | seq uvarint | idx u32 | crc32c u32`.
//! The CRC covers every preceding byte. Cursors are positional only: they
//! grant no access (the tenant still comes from the credential), so they are
//! checksummed, not signed.

use stratadb_common::checksum::crc32c;
use stratadb_common::codec::{Decoder, Encoder};
use stratadb_common::{Error, Result, TenantId};
use stratadb_schema::codec::{decode_value, encode_value, DecodeLimits};

use crate::eval::Datum;

/// Cursor wire format version.
pub const CURSOR_VERSION: u8 = 1;
/// Upper bound on an encoded cursor, checked before decoding.
pub const MAX_CURSOR_CHARS: usize = 64 * 1024;
const MAX_KEYS: usize = 64;

/// Decoded cursor. Construct only through [`Cursor::decode`] or the executor.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Cursor {
    /// Binds the cursor to one tenant and query text.
    pub fingerprint: u32,
    pub as_of_seq: u64,
    /// Rows returned by all previous pages (enforces the query's LIMIT).
    pub returned: u64,
    /// `ORDER BY` key values of the last returned row, in order-key order.
    pub keys: Vec<Datum>,
    pub seq: u64,
    pub idx: u32,
}

/// Fingerprint of (tenant, query text). Surrounding whitespace is ignored;
/// any other change to the text makes previously issued cursors invalid.
pub(crate) fn fingerprint(tenant: &TenantId, sql: &str) -> u32 {
    let mut e = Encoder::new();
    e.put_str("stratadb-cursor-v1");
    e.put_str(tenant.as_str());
    e.put_str(sql.trim());
    crc32c(e.as_slice())
}

fn bad(msg: &str) -> Error {
    Error::invalid_input(format!("invalid pagination cursor: {msg}"))
}

impl Cursor {
    pub fn encode(&self) -> String {
        let mut e = Encoder::new();
        e.put_u8(CURSOR_VERSION);
        e.put_u32(self.fingerprint);
        e.put_uvarint(self.as_of_seq);
        e.put_uvarint(self.returned);
        e.put_uvarint(self.keys.len() as u64);
        for k in &self.keys {
            match k {
                None => e.put_u8(0),
                Some(v) => {
                    e.put_u8(1);
                    encode_value(&mut e, v);
                }
            }
        }
        e.put_uvarint(self.seq);
        e.put_u32(self.idx);
        let crc = crc32c(e.as_slice());
        e.put_u32(crc);
        base64url_encode(e.as_slice())
    }

    pub fn decode(text: &str) -> Result<Cursor> {
        if text.len() > MAX_CURSOR_CHARS {
            return Err(bad("too long"));
        }
        let bytes = base64url_decode(text).ok_or_else(|| bad("not base64url"))?;
        if bytes.len() < 1 + 4 + 4 {
            return Err(bad("truncated"));
        }
        let (body, tail) = bytes.split_at(bytes.len() - 4);
        let stored = u32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]);
        if crc32c(body) != stored {
            return Err(bad("checksum mismatch (corrupted or edited)"));
        }
        let parse = || decode_body(body);
        // Decoder errors are "corruption" for storage; for a client-supplied
        // token they are invalid input.
        parse().map_err(|e| {
            if e.kind() == stratadb_common::ErrorKind::InvalidInput {
                e
            } else {
                bad(&e.to_string())
            }
        })
    }
}

/// Decode a cursor body (after base64 and CRC checks).
fn decode_body(body: &[u8]) -> Result<Cursor> {
    let lim = DecodeLimits {
        max_depth: 16,
        max_string_bytes: MAX_CURSOR_CHARS,
        max_items: 4096,
    };
    let mut d = Decoder::new(body, "cursor");
    let version = d.get_u8()?;
    if version != CURSOR_VERSION {
        return Err(bad(&format!("unsupported version {version}")));
    }
    let fingerprint = d.get_u32()?;
    let as_of_seq = d.get_uvarint()?;
    let returned = d.get_uvarint()?;
    let n = d.get_len(MAX_KEYS, 1)?;
    let mut keys = Vec::with_capacity(n);
    for _ in 0..n {
        keys.push(match d.get_u8()? {
            0 => None,
            1 => Some(decode_value(&mut d, &lim)?),
            t => return Err(bad(&format!("bad key tag {t}"))),
        });
    }
    let seq = d.get_uvarint()?;
    let idx = d.get_u32()?;
    d.expect_end()?;
    Ok(Cursor {
        fingerprint,
        as_of_seq,
        returned,
        keys,
        seq,
        idx,
    })
}

/// Fuzzing hooks (not a stable API).
#[doc(hidden)]
pub mod fuzz_hooks {
    use super::*;

    /// Decode a cursor body directly (no base64, no CRC check).
    pub fn decode_body(body: &[u8]) -> bool {
        super::decode_body(body).is_ok()
    }

    /// The part of a real cursor that [`cursor_text`] takes as `rest`
    /// (corpus seeds): base64-decoded, without version, fingerprint and CRC.
    pub fn cursor_rest(text: &str) -> Option<Vec<u8>> {
        let b = base64url_decode(text)?;
        (b.len() >= 9).then(|| b[5..b.len() - 4].to_vec())
    }

    /// A cursor text for `(tenant, sql)` whose body is version ‖ correct
    /// fingerprint ‖ `rest`, with a valid CRC — so fuzzed key values reach
    /// the executor's cursor comparisons instead of failing a check.
    pub fn cursor_text(tenant: &TenantId, sql: &str, rest: &[u8]) -> String {
        let mut b = vec![CURSOR_VERSION];
        b.extend_from_slice(&fingerprint(tenant, sql).to_le_bytes());
        b.extend_from_slice(rest);
        let crc = crc32c(&b);
        b.extend_from_slice(&crc.to_le_bytes());
        base64url_encode(&b)
    }
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// RFC 4648 §5 base64url without padding.
pub(crate) fn base64url_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..chunk.len() + 1 {
            out.push(char::from(B64[((n >> (18 - 6 * i)) & 63) as usize]));
        }
    }
    out
}

/// Strict inverse of [`base64url_encode`]: rejects padding, other alphabets,
/// impossible lengths and non-zero trailing bits (so encodings are canonical).
pub(crate) fn base64url_decode(s: &str) -> Option<Vec<u8>> {
    let val = |c: u8| -> Option<u32> {
        Some(match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            _ => return None,
        } as u32)
    };
    let bytes = s.as_bytes();
    if bytes.len() % 4 == 1 {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3 + 2);
    for chunk in bytes.chunks(4) {
        let mut n = 0u32;
        for (i, c) in chunk.iter().enumerate() {
            n |= val(*c)? << (18 - 6 * i);
        }
        let produced = chunk.len() - 1;
        // Bits beyond the produced bytes must be zero.
        let used_bits = 8 * produced;
        if used_bits < 24 && n & ((1 << (24 - used_bits)) - 1) != 0 {
            return None;
        }
        for i in 0..produced {
            out.push((n >> (16 - 8 * i)) as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use proptest::prelude::*;
    use stratadb_schema::Value;

    fn sample() -> Cursor {
        Cursor {
            fingerprint: 0xdead_beef,
            as_of_seq: 1 << 40,
            returned: 250,
            keys: vec![
                Some(Value::Timestamp(-5)),
                None,
                Some(Value::String("x".repeat(70))),
                Some(Value::Float(f64::NAN)),
                Some(Value::Null),
            ],
            seq: 99,
            idx: 7,
        }
    }

    #[test]
    fn round_trip() {
        let c = sample();
        let d = Cursor::decode(&c.encode()).unwrap();
        // NaN != NaN, so compare encodings.
        assert_eq!(d.encode(), c.encode());
        assert_eq!((d.seq, d.idx, d.returned), (99, 7, 250));
    }

    #[test]
    fn rejects_tampering_and_garbage() {
        let text = sample().encode();
        for i in 0..text.len() {
            let mut b = text.clone().into_bytes();
            b[i] = if b[i] == b'A' { b'B' } else { b'A' };
            let s = String::from_utf8(b).unwrap();
            if s != text {
                let e = Cursor::decode(&s).unwrap_err();
                assert_eq!(e.kind(), stratadb_common::ErrorKind::InvalidInput);
            }
        }
        for s in [
            "",
            "A",
            "====",
            "ab+/",
            "\u{e9}t\u{e9}",
            &"A".repeat(MAX_CURSOR_CHARS + 1),
        ] {
            assert!(Cursor::decode(s).is_err(), "{s:?}");
        }
        // Truncation.
        assert!(Cursor::decode(&text[..text.len() - 3]).is_err());
    }

    #[test]
    fn base64_known_vectors() {
        // RFC 4648 §10 vectors, unpadded.
        for (raw, enc) in [
            ("", ""),
            ("f", "Zg"),
            ("fo", "Zm8"),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg"),
            ("fooba", "Zm9vYmE"),
            ("foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(base64url_encode(raw.as_bytes()), enc);
            assert_eq!(base64url_decode(enc).unwrap(), raw.as_bytes());
        }
        assert_eq!(base64url_encode(&[0xfb, 0xff]), "-_8");
        // Non-canonical trailing bits are rejected.
        assert!(base64url_decode("Zh").is_none());
    }

    proptest! {
        #[test]
        fn base64_round_trip(data in proptest::collection::vec(any::<u8>(), 0..300)) {
            let e = base64url_encode(&data);
            prop_assert_eq!(base64url_decode(&e).unwrap(), data);
        }

        #[test]
        fn decode_never_panics(s in "[A-Za-z0-9_-]{0,200}") {
            let _ = Cursor::decode(&s);
        }
    }
}
