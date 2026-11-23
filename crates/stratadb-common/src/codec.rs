//! Little-endian binary encoding primitives with bounds-checked decoding.
//!
//! Invariants (spec §26.3):
//! * Decoding untrusted bytes never panics.
//! * Decoding never allocates more than the remaining input length for any
//!   length-prefixed field, so memory use is bounded by input size.
//! * Varints are canonical: overlong or overflowing encodings are rejected.

use crate::error::{Error, Result};

/// Maximum bytes in a LEB128-encoded u64.
pub const MAX_VARINT_LEN: usize = 10;

/// Append-only encoder over a `Vec<u8>`.
#[derive(Debug, Default)]
pub struct Encoder {
    buf: Vec<u8>,
}

impl Encoder {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buf: Vec::with_capacity(cap),
        }
    }

    pub fn from_vec(buf: Vec<u8>) -> Self {
        Self { buf }
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.buf
    }

    pub fn put_u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    pub fn put_u16(&mut self, v: u16) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn put_u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn put_u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn put_i64(&mut self, v: i64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn put_f64(&mut self, v: f64) {
        self.buf.extend_from_slice(&v.to_bits().to_le_bytes());
    }

    pub fn put_uvarint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.buf.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.buf.push(v as u8);
    }

    /// Zig-zag encoded signed varint.
    pub fn put_ivarint(&mut self, v: i64) {
        self.put_uvarint(((v << 1) ^ (v >> 63)) as u64);
    }

    pub fn put_raw(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// Length-prefixed (uvarint) byte string.
    pub fn put_bytes(&mut self, bytes: &[u8]) {
        self.put_uvarint(bytes.len() as u64);
        self.buf.extend_from_slice(bytes);
    }

    pub fn put_str(&mut self, s: &str) {
        self.put_bytes(s.as_bytes());
    }
}

/// Bounds-checked decoder. `what` names the structure being decoded so that
/// corruption errors identify their location.
#[derive(Debug, Clone)]
pub struct Decoder<'a> {
    buf: &'a [u8],
    pos: usize,
    what: &'static str,
}

impl<'a> Decoder<'a> {
    pub fn new(buf: &'a [u8], what: &'static str) -> Self {
        Self { buf, pos: 0, what }
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    fn err(&self, detail: impl Into<String>) -> Error {
        Error::corruption(format!("{} at byte {}", self.what, self.pos), detail.into())
    }

    /// Fail unless every byte has been consumed.
    pub fn expect_end(&self) -> Result<()> {
        if self.is_empty() {
            Ok(())
        } else {
            Err(self.err(format!("{} unexpected trailing bytes", self.remaining())))
        }
    }

    pub fn get_raw(&mut self, n: usize) -> Result<&'a [u8]> {
        if n > self.remaining() {
            return Err(self.err(format!(
                "truncated: need {n} bytes, have {}",
                self.remaining()
            )));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn get_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let s = self.get_raw(N)?;
        let mut a = [0u8; N];
        a.copy_from_slice(s);
        Ok(a)
    }

    pub fn get_u8(&mut self) -> Result<u8> {
        Ok(self.get_array::<1>()?[0])
    }

    pub fn get_u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.get_array()?))
    }

    pub fn get_u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.get_array()?))
    }

    pub fn get_u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.get_array()?))
    }

    pub fn get_i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.get_array()?))
    }

    pub fn get_f64(&mut self) -> Result<f64> {
        Ok(f64::from_bits(u64::from_le_bytes(self.get_array()?)))
    }

    pub fn get_uvarint(&mut self) -> Result<u64> {
        let mut result: u64 = 0;
        for i in 0..MAX_VARINT_LEN {
            let b = self.get_u8()?;
            let low = u64::from(b & 0x7f);
            if i == MAX_VARINT_LEN - 1 && low > 1 {
                return Err(self.err("varint overflows u64"));
            }
            result |= low << (7 * i);
            if b & 0x80 == 0 {
                if i > 0 && b == 0 {
                    return Err(self.err("non-canonical (overlong) varint"));
                }
                return Ok(result);
            }
        }
        Err(self.err("varint longer than 10 bytes"))
    }

    pub fn get_ivarint(&mut self) -> Result<i64> {
        let u = self.get_uvarint()?;
        Ok(((u >> 1) as i64) ^ -((u & 1) as i64))
    }

    /// Read a varint used as a length or count and check it against both an
    /// explicit cap and the remaining input (each element needs >= `min_elem_bytes`).
    pub fn get_len(&mut self, max: usize, min_elem_bytes: usize) -> Result<usize> {
        let n = self.get_uvarint()?;
        let n = usize::try_from(n).map_err(|_| self.err("length does not fit in usize"))?;
        if n > max {
            return Err(self.err(format!("length {n} exceeds limit {max}")));
        }
        if n.saturating_mul(min_elem_bytes.max(1)) > self.remaining() && min_elem_bytes > 0 {
            return Err(self.err(format!(
                "length {n} cannot fit in remaining {} bytes",
                self.remaining()
            )));
        }
        Ok(n)
    }

    pub fn get_bytes(&mut self, max: usize) -> Result<&'a [u8]> {
        let n = self.get_uvarint()?;
        let n = usize::try_from(n).map_err(|_| self.err("length does not fit in usize"))?;
        if n > max {
            return Err(self.err(format!("byte string length {n} exceeds limit {max}")));
        }
        self.get_raw(n)
    }

    pub fn get_str(&mut self, max: usize) -> Result<&'a str> {
        let b = self.get_bytes(max)?;
        std::str::from_utf8(b).map_err(|e| self.err(format!("invalid UTF-8: {e}")))
    }

    pub fn get_string(&mut self, max: usize) -> Result<String> {
        self.get_str(max).map(str::to_owned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn varint_boundaries() {
        for v in [0u64, 1, 127, 128, 16383, 16384, u32::MAX as u64, u64::MAX] {
            let mut e = Encoder::new();
            e.put_uvarint(v);
            let mut d = Decoder::new(e.as_slice(), "t");
            assert_eq!(d.get_uvarint().unwrap(), v);
            d.expect_end().unwrap();
        }
    }

    #[test]
    fn rejects_overlong_and_overflow() {
        // 0 encoded with a redundant continuation byte.
        assert!(Decoder::new(&[0x80, 0x00], "t").get_uvarint().is_err());
        // 11-byte varint.
        assert!(Decoder::new(&[0xff; 11], "t").get_uvarint().is_err());
        // 10th byte > 1 overflows u64.
        let mut b = vec![0xffu8; 9];
        b.push(0x02);
        assert!(Decoder::new(&b, "t").get_uvarint().is_err());
    }

    #[test]
    fn truncated_reads_error() {
        let mut d = Decoder::new(&[1, 2, 3], "t");
        assert!(d.get_u32().is_err());
        let mut d = Decoder::new(&[5, b'a'], "t");
        assert!(d.get_bytes(100).is_err());
    }

    #[test]
    fn len_respects_remaining() {
        let mut e = Encoder::new();
        e.put_uvarint(1_000_000);
        let mut d = Decoder::new(e.as_slice(), "t");
        assert!(d.get_len(usize::MAX, 1).is_err());
    }

    proptest! {
        #[test]
        fn ivarint_roundtrip(v in any::<i64>()) {
            let mut e = Encoder::new();
            e.put_ivarint(v);
            let mut d = Decoder::new(e.as_slice(), "t");
            prop_assert_eq!(d.get_ivarint().unwrap(), v);
        }

        #[test]
        fn decoder_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..64)) {
            let mut d = Decoder::new(&bytes, "fuzz");
            let _ = d.get_uvarint();
            let _ = d.get_str(16);
            let _ = d.get_len(8, 1);
            let _ = d.get_u64();
        }
    }
}
