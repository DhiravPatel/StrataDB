//! CRC-32C (Castagnoli) checksums used by every persistent format.
//!
//! CRC-32C detects accidental corruption (torn writes, bit rot). It is not a
//! cryptographic integrity mechanism; tamper evidence requires signatures.

/// Compute CRC-32C of `data`.
#[inline]
pub fn crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// Extend a running CRC-32C with more bytes.
#[inline]
pub fn crc32c_append(crc: u32, data: &[u8]) -> u32 {
    crc32c::crc32c_append(crc, data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vectors() {
        // RFC 3720 B.4 test vectors.
        assert_eq!(crc32c(&[0u8; 32]), 0x8A91_36AA);
        assert_eq!(crc32c(&[0xFFu8; 32]), 0x62A8_AB43);
        let inc: Vec<u8> = (0u8..32).collect();
        assert_eq!(crc32c(&inc), 0x46DD_794E);
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn append_equals_whole() {
        let a = b"hello ";
        let b = b"world";
        assert_eq!(crc32c_append(crc32c(a), b), crc32c(b"hello world"));
    }
}
