//! Host facts for `strata doctor` (spec §22.3) without `unsafe` or new
//! dependencies: open-file limit, physical memory, and the expiry date of an
//! X.509 certificate.

use std::process::Command;

/// Soft limit on open file descriptors for processes started from this
/// environment (`None` if it cannot be determined; `u64::MAX` = unlimited).
/// Linux: `/proc/self/limits`; elsewhere the POSIX `ulimit -n` builtin.
pub fn open_file_limit() -> Option<u64> {
    if let Ok(s) = std::fs::read_to_string("/proc/self/limits") {
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("Max open files") {
                return parse_limit(rest.split_whitespace().next()?);
            }
        }
    }
    let out = Command::new("/bin/sh")
        .args(["-c", "ulimit -n"])
        .output()
        .ok()?;
    parse_limit(String::from_utf8_lossy(&out.stdout).trim())
}

fn parse_limit(s: &str) -> Option<u64> {
    if s == "unlimited" {
        Some(u64::MAX)
    } else {
        s.parse().ok()
    }
}

/// Kernel release (`uname -r`), for hosts without `/proc`.
pub fn uname_release() -> Option<String> {
    let out = Command::new("uname").arg("-r").output().ok()?;
    let s = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    (out.status.success() && !s.is_empty()).then_some(s)
}

/// CPU model on hosts without `/proc/cpuinfo` (macOS `sysctl`).
pub fn cpu_model() -> Option<String> {
    let out = Command::new("sysctl")
        .args(["-n", "machdep.cpu.brand_string"])
        .output()
        .ok()?;
    let s = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    (out.status.success() && !s.is_empty()).then_some(s)
}

/// Physical memory in bytes (Linux `/proc/meminfo`, macOS/BSD `sysctl`).
pub fn total_memory() -> Option<u64> {
    stratadb_common::memory::physical_memory()
}

/// Minimal bounded DER reader (definite lengths only, as DER requires).
struct Der<'a> {
    b: &'a [u8],
}

impl<'a> Der<'a> {
    /// Next TLV: (tag, contents).
    fn next(&mut self) -> Option<(u8, &'a [u8])> {
        let (&tag, rest) = self.b.split_first()?;
        let (&l0, mut rest) = rest.split_first()?;
        let len = if l0 & 0x80 == 0 {
            usize::from(l0)
        } else {
            let n = usize::from(l0 & 0x7f);
            if n == 0 || n > 4 || rest.len() < n {
                return None;
            }
            let mut len = 0usize;
            for &x in &rest[..n] {
                len = (len << 8) | usize::from(x);
            }
            rest = &rest[n..];
            len
        };
        if rest.len() < len {
            return None;
        }
        let (body, after) = rest.split_at(len);
        self.b = after;
        Some((tag, body))
    }
}

/// `notAfter` of a DER-encoded X.509 certificate as Unix seconds.
///
/// Certificate ::= SEQUENCE { tbsCertificate SEQUENCE { [0] version OPTIONAL,
/// serialNumber, signature, issuer, validity SEQUENCE { notBefore, notAfter },
/// … } … }. Time is UTCTime (`YYMMDDHHMMSSZ`, 1950–2049) or GeneralizedTime
/// (`YYYYMMDDHHMMSSZ`).
pub fn cert_not_after(der: &[u8]) -> Option<i64> {
    let (t, cert) = Der { b: der }.next()?;
    if t != 0x30 {
        return None;
    }
    let (t, tbs) = Der { b: cert }.next()?;
    if t != 0x30 {
        return None;
    }
    let mut d = Der { b: tbs };
    let (mut t, _) = d.next()?;
    if t == 0xa0 {
        // [0] EXPLICIT version present: next is serialNumber.
        t = d.next()?.0;
    }
    if t != 0x02 {
        return None; // serialNumber INTEGER
    }
    d.next()?; // signature AlgorithmIdentifier
    d.next()?; // issuer Name
    let (t, validity) = d.next()?;
    if t != 0x30 {
        return None;
    }
    let mut v = Der { b: validity };
    v.next()?; // notBefore
    let (tag, s) = v.next()?;
    parse_time(tag, std::str::from_utf8(s).ok()?)
}

fn parse_time(tag: u8, s: &str) -> Option<i64> {
    let s = s.strip_suffix('Z')?;
    let (year, rest) = match (tag, s.len()) {
        (0x17, 12) => {
            let yy: i64 = s.get(0..2)?.parse().ok()?;
            (if yy >= 50 { 1900 + yy } else { 2000 + yy }, s.get(2..)?)
        }
        (0x18, 14) => (s.get(0..4)?.parse().ok()?, s.get(4..)?),
        _ => return None,
    };
    let n = |r: std::ops::Range<usize>| -> Option<i64> { rest.get(r)?.parse().ok() };
    let (mo, day, h, mi, se) = (n(0..2)?, n(2..4)?, n(4..6)?, n(6..8)?, n(8..10)?);
    if !(1..=12).contains(&mo) || !(1..=31).contains(&day) || h > 23 || mi > 59 || se > 60 {
        return None;
    }
    let text = format!("{year:04}-{mo:02}-{day:02}T{h:02}:{mi:02}:{se:02}Z");
    stratadb_common::time::parse_rfc3339(&text)
        .ok()
        .map(|ns| ns.div_euclid(1_000_000_000))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_and_memory_are_readable_here() {
        let l = open_file_limit().unwrap_or(0);
        assert!(l >= 64, "open file limit {l}");
        assert!(total_memory().unwrap_or(0) > 256 << 20);
        assert_eq!(parse_limit("unlimited"), Some(u64::MAX));
        assert_eq!(parse_limit("abc"), None);
    }

    /// Dates written by an independent encoder (rcgen) parse exactly, for
    /// both UTCTime (< 2050) and GeneralizedTime (>= 2050).
    #[test]
    fn reads_not_after_of_real_certificates() {
        for (y, m, d, unix) in [(2027, 3, 9, 1_804_550_400i64), (2051, 1, 2, 2_556_230_400)] {
            let mut p = rcgen::CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
            p.not_before = rcgen::date_time_ymd(2026, 1, 1);
            p.not_after = rcgen::date_time_ymd(y, m, d);
            let key = rcgen::KeyPair::generate().unwrap();
            let cert = p.self_signed(&key).unwrap();
            assert_eq!(cert_not_after(cert.der()), Some(unix), "{y}-{m}-{d}");
            // Truncations never panic.
            for cut in 0..cert.der().len() {
                let _ = cert_not_after(&cert.der()[..cut]);
            }
        }
    }

    #[test]
    fn times_and_malformed_der() {
        assert_eq!(parse_time(0x17, "491231235959Z"), Some(2_524_607_999));
        assert_eq!(parse_time(0x17, "500101000000Z"), Some(-631_152_000));
        assert_eq!(parse_time(0x18, "20261125000000Z"), Some(1_795_564_800));
        assert_eq!(parse_time(0x17, "2611250000Z"), None);
        assert_eq!(parse_time(0x18, "20261325000000Z"), None);
        for bad in [
            &[][..],
            &[0x30],
            &[0x30, 0x84, 0xff, 0xff, 0xff, 0xff],
            &[0x30, 0x02, 0x30, 0x00],
        ] {
            assert_eq!(cert_not_after(bad), None);
        }
    }
}
