//! Timestamps: signed nanoseconds since the Unix epoch, UTC.
//!
//! Range: roughly years 1678..2262 (i64 nanoseconds). Validation layers apply
//! narrower windows. RFC 3339 parsing is strict: a timezone designator is
//! mandatory, leap seconds (`:60`) are rejected, and at most 9 fractional
//! digits are accepted.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Error, Result};

pub const NANOS_PER_SEC: i64 = 1_000_000_000;

/// Current wall-clock time in Unix nanoseconds.
pub fn now_unix_nanos() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_nanos()).unwrap_or(i64::MAX),
        Err(e) => -i64::try_from(e.duration().as_nanos()).unwrap_or(i64::MAX),
    }
}

/// Current wall-clock time in Unix milliseconds.
pub fn now_unix_millis() -> i64 {
    now_unix_nanos() / 1_000_000
}

// Howard Hinnant's days_from_civil / civil_from_days (public domain algorithms).
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let m = i64::from(m);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(y) => 29,
        _ => 28,
    }
}

fn bad(s: &str, why: &str) -> Error {
    Error::invalid_input(format!("invalid RFC 3339 timestamp {s:?}: {why}"))
}

fn digits(b: &[u8], s: &str) -> Result<u32> {
    if b.is_empty() || !b.iter().all(u8::is_ascii_digit) {
        return Err(bad(s, "expected digits"));
    }
    Ok(b.iter()
        .fold(0u32, |acc, &c| acc * 10 + u32::from(c - b'0')))
}

/// Parse an RFC 3339 timestamp (e.g. `2026-11-20T10:30:00.123Z`) into Unix nanoseconds.
/// A space is accepted in place of `T`.
pub fn parse_rfc3339(s: &str) -> Result<i64> {
    let b = s.as_bytes();
    if b.len() < 20 || b.len() > 40 {
        return Err(bad(s, "wrong length"));
    }
    if b[4] != b'-' || b[7] != b'-' || !(b[10] == b'T' || b[10] == b't' || b[10] == b' ') {
        return Err(bad(s, "expected YYYY-MM-DDTHH:MM:SS"));
    }
    if b[13] != b':' || b[16] != b':' {
        return Err(bad(s, "expected HH:MM:SS"));
    }
    let year = i64::from(digits(&b[0..4], s)?);
    let month = digits(&b[5..7], s)?;
    let day = digits(&b[8..10], s)?;
    let hour = digits(&b[11..13], s)?;
    let min = digits(&b[14..16], s)?;
    let sec = digits(&b[17..19], s)?;
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        return Err(bad(s, "date out of range"));
    }
    if hour > 23 || min > 59 || sec > 59 {
        return Err(bad(s, "time out of range"));
    }
    let mut i = 19;
    let mut nanos: i64 = 0;
    if b[i] == b'.' {
        i += 1;
        let start = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        let frac = &b[start..i];
        if frac.is_empty() || frac.len() > 9 {
            return Err(bad(s, "fraction must have 1..=9 digits"));
        }
        nanos = i64::from(digits(frac, s)?) * 10i64.pow(9 - frac.len() as u32);
    }
    let offset_secs: i64 = match b.get(i) {
        Some(b'Z') | Some(b'z') if i + 1 == b.len() => 0,
        Some(&sign @ (b'+' | b'-')) if i + 6 == b.len() && b[i + 3] == b':' => {
            let oh = i64::from(digits(&b[i + 1..i + 3], s)?);
            let om = i64::from(digits(&b[i + 4..i + 6], s)?);
            if oh > 23 || om > 59 {
                return Err(bad(s, "offset out of range"));
            }
            let o = oh * 3600 + om * 60;
            if sign == b'+' {
                o
            } else {
                -o
            }
        }
        _ => return Err(bad(s, "missing or malformed timezone")),
    };
    let days = days_from_civil(year, month, day);
    let secs =
        days * 86_400 + i64::from(hour) * 3600 + i64::from(min) * 60 + i64::from(sec) - offset_secs;
    secs.checked_mul(NANOS_PER_SEC)
        .and_then(|n| n.checked_add(nanos))
        .ok_or_else(|| bad(s, "outside representable range (1678..2262)"))
}

/// Format Unix nanoseconds as RFC 3339 UTC with 9 fractional digits.
pub fn format_rfc3339(nanos: i64) -> String {
    let secs = nanos.div_euclid(NANOS_PER_SEC);
    let frac = nanos.rem_euclid(NANOS_PER_SEC);
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{frac:09}Z",
        sod / 3600,
        (sod % 3600) / 60,
        sod % 60
    )
}

/// Append [`format_rfc3339`]'s text to `out` without allocating (the result
/// serializer's hot path, spec STRATA-SYS-06-003).
pub fn write_rfc3339(nanos: i64, out: &mut Vec<u8>) {
    let secs = nanos.div_euclid(NANOS_PER_SEC);
    let frac = nanos.rem_euclid(NANOS_PER_SEC);
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    let mut b = *b"0000-00-00T00:00:00.000000000Z";
    let mut put = |at: usize, width: usize, mut v: i64| {
        for k in (at..at + width).rev() {
            b[k] = b'0' + (v % 10) as u8;
            v /= 10;
        }
    };
    // Every i64 of nanoseconds falls in years 1677..=2262: four digits.
    put(0, 4, y);
    put(5, 2, i64::from(m));
    put(8, 2, i64::from(d));
    put(11, 2, sod / 3600);
    put(14, 2, (sod % 3600) / 60);
    put(17, 2, sod % 60);
    put(20, 9, frac);
    out.extend_from_slice(&b);
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn parses_examples() {
        assert_eq!(parse_rfc3339("1970-01-01T00:00:00Z").unwrap(), 0);
        assert_eq!(
            parse_rfc3339("2026-11-20T10:30:00.123Z").unwrap(),
            1_795_170_600_123_000_000
        );
        assert_eq!(
            parse_rfc3339("2026-11-20T16:00:00.123+05:30").unwrap(),
            parse_rfc3339("2026-11-20T10:30:00.123Z").unwrap()
        );
        assert_eq!(parse_rfc3339("1969-12-31T23:59:59.999999999Z").unwrap(), -1);
        assert_eq!(
            parse_rfc3339("2026-11-20 10:30:00Z").unwrap(),
            parse_rfc3339("2026-11-20T10:30:00Z").unwrap()
        );
    }

    #[test]
    fn rejects_malformed() {
        for s in [
            "2026-11-20T10:30:00",
            "2026-02-30T00:00:00Z",
            "2026-13-01T00:00:00Z",
            "2026-11-20T24:00:00Z",
            "2026-11-20T23:59:60Z",
            "2026-11-20T10:30:00.Z",
            "2026-11-20T10:30:00.1234567890Z",
            "2026-11-20T10:30:00+5:30",
            "9999-12-31T00:00:00Z",
            "２０２６-11-20T10:30:00Z",
        ] {
            assert!(parse_rfc3339(s).is_err(), "{s} accepted");
        }
        assert!(parse_rfc3339("2024-02-29T00:00:00Z").is_ok());
        assert!(parse_rfc3339("2023-02-29T00:00:00Z").is_err());
    }

    #[test]
    fn formats() {
        assert_eq!(format_rfc3339(0), "1970-01-01T00:00:00.000000000Z");
        assert_eq!(format_rfc3339(-1), "1969-12-31T23:59:59.999999999Z");
    }

    proptest! {
        #[test]
        fn roundtrip(n in -9_000_000_000_000_000_000i64..9_000_000_000_000_000_000i64) {
            prop_assert_eq!(parse_rfc3339(&format_rfc3339(n)).unwrap(), n);
        }

        #[test]
        fn write_equals_format(n in any::<i64>()) {
            let mut out = Vec::new();
            write_rfc3339(n, &mut out);
            prop_assert_eq!(out, format_rfc3339(n).into_bytes());
        }
    }
}
