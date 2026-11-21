//! The track's deliberately restricted CSV and timestamp formats.

/// Split one record according to the challenge dialect. A carriage return is
/// data, and quotes are valid only as the first and last bytes of a field.
pub fn fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let mut result = [b"".as_slice(); N];
    let mut parts = line.split(|&b| b == b',');
    for slot in &mut result {
        let raw = parts.next()?;
        *slot = if raw.first() == Some(&b'"') {
            if raw.len() < 2 || raw.last() != Some(&b'"') {
                return None;
            }
            let inner = &raw[1..raw.len() - 1];
            if inner.contains(&b'"') {
                return None;
            }
            inner
        } else {
            if raw.contains(&b'"') {
                return None;
            }
            raw
        };
    }
    if parts.next().is_some() {
        return None;
    }
    Some(result)
}

fn digits(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    s.iter().try_fold(0i64, |n, &b| {
        if !b.is_ascii_digit() {
            return None;
        }
        n.checked_mul(10)?.checked_add((b - b'0') as i64)
    })
}

/// Convert an allowed timestamp to Unix milliseconds, with overflow rejected.
pub fn timestamp(s: &[u8]) -> Option<i64> {
    if s.iter().all(u8::is_ascii_digit) {
        return digits(s);
    }
    if s.len() != 20 && s.len() != 25 {
        return None;
    }
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let (y, m, d) = (digits(&s[0..4])?, digits(&s[5..7])?, digits(&s[8..10])?);
    let (h, minute, sec) = (
        digits(&s[11..13])?,
        digits(&s[14..16])?,
        digits(&s[17..19])?,
    );
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let lengths = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if !(1..=12).contains(&m)
        || d < 1
        || d > lengths[(m - 1) as usize]
        || h > 23
        || minute > 59
        || sec > 59
    {
        return None;
    }
    let offset = if s.len() == 20 {
        if s[19] != b'Z' {
            return None;
        }
        0
    } else {
        if (s[19] != b'+' && s[19] != b'-') || s[22] != b':' {
            return None;
        }
        let oh = digits(&s[20..22])?;
        let om = digits(&s[23..25])?;
        if oh > 23 || om > 59 {
            return None;
        }
        (if s[19] == b'+' { 1 } else { -1 }) * (oh * 60 + om) * 60
    };
    let year = y - i64::from(m <= 2);
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let mp = m + if m > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let days = era * 146097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719468;
    Some((days * 86400 + h * 3600 + minute * 60 + sec - offset) * 1000)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dialect_and_timestamps() {
        assert_eq!(
            fields::<2>(b"\"u1\",\"\""),
            Some([b"u1".as_slice(), b"".as_slice()])
        );
        assert!(fields::<2>(b"a\"b,c").is_none());
        assert_eq!(fields::<2>(b"a,b\r").unwrap()[1], b"b\r");
        assert_eq!(
            timestamp(b"2026-10-03T23:58:10Z"),
            timestamp(b"2026-10-03T19:58:10-04:00")
        );
        assert!(timestamp(b"2025-02-29T00:00:00Z").is_none());
        assert!(timestamp(b"2028-02-29T00:00:00Z").is_some());
        assert_eq!(timestamp(b"00086400000"), Some(86400000));
        assert!(timestamp(b"999999999999999999999999999999999").is_none());
    }
}
