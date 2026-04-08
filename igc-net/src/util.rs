//! Shared validation and formatting helpers used across the igc-net crate.

use chrono::{NaiveDateTime, SecondsFormat, Utc};

/// True when `value` is exactly 64 lowercase hexadecimal characters.
pub(crate) fn is_lower_hex_64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// True when `value` matches the canonical `YYYY-MM-DDTHH:MM:SSZ` format.
pub(crate) fn is_canonical_utc_timestamp(value: &str) -> bool {
    if value.len() != 20 {
        return false;
    }
    NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%SZ").is_ok()
}

/// Current wall-clock time as a canonical UTC timestamp string.
pub(crate) fn canonical_utc_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_lower_hex_64_accepts_valid() {
        assert!(is_lower_hex_64(&"a".repeat(64)));
        assert!(is_lower_hex_64(
            "abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234"
        ));
    }

    #[test]
    fn is_lower_hex_64_rejects_uppercase() {
        assert!(!is_lower_hex_64(
            "ABCD1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234"
        ));
    }

    #[test]
    fn is_lower_hex_64_rejects_wrong_length() {
        assert!(!is_lower_hex_64("abcd"));
        assert!(!is_lower_hex_64(&"a".repeat(63)));
        assert!(!is_lower_hex_64(&"a".repeat(65)));
    }

    #[test]
    fn canonical_utc_now_is_valid_timestamp() {
        let ts = canonical_utc_now();
        assert!(is_canonical_utc_timestamp(&ts), "got: {ts}");
    }

    #[test]
    fn is_canonical_utc_timestamp_accepts_valid() {
        assert!(is_canonical_utc_timestamp("2026-04-03T10:00:00Z"));
    }

    #[test]
    fn is_canonical_utc_timestamp_rejects_offset() {
        assert!(!is_canonical_utc_timestamp("2026-04-03T10:00:00+00:00"));
    }
}
