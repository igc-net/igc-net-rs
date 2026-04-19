//! Shared validation and formatting helpers used across the igc-net crate.

use std::path::Path;

use chrono::{NaiveDateTime, SecondsFormat, Utc};
use serde::Serialize;

const ISO_3166_ALPHA2_CODES: [&str; 249] = [
    "AD", "AE", "AF", "AG", "AI", "AL", "AM", "AO", "AQ", "AR", "AS", "AT", "AU", "AW", "AX", "AZ",
    "BA", "BB", "BD", "BE", "BF", "BG", "BH", "BI", "BJ", "BL", "BM", "BN", "BO", "BQ", "BR", "BS",
    "BT", "BV", "BW", "BY", "BZ", "CA", "CC", "CD", "CF", "CG", "CH", "CI", "CK", "CL", "CM", "CN",
    "CO", "CR", "CU", "CV", "CW", "CX", "CY", "CZ", "DE", "DJ", "DK", "DM", "DO", "DZ", "EC", "EE",
    "EG", "EH", "ER", "ES", "ET", "FI", "FJ", "FK", "FM", "FO", "FR", "GA", "GB", "GD", "GE", "GF",
    "GG", "GH", "GI", "GL", "GM", "GN", "GP", "GQ", "GR", "GS", "GT", "GU", "GW", "GY", "HK", "HM",
    "HN", "HR", "HT", "HU", "ID", "IE", "IL", "IM", "IN", "IO", "IQ", "IR", "IS", "IT", "JE", "JM",
    "JO", "JP", "KE", "KG", "KH", "KI", "KM", "KN", "KP", "KR", "KW", "KY", "KZ", "LA", "LB", "LC",
    "LI", "LK", "LR", "LS", "LT", "LU", "LV", "LY", "MA", "MC", "MD", "ME", "MF", "MG", "MH", "MK",
    "ML", "MM", "MN", "MO", "MP", "MQ", "MR", "MS", "MT", "MU", "MV", "MW", "MX", "MY", "MZ", "NA",
    "NC", "NE", "NF", "NG", "NI", "NL", "NO", "NP", "NR", "NU", "NZ", "OM", "PA", "PE", "PF", "PG",
    "PH", "PK", "PL", "PM", "PN", "PR", "PS", "PT", "PW", "PY", "QA", "RE", "RO", "RS", "RU", "RW",
    "SA", "SB", "SC", "SD", "SE", "SG", "SH", "SI", "SJ", "SK", "SL", "SM", "SN", "SO", "SR", "SS",
    "ST", "SV", "SX", "SY", "SZ", "TC", "TD", "TF", "TG", "TH", "TJ", "TK", "TL", "TM", "TN", "TO",
    "TR", "TT", "TV", "TW", "TZ", "UA", "UG", "UM", "US", "UY", "UZ", "VA", "VC", "VE", "VG", "VI",
    "VN", "VU", "WF", "WS", "YE", "YT", "ZA", "ZM", "ZW",
];

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

/// True when `value` is a current ISO 3166-1 alpha-2 country code.
pub(crate) fn is_iso_3166_alpha2_country_code(value: &str) -> bool {
    ISO_3166_ALPHA2_CODES.binary_search(&value).is_ok()
}

/// Current wall-clock time as a canonical UTC timestamp string.
pub(crate) fn canonical_utc_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

pub(crate) fn write_json_file_atomic<T, E, EnsureDir, WriteFile>(
    path: &Path,
    value: &T,
    ensure_dir: EnsureDir,
    write_file: WriteFile,
    missing_parent_error: E,
) -> Result<(), E>
where
    T: Serialize,
    E: From<std::io::Error> + From<serde_json::Error>,
    EnsureDir: FnOnce(&Path) -> Result<(), E>,
    WriteFile: FnOnce(&Path, &[u8]) -> Result<(), E>,
{
    let parent = path.parent().ok_or(missing_parent_error)?;
    ensure_dir(parent)?;

    let serialized = serde_json::to_vec_pretty(value)?;
    let tmp_name = format!(
        ".{}.tmp-{}",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("data"),
        rand::random::<u64>()
    );
    let tmp_path = parent.join(tmp_name);
    write_file(&tmp_path, &serialized)?;
    std::fs::rename(&tmp_path, path)?;
    Ok(())
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

    #[test]
    fn is_iso_3166_alpha2_country_code_accepts_current_codes() {
        assert!(is_iso_3166_alpha2_country_code("NO"));
        assert!(is_iso_3166_alpha2_country_code("US"));
        assert!(is_iso_3166_alpha2_country_code("AX"));
    }

    #[test]
    fn is_iso_3166_alpha2_country_code_rejects_unknown_codes() {
        assert!(!is_iso_3166_alpha2_country_code("ZZ"));
        assert!(!is_iso_3166_alpha2_country_code("XK"));
        assert!(!is_iso_3166_alpha2_country_code("N0"));
    }
}
