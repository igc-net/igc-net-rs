//! igc-net metadata blob (schema_version 1).
//!
//! See the igc-net protocol specification, Part I metadata schema.

use serde::{Deserialize, Serialize};

use crate::id::{Blake3Hex, NodeIdHex};
use crate::util::{canonical_utc_now, is_canonical_utc_timestamp};

// ── Error types ──────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    #[error("wrong schema: expected \"igc-net/metadata\", got {0:?}")]
    WrongSchema(String),
    #[error("unsupported schema_version: {0}")]
    WrongVersion(u32),
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("{field} is not a valid 64-char lowercase hex string")]
    MalformedLowerHex { field: &'static str },
    #[error("{field} is not a canonical UTC timestamp: {value}")]
    MalformedTimestamp { field: &'static str, value: String },
    #[error("{field} is not a valid YYYY-MM-DD date: {value}")]
    MalformedDate { field: &'static str, value: String },
    #[error("{field} is out of range or non-finite: {value}")]
    InvalidCoordinate { field: &'static str, value: f64 },
    #[error("{field} has invalid bounds: {message}")]
    InvalidBounds {
        field: &'static str,
        message: &'static str,
    },
}

// ── Structs ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BoundingBox {
    pub min_lat: f64,
    pub max_lat: f64,
    pub min_lon: f64,
    pub max_lon: f64,
}

/// igc-net metadata blob (schema_version 1).
///
/// Serialises to/from the JSON wire format defined in specs_meta.md §2.
/// Optional fields absent from the struct MUST be omitted from JSON (not null)
/// — enforced by `#[serde(skip_serializing_if = "Option::is_none")]`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FlightMetadata {
    pub schema: String,
    pub schema_version: u32,
    pub igc_hash: Blake3Hex,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_filename: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flight_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_s: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pilot_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub glider_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub glider_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fix_count: Option<u32>,
    /// Count of B records with validity flag 'A'. Always ≤ fix_count.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub valid_fix_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbox: Option<BoundingBox>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub launch_lat: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub launch_lon: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub landing_lat: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub landing_lon: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_alt_m: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_alt_m: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publisher_node_id: Option<NodeIdHex>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub published_at: Option<String>,
}

// ── impl FlightMetadata ───────────────────────────────────────────────────────

impl FlightMetadata {
    /// Construct a minimal valid metadata blob with only the required fields.
    pub fn new(igc_hash: Blake3Hex) -> Self {
        FlightMetadata {
            schema: "igc-net/metadata".to_string(),
            schema_version: 1,
            igc_hash,
            original_filename: None,
            flight_date: None,
            started_at: None,
            ended_at: None,
            duration_s: None,
            pilot_name: None,
            glider_type: None,
            glider_id: None,
            device_id: None,
            fix_count: None,
            valid_fix_count: None,
            bbox: None,
            launch_lat: None,
            launch_lon: None,
            landing_lat: None,
            landing_lon: None,
            max_alt_m: None,
            min_alt_m: None,
            publisher_node_id: None,
            published_at: None,
        }
    }

    /// Parse IGC headers and B-records to build a fully-populated metadata blob.
    ///
    /// Falls back gracefully: if a field cannot be parsed it is omitted rather
    /// than causing an error.  Only requires at least one B-record to be present.
    ///
    /// # Arguments
    /// - `igc_bytes`           — raw IGC file bytes (unmodified)
    /// - `igc_hash`            — pre-computed BLAKE3 hex string of those bytes
    /// - `original_filename`   — the upload filename, if available
    /// - `publisher_node_id`   — hex-encoded public key of the publishing node
    pub fn from_igc_bytes(
        igc_bytes: &[u8],
        igc_hash: Blake3Hex,
        original_filename: Option<&str>,
        publisher_node_id: Option<NodeIdHex>,
    ) -> Self {
        let mut meta = FlightMetadata::new(igc_hash);
        meta.original_filename = original_filename.map(str::to_string);
        meta.publisher_node_id = publisher_node_id;
        meta.published_at = Some(canonical_utc_now());

        let text = match std::str::from_utf8(igc_bytes) {
            Ok(t) => t,
            Err(_) => return meta, // not UTF-8, return minimal
        };

        parse_igc_into(text, &mut meta);
        meta
    }

    /// Serialize to canonical UTF-8 JSON bytes (no BOM, no trailing newline).
    ///
    /// These bytes are what get content-addressed as the metadata blob.
    pub fn to_blob_bytes(&self) -> Result<Vec<u8>, MetadataError> {
        serde_json::to_vec(self).map_err(MetadataError::from)
    }

    /// Validate the wire-format constraints for a received metadata blob.
    pub fn validate(&self) -> Result<(), MetadataError> {
        if self.schema != "igc-net/metadata" {
            return Err(MetadataError::WrongSchema(self.schema.clone()));
        }
        if self.schema_version != 1 {
            return Err(MetadataError::WrongVersion(self.schema_version));
        }
        if let Some(value) = self.flight_date.as_deref()
            && !is_canonical_date(value)
        {
            return Err(MetadataError::MalformedDate {
                field: "flight_date",
                value: value.to_string(),
            });
        }
        for (field, value) in [
            ("started_at", self.started_at.as_deref()),
            ("ended_at", self.ended_at.as_deref()),
            ("published_at", self.published_at.as_deref()),
        ] {
            if let Some(value) = value
                && !is_canonical_utc_timestamp(value)
            {
                return Err(MetadataError::MalformedTimestamp {
                    field,
                    value: value.to_string(),
                });
            }
        }
        for (field, value, min, max) in [
            ("launch_lat", self.launch_lat, -90.0, 90.0),
            ("launch_lon", self.launch_lon, -180.0, 180.0),
            ("landing_lat", self.landing_lat, -90.0, 90.0),
            ("landing_lon", self.landing_lon, -180.0, 180.0),
        ] {
            if let Some(value) = value {
                validate_coordinate(field, value, min, max)?;
            }
        }
        if let Some(bb) = &self.bbox {
            validate_coordinate("bbox.min_lat", bb.min_lat, -90.0, 90.0)?;
            validate_coordinate("bbox.max_lat", bb.max_lat, -90.0, 90.0)?;
            validate_coordinate("bbox.min_lon", bb.min_lon, -180.0, 180.0)?;
            validate_coordinate("bbox.max_lon", bb.max_lon, -180.0, 180.0)?;
            if bb.max_lat < bb.min_lat {
                return Err(MetadataError::InvalidBounds {
                    field: "bbox",
                    message: "max_lat < min_lat",
                });
            }
            if bb.max_lon < bb.min_lon {
                return Err(MetadataError::InvalidBounds {
                    field: "bbox",
                    message: "max_lon < min_lon",
                });
            }
        }
        Ok(())
    }
}

// ── IGC parsing helpers ───────────────────────────────────────────────────────

fn parse_igc_into(text: &str, meta: &mut FlightMetadata) {
    let mut fix_count: u32 = 0;
    let mut valid_fix_count: u32 = 0;
    let mut first_valid_fix_time: Option<String> = None;
    let mut last_valid_fix_time: Option<String> = None;

    let mut lats: Vec<f64> = Vec::new();
    let mut lons: Vec<f64> = Vec::new();
    let mut pressure_alts: Vec<i32> = Vec::new();
    let mut gps_alts: Vec<i32> = Vec::new();
    let mut pressure_alt_all_non_zero = true;

    let mut first_lat: Option<f64> = None;
    let mut first_lon: Option<f64> = None;
    let mut last_lat: Option<f64> = None;
    let mut last_lon: Option<f64> = None;

    for raw_line in text.lines() {
        let line = raw_line.trim_end_matches('\r');
        if line.is_empty() {
            continue;
        }
        let bytes = line.as_bytes();

        match bytes.first() {
            Some(b'A') if meta.device_id.is_none() && line.len() >= 7 => {
                // First A record: manufacturer (3 chars) + serial (3 chars)
                meta.device_id = Some(line[1..7].trim().to_string());
            }
            Some(b'H') => {
                parse_h_record(line, meta);
            }
            Some(b'B') if line.len() >= 35 => {
                fix_count += 1;

                let time_str = &line[1..7]; // HHMMSS
                let valid = bytes[24] == b'A';
                if valid {
                    valid_fix_count += 1;
                    if first_valid_fix_time.is_none() {
                        first_valid_fix_time = Some(time_str.to_string());
                    }
                    last_valid_fix_time = Some(time_str.to_string());
                }

                if let (Some(lat), Some(lon)) =
                    (parse_lat(&bytes[7..15]), parse_lon(&bytes[15..24]))
                {
                    lats.push(lat);
                    lons.push(lon);
                    if first_lat.is_none() {
                        first_lat = Some(lat);
                        first_lon = Some(lon);
                    }
                    last_lat = Some(lat);
                    last_lon = Some(lon);
                }

                let pressure_alt = parse_altitude(&bytes[25..30]);
                let gps_alt = parse_altitude(&bytes[30..35]);

                match pressure_alt {
                    Some(alt) if alt != 0 => pressure_alts.push(alt),
                    _ => pressure_alt_all_non_zero = false,
                }
                if let Some(alt) = gps_alt {
                    gps_alts.push(alt);
                }
            }
            _ => {}
        }
    }

    meta.fix_count = Some(fix_count);
    meta.valid_fix_count = Some(valid_fix_count);

    meta.started_at = build_timestamp(
        meta.flight_date.as_deref(),
        first_valid_fix_time.as_deref(),
    );

    // Detect midnight crossing: if end time < start time, advance the date by
    // one day for the ended_at timestamp.
    let crossed_midnight = is_midnight_crossing(
        first_valid_fix_time.as_deref(),
        last_valid_fix_time.as_deref(),
    );
    let end_date = if crossed_midnight {
        next_day(&meta.flight_date)
    } else {
        meta.flight_date.clone()
    };
    meta.ended_at = build_timestamp(end_date.as_deref(), last_valid_fix_time.as_deref());

    meta.duration_s = compute_duration_s(
        first_valid_fix_time.as_deref(),
        last_valid_fix_time.as_deref(),
    );

    if !lats.is_empty() {
        meta.bbox = Some(BoundingBox {
            min_lat: lats.iter().cloned().fold(f64::INFINITY, f64::min),
            max_lat: lats.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
            min_lon: lons.iter().cloned().fold(f64::INFINITY, f64::min),
            max_lon: lons.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
        });
    }

    meta.launch_lat = first_lat;
    meta.launch_lon = first_lon;
    meta.landing_lat = last_lat;
    meta.landing_lon = last_lon;

    let altitudes = if pressure_alt_all_non_zero && !pressure_alts.is_empty() {
        &pressure_alts
    } else {
        &gps_alts
    };
    meta.max_alt_m = altitudes.iter().copied().max();
    meta.min_alt_m = altitudes.iter().copied().min();
}

fn parse_h_record(line: &str, meta: &mut FlightMetadata) {
    let upper = line.to_ascii_uppercase();
    // Check the 3-char field code at positions 2..5
    if upper.len() < 5 {
        return;
    }
    let code = &upper[2..5];
    match code {
        "DTE" => {
            if meta.flight_date.is_none() {
                meta.flight_date = parse_hfdte(line);
            }
        }
        "PLT" => {
            if meta.pilot_name.is_none() {
                meta.pilot_name = h_colon_value(line);
            }
        }
        "GTY" => {
            if meta.glider_type.is_none() {
                meta.glider_type = h_colon_value(line);
            }
        }
        "GID" => {
            if meta.glider_id.is_none() {
                meta.glider_id = h_colon_value(line);
            }
        }
        _ => {}
    }
}

/// Parse latitude from an 8-byte IGC field: `DDMMmmmN` or `DDMMmmmS`
fn parse_lat(bytes: &[u8]) -> Option<f64> {
    if bytes.len() < 8 {
        return None;
    }
    let dd: f64 = std::str::from_utf8(&bytes[0..2]).ok()?.parse().ok()?;
    let mm: f64 = std::str::from_utf8(&bytes[2..4]).ok()?.parse().ok()?;
    let mmm: f64 = std::str::from_utf8(&bytes[4..7]).ok()?.parse().ok()?;
    let decimal = dd + (mm + mmm / 1000.0) / 60.0;
    match bytes[7] {
        b'S' => Some(-decimal),
        _ => Some(decimal),
    }
}

/// Parse longitude from a 9-byte IGC field: `DDDMMmmmE` or `DDDMMmmmW`
fn parse_lon(bytes: &[u8]) -> Option<f64> {
    if bytes.len() < 9 {
        return None;
    }
    let ddd: f64 = std::str::from_utf8(&bytes[0..3]).ok()?.parse().ok()?;
    let mm: f64 = std::str::from_utf8(&bytes[3..5]).ok()?.parse().ok()?;
    let mmm: f64 = std::str::from_utf8(&bytes[5..8]).ok()?.parse().ok()?;
    let decimal = ddd + (mm + mmm / 1000.0) / 60.0;
    match bytes[8] {
        b'W' => Some(-decimal),
        _ => Some(decimal),
    }
}

fn parse_altitude(bytes: &[u8]) -> Option<i32> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

fn is_canonical_date(value: &str) -> bool {
    chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok()
}

fn validate_coordinate(
    field: &'static str,
    value: f64,
    min: f64,
    max: f64,
) -> Result<(), MetadataError> {
    if value.is_finite() && (min..=max).contains(&value) {
        Ok(())
    } else {
        Err(MetadataError::InvalidCoordinate { field, value })
    }
}

/// Parse an HFDTE record into `YYYY-MM-DD`.
///
/// Handles both `HFDTE020714` and `HFDTEDATE:020714` forms.
fn parse_hfdte(line: &str) -> Option<String> {
    // Collect all digit runs
    let digits: String = line.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.len() < 6 {
        return None;
    }
    let d = &digits[digits.len() - 6..];
    let dd = &d[0..2];
    let mm = &d[2..4];
    let yy = &d[4..6];
    let yyyy = format!("20{yy}");
    Some(format!("{yyyy}-{mm}-{dd}"))
}

/// Extract the value after the first `:` in a H record, trimmed.
fn h_colon_value(line: &str) -> Option<String> {
    line.find(':')
        .map(|i| line[i + 1..].trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Combine a flight date (`YYYY-MM-DD`) and a B-record time (`HHMMSS`) into
/// an RFC 3339 UTC timestamp.
fn build_timestamp(date: Option<&str>, time: Option<&str>) -> Option<String> {
    match (date, time) {
        (Some(d), Some(t)) if t.len() == 6 => {
            let h = &t[0..2];
            let m = &t[2..4];
            let s = &t[4..6];
            Some(format!("{d}T{h}:{m}:{s}Z"))
        }
        _ => None,
    }
}

/// True if the HHMMSS end time is earlier than the HHMMSS start time,
/// indicating a midnight crossing.
fn is_midnight_crossing(start: Option<&str>, end: Option<&str>) -> bool {
    let to_secs = |t: &str| -> Option<u64> {
        if t.len() != 6 {
            return None;
        }
        let h: u64 = t[0..2].parse().ok()?;
        let m: u64 = t[2..4].parse().ok()?;
        let s: u64 = t[4..6].parse().ok()?;
        Some(h * 3600 + m * 60 + s)
    };
    match (start.and_then(to_secs), end.and_then(to_secs)) {
        (Some(ss), Some(es)) => es < ss,
        _ => false,
    }
}

/// Advance a `YYYY-MM-DD` date string by one day.  Returns `None` if the
/// input is absent or unparseable.
fn next_day(date: &Option<String>) -> Option<String> {
    use chrono::NaiveDate;
    let d = date.as_deref()?;
    let parsed = NaiveDate::parse_from_str(d, "%Y-%m-%d").ok()?;
    Some(parsed.succ_opt()?.format("%Y-%m-%d").to_string())
}

/// Compute duration in seconds between two HHMMSS strings, handling midnight wrap.
fn compute_duration_s(start: Option<&str>, end: Option<&str>) -> Option<u64> {
    let (start, end) = (start?, end?);
    if start.len() != 6 || end.len() != 6 {
        return None;
    }
    let to_secs = |t: &str| -> Option<u64> {
        let h: u64 = t[0..2].parse().ok()?;
        let m: u64 = t[2..4].parse().ok()?;
        let s: u64 = t[4..6].parse().ok()?;
        Some(h * 3600 + m * 60 + s)
    };
    let ss = to_secs(start)?;
    let es = to_secs(end)?;
    if es >= ss {
        Some(es - ss)
    } else {
        Some(es + 86400 - ss) // midnight crossing
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{Blake3Hex, NodeIdHex};

    // Valid IGC B record layout (35 chars minimum):
    //   B HHMMSS DDMMmmmN DDDMMmmmE/W V PPPPP GGGGG
    //   0 123456 78901234 567890123   4 56789 01234
    //   positions in the line (0-indexed)
    //
    // Validity 'V' at position 24; 'A' = 3D fix, 'V' = invalid.
    const MINIMAL_IGC: &str = "\
AXXX001 TestDevice\r\n\
HFDTE020714\r\n\
HFPLTPILOTINCHARGE:Jane Doe\r\n\
HFGTYGLIDERTYPE:Advance Sigma 10\r\n\
HFGIDGLIDERID:HB-1234\r\n\
B1200004728000N00836000EV0010001000\r\n\
B1300004730000N00837000EA0030003000\r\n\
B1400004732000N00838000EA0150001500\r\n\
";

    fn fake_hash() -> Blake3Hex {
        Blake3Hex::parse(
            "abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234",
        )
        .unwrap()
    }

    fn fake_node_id() -> NodeIdHex {
        NodeIdHex::parse(
            "aabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd",
        )
        .unwrap()
    }

    #[test]
    fn from_igc_bytes_populates_fields() {
        let meta = FlightMetadata::from_igc_bytes(
            MINIMAL_IGC.as_bytes(),
            fake_hash(),
            Some("test.igc"),
            Some(fake_node_id()),
        );
        assert_eq!(meta.schema, "igc-net/metadata");
        assert_eq!(meta.schema_version, 1);
        assert_eq!(meta.igc_hash, fake_hash());
        assert_eq!(meta.original_filename.as_deref(), Some("test.igc"));
        assert_eq!(meta.flight_date.as_deref(), Some("2014-07-02"));
        assert_eq!(meta.pilot_name.as_deref(), Some("Jane Doe"));
        assert_eq!(meta.glider_type.as_deref(), Some("Advance Sigma 10"));
        assert_eq!(meta.glider_id.as_deref(), Some("HB-1234"));
        assert_eq!(meta.fix_count, Some(3));
        assert_eq!(meta.valid_fix_count, Some(2)); // first record is 'V', last two are 'A'
        assert_eq!(meta.launch_lat, Some(47.46666666666667));
        assert_eq!(meta.launch_lon, Some(8.6));
        assert_eq!(meta.landing_lat, Some(47.53333333333333));
        assert_eq!(meta.landing_lon, Some(8.633333333333333));
        assert_eq!(meta.max_alt_m, Some(1500));
        assert_eq!(meta.min_alt_m, Some(100));
        assert!(meta.bbox.is_some());
        assert_eq!(
            meta.published_at.as_deref(),
            meta.published_at
                .as_deref()
                .filter(|value| is_canonical_utc_timestamp(value))
        );
    }

    #[test]
    fn to_blob_bytes_round_trip() {
        let meta = FlightMetadata::new(fake_hash());
        let bytes = meta.to_blob_bytes().unwrap();
        let parsed: FlightMetadata = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed.igc_hash, fake_hash());
    }

    #[test]
    fn null_omission_no_null_in_json() {
        let meta = FlightMetadata::new(fake_hash());
        let json = String::from_utf8(meta.to_blob_bytes().unwrap()).unwrap();
        assert!(
            !json.contains("null"),
            "JSON must not contain 'null': {json}"
        );
    }

    #[test]
    fn validate_rejects_wrong_schema() {
        let mut meta = FlightMetadata::new(fake_hash());
        meta.schema = "wrong".to_string();
        assert!(matches!(
            meta.validate(),
            Err(MetadataError::WrongSchema(_))
        ));
    }

    #[test]
    fn validate_rejects_wrong_version() {
        let mut meta = FlightMetadata::new(fake_hash());
        meta.schema_version = 99;
        assert!(matches!(
            meta.validate(),
            Err(MetadataError::WrongVersion(99))
        ));
    }

    #[test]
    fn deserialize_rejects_malformed_hash() {
        let json = r#"{"schema":"igc-net/metadata","schema_version":1,"igc_hash":"not-a-hash"}"#;
        assert!(serde_json::from_str::<FlightMetadata>(json).is_err());
    }

    #[test]
    fn deserialize_rejects_uppercase_hash() {
        let json = r#"{"schema":"igc-net/metadata","schema_version":1,"igc_hash":"ABCD1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234"}"#;
        assert!(serde_json::from_str::<FlightMetadata>(json).is_err());
    }

    #[test]
    fn validate_rejects_non_canonical_timestamp() {
        let mut meta = FlightMetadata::new(fake_hash());
        meta.published_at = Some("2026-03-29T18:07:55+00:00".to_string());
        assert!(matches!(
            meta.validate(),
            Err(MetadataError::MalformedTimestamp {
                field: "published_at",
                ..
            })
        ));
    }

    #[test]
    fn validate_rejects_invalid_flight_date() {
        let mut meta = FlightMetadata::new(fake_hash());
        meta.flight_date = Some("2026-02-31".to_string());
        assert!(matches!(
            meta.validate(),
            Err(MetadataError::MalformedDate {
                field: "flight_date",
                ..
            })
        ));
    }

    #[test]
    fn validate_rejects_out_of_range_coordinates() {
        let mut meta = FlightMetadata::new(fake_hash());
        meta.launch_lat = Some(91.0);
        assert!(matches!(
            meta.validate(),
            Err(MetadataError::InvalidCoordinate {
                field: "launch_lat",
                ..
            })
        ));
    }

    #[test]
    fn validate_rejects_invalid_bbox_bounds() {
        let mut meta = FlightMetadata::new(fake_hash());
        meta.bbox = Some(BoundingBox {
            min_lat: 10.0,
            max_lat: 5.0,
            min_lon: 20.0,
            max_lon: 25.0,
        });
        assert!(matches!(
            meta.validate(),
            Err(MetadataError::InvalidBounds { field: "bbox", .. })
        ));
    }

    #[test]
    fn validate_accepts_valid_metadata() {
        let meta = FlightMetadata::new(fake_hash());
        assert!(meta.validate().is_ok());
    }

    #[test]
    fn midnight_crossing_duration() {
        let d = compute_duration_s(Some("235900"), Some("000100"));
        assert_eq!(d, Some(120));
    }

    #[test]
    fn midnight_crossing_ended_at_uses_next_day() {
        // IGC file where flight starts at 23:50 and ends at 00:10 the next day.
        let igc = "\
AXXX001 TestDevice\r\n\
HFDTE310714\r\n\
B2350004730000N00837000EA0030003000\r\n\
B0010004732000N00838000EA0050005000\r\n\
";
        let meta =
            FlightMetadata::from_igc_bytes(
                igc.as_bytes(),
                fake_hash(),
                None,
                Some(NodeIdHex::parse("cc".repeat(32)).unwrap()),
            );
        assert_eq!(meta.flight_date.as_deref(), Some("2014-07-31"));
        assert_eq!(meta.started_at.as_deref(), Some("2014-07-31T23:50:00Z"));
        assert_eq!(meta.ended_at.as_deref(), Some("2014-08-01T00:10:00Z"));
        assert_eq!(meta.duration_s, Some(1200));
    }

    #[test]
    fn normal_flight_ended_at_uses_same_day() {
        let igc = "\
AXXX001 TestDevice\r\n\
HFDTE020714\r\n\
B1200004730000N00837000EA0030003000\r\n\
B1400004732000N00838000EA0050005000\r\n\
";
        let meta =
            FlightMetadata::from_igc_bytes(
                igc.as_bytes(),
                fake_hash(),
                None,
                Some(NodeIdHex::parse("cc".repeat(32)).unwrap()),
            );
        assert_eq!(meta.started_at.as_deref(), Some("2014-07-02T12:00:00Z"));
        assert_eq!(meta.ended_at.as_deref(), Some("2014-07-02T14:00:00Z"));
    }

    #[test]
    fn next_day_advances_correctly() {
        assert_eq!(
            next_day(&Some("2014-07-31".to_string())),
            Some("2014-08-01".to_string())
        );
        assert_eq!(
            next_day(&Some("2024-12-31".to_string())),
            Some("2025-01-01".to_string())
        );
        assert_eq!(next_day(&None), None);
    }
}
