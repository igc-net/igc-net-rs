# `igc-net` Metadata Schema in Rust

The canonical metadata schema is defined by the `igc-net` protocol
specification. This document describes how the Rust reference implementation
maps that schema into the public `igc_net::FlightMetadata` type.

## Public Type

```rust
use igc_net::FlightMetadata;
```

The struct represents the base metadata blob with:

- `schema = "igc-net/metadata"`
- `schema_version = 1`
- optional fields modeled as `Option<T>`
- `serde(skip_serializing_if = "Option::is_none")` for omission rather than
  `null`

## Field Mapping

Implemented fields:

- `schema`
- `schema_version`
- `igc_hash`
- `original_filename`
- `flight_date`
- `started_at`
- `ended_at`
- `duration_s`
- `pilot_name`
- `glider_type`
- `glider_id`
- `device_id`
- `fix_count`
- `valid_fix_count`
- `bbox`
- `launch_lat`
- `launch_lon`
- `landing_lat`
- `landing_lon`
- `max_alt_m`
- `min_alt_m`
- `publisher_node_id`
- `published_at`

## Constructors and Validation

- `FlightMetadata::new(igc_hash)` creates the minimal valid metadata blob
- `FlightMetadata::from_igc_bytes(...)` derives metadata from raw IGC bytes
- `FlightMetadata::to_blob_bytes()` serializes canonical JSON bytes
- `FlightMetadata::validate()` enforces the wire-format constraints implemented
  by the Rust reference implementation

## Important Constraints

- hashes and `publisher_node_id` must be lowercase 64-character hex strings
- canonical timestamps must use `YYYY-MM-DDTHH:MM:SSZ`
- optional fields are omitted when unavailable
- the Rust implementation does not treat local field order as semantically
  significant

## Source of Truth

If this document and the protocol spec diverge, the protocol spec is correct and
the Rust implementation/docs must be updated in the same change.
