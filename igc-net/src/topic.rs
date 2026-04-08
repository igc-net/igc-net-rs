//! Well-known gossip topic IDs for the igc-net protocol.
//!
//! Topic IDs are fixed protocol constants.
//!
//! The values are precomputed from the canonical derivation strings and embedded
//! directly so the implementation matches the spec's interoperability rules.

/// Derivation string for the primary announcement topic.
/// All compliant igc-net nodes MUST join this topic on startup.
pub const ANNOUNCE_TOPIC_STR: &str = "igc-net/announce/v1";

/// Derivation string for the analytics announcement topic.
/// Nodes that publish or consume IGC_META_DOC analytics SHOULD join this topic.
pub const ANALYTICS_TOPIC_STR: &str = "igc-net/analytics/v1";

/// `BLAKE3("igc-net/announce/v1")` as a 32-byte array.
pub const ANNOUNCE_TOPIC_ID: [u8; 32] = [
    0x2f, 0x06, 0x56, 0x7e, 0x5d, 0x71, 0x48, 0xb5, 0x63, 0x49, 0xa7, 0x53, 0xf8, 0xb4, 0x07, 0xfb,
    0xc3, 0x5b, 0x2f, 0x0d, 0x90, 0xff, 0x36, 0x6f, 0xf2, 0x67, 0x3d, 0x06, 0x02, 0x45, 0xdd, 0xa9,
];

/// `BLAKE3("igc-net/analytics/v1")` as a 32-byte array.
pub const ANALYTICS_TOPIC_ID: [u8; 32] = [
    0x4f, 0x29, 0x34, 0x5b, 0x69, 0x86, 0x88, 0x63, 0x51, 0xdb, 0xff, 0x97, 0xb4, 0x23, 0x1b, 0x7d,
    0x9a, 0x60, 0x1a, 0x50, 0x98, 0xf1, 0x84, 0x80, 0xc2, 0xcb, 0x4d, 0x28, 0xfb, 0xe3, 0x09, 0x50,
];

/// `BLAKE3("igc-net/announce/v1")` as a 32-byte array.
///
/// This is the well-known gossip topic for flight announcements.
/// The canonical hex value is defined in specs/specs_igc.md §2.
pub fn announce_topic_id() -> [u8; 32] {
    ANNOUNCE_TOPIC_ID
}

/// `BLAKE3("igc-net/analytics/v1")` as a 32-byte array.
///
/// Single well-known gossip topic for IGC_META_DOC analytics announcements.
/// Defined in specs/specs_meta.md §11.1.
pub fn analytics_topic_id() -> [u8; 32] {
    ANALYTICS_TOPIC_ID
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn announce_topic_id_is_32_bytes() {
        assert_eq!(announce_topic_id().len(), 32);
    }

    #[test]
    fn analytics_topic_id_is_32_bytes() {
        assert_eq!(analytics_topic_id().len(), 32);
    }

    #[test]
    fn topic_ids_are_distinct() {
        assert_ne!(announce_topic_id(), analytics_topic_id());
    }

    #[test]
    fn announce_topic_id_matches_derivation() {
        // Verify the runtime value matches a directly-computed BLAKE3.
        let expected = *blake3::hash(ANNOUNCE_TOPIC_STR.as_bytes()).as_bytes();
        assert_eq!(announce_topic_id(), expected);
    }

    #[test]
    fn analytics_topic_id_matches_derivation() {
        let expected = *blake3::hash(ANALYTICS_TOPIC_STR.as_bytes()).as_bytes();
        assert_eq!(analytics_topic_id(), expected);
    }

    /// Snapshot test — verifies the announce topic ID against the value published
    /// in specs/specs_igc.md §2.  If this fails, the spec and implementation have
    /// diverged; update one of them.
    #[test]
    fn announce_topic_id_matches_spec_constant() {
        let hex = hex::encode(announce_topic_id());
        // Pre-computed: BLAKE3("igc-net/announce/v1")
        assert_eq!(
            hex,
            "2f06567e5d7148b56349a753f8b407fbc35b2f0d90ff366ff2673d060245dda9"
        );
    }

    #[test]
    fn analytics_topic_id_matches_spec_constant() {
        let hex = hex::encode(analytics_topic_id());
        // Pre-computed: BLAKE3("igc-net/analytics/v1")
        assert_eq!(
            hex,
            "4f29345b6986886351dbff97b4231b7d9a601a5098f18480c2cb4d28fbe30950"
        );
    }
}
