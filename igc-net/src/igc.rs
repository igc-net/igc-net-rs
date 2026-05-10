/// Return true when raw IGC bytes contain at least one line whose first byte is
/// ASCII `G` (0x47).
///
/// Lines are delimited by LF, CRLF, or EOF. The function intentionally performs
/// no normalization, BOM stripping, or transcoding.
pub fn g_record_present(raw_igc_bytes: &[u8]) -> bool {
    if raw_igc_bytes.first() == Some(&b'G') {
        return true;
    }

    raw_igc_bytes
        .windows(2)
        .any(|window| window[0] == b'\n' && window[1] == b'G')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_g_record_presence_by_line_start() {
        assert!(!g_record_present(b""));
        assert!(g_record_present(b"Gabc"));
        assert!(g_record_present(b"Aabc\nGabc\nBabc"));
        assert!(g_record_present(b"Aabc\r\nGabc\r\nBabc"));
        assert!(g_record_present(b"Aabc\r\nBabc\nGabc"));
        assert!(g_record_present(b"Aabc\nGabc"));
    }

    #[test]
    fn rejects_non_line_start_g_and_non_spec_line_breaks() {
        assert!(!g_record_present(b"Aabc\nBhasG\nCabc"));
        assert!(!g_record_present(b"Aabc\rGabc"));
        assert!(!g_record_present(b"\xef\xbb\xbfGabc"));
    }
}
