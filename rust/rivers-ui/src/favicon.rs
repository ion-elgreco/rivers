//! Brand favicon, embedded inline as a data URI.
//!
//! The HTML `<link rel="icon">` carries `data:image/svg+xml;base64,<...>`
//! directly so the browser never consults `/favicon.ico` The
//! SVG itself is hand-stripped of `<defs>`, `<style>`, gradients, and
//! CSS custom properties — Chromium's favicon SVG path silently drops
//! images whose paint is sourced through `<style>` or `<linearGradient>`,
//! and a blank render falls back to whatever was previously cached.

use std::sync::LazyLock;

pub const SVG: &str = r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32" fill="none"><path d="M3 11c4 0 6 5 13 5s9-5 13-5" stroke="#ff8f78" stroke-width="2.2" stroke-linecap="round"/><path d="M3 21c4 0 6-5 13-5s9 5 13 5" stroke="#50e1f9" stroke-width="2.2" stroke-linecap="round" opacity="0.85"/></svg>"##;

pub fn data_uri() -> &'static str {
    use base64::Engine;
    static URI: LazyLock<String> = LazyLock::new(|| {
        let b64 = base64::engine::general_purpose::STANDARD.encode(SVG.as_bytes());
        format!("data:image/svg+xml;base64,{b64}")
    });
    URI.as_str()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_uri_round_trips_to_svg() {
        use base64::Engine;
        let uri = data_uri();
        let prefix = "data:image/svg+xml;base64,";
        assert!(uri.starts_with(prefix), "got {uri}");
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(uri.trim_start_matches(prefix))
            .expect("valid base64");
        assert_eq!(std::str::from_utf8(&bytes).unwrap(), SVG);
    }
}
