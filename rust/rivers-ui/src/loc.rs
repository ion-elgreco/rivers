//! URL helpers for the namespaced routing scheme.
//!
//! Every UI route lives under `/locations/:loc_ns/:loc_name/...`. The two
//! identifiers are derived from the current URL pathname via [`use_location`]
//! — *not* from `use_params_map`, because the sidebar lives in `Shell` which
//! sits **outside** any matched `<Route>` and therefore has no route context.
//! Parsing the path directly works from anywhere inside `<Router>`.

use leptos::prelude::*;
use leptos_router::hooks::use_location;

/// Build a path under the current code location prefix.
/// `suffix` is appended verbatim; pass `""` for the location overview.
pub fn loc_path(loc_ns: &str, loc_name: &str, suffix: &str) -> String {
    let trimmed = suffix.trim_start_matches('/');
    if trimmed.is_empty() {
        format!("/locations/{loc_ns}/{loc_name}")
    } else {
        format!("/locations/{loc_ns}/{loc_name}/{trimmed}")
    }
}

/// Parse `(loc_ns, loc_name)` out of a `/locations/<ns>/<name>/...` pathname.
/// Returns empty strings for any path that doesn't carry the prefix (e.g. the
/// bare `/` redirect or a 404).
fn parse_location_from_path(path: &str) -> (String, String) {
    let Some(rest) = path.strip_prefix("/locations/") else {
        return (String::new(), String::new());
    };
    let mut parts = rest.splitn(3, '/');
    let ns = parts.next().unwrap_or("");
    let name = parts.next().unwrap_or("");
    (ns.to_string(), name.to_string())
}

/// Reactive `(loc_ns, loc_name)` signal derived from the current URL.
/// Updates on every navigation. Safe to call from any component rendered
/// inside `<Router>` — including the sidebar in `Shell`, which has no
/// matched-route context and therefore can't use `use_params_map`.
pub fn use_current_location() -> Signal<(String, String)> {
    let location = use_location();
    Signal::derive(move || parse_location_from_path(&location.pathname.get()))
}

/// Strip the `/locations/:ns/:name/` prefix from a path, returning the
/// "section" suffix. Used by the sidebar to preserve the user's current
/// section when they click a different code location.
pub fn section_of(path: &str) -> &str {
    let Some(rest) = path.strip_prefix("/locations/") else {
        return "";
    };
    let mut parts = rest.splitn(3, '/');
    parts.next();
    parts.next();
    parts.next().unwrap_or("")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loc_path_with_suffix() {
        assert_eq!(
            loc_path("team-data", "analytics", "runs/abc-123"),
            "/locations/team-data/analytics/runs/abc-123"
        );
    }

    #[test]
    fn loc_path_empty_suffix_omits_trailing_slash() {
        assert_eq!(loc_path("dev", "default", ""), "/locations/dev/default");
    }

    #[test]
    fn loc_path_strips_leading_slash() {
        assert_eq!(
            loc_path("dev", "default", "/runs"),
            "/locations/dev/default/runs"
        );
    }

    #[test]
    fn section_of_extracts_suffix_after_loc_prefix() {
        assert_eq!(section_of("/locations/a/b/runs/123"), "runs/123");
        assert_eq!(section_of("/locations/a/b/runs"), "runs");
        assert_eq!(section_of("/locations/a/b"), "");
        assert_eq!(section_of("/locations/a/b/"), "");
        assert_eq!(section_of("/runs/123"), "");
        assert_eq!(section_of("/"), "");
    }
}
