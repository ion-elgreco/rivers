//! Minimal server-rendered pages for auth failures — no Leptos, no assets,
//! so they render even when the session/WASM state is broken.

use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};

use super::identity::Identity;

fn page(status: StatusCode, title: &str, body_html: String) -> Response {
    let html = format!(
        r#"<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><title>{title} · rivers</title>
<style>
body{{font-family:system-ui,sans-serif;background:#0d1117;color:#e6edf3;display:grid;place-items:center;min-height:100vh;margin:0}}
main{{max-width:32rem;padding:2rem;text-align:center}}
h1{{font-size:1.25rem;margin-bottom:.5rem}}
p{{color:#9198a1;line-height:1.5;overflow-wrap:anywhere}}
a{{color:#4493f8}}
</style></head><body><main><h1>{title}</h1>{body_html}</main></body></html>
"#
    );
    (status, Html(html)).into_response()
}

fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

pub fn error_page(
    status: StatusCode,
    title: &str,
    detail: &str,
    retry_href: Option<&str>,
) -> Response {
    let mut body = String::new();
    if !detail.is_empty() {
        body.push_str(&format!("<p>{}</p>", escape(detail)));
    }
    if let Some(href) = retry_href {
        body.push_str(&format!(r#"<p><a href="{href}">Try signing in again</a></p>"#));
    }
    page(status, title, body)
}

pub fn forbidden_page(identity: &Identity, logout_href: Option<&str>) -> Response {
    let who = escape(identity.display());
    let detail = match &identity.email {
        Some(email) if email != identity.display() => {
            format!("{who} ({})", escape(email))
        }
        _ => who,
    };
    let logout = logout_href
        .map(|href| format!(r#"<p><a href="{href}">Sign out</a></p>"#))
        .unwrap_or_default();
    page(
        StatusCode::FORBIDDEN,
        "Access denied",
        format!(
            "<p>Signed in as <strong>{detail}</strong>, but this identity is not on the \
             configured allowlists (<code>RIVERS_AUTH_ALLOWED_*</code>).</p>{logout}"
        ),
    )
}
