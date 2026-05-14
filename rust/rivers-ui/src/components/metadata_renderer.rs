//! Metadata value renderer.

use crate::types::MetadataDisplay;
use leptos::prelude::*;
use pulldown_cmark::{Options, Parser};

pub(crate) fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;
    if bytes >= TB {
        format!("{:.1} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

pub(crate) fn format_duration_secs(secs: f64) -> String {
    if secs < 1.0 {
        format!("{:.0}ms", secs * 1000.0)
    } else if secs < 60.0 {
        format!("{:.1}s", secs)
    } else if secs < 3600.0 {
        let m = (secs / 60.0).floor();
        let s = secs % 60.0;
        format!("{:.0}m {:.0}s", m, s)
    } else {
        let h = (secs / 3600.0).floor();
        let m = ((secs % 3600.0) / 60.0).floor();
        format!("{:.0}h {:.0}m", h, m)
    }
}

pub(crate) fn format_ts(ts: i64) -> String {
    crate::helpers::nanos_to_datetime(ts)
        .map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| ts.to_string())
}

#[component]
pub fn MetadataValue(value: MetadataDisplay) -> impl IntoView {
    match value {
        MetadataDisplay::Text(t) => view! { <span>{t}</span> }.into_any(),
        MetadataDisplay::Int(n) => view! { <span class="metadata-number">{n.to_string()}</span> }.into_any(),
        MetadataDisplay::Float(n) => view! { <span class="metadata-number">{format!("{:.4}", n)}</span> }.into_any(),
        MetadataDisplay::Bool(b) => {
            let icon = if b { "check" } else { "x" };
            let class = if b { "badge badge-success" } else { "badge badge-muted" };
            view! { <span class={class}>{icon}</span> }.into_any()
        }
        MetadataDisplay::Url { text, url } => {
            view! { <a href={url.clone()} target="_blank" rel="noopener" class="metadata-url">{text}</a> }.into_any()
        }
        MetadataDisplay::Path(p) => {
            view! { <code class="metadata-path">{p}</code> }.into_any()
        }
        MetadataDisplay::Json(j) => {
            view! { <pre class="metadata-json"><code>{j}</code></pre> }.into_any()
        }
        MetadataDisplay::Markdown(md) => {
            let parser = Parser::new_ext(&md, Options::all());
            let mut html = String::new();
            pulldown_cmark::html::push_html(&mut html, parser);
            view! { <div class="metadata-markdown" inner_html={html}></div> }.into_any()
        }
        MetadataDisplay::CodeBlock { code, language } => {
            let lang_label = language.unwrap_or_default();
            view! {
                <div class="metadata-codeblock">
                    {(!lang_label.is_empty()).then(|| view! { <span class="codeblock-lang">{lang_label.clone()}</span> })}
                    <pre><code>{code}</code></pre>
                </div>
            }.into_any()
        }
        MetadataDisplay::Sql(sql) => {
            view! {
                <div class="metadata-codeblock">
                    <span class="codeblock-lang">"SQL"</span>
                    <pre><code>{sql}</code></pre>
                </div>
            }.into_any()
        }
        MetadataDisplay::Image(src) => {
            view! { <img class="metadata-image" src={src} alt="metadata image"/> }.into_any()
        }
        MetadataDisplay::Timestamp(ts) => {
            let formatted = format_ts(ts);
            view! { <span class="metadata-timestamp" title={ts.to_string()}>{formatted}</span> }.into_any()
        }
        MetadataDisplay::Duration(secs) => {
            let formatted = format_duration_secs(secs);
            view! { <span class="metadata-duration">{formatted}</span> }.into_any()
        }
        MetadataDisplay::DateRange { start, end } => {
            let s = format_ts(start);
            let e = format_ts(end);
            view! { <span class="metadata-daterange">{format!("{s} - {e}")}</span> }.into_any()
        }
        MetadataDisplay::Bytes(b) => {
            let formatted = format_bytes(b);
            view! { <span class="metadata-bytes">{formatted}</span> }.into_any()
        }
        MetadataDisplay::Percentage(p) => {
            let pct = (p * 100.0).clamp(0.0, 100.0);
            let width = format!("{}%", pct);
            view! {
                <div class="metadata-percentage">
                    <div class="progress-bar">
                        <div class="progress-fill" style=format!("width: {width}")></div>
                    </div>
                    <span>{format!("{:.1}%", pct)}</span>
                </div>
            }.into_any()
        }
        MetadataDisplay::Schema(cols) => {
            view! {
                <table class="metadata-schema">
                    <thead><tr><th>"Column"</th><th>"Type"</th></tr></thead>
                    <tbody>
                        {cols.into_iter().map(|(name, typ)| {
                            view! { <tr><td>{name}</td><td><code>{typ}</code></td></tr> }
                        }).collect::<Vec<_>>()}
                    </tbody>
                </table>
            }.into_any()
        }
        MetadataDisplay::DataVersion(v) => {
            view! { <code class="metadata-data-version">{v}</code> }.into_any()
        }
        MetadataDisplay::Null => {
            view! { <span class="metadata-null">"null"</span> }.into_any()
        }
    }
}

#[component]
pub fn MetadataTable(entries: Vec<(String, MetadataDisplay)>) -> impl IntoView {
    if entries.is_empty() {
        return view! { <span class="metadata-null">"No metadata"</span> }.into_any();
    }
    view! {
        <div class="metadata-table">
            {entries.into_iter().map(|(key, val)| {
                view! {
                    <div class="metadata-row">
                        <span class="metadata-key">{key}</span>
                        <span class="metadata-val"><MetadataValue value={val}/></span>
                    </div>
                }
            }).collect::<Vec<_>>()}
        </div>
    }
    .into_any()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1048576), "1.0 MB");
        assert_eq!(format_bytes(1073741824), "1.0 GB");
        assert_eq!(format_bytes(1099511627776), "1.0 TB");
    }

    #[test]
    fn test_format_duration_secs_millis() {
        assert_eq!(format_duration_secs(0.5), "500ms");
        assert_eq!(format_duration_secs(0.001), "1ms");
    }

    #[test]
    fn test_format_duration_secs_seconds() {
        assert_eq!(format_duration_secs(1.0), "1.0s");
        assert_eq!(format_duration_secs(45.3), "45.3s");
    }

    #[test]
    fn test_format_duration_secs_minutes() {
        assert_eq!(format_duration_secs(90.0), "1m 30s");
        assert_eq!(format_duration_secs(125.0), "2m 5s");
    }

    #[test]
    fn test_format_duration_secs_hours() {
        assert_eq!(format_duration_secs(3661.0), "1h 1m");
        assert_eq!(format_duration_secs(7200.0), "2h 0m");
    }

    #[test]
    fn test_format_ts() {
        assert_eq!(format_ts(0), "1970-01-01 00:00:00");
        assert_eq!(format_ts(1_700_000_000_000_000_000), "2023-11-14 22:13:20");
    }
}
