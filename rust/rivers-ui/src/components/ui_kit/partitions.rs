use leptos::prelude::*;

/// Partition summary cell — `[D] 12 of 365` style, with hover title for full key.
///
/// Scheme is derived from the partition key format: a hyphenated ISO date like
/// `2025-04-19` → `D` (daily), an ISO datetime like `2025-04-19T14` → `H`
/// (hourly). Any other scheme shows no badge.
#[component]
pub fn PartitionCell(
    /// 'D' daily, 'H' hourly; anything else hides the badge.
    #[prop(into, default = "·".to_string())]
    scheme: String,
    #[prop(optional, into)] count_label: Option<String>,
) -> impl IntoView {
    let tip = match scheme.as_str() {
        "D" => Some("daily partition"),
        "H" => Some("hourly partition"),
        _ => None,
    };
    view! {
        <span class="partition-cell">
            {tip.map(|t| view! { <span class="partition-cell-badge" data-tip=t>{scheme}</span> })}
            {count_label.map(|c| {
                let tip = c.clone();
                view! { <span class="partition-cell-count" title=tip>{c}</span> }
            })}
        </span>
    }
}

pub fn partition_scheme_for(key: &str) -> &'static str {
    // Daily: "YYYY-MM-DD"
    if key.len() == 10 && &key[4..5] == "-" && &key[7..8] == "-" {
        return "D";
    }
    // Hourly: contains "T" separator
    if key.contains('T') && key.len() >= 13 {
        return "H";
    }
    "·"
}
