//! Color-coded status badge components for run states and staleness indicators.

use leptos::prelude::*;

#[component]
pub fn StatusBadge(status: String) -> impl IntoView {
    let class = match status.to_uppercase().as_str() {
        "SUCCESS" => "badge badge-success",
        "RUNNING" => "badge badge-warning",
        "FAILURE" => "badge badge-error",
        "STARTED" => "badge badge-warning",
        "NOTSTARTED" | "STOPPED" => "badge badge-muted",
        "CANCELED" => "badge badge-muted",
        _ => "badge badge-info",
    };
    view! { <span class={class}>{status}</span> }
}
