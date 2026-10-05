use leptos::prelude::*;

/// Small pill with a colored dot indicator and a lowercase label.
///
/// Callers pass one of `helpers::CHIP_KINDS` (typically via
/// `run_status_kind` / `backfill_status_kind`). The kind is used directly as
/// the `.dot-{kind}` class suffix and the visible label — no free-form
/// string normalization, so a mistyped kind shows up in the DOM instead of
/// being silently rewritten.
#[component]
pub fn StatusChip(#[prop(into)] kind: String) -> impl IntoView {
    let dot_cls = format!("dot dot-{kind}");
    view! {
        <span class="chip">
            <span class={dot_cls}></span>
            {kind}
        </span>
    }
}

#[component]
pub fn Tag(
    #[prop(into)] label: String,
    #[prop(optional, into)] color: Option<String>,
) -> impl IntoView {
    let style = color.map(|c| format!("color:{c}"));
    view! { <span class="rv-tag" style=style>{label}</span> }
}

/// Kind badge — colored by language family (`python` / `sql` / `api`).
#[component]
pub fn KindBadge(#[prop(into)] kind: String) -> impl IntoView {
    let k = kind.to_ascii_lowercase();
    let cls = match k.as_str() {
        "python" | "py" => "kind-badge kind-badge--python",
        "sql" => "kind-badge kind-badge--sql",
        "api" | "http" => "kind-badge kind-badge--api",
        _ => "kind-badge kind-badge--other",
    };
    view! { <span class=cls>{kind}</span> }
}

#[component]
pub fn ProgressBar(
    /// 0.0 .. 1.0
    #[prop(into)]
    value: Signal<f64>,
    #[prop(optional, into, default = "var(--secondary)".to_string())] color: String,
    #[prop(optional, default = 4)] height_px: u32,
) -> impl IntoView {
    let bar_style = format!("height:{height_px}px");
    let color_c = color.clone();
    let fill_style = move || {
        let pct = (value.get() * 100.0).clamp(0.0, 100.0);
        format!("width:{pct:.1}%; background:{color_c}")
    };
    view! {
        <div class="rv-progress" style=bar_style>
            <div class="rv-progress-fill" style=fill_style></div>
        </div>
    }
}
