use leptos::prelude::*;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum HeatCell {
    Done,
    Running,
    Failed,
    Pending,
    Canceled,
}

/// Partition / backfill heatmap — CSS-grid of status-colored cells.
/// Default layout is 30 columns; pass `compact=true` for a 20-column variant.
/// Pass `freshness_gradient=true` to apply an opacity gradient across `Done`
/// cells so oldest cells look faded and newest look vivid (Rivers' partition
/// freshness pattern for asset detail).
#[component]
pub fn PartitionHeatmap(
    #[prop(into)] cells: Vec<HeatCell>,
    #[prop(optional)] compact: bool,
    #[prop(optional)] legend: bool,
    #[prop(optional)] freshness_gradient: bool,
    /// Optional per-cell labels (e.g. partition keys). When present, hovering a
    /// cell reveals a Rivers-style inline info bar below the grid showing the
    /// key, index, and status.
    #[prop(optional, into)]
    labels: Vec<String>,
) -> impl IntoView {
    let grid_cls = if compact {
        "heatmap heatmap--sm"
    } else {
        "heatmap"
    };
    let total = cells.len();
    let (hover, set_hover) = signal(Option::<usize>::None);
    let labels_arc = std::sync::Arc::new(labels);

    let rendered = cells
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let cls = match c {
                HeatCell::Done => "heatmap-cell heatmap-cell--done",
                HeatCell::Running => "heatmap-cell heatmap-cell--running",
                HeatCell::Failed => "heatmap-cell heatmap-cell--failed",
                HeatCell::Pending => "heatmap-cell heatmap-cell--pending",
                HeatCell::Canceled => "heatmap-cell heatmap-cell--canceled",
            };
            let status_word = match c {
                HeatCell::Done => "done",
                HeatCell::Running => "running",
                HeatCell::Failed => "failed",
                HeatCell::Pending => "pending",
                HeatCell::Canceled => "canceled",
            };
            let style = if freshness_gradient && matches!(c, HeatCell::Done) {
                let op = 0.4 + (i as f64 / (total.max(1)) as f64) * 0.55;
                Some(format!("opacity:{op:.2}"))
            } else {
                None
            };
            let title = labels_arc
                .get(i)
                .map(|k| format!("{k} · {status_word}"))
                .unwrap_or_else(|| status_word.to_string());
            view! {
                <div
                    class=cls
                    style=style
                    title=title
                    on:mouseenter=move |_| set_hover.set(Some(i))
                    on:mouseleave=move |_| set_hover.set(None)
                ></div>
            }
        })
        .collect::<Vec<_>>();

    // Snapshot per-cell data for the info-bar lookup — wrap in Arc so the view
    // closure can be cloned (Leptos needs Fn, not FnOnce).
    let info_lookup: std::sync::Arc<Vec<(String, &'static str, &'static str)>> =
        std::sync::Arc::new(
            cells
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    let key = labels_arc
                        .get(i)
                        .cloned()
                        .unwrap_or_else(|| format!("#{}", i + 1));
                    let word = match c {
                        HeatCell::Done => "done",
                        HeatCell::Running => "running",
                        HeatCell::Failed => "failed",
                        HeatCell::Pending => "pending",
                        HeatCell::Canceled => "canceled",
                    };
                    let cls = match c {
                        HeatCell::Done => "heatmap-info-dot--done",
                        HeatCell::Running => "heatmap-info-dot--running",
                        HeatCell::Failed => "heatmap-info-dot--failed",
                        HeatCell::Pending => "heatmap-info-dot--pending",
                        HeatCell::Canceled => "heatmap-info-dot--canceled",
                    };
                    (key, word, cls)
                })
                .collect(),
        );
    let has_labels = !labels_arc.is_empty();
    let total_cells = total;

    view! {
        <div>
            <div class=grid_cls>{rendered}</div>
            <Show when=move || has_labels && hover.get().is_some()>
                {
                    let info_lookup = info_lookup.clone();
                    move || {
                        let i = hover.get()?;
                        let (key, word, cls) = info_lookup.get(i)?.clone();
                        Some(view! {
                            <div class="heatmap-info">
                                <span class=format!("heatmap-info-dot {cls}")></span>
                                <span class="heatmap-info-label">"partition"</span>
                                <span class="heatmap-info-key">{key}</span>
                                <span class="heatmap-info-sep">"·"</span>
                                <span class="heatmap-info-pos">{format!("#{} of {}", i + 1, total_cells)}</span>
                                <span class="heatmap-info-sep">"·"</span>
                                <span class=format!("heatmap-info-state heatmap-info-state--{word}")>{word}</span>
                            </div>
                        })
                    }
                }
            </Show>
            {legend.then(|| view! {
                <div class="heatmap-legend">
                    <span><span class="heatmap-legend-swatch" style="background:var(--success)"></span>"done"</span>
                    <span><span class="heatmap-legend-swatch" style="background:var(--secondary)"></span>"running"</span>
                    <span><span class="heatmap-legend-swatch" style="background:var(--error)"></span>"failed"</span>
                    <span><span class="heatmap-legend-swatch" style="background:var(--bg-highest)"></span>"pending"</span>
                </div>
            })}
        </div>
    }
}

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
