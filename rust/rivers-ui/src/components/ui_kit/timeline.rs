use leptos::prelude::*;

#[derive(Clone, Debug)]
pub struct GlyphEvent {
    /// Minutes before "now" (0 = now, horizon = oldest shown).
    pub minutes_ago: f64,
    /// "◆" materialization, "▲" failure, "◐" retry, "○" observation.
    pub glyph: &'static str,
    pub status: &'static str, // "ok" | "err" | "warn" | "info"
    /// Optional run-id; if `selected_run` matches this, event stays vivid.
    pub run: Option<String>,
    pub label: String,
}

/// Horizontal time axis with glyph-plotted events.
/// Oldest on the left, "now" on the right.
#[component]
pub fn EventGlyphTimeline(
    #[prop(into)] events: Vec<GlyphEvent>,
    #[prop(optional, default = 180.0)] horizon_minutes: f64,
    #[prop(optional, into)] selected_run: Option<String>,
) -> impl IntoView {
    let ticks = [0, 30, 60, 90, 120, 150, 180]
        .iter()
        .map(|m| {
            let pct = 100.0 - (*m as f64 / horizon_minutes * 100.0);
            view! {
                <span class="glyph-timeline-tick" style=format!("left:{pct:.1}%")>
                    {if *m == 0 { "now".to_string() } else { format!("-{m}m") }}
                </span>
            }
        })
        .collect::<Vec<_>>();

    // Distinct runs referenced in the events — assign a palette color per run
    // so the per-event run-bar matches the runs legend at the bottom.
    let run_palette = [
        "var(--accent)",
        "var(--secondary)",
        "#d4a5ff",
        "#f5b342",
        "#7dd67a",
    ];
    let mut run_ids: Vec<String> = Vec::new();
    for e in &events {
        if let Some(ref r) = e.run
            && !r.is_empty()
            && !run_ids.contains(r)
        {
            run_ids.push(r.clone());
        }
    }
    let run_color_for = |r: &str| -> &'static str {
        let idx = run_ids.iter().position(|x| x == r).unwrap_or(0);
        run_palette[idx % run_palette.len()]
    };

    let sel = selected_run.as_ref().cloned();
    let events_v = events
        .iter()
        .map(|e| {
            let pct = 100.0 - (e.minutes_ago / horizon_minutes * 100.0).clamp(0.0, 100.0);
            let icon_cls = match e.status {
                "err" => "glyph-event-icon glyph-event-icon--err",
                "warn" => "glyph-event-icon glyph-event-icon--warn",
                "info" => "glyph-event-icon glyph-event-icon--info",
                _ => "glyph-event-icon glyph-event-icon--ok",
            };
            let bar_color = e
                .run
                .as_deref()
                .map(run_color_for)
                .unwrap_or("var(--text-muted)");
            let dim = match (&sel, &e.run) {
                (Some(want), Some(have)) => want != have,
                (Some(_), None) => true,
                _ => false,
            };
            let cls = if dim {
                "glyph-event glyph-event--dim"
            } else {
                "glyph-event"
            };
            view! {
                <div class=cls style=format!("left:{pct:.1}%") title=e.label.clone()>
                    <span class=icon_cls>{e.glyph}</span>
                    <span class="glyph-event-bar" style=format!("background:{bar_color}")></span>
                </div>
            }
        })
        .collect::<Vec<_>>();

    // Glyph legend — fixed set matching Rivers' cmap (MAT/FAI/OBS colored)
    let glyph_legend: Vec<_> = [
        ("◆", "MAT", "var(--success)"),
        ("▲", "FAI", "var(--error)"),
        ("◐", "RET", "var(--warning)"),
        ("○", "OBS", "var(--secondary)"),
    ]
    .iter()
    .map(|(g, label, color)| {
        view! {
            <span class="glyph-legend-item">
                <span class="glyph-legend-icon" style=format!("color:{color}")>{*g}</span>
                <span class="glyph-legend-label">{*label}</span>
            </span>
        }
    })
    .collect();

    // Runs legend — one swatch per distinct run id present in events
    let runs_legend = run_ids.iter().map(|r| {
        let color = run_color_for(r);
        let short = crate::helpers::short_id(r, 8);
        view! {
            <span class="glyph-runs-legend-item" title=r.clone()>
                <span class="glyph-runs-legend-swatch" style=format!("background:{color}")></span>
                <span>{format!("#{short}")}</span>
            </span>
        }
    }).collect::<Vec<_>>();
    let show_runs_legend = !run_ids.is_empty();

    view! {
        <div class="glyph-timeline-panel">
            <div class="glyph-timeline-header">
                <span class="section-header-label">"EVENT TIMELINE · LAST 3H"</span>
                <div class="glyph-legend">{glyph_legend}</div>
            </div>
            <div class="glyph-timeline">
                <span class="glyph-timeline-axis"></span>
                {ticks}
                {events_v}
            </div>
            <Show when=move || show_runs_legend>
                <div class="glyph-runs-legend">
                    <span class="section-header-label">"RUNS"</span>
                    {runs_legend.clone()}
                </div>
            </Show>
        </div>
    }
}
