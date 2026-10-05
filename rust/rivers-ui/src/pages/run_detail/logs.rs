use leptos::prelude::*;

use crate::components::pagination::InfiniteEventList;
use crate::helpers::{nanos_to_datetime, short_id};
use crate::server_fns::runs::get_run_structured_events_page;
use crate::types::{EventType, RunLog, StoredEvent};

/// Flatten a run's log rows into `(step, content)` pairs for one stream
/// (`stdout`/`stderr`/`logs`).
fn extract_log_lines(logs: &[RunLog], key: &str) -> Vec<(String, String)> {
    logs.iter()
        .filter_map(|l| {
            let content = match key {
                "stdout" => l.stdout.as_ref(),
                "stderr" => l.stderr.as_ref(),
                _ => l.logs.as_ref(),
            }?;
            (!content.is_empty()).then(|| (l.step_key.clone(), content.clone()))
        })
        .collect()
}

pub(super) fn format_log_timestamp(ts: i64) -> String {
    nanos_to_datetime(ts)
        .map(|d| d.strftime("%H:%M:%S%.3f").to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn event_row_class(evt: &StoredEvent) -> &'static str {
    match evt.event_type {
        EventType::StepStart => "log-row--info",
        EventType::StepSuccess => "log-row--success",
        EventType::StepFailure => "log-row--error",
        EventType::StepRetry => "log-row--warn",
        EventType::Materialization => "log-row--success",
        EventType::Observation => "log-row--info",
        EventType::RunQueued | EventType::RunDequeued => "log-row--info",
        EventType::RunLaunchFailed => "log-row--error",
        EventType::StepSlotClaimed | EventType::StepSlotReleased => "log-row--info",
        EventType::StepSlotWaiting => "log-row--warn",
        EventType::StepSlotRenewed => "log-row--muted",
        EventType::ActionCompleted => "log-row--success",
        EventType::Deletion => "log-row--warn",
    }
}

fn metadata_value(evt: &StoredEvent, key: &str) -> Option<String> {
    evt.metadata
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_text())
}

fn event_info(evt: &StoredEvent) -> String {
    match evt.event_type {
        EventType::StepStart => "Step started".to_string(),
        EventType::StepSuccess => "Step succeeded".to_string(),
        EventType::StepFailure => evt
            .metadata
            .iter()
            .find(|(k, _)| k == "error")
            .map(|(_, v)| v.as_text())
            .unwrap_or_else(|| "Step failed".to_string()),
        EventType::StepRetry => {
            let mut s = "Retrying".to_string();
            if let Some(a) = metadata_value(evt, "rivers/attempt") {
                s.push_str(&format!(" after attempt {a}"));
            }
            if let Some(r) = metadata_value(evt, "rivers/failure_reason") {
                s.push_str(&format!(" ({r})"));
            }
            if let Some(d) = metadata_value(evt, "rivers/next_delay_ms") {
                s.push_str(&format!(" in {d}ms"));
            }
            s
        }
        EventType::Materialization => {
            let mut parts = Vec::new();
            if let Some(p) = &evt.partition_key {
                parts.push(format!("partition {p}"));
            }
            if let Some(v) = &evt.data_version {
                parts.push(format!("v:{}", short_id(v, 8)));
            }
            if parts.is_empty() {
                "Materialized".to_string()
            } else {
                format!("Materialized ({})", parts.join(", "))
            }
        }
        EventType::Observation => "Observed".to_string(),
        EventType::RunQueued => "Run queued".to_string(),
        EventType::RunDequeued => "Run dequeued".to_string(),
        EventType::RunLaunchFailed => metadata_value(evt, "error")
            .map(|e| format!("Run launch failed: {e}"))
            .unwrap_or_else(|| "Run launch failed".to_string()),
        EventType::StepSlotClaimed => {
            let pools = metadata_value(evt, "pools");
            format!(
                "Claimed pool slots{}",
                pools.map(|p| format!(" ({})", p)).unwrap_or_default()
            )
        }
        EventType::StepSlotWaiting => {
            let reason = metadata_value(evt, "reason");
            format!(
                "Waiting for pool slots{}",
                reason.map(|r| format!(": {}", r)).unwrap_or_default()
            )
        }
        EventType::StepSlotRenewed => "Lease renewed".to_string(),
        EventType::StepSlotReleased => "Released pool slots".to_string(),
        EventType::ActionCompleted => {
            let action = metadata_value(evt, "action");
            format!(
                "Action completed{}",
                action.map(|a| format!(" ({a})")).unwrap_or_default()
            )
        }
        EventType::Deletion => {
            let action = metadata_value(evt, "action");
            format!(
                "Materialization state cleared{}",
                action.map(|a| format!(" ({a})")).unwrap_or_default()
            )
        }
    }
}

/// Case-insensitive whole-word match. Used only for filtering — the raw line
/// is still displayed unchanged.
fn line_matches_level(line: &str, level: &str) -> bool {
    let aliases: &[&str] = match level {
        "info" => &["INFO"],
        "debug" => &["DEBUG", "TRACE"],
        "warn" => &["WARN", "WARNING"],
        "error" => &["ERROR", "CRITICAL", "FATAL", "ERR"],
        _ => return true,
    };
    let upper = line.to_ascii_uppercase();
    let bytes = upper.as_bytes();
    let is_word_boundary = |i: usize| -> bool {
        if i >= bytes.len() {
            return true;
        }
        !bytes[i].is_ascii_alphabetic()
    };
    for needle in aliases {
        let n = needle.as_bytes();
        let mut i = 0usize;
        while i + n.len() <= bytes.len() {
            if &bytes[i..i + n.len()] == n {
                let left_ok = i == 0 || !bytes[i - 1].is_ascii_alphabetic();
                let right_ok = is_word_boundary(i + n.len());
                if left_ok && right_ok {
                    return true;
                }
            }
            i += 1;
        }
    }
    false
}

/// Preserves the raw text (including levels, timestamps, or `[tag]` prefixes the user wrote).
/// When `level_filter` is not `"all"`, drops lines that don't mention the level.
fn render_log_rows(data: Vec<(String, String)>, level_filter: &str) -> Vec<impl IntoView + use<>> {
    let mut out: Vec<_> = Vec::new();
    for (asset, blob) in data {
        for raw in blob.split('\n') {
            if raw.trim().is_empty() {
                continue;
            }
            if level_filter != "all" && !line_matches_level(raw, level_filter) {
                continue;
            }
            let msg_html = ansi_to_html::convert(raw).unwrap_or_else(|_| raw.to_string());
            out.push(view! {
                <div class="log-row">
                    <span class="log-row-source" title=asset.clone()>{asset.clone()}</span>
                    <span class="log-row-msg" inner_html=msg_html></span>
                </div>
            });
        }
    }
    out
}

#[component]
pub(super) fn RunLogPanel(
    run_id: Memo<String>,
    refresh_tick: ReadSignal<u32>,
    run_logs: Signal<Vec<RunLog>>,
    logs_error: Signal<Option<String>>,
    selected_step: ReadSignal<Option<String>>,
    on_clear: WriteSignal<Option<String>>,
    log_tab: ReadSignal<String>,
    set_log_tab: WriteSignal<String>,
    log_level: ReadSignal<String>,
    set_log_level: WriteSignal<String>,
) -> impl IntoView {
    // Server-paginated — a `single_run` run can emit 15k+ events, never all at once.
    let (ev_page, set_ev_page) = signal(0u64);
    const EV_PAGE_SIZE: u64 = 50;
    let structured_page = Resource::new(
        move || {
            (
                run_id.get(),
                selected_step.get(),
                ev_page.get(),
                refresh_tick.get(),
            )
        },
        move |(rid, sel, p, _)| async move {
            get_run_structured_events_page(rid, sel, p * EV_PAGE_SIZE, EV_PAGE_SIZE).await
        },
    );
    // Reset to page 0 when the step filter or the run changes.
    Effect::new(move |_| {
        selected_step.track();
        run_id.track();
        set_ev_page.set(0);
    });

    // stdout/stderr/logs derive from the small run_logs stream, which refreshes live.
    let all_stdout = Signal::derive(move || extract_log_lines(&run_logs.get(), "stdout"));
    let all_stderr = Signal::derive(move || extract_log_lines(&run_logs.get(), "stderr"));
    let all_logs = Signal::derive(move || extract_log_lines(&run_logs.get(), "logs"));

    let make_filtered = move |data: Signal<Vec<(String, String)>>| {
        Signal::derive(move || {
            let d = data.get();
            match selected_step.get() {
                Some(name) => d
                    .into_iter()
                    .filter(|(a, _)| *a == name)
                    .collect::<Vec<_>>(),
                None => d,
            }
        })
    };
    let stdout_lines = make_filtered(all_stdout);
    let stderr_lines = make_filtered(all_stderr);
    let log_lines = make_filtered(all_logs);

    view! {
        <div class="log-panel">
            <div class="log-panel-header">
                <span class="log-panel-label">"LOGS"</span>
                <div class="filter-pill-group">
                    {[("events", "Events"), ("logs", "Logs"), ("stdout", "Stdout"), ("stderr", "Stderr")]
                        .into_iter()
                        .map(|(tab, label)| {
                            let lines = move || match tab {
                                "logs" => all_logs.get().len(),
                                "stdout" => all_stdout.get().len(),
                                "stderr" => all_stderr.get().len(),
                                _ => 0,
                            };
                            let count_cls = if tab == "stderr" { "count count--error" } else { "count" };
                            view! {
                                <button
                                    class=move || if log_tab.get() == tab { "filter-pill filter-pill--active" } else { "filter-pill" }
                                    aria-pressed=move || (log_tab.get() == tab).to_string()
                                    on:click=move |_| set_log_tab.set(tab.to_string())
                                    disabled=move || tab != "events" && lines() == 0
                                >
                                    {label}
                                    {move || (lines() > 0).then(|| view! { <span class=count_cls>{lines()}</span> })}
                                </button>
                            }
                        })
                        .collect::<Vec<_>>()}
                </div>
                <Show when=move || selected_step.get().is_some()>
                    <button
                        class="log-filter-chip"
                        title="Clear the asset filter"
                        on:click=move |_| on_clear.set(None)
                    >
                        {move || format!("Filtered: {}", selected_step.get().unwrap_or_default())}
                        <span class="log-filter-chip-x" aria-hidden="true">" ×"</span>
                    </button>
                </Show>
                <Show when=move || matches!(log_tab.get().as_str(), "logs" | "stdout" | "stderr")>
                    <div class="filter-pill-group" style="margin-left:auto">
                        {[("all", "All"), ("info", "Info"), ("debug", "Debug"), ("warn", "Warn"), ("error", "Error")]
                            .into_iter()
                            .map(|(lvl, label)| {
                                let tint = match lvl {
                                    "info" => " filter-pill--info",
                                    "warn" => " filter-pill--warn",
                                    "error" => " filter-pill--error",
                                    _ => "",
                                };
                                view! {
                                    <button
                                        class=move || if log_level.get() == lvl {
                                            format!("filter-pill filter-pill--active{tint}")
                                        } else {
                                            format!("filter-pill{tint}")
                                        }
                                        aria-pressed=move || (log_level.get() == lvl).to_string()
                                        on:click=move |_| set_log_level.set(lvl.to_string())
                                    >{label}</button>
                                }
                            })
                            .collect::<Vec<_>>()}
                    </div>
                </Show>
                <div
                    class=move || if selected_step.get().is_some() { "log-live-indicator log-live-indicator--paused" } else { "log-live-indicator log-live-indicator--live" }
                    style=move || if matches!(log_tab.get().as_str(), "logs" | "stdout" | "stderr") { "margin-left: 12px" } else { "margin-left: auto" }
                >
                    <span class="log-live-indicator-dot"></span>
                    <span>{move || if selected_step.get().is_some() { "paused" } else { "live" }}</span>
                </div>
            </div>

            {move || logs_error.get().map(|msg| view! {
                <div class="error-msg log-panel-error">{format!("Couldn't load logs: {msg}")}</div>
            })}
            <div style=move || if log_tab.get() == "events" { "" } else { "display:none" }>
                <div class="log-event-row log-event-head">
                    <span class="log-col-time">"Time"</span>
                    <span class="log-col-asset">"Asset"</span>
                    <span class="log-col-type">"Event"</span>
                    <span class="log-col-info">"Info"</span>
                </div>
                <InfiniteEventList
                    data=structured_page
                    page=ev_page
                    set_page=set_ev_page
                    empty=move || view! { <div class="log-empty">"No events recorded."</div> }
                    row={move |evt: crate::types::StoredEvent| {
                        let row_class = event_row_class(&evt);
                        let ts = format_log_timestamp(evt.timestamp);
                        let asset = evt.asset_key.clone().unwrap_or_default();
                        let etype = evt.event_type.label();
                        let info = event_info(&evt);
                        view! {
                            <div class=format!("log-event-row {row_class}")>
                                <span class="log-col-time"><code>{ts}</code></span>
                                <span class="log-col-asset">{asset}</span>
                                <span class="log-col-type"><span class="log-type-badge">{etype}</span></span>
                                <span class="log-col-info">{info}</span>
                            </div>
                        }.into_any()
                    }}
                />
            </div>

            <div class="log-panel-body" style=move || if log_tab.get() == "logs" { "" } else { "display:none" }>
                {
                    let data = log_lines;
                    move || {
                        let rendered = render_log_rows(data.get(), &log_level.get());
                        if rendered.is_empty() {
                            view! { <div class="log-empty">"No logs captured."</div> }.into_any()
                        } else {
                            view! { <div class="log-rows">{rendered}</div> }.into_any()
                        }
                    }
                }
            </div>
            <div class="log-panel-body" style=move || if log_tab.get() == "stdout" { "" } else { "display:none" }>
                {
                    let data = stdout_lines;
                    move || {
                        let rendered = render_log_rows(data.get(), &log_level.get());
                        if rendered.is_empty() {
                            view! { <div class="log-empty">"No stdout captured."</div> }.into_any()
                        } else {
                            view! { <div class="log-rows">{rendered}</div> }.into_any()
                        }
                    }
                }
            </div>
            <div class="log-panel-body" style=move || if log_tab.get() == "stderr" { "" } else { "display:none" }>
                {
                    let data = stderr_lines;
                    move || {
                        let rendered = render_log_rows(data.get(), &log_level.get());
                        if rendered.is_empty() {
                            view! { <div class="log-empty">"No stderr captured."</div> }.into_any()
                        } else {
                            view! { <div class="log-rows">{rendered}</div> }.into_any()
                        }
                    }
                }
            </div>
        </div>
    }
}
