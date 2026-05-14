//! Event timeline component.

use crate::components::metadata_renderer::MetadataTable;
use crate::helpers::format_relative_time;
use crate::types::StoredEvent;
use leptos::prelude::*;

fn format_timestamp(ts: i64) -> String {
    let dt = crate::helpers::nanos_to_datetime(ts);
    match dt {
        Some(d) => d.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
        None => ts.to_string(),
    }
}

fn event_type_label(event: &StoredEvent) -> String {
    format!("{:?}", event.event_type)
}

#[component]
pub fn EventTimeline(events: Vec<StoredEvent>) -> impl IntoView {
    view! {
        <div class="timeline">
            {events
                .into_iter()
                .map(|event| {
                    let time_abs = format_timestamp(event.timestamp);
                    // Tooltip text only — no reactive tick needed.
                    let time_rel = format_relative_time(event.timestamp, chrono::Utc::now().timestamp());
                    let label = event_type_label(&event);
                    let badge_class = format!("badge badge-{}", event_badge_class(&label));
                    let asset = event.asset_key.clone().unwrap_or_default();
                    let partition = event.partition_key.clone().unwrap_or_default();
                    let data_version = event.data_version.clone().unwrap_or_default();
                    let metadata = event.metadata.clone();
                    view! {
                        <div class="timeline-item">
                            <div class="timeline-item-header">
                                <span class="time" title={time_rel}>{time_abs}</span>
                                <span class={badge_class}>{label}</span>
                                {(!asset.is_empty())
                                    .then(|| view! { <span class="tag">{asset}</span> })}
                                {(!partition.is_empty())
                                    .then(|| view! { <span class="tag">{partition}</span> })}
                                {(!data_version.is_empty())
                                    .then(|| view! { <span class="tag tag-muted">{format!("v:{}", &data_version[..8.min(data_version.len())])}</span> })}
                            </div>
                            {(!metadata.is_empty()).then(|| view! {
                                <div class="timeline-item-metadata">
                                    <MetadataTable entries={metadata}/>
                                </div>
                            })}
                        </div>
                    }
                })
                .collect::<Vec<_>>()}
        </div>
    }
}

fn event_badge_class(label: &str) -> &'static str {
    match label {
        "Materialization" => "success",
        "StepSuccess" => "success",
        "StepFailure" => "error",
        "StepStart" => "warning",
        "Observation" => "info",
        _ => "muted",
    }
}
