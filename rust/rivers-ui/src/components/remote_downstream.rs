//! Assets of other code locations that depend on an asset.

use leptos::prelude::*;
use leptos_router::components::A;

use crate::components::ui_kit::SectionHeader;
use crate::types::RemoteDownstream;

/// One dotted row per asset, linking to its page in its code location.
/// Renders nothing when no other code location depends on the asset.
#[component]
pub fn RemoteDownstreamSection(rows: Vec<RemoteDownstream>) -> impl IntoView {
    (!rows.is_empty()).then(|| {
        let count = rows.len().to_string();
        let rows: Vec<_> = rows
            .into_iter()
            .map(|row| {
                let location = row.location().1.to_string();
                view! {
                    <A
                        href=row.href()
                        attr:class="lineage-row"
                        attr:style="border-left-color: var(--text-muted); border-left-style: dotted"
                    >
                        <span class="lineage-row-key">{row.asset_key}</span>
                        <span class="lineage-row-kind">"remote"</span>
                        <span class="lineage-row-mat">{location}</span>
                    </A>
                }
            })
            .collect();
        view! {
            <div class="lineage-remote-downstream">
                <SectionHeader label="IN OTHER CODE LOCATIONS" count=count/>
                <div class="lineage-rows">{rows}</div>
            </div>
        }
    })
}
