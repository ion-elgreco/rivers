//! Deployment information page.

use leptos::prelude::*;

use crate::components::loading_skeleton::CardSkeleton;
use crate::components::ui_kit::{
    Crumb, DeployCard, DeployRow, DeploymentDiagram, DeploymentNode, Topbar,
};
use crate::loc::use_current_location;
use crate::server_fns::overview::get_deployment_info;

#[component]
pub fn DeploymentPage() -> impl IntoView {
    let loc = use_current_location();
    let info = Resource::new(
        move || loc.get(),
        |(ns, name)| async move { get_deployment_info(ns, name).await },
    );

    view! {
        <Topbar crumbs=vec![Crumb::new("Deployment")]/>

        <Transition fallback=move || view! { <CardSkeleton/> }>
            {move || {
                info.get().map(|result| match result {
                    Ok(info) => {
                        let nodes = vec![
                            DeploymentNode {
                                label: "Daemon".into(),
                                sub: if info.daemon_active {
                                    format!("{} schedules · {} sensors", info.daemon_schedules, info.daemon_sensors)
                                } else {
                                    "inactive".into()
                                },
                                glyph: "⚙".into(),
                                ok: info.daemon_active,
                            },
                            DeploymentNode {
                                label: "Code Locations".into(),
                                sub: if info.code_location_mode == "embedded" {
                                    "embedded".into()
                                } else {
                                    format!("{}/{} ready", info.code_locations_ready, info.code_locations_total)
                                },
                                glyph: "⇄".into(),
                                ok: info.code_locations_total > 0
                                    && info.code_locations_ready == info.code_locations_total,
                            },
                            DeploymentNode {
                                label: "Storage".into(),
                                sub: info.storage_type.clone(),
                                glyph: "⬢".into(),
                                ok: true,
                            },
                        ];

                        let daemon_status = if info.daemon_active { "healthy" } else { "inactive" };
                        let grpc_status = if info.grpc_connected { "connected" } else { "disconnected" };

                        view! {
                            <DeploymentDiagram nodes=nodes/>

                            <div class="deploy-grid">
                                <DeployCard label="VERSION">
                                    <DeployRow label="rivers".to_string() value=info.version.clone() mono=true/>
                                    <DeployRow label="abi".to_string() value="cp310-abi3".to_string() mono=true/>
                                </DeployCard>

                                <DeployCard
                                    label="DAEMON"
                                    status_label=daemon_status.to_string()
                                    status_ok=info.daemon_active
                                >
                                    <DeployRow label="Schedules".to_string() value=info.daemon_schedules.to_string() mono=true/>
                                    <DeployRow label="Sensors".to_string() value=info.daemon_sensors.to_string() mono=true/>
                                </DeployCard>

                                <DeployCard
                                    label="CODE LOCATION"
                                    status_label=grpc_status.to_string()
                                    status_ok=info.grpc_connected
                                >
                                    <DeployRow label="gRPC".to_string() value={
                                        if info.grpc_url.is_empty() { "—".into() } else { info.grpc_url.clone() }
                                    } mono=true/>
                                </DeployCard>

                                <DeployCard label="STORAGE">
                                    <DeployRow label="Backend".to_string() value=info.storage_type.clone() mono=true/>
                                    <DeployRow label="Assets".to_string() value=info.asset_count.to_string() mono=true/>
                                    <DeployRow label="Runs".to_string() value=info.run_count.to_string() mono=true/>
                                    <DeployRow label="Events".to_string() value=info.event_count.to_string() mono=true/>
                                    <DeployRow label="Ticks".to_string() value=info.tick_count.to_string() mono=true/>
                                </DeployCard>
                            </div>
                        }.into_any()
                    }
                    Err(e) => view! { <div class="error-msg">{format!("Error: {e}")}</div> }.into_any(),
                })
            }}
        </Transition>
    }
}
