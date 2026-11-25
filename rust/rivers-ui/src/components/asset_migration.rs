//! The migration of an asset's stored data, on the asset page.

use leptos::prelude::*;
use leptos_router::components::A;

use crate::helpers::short_id;
use crate::loc::{loc_path, use_current_location};
use crate::server_fns::mutations::{cancel_migration, retry_migration};
use crate::types::{AssetMigration, MigrationState};

/// What the migration of the asset waits for or runs, or why it stopped.
/// Retry for a failed migration; Cancel for one that waits while the code
/// declares no higher version than the stored data (after a rollback).
/// `declared` is the version that the code declares.
#[component]
pub fn AssetMigrationPanel(migration: AssetMigration, declared: Option<u32>) -> impl IntoView {
    let loc = use_current_location();
    let (lns, lnm) = loc.get_untracked();
    let link = move |path: String| loc_path(&lns, &lnm, &path);

    let applied = migration.applied_version;
    let target = migration.target_version.unwrap_or(applied);
    let title = match migration.state {
        MigrationState::Failed => format!("The migration to version {target} failed"),
        MigrationState::Blocked => format!("The migration to version {target} cannot start"),
        _ => format!("Migrating to version {target}"),
    };

    let lines: Vec<AnyView> = match migration.state {
        MigrationState::Pending => {
            vec![view! { <span>"Waits for the daemon to start it."</span> }.into_any()]
        }
        MigrationState::Draining => {
            let mut lines = Vec::new();
            if !migration.upstream.is_empty() {
                let upstream = migration.upstream.join(", ");
                lines.push(
                    view! { <span>{format!("Waits for {upstream} to migrate first.")}</span> }
                        .into_any(),
                );
            }
            if !migration.drain_runs.is_empty() {
                let count = migration.drain_runs.len();
                let runs = if count == 1 { "run" } else { "runs" };
                let links = migration
                    .drain_runs
                    .iter()
                    .map(|run_id| {
                        let href = link(format!("runs/{run_id}"));
                        let label = short_id(run_id, 8);
                        view! { <A href=href>{label}</A> }
                    })
                    .collect::<Vec<_>>();
                lines.push(
                    view! {
                        <span>
                            {format!("Waits for {count} started {runs} that write or read the asset: ")}
                            <span class="migration-panel-runs">{links}</span>
                        </span>
                    }
                    .into_any(),
                );
            }
            if lines.is_empty() {
                lines.push(view! { <span>"Starts on the next pass."</span> }.into_any());
            }
            lines
        }
        MigrationState::Running => {
            let work = match (&migration.backfill_id, &migration.run_id) {
                (Some(backfill_id), _) => {
                    let href = link(format!("backfills/{backfill_id}"));
                    let label = short_id(backfill_id, 8);
                    view! { <span>"Rebuilds in backfill " <A href=href>{label}</A></span> }
                        .into_any()
                }
                (None, Some(run_id)) => {
                    let href = link(format!("runs/{run_id}"));
                    let label = short_id(run_id, 8);
                    view! { <span>"Runs in run " <A href=href>{label}</A></span> }.into_any()
                }
                (None, None) => view! { <span>"Runs."</span> }.into_any(),
            };
            vec![work]
        }
        MigrationState::Failed | MigrationState::Blocked => {
            let reason = migration.reason.clone().unwrap_or_default();
            vec![view! { <pre class="migration-panel-reason">{reason}</pre> }.into_any()]
        }
    };

    let key = migration.asset_key.clone();
    let retry = Action::new(move |_: &()| {
        let (ns, name) = loc.get_untracked();
        let key = key.clone();
        async move { retry_migration(ns, name, key).await }
    });
    let key = migration.asset_key.clone();
    let cancel = Action::new(move |_: &()| {
        let (ns, name) = loc.get_untracked();
        let key = key.clone();
        async move { cancel_migration(ns, name, key).await }
    });
    let busy = Signal::derive(move || retry.pending().get() || cancel.pending().get());
    let error = Signal::derive(move || {
        [retry.value().get(), cancel.value().get()]
            .into_iter()
            .flatten()
            .find_map(|outcome| outcome.err())
            .map(|e| crate::helpers::err_text(&e))
    });
    let can_retry = migration.state == MigrationState::Failed;
    let can_cancel = matches!(
        migration.state,
        MigrationState::Pending | MigrationState::Draining
    ) && declared.is_some_and(|declared| declared <= applied);

    let class = if migration.stopped() {
        "migration-panel migration-panel--stopped"
    } else {
        "migration-panel"
    };
    view! {
        <div class=class>
            <div class="migration-panel-title">{title}</div>
            <div class="migration-panel-line">{format!("The stored data is at version {applied}.")}</div>
            {lines.into_iter().map(|line| view! { <div class="migration-panel-line">{line}</div> }).collect::<Vec<_>>()}
            {(can_retry || can_cancel).then(|| view! {
                <div class="migration-panel-actions">
                    {can_retry.then(|| view! {
                        <button class="btn" disabled=busy on:click=move |_| { retry.dispatch(()); }>
                            "Retry"
                        </button>
                    })}
                    {can_cancel.then(|| view! {
                        <button class="btn" disabled=busy on:click=move |_| { cancel.dispatch(()); }>
                            "Cancel migration"
                        </button>
                    })}
                </div>
            })}
            {move || error.get().map(|e| view! { <div class="error-msg">{e}</div> })}
        </div>
    }
}
