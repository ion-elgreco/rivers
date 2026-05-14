//! Top-level Leptos application component and route definitions.
//!
//! Configures the client-side router. Every functional route lives under
//! `/locations/:loc_ns/:loc_name/...` so that every page operates on an
//! explicit code location. The bare root path `/` is a thin redirect
//! component that resolves the first Ready location and navigates there.

use leptos::prelude::*;
#[cfg(feature = "ssr")]
use leptos_meta::MetaTags;
use leptos_meta::provide_meta_context;
use leptos_router::components::{FlatRoutes, Redirect, Route, Router};
use leptos_router::path;

use crate::components::layout::Shell;
use crate::pages::asset_detail::AssetDetailPage;
use crate::pages::assets_list::AssetsListPage;
use crate::pages::automation::AutomationPage;
use crate::pages::backfill_detail::BackfillDetailPage;
use crate::pages::backfills_list::BackfillsListPage;
use crate::pages::deployment::DeploymentPage;
use crate::pages::graph::GraphPage;
use crate::pages::job_detail::JobDetailPage;
use crate::pages::jobs::JobsListPage;
use crate::pages::overview::OverviewPage;
use crate::pages::pools::PoolsPage;
use crate::pages::queue::QueuePage;
use crate::pages::run_detail::RunDetailPage;
use crate::pages::runs_list::RunsListPage;
use crate::pages::schedule_detail::ScheduleDetailPage;
use crate::pages::sensor_detail::SensorDetailPage;
use crate::server_fns::locations::list_code_locations;

/// SSR HTML shell wrapping `<App/>` with `<head>` (fonts, stylesheet, meta
/// tags) and `<body>` (which carries the SSR-stamped now timestamp via a
/// data attribute the WASM hydrate path reads back).
#[cfg(feature = "ssr")]
pub fn shell() -> impl IntoView {
    let options = expect_context::<leptos::config::LeptosOptions>();
    // Stamp server-now so `App` uses the same value SSR rendered with, and
    // so the WASM hydrate path can read it back from the body before the
    // first reactive render. See `crate::now` for the full handshake.
    let server_now = chrono::Utc::now().timestamp();
    provide_context(crate::now::SsrInitialNow(server_now));
    let favicon_href = crate::favicon::data_uri();
    view! {
        <!DOCTYPE html>
        <html lang="en">
            <head>
                <meta charset="utf-8"/>
                <meta name="viewport" content="width=device-width, initial-scale=1"/>
                <link rel="icon" type="image/svg+xml" href=favicon_href/>
                <link rel="preconnect" href="https://fonts.googleapis.com"/>
                <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin="anonymous"/>
                <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&family=Space+Grotesk:wght@500;600;700&display=swap"/>
                <link rel="stylesheet" href="/style.css"/>
                <MetaTags/>
            </head>
            <body data-ssr-now=server_now.to_string()>
                <App/>
                <HydrationScripts options/>
                <script>{r#"
                    document.addEventListener('click', function(e) {
                        var el = e.target.closest('.copyable[data-copy]');
                        if (!el) return;
                        var text = el.getAttribute('data-copy');
                        navigator.clipboard.writeText(text).then(function() {
                            var orig = el.textContent;
                            el.textContent = 'Copied!';
                            setTimeout(function() { el.textContent = orig; }, 1000);
                        });
                    });
                "#}</script>
            </body>
        </html>
    }
}

/// Root redirect: resolves the first Ready code location and sends the user
/// to its overview. Renders a "no Ready code location" message if the
/// registry is empty or every entry is non-Ready.
#[component]
fn RootRedirect() -> impl IntoView {
    let first = Resource::new(
        || (),
        |_| async move {
            list_code_locations().await.map(|entries| {
                entries
                    .into_iter()
                    .find(|e| e.phase == "Ready")
                    .map(|e| (e.namespace, e.name))
            })
        },
    );

    view! {
        <Suspense fallback=|| view! { <div class="redirect-msg">"Discovering code locations…"</div> }>
            {move || first.get().map(|res| match res {
                Ok(Some((ns, name))) => {
                    let path = format!("/locations/{ns}/{name}");
                    view! { <Redirect path=path/> }.into_any()
                }
                Ok(None) => view! {
                    <div class="redirect-msg">
                        "No Ready code location yet. Apply a CodeLocation CR or wait for the operator to reconcile."
                    </div>
                }.into_any(),
                Err(e) => view! {
                    <div class="redirect-msg redirect-msg--error">
                        {format!("Failed to query code locations: {e}")}
                    </div>
                }.into_any(),
            })}
        </Suspense>
    }
}

/// Top-level app component. Wires the router and route table — every
/// `<Route/>` here corresponds to one page in `crate::pages`.
#[component]
pub fn App() -> impl IntoView {
    provide_meta_context();
    crate::now::provide_now_context();

    view! {
        <Router>
            <Shell>
                <FlatRoutes fallback=|| view! { <div class="error-msg">"Page not found."</div> }>
                    <Route path=path!("/") view=RootRedirect/>
                    <Route path=path!("/locations/:loc_ns/:loc_name") view=OverviewPage/>
                    <Route path=path!("/locations/:loc_ns/:loc_name/assets") view=AssetsListPage/>
                    <Route path=path!("/locations/:loc_ns/:loc_name/assets/:key") view=AssetDetailPage/>
                    <Route path=path!("/locations/:loc_ns/:loc_name/runs") view=RunsListPage/>
                    <Route path=path!("/locations/:loc_ns/:loc_name/runs/:id") view=RunDetailPage/>
                    <Route path=path!("/locations/:loc_ns/:loc_name/graph") view=GraphPage/>
                    <Route path=path!("/locations/:loc_ns/:loc_name/jobs") view=JobsListPage/>
                    <Route path=path!("/locations/:loc_ns/:loc_name/jobs/:name") view=JobDetailPage/>
                    <Route path=path!("/locations/:loc_ns/:loc_name/automation") view=AutomationPage/>
                    <Route
                        path=path!("/locations/:loc_ns/:loc_name/automation/schedules/:name")
                        view=ScheduleDetailPage
                    />
                    <Route
                        path=path!("/locations/:loc_ns/:loc_name/automation/sensors/:name")
                        view=SensorDetailPage
                    />
                    <Route path=path!("/locations/:loc_ns/:loc_name/backfills") view=BackfillsListPage/>
                    <Route path=path!("/locations/:loc_ns/:loc_name/backfills/:id") view=BackfillDetailPage/>
                    <Route path=path!("/locations/:loc_ns/:loc_name/pools") view=PoolsPage/>
                    <Route path=path!("/locations/:loc_ns/:loc_name/queue") view=QueuePage/>
                    <Route path=path!("/locations/:loc_ns/:loc_name/deployment") view=DeploymentPage/>
                </FlatRoutes>
            </Shell>
        </Router>
    }
}
