//! Main application layout: sidebar navigation, top bar, and content area.

use leptos::prelude::*;
use leptos_router::hooks::use_location;

use crate::components::global_search::GlobalSearch;
use crate::components::location_switcher::LocationSwitcher;
use crate::loc::{loc_path, use_current_location};
use crate::server_fns::locations::list_code_locations;
use crate::server_fns::user::get_current_user;

fn nav_svg(path: &str) -> String {
    format!(
        r#"<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">{path}</svg>"#
    )
}

/// Rivers brand logo. Canonical SVG lives at `<repo>/assets/logo.svg`
/// (also served as the favicon and used by mkdocs); CSS-var fallbacks
/// inside the SVG mean it themes with the page when inlined and shows
/// the default palette when loaded standalone. `.sidebar-brand svg`
/// in `style/layout.css` sizes it.
const BRAND_LOGO_SVG: &str = include_str!("../../../../assets/logo.svg");

#[component]
fn NavLink(
    /// Section name appended to `/locations/:ns/:name/` (e.g. `"runs"`).
    /// Pass `""` for the location overview.
    section: &'static str,
    label: &'static str,
    icon_path: &'static str,
    collapsed: Signal<bool>,
    #[prop(optional)] exact: bool,
    #[prop(optional, into)] badge: Option<Signal<Option<usize>>>,
) -> impl IntoView {
    let location = use_location();
    let loc = use_current_location();
    // When no location is active (root path / pre-redirect), point at `/` so
    // clicks land on the redirect rather than producing a malformed
    // `/locations//assets` URL the router can't match.
    let href = move || {
        let (ns, name) = loc.get();
        if ns.is_empty() || name.is_empty() {
            "/".to_string()
        } else {
            loc_path(&ns, &name, section)
        }
    };
    let is_active = move || {
        let path = location.pathname.get();
        let h = href();
        if exact {
            path == h
        } else {
            path == h || path.starts_with(&format!("{h}/"))
        }
    };

    let svg = nav_svg(icon_path);

    view! {
        <a href=href class:active=is_active title=label>
            <span class="nav-icon" inner_html=svg></span>
            <span class="nav-label" class:nav-label--hidden=move || collapsed.get()>{label}</span>
            {move || badge.and_then(|b| b.get()).map(|n| view! {
                <span class="nav-badge">{n}</span>
            })}
        </a>
    }
}

// SVG icon paths (Lucide-style)
const ICON_OVERVIEW: &str = r#"<rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/>"#;
// Rivers Runs icon: three horizontal lines of decreasing length (viewBox 0 0 16 16 rescaled to 24x24)
const ICON_RUNS: &str = r#"<line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="12" x2="16" y2="12"/><line x1="3" y1="18" x2="12" y2="18"/>"#;
// Rivers Asset icon: 3D cube (hexagonal outline + inner fold)
const ICON_ASSETS: &str = r#"<path d="M3 8l9-5 9 5v8l-9 5-9-5z"/><path d="M3 8l9 5 9-5M12 13v8"/>"#;
const ICON_LINEAGE: &str = r#"<circle cx="5" cy="6" r="3"/><circle cx="19" cy="6" r="3"/><circle cx="12" cy="18" r="3"/><line x1="7.5" y1="7.5" x2="10.5" y2="16"/><line x1="16.5" y1="7.5" x2="13.5" y2="16"/>"#;
const ICON_JOBS: &str = r#"<polygon points="12 2 2 7 12 12 22 7 12 2"/><polyline points="2 17 12 22 22 17"/><polyline points="2 12 12 17 22 12"/>"#;
const ICON_AUTOMATION: &str =
    r#"<circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/>"#;
const ICON_BACKFILLS: &str =
    r#"<polyline points="1 4 1 10 7 10"/><path d="M3.51 15a9 9 0 1 0 2.13-9.36L1 10"/>"#;
// Rivers Pools icon: three tight stacked rounded rectangles (16x16 viewBox scaled to 24x24)
const ICON_POOLS: &str = r#"<rect x="3" y="4" width="18" height="5" rx="1"/><rect x="3" y="10" width="18" height="5" rx="1"/><rect x="3" y="16" width="18" height="5" rx="1"/>"#;
// Rivers Queue icon: three filled dots in a horizontal row
const ICON_QUEUE: &str = r#"<circle cx="6" cy="12" r="2" fill="currentColor" stroke="none"/><circle cx="12" cy="12" r="2" fill="currentColor" stroke="none"/><circle cx="18" cy="12" r="2" fill="currentColor" stroke="none"/>"#;
const ICON_DEPLOYMENT: &str = r#"<rect x="4" y="4" width="16" height="16" rx="2"/><line x1="4" y1="10" x2="20" y2="10"/><circle cx="8" cy="7" r="1"/><circle cx="12" cy="7" r="1"/>"#;

const ICON_COLLAPSE: &str = r#"<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="15 18 9 12 15 6"/></svg>"#;

/// Browser `encodeURIComponent` — encodes the query delimiters so a path with
/// a query survives as a single `rd` value. Only reachable on the client.
#[cfg(target_arch = "wasm32")]
mod js {
    use wasm_bindgen::prelude::wasm_bindgen;
    #[wasm_bindgen]
    extern "C" {
        #[wasm_bindgen(js_name = encodeURIComponent)]
        pub fn encode_uri_component(s: &str) -> String;
    }
}

#[cfg(target_arch = "wasm32")]
fn encode_rd(s: &str) -> String {
    js::encode_uri_component(s)
}
#[cfg(not(target_arch = "wasm32"))]
fn encode_rd(s: &str) -> String {
    s.to_string()
}

/// Signed-in user + sign-out link; renders nothing in auth mode `none`.
/// Refetches on SPA navigation; a 401 hard-redirects into the login flow.
#[component]
fn CurrentUserChip(collapsed: Signal<bool>) -> impl IntoView {
    let location = use_location();
    let user = Resource::new(move || location.pathname.get(), |_| get_current_user());

    Effect::new(move |_| {
        if let Some(Err(e)) = user.get() {
            if crate::helpers::is_unauthorized(&e) {
                let loc = window().location();
                // Preserve the full path + query so filters/pagination survive
                // the round-trip through login (encoded to stay one rd value).
                let path = loc.pathname().unwrap_or_else(|_| "/".into());
                let query = loc.search().unwrap_or_default();
                let rd = encode_rd(&format!("{path}{query}"));
                let _ = loc.assign(&format!("{}?rd={rd}", crate::routes::LOGIN));
            }
        }
    });

    view! {
        // `Transition`, not `Suspense`: keep the chip visible across SPA
        // navigations instead of blinking to the empty fallback each time.
        <Transition fallback=|| ()>
            {move || user.get().and_then(|res| res.ok().flatten()).map(|u| {
                let title = u.user.email.clone().unwrap_or_else(|| u.user.subject.clone());
                let display = u.display().to_string();
                view! {
                    <div class="user-chip" title=title>
                        <span class="user-chip-name" class:nav-label--hidden=move || collapsed.get()>
                            {display}
                        </span>
                        {u.logout_url.clone().map(|href| view! {
                            <a class="user-chip-logout" class:nav-label--hidden=move || collapsed.get() href=href rel="external">
                                "Sign out"
                            </a>
                        })}
                    </div>
                }
            })}
        </Transition>
    }
}

#[component]
pub fn Shell(children: Children) -> impl IntoView {
    let collapsed = RwSignal::new(false);
    let collapsed_signal = Signal::derive(move || collapsed.get());

    // Populated once per Shell mount. Registry state is long-lived on the
    // server side; a full `Watch` subscription is follow-up work.
    let locations = Resource::new(|| (), |_| list_code_locations());

    view! {
        <div class="layout">
            <nav class="sidebar" class:sidebar--collapsed=move || collapsed.get()>
                <div class="sidebar-brand">
                    <span inner_html=BRAND_LOGO_SVG></span>
                    <div class="brand-stack" class:nav-label--hidden=move || collapsed.get()>
                        <span class="brand-text">"rivers"</span>
                        <span class="brand-version">{format!("v{}", env!("CARGO_PKG_VERSION"))}</span>
                    </div>
                </div>

                <div class="locations-panel" class:locations-panel--collapsed=move || collapsed.get()>
                    <div class="locations-label" class:nav-label--hidden=move || collapsed.get()>
                        "CODE LOCATION"
                    </div>
                    <Transition fallback=move || view! { <div class="locations-empty" class:nav-label--hidden=move || collapsed.get()>"…"</div> }>
                        {move || locations.get().map(|result| match result {
                            Ok(entries) if entries.is_empty() => view! {
                                <div class="locations-empty" class:nav-label--hidden=move || collapsed.get()>
                                    "none"
                                </div>
                            }.into_any(),
                            Ok(entries) => view! {
                                <LocationSwitcher entries=entries collapsed=collapsed_signal/>
                            }.into_any(),
                            Err(_) => view! {
                                <div class="locations-empty locations-empty--error" class:nav-label--hidden=move || collapsed.get()>
                                    "error"
                                </div>
                            }.into_any(),
                        })}
                    </Transition>
                </div>

                <div class="sidebar-nav">
                    <NavLink section="" label="Overview" icon_path=ICON_OVERVIEW collapsed=collapsed_signal exact=true/>
                    <NavLink section="runs" label="Runs" icon_path=ICON_RUNS collapsed=collapsed_signal/>
                    <NavLink section="backfills" label="Backfills" icon_path=ICON_BACKFILLS collapsed=collapsed_signal/>
                    <NavLink section="assets" label="Assets" icon_path=ICON_ASSETS collapsed=collapsed_signal/>
                    <NavLink section="graph" label="Lineage" icon_path=ICON_LINEAGE collapsed=collapsed_signal/>
                    <NavLink section="jobs" label="Jobs" icon_path=ICON_JOBS collapsed=collapsed_signal/>
                    <NavLink section="automation" label="Automation" icon_path=ICON_AUTOMATION collapsed=collapsed_signal/>
                    <NavLink section="pools" label="Pools" icon_path=ICON_POOLS collapsed=collapsed_signal/>
                    <NavLink section="queue" label="Queue" icon_path=ICON_QUEUE collapsed=collapsed_signal/>
                    <NavLink section="deployment" label="Deployment" icon_path=ICON_DEPLOYMENT collapsed=collapsed_signal/>
                </div>

                <div class="sidebar-footer">
                    <CurrentUserChip collapsed=collapsed_signal/>
                    <button
                        class="sidebar-toggle"
                        on:click=move |_| collapsed.update(|c| *c = !*c)
                        title=move || if collapsed.get() { "Expand sidebar" } else { "Collapse sidebar" }
                    >
                        <span class="sidebar-toggle-icon" class:sidebar-toggle-icon--collapsed=move || collapsed.get() inner_html=ICON_COLLAPSE>
                        </span>
                    </button>
                    <div class="search-hint" class:nav-label--hidden=move || collapsed.get()>
                        "Press "<kbd>"Cmd+K"</kbd>" to search"
                    </div>
                </div>
            </nav>
            <main class="main-content">
                {children()}
            </main>
            <GlobalSearch/>
        </div>
    }
}
