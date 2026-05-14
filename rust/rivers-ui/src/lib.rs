//! rivers-ui crate root.
//!
//! Re-exports the Leptos app, components, pages, and server functions that make
//! up the rivers web dashboard.

#![recursion_limit = "256"]
// Leptos view! macros and component signatures generate these patterns.
#![allow(
    clippy::redundant_closure,
    clippy::no_effect,
    clippy::unused_unit,
    clippy::let_unit_value,
    clippy::unit_arg,
    clippy::too_many_arguments
)]

pub mod app;
#[cfg(feature = "ssr")]
pub mod code_location_registry;
pub mod components;
#[cfg(feature = "ssr")]
pub mod favicon;
pub mod helpers;
#[cfg(feature = "ssr")]
pub mod live;
pub mod loc;
pub mod now;
pub mod pages;
pub mod server_fns;
pub mod state;
pub mod synthetic;
pub mod types;

/// WASM entry point — invoked from `pkg/rivers_ui.js` after the SSR HTML
/// loads. Installs the panic hook and tracing logger, then hands the body
/// over to Leptos for hydration.
#[cfg(feature = "hydrate")]
#[wasm_bindgen::prelude::wasm_bindgen]
pub fn hydrate() {
    // Route Rust panics to console.error with a full backtrace so "unreachable"
    // traps surface the actual message and file:line instead of a generic WASM
    // runtime error. Must run before anything else can panic.
    console_error_panic_hook::set_once();
    // Route `log::info!` / `log::error!` / `tracing` emits to the console with
    // proper levels — default filter is `Debug`, override via LOG env.
    let _ = wasm_logger::init(wasm_logger::Config::default());

    use crate::app::App;
    leptos::mount::hydrate_body(App);
}

#[cfg(feature = "ssr")]
mod server {
    use crate::app::{App, shell};
    use crate::live::{debug_live, events_sse, spawn_live_broadcasters};
    use crate::state::AppState;
    use axum::Router;
    use axum::extract::Query;
    use axum::http::{StatusCode, header};
    use axum::response::IntoResponse;
    use axum::routing::get;
    use leptos::config::LeptosOptions;
    use leptos_axum::{LeptosRoutes, generate_route_list};
    use rivers_core::assets::graph::GraphTopology;
    use rivers_core::storage::surrealdb_backend::SurrealStorage;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;
    use tower_http::compression::CompressionLayer;
    use tower_http::compression::predicate::{DefaultPredicate, NotForContentType, Predicate};

    const CSS: &str = concat!(
        include_str!("../style/tokens.css"),
        include_str!("../style/layout.css"),
        include_str!("../style/components.css"),
        include_str!("../style/run-detail.css"),
        include_str!("../style/dialogs.css"),
        include_str!("../style/detail-pages.css"),
        include_str!("../style/runs-list.css"),
        include_str!("../style/panels.css"),
        include_str!("../style/rivers-widgets.css"),
    );
    const WASM_JS: &str = include_str!("../pkg/rivers_ui.js");
    const WASM_BG: &[u8] = include_bytes!("../pkg/rivers_ui_bg.wasm");
    /// Brotli-precompressed `WASM_BG`, produced by `build.rs` at compile
    /// time. Serving it is a memcpy — no per-request CPU cost, no startup
    /// blocking, no tokio worker pinning. Only built for release profile;
    /// debug builds skip brotli entirely and serve raw wasm.
    #[cfg(precompressed_wasm)]
    const WASM_BG_BR: &[u8] = include_bytes!("../pkg/rivers_ui_bg.wasm.br");

    async fn serve_css() -> impl IntoResponse {
        (StatusCode::OK, [(header::CONTENT_TYPE, "text/css")], CSS)
    }

    async fn serve_favicon() -> impl IntoResponse {
        (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "image/svg+xml"),
                (header::CACHE_CONTROL, "no-cache"),
            ],
            crate::favicon::SVG,
        )
    }

    async fn serve_wasm_js() -> impl IntoResponse {
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/javascript")],
            WASM_JS,
        )
    }

    #[cfg(precompressed_wasm)]
    fn accepts_br(headers: &axum::http::HeaderMap) -> bool {
        let Some(value) = headers.get(header::ACCEPT_ENCODING) else {
            return false;
        };
        let Ok(s) = value.to_str() else {
            return false;
        };
        s.split(',').any(|enc| {
            let token = enc.split(';').next().unwrap_or("").trim();
            token.eq_ignore_ascii_case("br")
        })
    }

    async fn serve_wasm_bg(_headers: axum::http::HeaderMap) -> axum::response::Response {
        #[cfg(precompressed_wasm)]
        if accepts_br(&_headers) {
            return (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, "application/wasm"),
                    (header::CONTENT_ENCODING, "br"),
                    (header::VARY, "accept-encoding"),
                ],
                WASM_BG_BR,
            )
                .into_response();
        }
        (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "application/wasm"),
                (header::VARY, "accept-encoding"),
            ],
            WASM_BG,
        )
            .into_response()
    }

    async fn find_available_port(
        host: &str,
        start: u16,
        max: u16,
    ) -> anyhow::Result<tokio::net::TcpListener> {
        for p in start..=max {
            match tokio::net::TcpListener::bind(format!("{host}:{p}")).await {
                Ok(listener) => return Ok(listener),
                Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => continue,
                Err(e) => return Err(e.into()),
            }
        }
        anyhow::bail!("No available port in range {start}-{max}")
    }

    /// Start the SSR axum server on `host:port` (with port-fallback up to 3099),
    /// wiring `/style.css`, the embedded WASM blob, the SSE live-broadcaster,
    /// and the Leptos route table. Returns when `shutdown` fires.
    pub async fn start_server(
        storage: Arc<SurrealStorage>,
        graph: Option<Arc<GraphTopology>>,
        host: String,
        port: u16,
        registry: crate::code_location_registry::Registry,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        let state = AppState {
            storage,
            graph,
            registry,
        };

        let listener = find_available_port(&host, port, 3099).await?;
        let actual_addr = listener.local_addr()?;

        let leptos_options = LeptosOptions::builder()
            .output_name("rivers_ui")
            .site_root("site")
            .site_pkg_dir("pkg")
            .site_addr(actual_addr)
            .build();
        let routes = generate_route_list(App);

        let is_draining = shutdown.clone();
        let (live_tx, live_metrics) =
            spawn_live_broadcasters(state.storage.clone(), shutdown.clone());
        let debug_metrics = live_metrics.clone();
        let app = Router::new()
            .route("/healthz", get(|| async { StatusCode::OK }))
            .route(
                "/readyz",
                get(move || {
                    let draining = is_draining.clone();
                    async move {
                        if draining.is_cancelled() {
                            StatusCode::SERVICE_UNAVAILABLE
                        } else {
                            StatusCode::OK
                        }
                    }
                }),
            )
            .route(
                "/api/events",
                get({
                    let sse_shutdown = shutdown.clone();
                    move |query: Query<crate::live::EventsQuery>| {
                        let tx = live_tx.clone();
                        let shutdown = sse_shutdown.clone();
                        async move { events_sse(tx, shutdown, query).await }
                    }
                }),
            )
            .route(
                "/debug/live",
                get(move || {
                    let metrics = debug_metrics.clone();
                    async move { debug_live(metrics).await }
                }),
            )
            .route("/style.css", get(serve_css))
            // Browsers auto-prefetch /favicon.ico ignoring the link tag;
            // serve our SVG bytes there so they don't 404 (and so a stale
            // /favicon.ico cached from a prior tool on the same dev port
            // is replaced rather than reused).
            .route("/favicon.ico", get(serve_favicon))
            .route("/pkg/rivers_ui.js", get(serve_wasm_js))
            .route("/pkg/rivers_ui_bg.wasm", get(serve_wasm_bg))
            .leptos_routes_with_context(
                &leptos_options,
                routes,
                {
                    let state = state.clone();
                    move || {
                        leptos::prelude::provide_context(state.clone());
                    }
                },
                shell,
            )
            .fallback(|| async { (StatusCode::NOT_FOUND, "Not found") })
            .with_state(leptos_options)
            // application/wasm is large; brotli on every fetch pins the runtime
            // worker for multiple seconds (~2s for the 5 MB blob, even worse for
            // dev builds). Release builds precompress to .wasm.br in build.rs and
            // serve_wasm_bg returns that directly — runtime compression here would
            // re-do that work for nothing.
            .layer(CompressionLayer::new().br(true).gzip(true).compress_when(
                DefaultPredicate::new().and(NotForContentType::const_new("application/wasm")),
            ));

        tracing::info!(target: "rivers::ui", %actual_addr, "rivers UI started");
        axum::serve(listener, app.into_make_service())
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await?;
        tracing::info!(target: "rivers::ui", "UI server stopped");
        Ok(())
    }
}

#[cfg(feature = "ssr")]
pub use server::start_server;
