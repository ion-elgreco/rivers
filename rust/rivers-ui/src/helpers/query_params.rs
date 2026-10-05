use leptos::prelude::*;
use leptos_router::hooks::use_location;
use leptos_router::params::ParamsMap;

/// Shared buffer so multiple `use_query_param` setters called in the same
/// tick accumulate into a single navigation instead of clobbering each other.
#[derive(Clone, Copy)]
struct PendingQueryParams(RwSignal<Option<ParamsMap>>);

fn query_params_ctx() -> PendingQueryParams {
    if let Some(ctx) = use_context::<PendingQueryParams>() {
        return ctx;
    }
    let sig = RwSignal::new(None::<ParamsMap>);
    let ctx = PendingQueryParams(sig);
    provide_context(ctx);

    let location = use_location();
    // Effect fires once per tick — collapses multiple setter calls into one navigate
    Effect::new(move |_| {
        if let Some(params) = sig.get() {
            let qs = params.to_query_string();
            let path = location.pathname.get_untracked();
            let new_url = format!("{path}{qs}");
            let current_path = location.pathname.get_untracked();
            let current_qs = location.query.get_untracked().to_query_string();
            if format!("{current_path}{current_qs}") != new_url {
                let navigate = leptos_router::hooks::use_navigate();
                let _ = navigate(&new_url, Default::default());
            }
            sig.update_untracked(|v| *v = None);
        }
    });

    ctx
}

/// Read a query parameter as a String, defaulting to `default` if absent.
pub fn use_query_param(
    key: &'static str,
    default: &str,
) -> (Signal<String>, impl Fn(String) + Clone + 'static + use<>) {
    let location = use_location();
    let pending = query_params_ctx();
    let default = default.to_string();
    let default_for_read = default.clone();

    let value = Signal::derive(move || {
        // Read from pending buffer first (for same-tick consistency),
        // then fall back to the actual location query
        if let Some(ref params) = pending.0.get_untracked() {
            params.get(key).unwrap_or(default_for_read.clone())
        } else {
            location
                .query
                .read()
                .get(key)
                .unwrap_or(default_for_read.clone())
        }
    });

    let set_value = move |new_val: String| {
        // navigate() requires window() — skip during SSR
        if cfg!(not(target_arch = "wasm32")) {
            return;
        }
        let base = pending
            .0
            .get_untracked()
            .unwrap_or_else(|| location.query.get_untracked());
        let mut new_map = ParamsMap::new();
        for (k, v) in base.into_iter() {
            if k.as_ref() != key {
                new_map.insert(k, v);
            }
        }
        if !new_val.is_empty() && new_val != default {
            new_map.insert(key, new_val);
        }
        // Effect will navigate at end of tick.
        pending.0.set(Some(new_map));
    };

    (value, set_value)
}

/// Read a query parameter as a comma-separated list of strings.
pub fn use_query_param_list(
    key: &'static str,
) -> (Signal<Vec<String>>, impl Fn(Vec<String>) + Clone + 'static) {
    let (raw, set_raw) = use_query_param(key, "");

    let value = Signal::derive(move || {
        let s = raw.get();
        if s.is_empty() {
            Vec::new()
        } else {
            s.split(',').map(|s| s.to_string()).collect()
        }
    });

    let set_value = move |vals: Vec<String>| {
        set_raw(vals.join(","));
    };

    (value, set_value)
}
