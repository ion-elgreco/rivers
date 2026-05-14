//! Prometheus metrics for the operator.
//!
//! Metrics are created lazily on first use and registered against the default
//! registry. [`scrape`] renders the current snapshot in the text-exposition
//! format consumed by Prometheus.

use once_cell::sync::Lazy;
use prometheus::{
    CounterVec, Encoder, Gauge, Histogram, HistogramOpts, IntCounter, Opts, Registry, TextEncoder,
};

pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

pub static REGISTRY_REQUESTS: Lazy<CounterVec> = Lazy::new(|| {
    let counter = CounterVec::new(
        Opts::new(
            "rivers_registry_request_total",
            "Total image-registry requests made by the operator.",
        ),
        &["registry", "outcome"],
    )
    .expect("valid metric opts");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("registry_request_total registers");
    counter
});

pub static CACHE_HITS: Lazy<IntCounter> = Lazy::new(|| {
    let counter = IntCounter::new(
        "rivers_digest_cache_hits_total",
        "Digest lookups served from the in-process cache without calling the registry.",
    )
    .expect("valid metric opts");
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cache_hits_total registers");
    counter
});

pub static RESOLUTION_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    let h = Histogram::with_opts(HistogramOpts::new(
        "rivers_codelocation_digest_resolution_seconds",
        "Wall time to resolve a CodeLocation tag to a digest, including cache hits.",
    ))
    .expect("valid metric opts");
    REGISTRY
        .register(Box::new(h.clone()))
        .expect("resolution_seconds registers");
    h
});

pub static LEADER: Lazy<Gauge> = Lazy::new(|| {
    let g = Gauge::new(
        "rivers_operator_leader",
        "1 if this replica currently holds the reconciler leader lease, 0 otherwise.",
    )
    .expect("valid metric opts");
    REGISTRY
        .register(Box::new(g.clone()))
        .expect("leader registers");
    g
});

pub fn registry_request(registry: &str, outcome: &str) {
    REGISTRY_REQUESTS
        .with_label_values(&[registry, outcome])
        .inc();
}

pub fn cache_hit() {
    CACHE_HITS.inc();
}

pub fn observe_resolution_seconds(seconds: f64) {
    RESOLUTION_SECONDS.observe(seconds);
}

/// Intended for the leader-loop's transition handler; callers outside
/// [`crate::leader`] should not invoke this directly.
pub fn set_leader(is_leader: bool) {
    LEADER.set(if is_leader { 1.0 } else { 0.0 });
}

pub fn scrape() -> String {
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();
    if encoder.encode(&REGISTRY.gather(), &mut buffer).is_err() {
        return String::new();
    }
    String::from_utf8(buffer).unwrap_or_default()
}
