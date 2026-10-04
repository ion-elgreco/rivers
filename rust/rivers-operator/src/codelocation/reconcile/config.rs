//! `codeLocation.workspace.*` and the registry-refresh interval: the
//! chart's settings, parsed once at startup.

use std::time::Duration;

use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use rivers_k8s::crd::code_location::CodeLocationSpec;
use rivers_k8s::workspace::{Prune, WorkspaceVolume};

use crate::codelocation::resources::{keep_config_map_name, workspace_pvc_name};

/// Default registry-refresh interval when the CR does not override it.
pub(super) const DEFAULT_REFRESH: Duration = Duration::from_secs(300);
/// Minimum acceptable refresh interval. Anything tighter could blow through
/// registry rate limits quickly.
pub(super) const MIN_REFRESH: Duration = Duration::from_secs(60);

/// `codeLocation.workspace.*` from the chart, via operator env.
#[derive(Clone, Debug)]
pub struct WorkspaceConfig {
    /// Shared RWX PVC per CL (true) vs per-pod emptyDir (false).
    pub shared_enabled: bool,
    pub storage_class: Option<String>,
    pub shared_size: Quantity,
    pub empty_dir_limit: Quantity,
    pub keep_revisions: u32,
    pub min_tree_age: Duration,
}

impl Default for WorkspaceConfig {
    fn default() -> Self {
        Self {
            shared_enabled: false,
            storage_class: None,
            shared_size: Quantity("20Gi".to_string()),
            empty_dir_limit: Quantity("2Gi".to_string()),
            keep_revisions: 3,
            min_tree_age: Duration::from_secs(3600),
        }
    }
}

impl WorkspaceConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_lookup(&|k| std::env::var(k).ok())
    }

    pub(super) fn from_lookup(get: &dyn Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        let mut cfg = Self::default();
        let env = |name: &str| get(name).filter(|v| !v.is_empty());
        if let Some(v) = env("RIVERS_WORKSPACE_SHARED_ENABLED") {
            cfg.shared_enabled = matches!(v.as_str(), "true" | "1");
        }
        cfg.storage_class = env("RIVERS_WORKSPACE_STORAGE_CLASS");
        let size = |var: &str, value: String| {
            if rivers_k8s::quantity::is_positive(&value) {
                Ok(Quantity(value))
            } else {
                Err(anyhow::anyhow!(
                    "{var}: expected a Kubernetes quantity more than zero, like 10Gi or 500M, \
                     got {value:?}"
                ))
            }
        };
        if let Some(v) = env("RIVERS_WORKSPACE_SHARED_SIZE") {
            cfg.shared_size = size(
                "RIVERS_WORKSPACE_SHARED_SIZE (codeLocation.workspace.shared.size)",
                v,
            )?;
        }
        if let Some(v) = env("RIVERS_WORKSPACE_EMPTYDIR_LIMIT") {
            cfg.empty_dir_limit = size(
                "RIVERS_WORKSPACE_EMPTYDIR_LIMIT (codeLocation.workspace.sizeLimit)",
                v,
            )?;
        }
        if let Some(v) = env("RIVERS_WORKSPACE_KEEP_REVISIONS") {
            cfg.keep_revisions = v.trim().parse().map_err(|_| {
                anyhow::anyhow!(
                    "RIVERS_WORKSPACE_KEEP_REVISIONS (codeLocation.workspace.keepRevisions): \
                     expected a whole number, got {v:?}"
                )
            })?;
        }
        if let Some(v) = env("RIVERS_WORKSPACE_MIN_AGE") {
            cfg.min_tree_age = humantime_lite::parse(&v).ok_or_else(|| {
                anyhow::anyhow!(
                    "RIVERS_WORKSPACE_MIN_AGE (codeLocation.workspace.minTreeAge): expected a \
                     whole number with s, m or h, like 90m or 24h, got {v:?}"
                )
            })?;
        }
        Ok(cfg)
    }

    /// Workspace volume for the pods of CodeLocation `cl_name` and of its
    /// runs: the CL's shared PVC, or an `emptyDir` capped at
    /// `spec.git.workspaceSize`, else at the chart's `sizeLimit`.
    pub fn volume(&self, cl_name: &str, cl_spec: &CodeLocationSpec) -> WorkspaceVolume {
        if self.shared_enabled {
            return WorkspaceVolume::SharedPvc {
                claim_name: workspace_pvc_name(cl_name),
            };
        }
        let size_limit = cl_spec
            .git
            .as_ref()
            .and_then(|git| git.workspace_size.clone())
            .unwrap_or_else(|| self.empty_dir_limit.clone());
        WorkspaceVolume::EmptyDir {
            size_limit: Some(size_limit),
        }
    }

    /// What the code-location pods of `cl_name` keep when they prune its
    /// shared PVC.
    pub fn prune(&self, cl_name: &str) -> Prune {
        Prune {
            keep_config_map: keep_config_map_name(cl_name),
            keep_revisions: self.keep_revisions,
            min_tree_age: self.min_tree_age,
        }
    }
}

pub(super) fn parse_refresh_interval(spec_value: Option<&str>) -> Duration {
    let Some(raw) = spec_value else {
        return DEFAULT_REFRESH;
    };
    let trimmed = raw.trim();
    if trimmed == "0" {
        return Duration::from_secs(3600 * 24);
    }
    match humantime_lite::parse(trimmed) {
        Some(d) if d >= MIN_REFRESH => d,
        Some(_) => MIN_REFRESH,
        None => DEFAULT_REFRESH,
    }
}

/// Minimal duration parser accepting `30s`, `5m`, `1h`; a bare number is
/// seconds.
mod humantime_lite {
    use std::time::Duration;

    pub fn parse(s: &str) -> Option<Duration> {
        let s = s.trim();
        if let Some(num) = s.strip_suffix('s') {
            return num.parse::<u64>().ok().map(Duration::from_secs);
        }
        if let Some(num) = s.strip_suffix('m') {
            return num
                .parse::<u64>()
                .ok()
                .and_then(|m| m.checked_mul(60))
                .map(Duration::from_secs);
        }
        if let Some(num) = s.strip_suffix('h') {
            return num
                .parse::<u64>()
                .ok()
                .and_then(|h| h.checked_mul(3600))
                .map(Duration::from_secs);
        }
        s.parse::<u64>().ok().map(Duration::from_secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codelocation::reconcile::tests::support::workspace_config;

    #[test]
    fn refresh_default() {
        assert_eq!(parse_refresh_interval(None), DEFAULT_REFRESH);
    }

    #[test]
    fn refresh_honors_minimum() {
        assert_eq!(parse_refresh_interval(Some("10s")), MIN_REFRESH);
        assert_eq!(parse_refresh_interval(Some("1m")), MIN_REFRESH);
    }

    #[test]
    fn refresh_parses_standard_units() {
        assert_eq!(parse_refresh_interval(Some("2m")), Duration::from_secs(120));
        assert_eq!(
            parse_refresh_interval(Some("1h")),
            Duration::from_secs(3600)
        );
        assert_eq!(
            parse_refresh_interval(Some("300s")),
            Duration::from_secs(300)
        );
    }

    #[test]
    fn workspace_config_refuses_floors_the_prune_cannot_read() {
        for (var, value) in [
            ("RIVERS_WORKSPACE_MIN_AGE", "1d"),
            ("RIVERS_WORKSPACE_MIN_AGE", "7d"),
            ("RIVERS_WORKSPACE_MIN_AGE", "2H"),
            ("RIVERS_WORKSPACE_MIN_AGE", "1h30m"),
            ("RIVERS_WORKSPACE_MIN_AGE", "1.5h"),
            ("RIVERS_WORKSPACE_MIN_AGE", "abc"),
            ("RIVERS_WORKSPACE_MIN_AGE", "-1h"),
            // Past u64::MAX seconds.
            ("RIVERS_WORKSPACE_MIN_AGE", "5124095576030432h"),
            ("RIVERS_WORKSPACE_KEEP_REVISIONS", "three"),
            ("RIVERS_WORKSPACE_KEEP_REVISIONS", "2.5"),
            ("RIVERS_WORKSPACE_KEEP_REVISIONS", "-1"),
        ] {
            let expected = if var == "RIVERS_WORKSPACE_MIN_AGE" {
                format!(
                    "RIVERS_WORKSPACE_MIN_AGE (codeLocation.workspace.minTreeAge): expected a \
                     whole number with s, m or h, like 90m or 24h, got {value:?}"
                )
            } else {
                format!(
                    "RIVERS_WORKSPACE_KEEP_REVISIONS (codeLocation.workspace.keepRevisions): \
                     expected a whole number, got {value:?}"
                )
            };
            match workspace_config(&[(var, value)]) {
                Ok(cfg) => panic!("{var}={value} accepted: {cfg:?}"),
                Err(e) => assert_eq!(e.to_string(), expected),
            }
        }
    }

    #[test]
    fn workspace_config_reads_the_chart_sizes() {
        let sizes = |vars: &[(&str, &str)]| {
            let cfg = workspace_config(vars).unwrap();
            (cfg.shared_size.0, cfg.empty_dir_limit.0)
        };
        assert_eq!(sizes(&[]), ("20Gi".into(), "2Gi".into()));
        assert_eq!(
            sizes(&[
                ("RIVERS_WORKSPACE_SHARED_SIZE", "50Gi"),
                ("RIVERS_WORKSPACE_EMPTYDIR_LIMIT", "1e9"),
            ]),
            ("50Gi".into(), "1e9".into())
        );
    }

    #[test]
    fn workspace_config_refuses_sizes_the_api_server_does_not_take() {
        for (var, setting) in [
            (
                "RIVERS_WORKSPACE_SHARED_SIZE",
                "codeLocation.workspace.shared.size",
            ),
            (
                "RIVERS_WORKSPACE_EMPTYDIR_LIMIT",
                "codeLocation.workspace.sizeLimit",
            ),
        ] {
            for value in ["5GB", "abc", "-1Gi", "0", "2 Gi", "1e"] {
                match workspace_config(&[(var, value)]) {
                    Ok(cfg) => panic!("{var}={value} accepted: {cfg:?}"),
                    Err(e) => assert_eq!(
                        e.to_string(),
                        format!(
                            "{var} ({setting}): expected a Kubernetes quantity more than zero, \
                             like 10Gi or 500M, got {value:?}"
                        )
                    ),
                }
            }
        }
    }
}
