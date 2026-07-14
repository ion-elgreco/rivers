//! Executor-neutral compute resources.
//!
//! [`Compute`] describes the CPU / memory / GPU a step needs as Kubernetes
//! quantity strings (`"4"`, `"500m"`, `"32Gi"`). Quantity arithmetic (scaling,
//! clamping, rendering to pod resources) lives in the executor adapters
//! (`rivers-k8s`), keeping this crate k8s-free.

use serde::{Deserialize, Serialize};

/// Compute resources for one step, executor-neutral. `None` on an axis means
/// "inherit the executor default" (see [`Compute::or_default`]).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Compute {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu: Option<String>,
}

impl Compute {
    /// Per-axis fill: `self` overrides, `default` supplies the `None` axes. This
    /// is the precedence rule for `@Asset(compute=…)` over the executor default —
    /// set only `memory` and CPU/GPU still come from the executor.
    pub fn or_default(&self, default: &Compute) -> Compute {
        Compute {
            cpu: self.cpu.clone().or_else(|| default.cpu.clone()),
            memory: self.memory.clone().or_else(|| default.memory.clone()),
            gpu: self.gpu.clone().or_else(|| default.gpu.clone()),
        }
    }

    /// True if every axis is unset.
    pub fn is_empty(&self) -> bool {
        self.cpu.is_none() && self.memory.is_none() && self.gpu.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn or_default_fills_per_axis() {
        let asset = Compute {
            memory: Some("32Gi".into()),
            ..Default::default()
        };
        let executor = Compute {
            cpu: Some("1".into()),
            memory: Some("512Mi".into()),
            gpu: None,
        };
        let resolved = asset.or_default(&executor);
        assert_eq!(resolved.cpu.as_deref(), Some("1")); // inherited
        assert_eq!(resolved.memory.as_deref(), Some("32Gi")); // overridden
        assert_eq!(resolved.gpu, None);
    }

    #[test]
    fn empty_when_all_axes_unset() {
        assert!(Compute::default().is_empty());
        assert!(
            !Compute {
                memory: Some("1Gi".into()),
                ..Default::default()
            }
            .is_empty()
        );
    }

    #[test]
    fn serde_round_trip_omits_none_axes() {
        let c = Compute {
            memory: Some("32Gi".into()),
            ..Default::default()
        };
        let json = serde_json::to_string(&c).unwrap();
        assert_eq!(json, r#"{"memory":"32Gi"}"#);
        let back: Compute = serde_json::from_str(&json).unwrap();
        assert_eq!(back, c);
    }
}
