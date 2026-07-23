//! Kubernetes-side quantity arithmetic for the executor-neutral [`Compute`].
//!
//! `rivers-core` carries the escalation *policy*; the scaling/clamping lives
//! here on `kube_quantity` (arbitrary-precision decimal, full k8s quantity
//! grammar, suffix-preserving), where `k8s-openapi` already is.

use kube_quantity::ParsedQuantity;
use rivers_core::execution::compute::Compute;
use rivers_core::execution::retry::{ComputeEscalation, FailureReason};
use rust_decimal::Decimal;

/// Multiply a Kubernetes quantity by `factor`, optionally clamped to `max`,
/// preserving the unit suffix (`"8Gi" * 2 -> "16Gi"`). Returns `None` if the
/// input, `max`, or `factor` can't be parsed — callers keep the current value.
pub fn scale_quantity(q: &str, factor: f64, max: Option<&str>) -> Option<String> {
    let factor = Decimal::try_from(factor).ok()?;
    let scaled = ParsedQuantity::try_from(q).ok()? * factor;
    let result = match max {
        Some(max) => {
            let cap = ParsedQuantity::try_from(max).ok()?;
            if scaled > cap { cap } else { scaled }
        }
        None => scaled,
    };
    Some(result.to_string())
}

/// Resources for the next attempt, given the current (resolved) [`Compute`] and
/// the classified reason. When the reason is in `esc.on`, memory is multiplied
/// by `factor` (clamped to `max_memory`) and CPU by `cpu_factor` if set. A
/// non-escalating reason, or an axis left unset, is returned unchanged.
pub fn escalate_compute(
    current: &Compute,
    esc: &ComputeEscalation,
    reason: FailureReason,
) -> Compute {
    if !esc.on.contains(&reason) {
        return current.clone();
    }
    let mut next = current.clone();
    if let Some(mem) = &current.memory
        && let Some(scaled) = scale_quantity(mem, esc.factor, Some(&esc.max_memory))
    {
        next.memory = Some(scaled);
    }
    if let (Some(cpu), Some(cpu_factor)) = (&current.cpu, esc.cpu_factor)
        && let Some(scaled) = scale_quantity(cpu, cpu_factor, esc.max_cpu.as_deref())
    {
        next.cpu = Some(scaled);
    }
    next
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compare two quantity strings semantically (16Gi == 16384Mi), so tests
    /// don't depend on `kube_quantity`'s exact output formatting.
    fn same(a: &str, b: &str) {
        assert_eq!(
            ParsedQuantity::try_from(a).unwrap(),
            ParsedQuantity::try_from(b).unwrap(),
            "{a} != {b}"
        );
    }

    fn oom(cpu_factor: Option<f64>, max_cpu: Option<&str>) -> ComputeEscalation {
        ComputeEscalation {
            factor: 2.0,
            max_memory: "64Gi".into(),
            cpu_factor,
            max_cpu: max_cpu.map(String::from),
            on: vec![FailureReason::OutOfMemory],
        }
    }

    #[test]
    fn scales_memory_preserving_unit() {
        same(&scale_quantity("8Gi", 2.0, None).unwrap(), "16Gi");
        same(&scale_quantity("512Mi", 2.0, None).unwrap(), "1Gi");
    }

    #[test]
    fn scales_by_fractional_factor() {
        // 1.5 is exact via rust_decimal — the whole reason we use kube_quantity.
        same(&scale_quantity("8Gi", 1.5, None).unwrap(), "12Gi");
        same(&scale_quantity("2", 2.5, None).unwrap(), "5");
    }

    #[test]
    fn clamps_at_max_even_across_units() {
        same(&scale_quantity("8Gi", 10.0, Some("64Gi")).unwrap(), "64Gi");
        same(&scale_quantity("512Mi", 100.0, Some("2Gi")).unwrap(), "2Gi");
    }

    #[test]
    fn unparsable_input_returns_none() {
        assert_eq!(scale_quantity("weird", 2.0, None), None);
    }

    #[test]
    fn escalation_grows_and_clamps_memory() {
        let esc = oom(None, None);
        let base = Compute {
            memory: Some("8Gi".into()),
            ..Default::default()
        };
        let a = escalate_compute(&base, &esc, FailureReason::OutOfMemory);
        same(a.memory.as_deref().unwrap(), "16Gi");
        let b = escalate_compute(&a, &esc, FailureReason::OutOfMemory);
        same(b.memory.as_deref().unwrap(), "32Gi");
        let c = escalate_compute(&b, &esc, FailureReason::OutOfMemory);
        same(c.memory.as_deref().unwrap(), "64Gi");
        // already at the ceiling: stays clamped
        let d = escalate_compute(&c, &esc, FailureReason::OutOfMemory);
        same(d.memory.as_deref().unwrap(), "64Gi");
    }

    #[test]
    fn escalation_skips_reasons_not_in_on() {
        let esc = oom(None, None);
        let base = Compute {
            memory: Some("8Gi".into()),
            ..Default::default()
        };
        let same_c = escalate_compute(&base, &esc, FailureReason::Error);
        same(same_c.memory.as_deref().unwrap(), "8Gi");
    }

    #[test]
    fn escalation_scales_cpu_when_configured() {
        let esc = oom(Some(2.0), Some("8"));
        let base = Compute {
            cpu: Some("2".into()),
            memory: Some("8Gi".into()),
            ..Default::default()
        };
        let a = escalate_compute(&base, &esc, FailureReason::OutOfMemory);
        same(a.cpu.as_deref().unwrap(), "4");
        same(a.memory.as_deref().unwrap(), "16Gi");
    }

    #[test]
    fn escalation_leaves_unset_memory_alone() {
        let esc = oom(None, None);
        let out = escalate_compute(&Compute::default(), &esc, FailureReason::OutOfMemory);
        assert_eq!(out.memory, None);
    }
}
