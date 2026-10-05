use crate::types::AssetDefinitionInfo;

/// What the partition picker should render for a job's selection.
///
/// Single source of truth for the dialog: callers route on the variant
/// instead of reasoning about partition kinds + key shapes themselves.
/// Resolve-time validation already rejects user-defined jobs whose
/// partitioned assets have disjoint definitions, so for any UI-visible
/// job this is guaranteed to expose every key the dialog will show.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobPartitionPicker {
    /// No partitioned assets in the selection — submit with no key.
    None,
    /// Static / TimeWindow / etc. — flat list of keys, intersection
    /// across the job's partitioned assets in first-encounter order.
    /// `truncated` is set when any contributing key list was a bounded
    /// window, i.e. shared keys beyond the window exist but can't be shown.
    SingleDim { keys: Vec<String>, truncated: bool },
    /// Single-dim with more partitions than fit in the inline key window —
    /// paged on demand by `asset_key` as the user scrolls (`total` sizes the
    /// scrollbar). Avoids ever shipping the full key list to the browser.
    SingleDimPaged { asset_key: String, total: u64 },
    /// Storage-managed keys (not in the in-memory def), paged from storage by
    /// `dynamic_name`; `total` sizes the scrollbar. Single-dim-shaped.
    Dynamic { dynamic_name: String, total: u64 },
    /// Multi — per-dimension selectors; each dimension's `keys` is the
    /// intersection across the job's Multi assets, in the first asset's order.
    /// `asset_key` is `Some` only for a single-Multi-asset job, enabling
    /// per-dimension paging; `None` (several Multi assets) keeps the intersected
    /// windows inline — same cross-asset guard as `SingleDimPaged`.
    /// `truncated` is set when an inline dimension window was bounded, i.e.
    /// shared keys beyond the windows exist but can't be shown (paged
    /// dimensions are exempt — paging exposes every key on demand).
    Multi {
        dimensions: Vec<crate::types::PartitionDimensionInfo>,
        asset_key: Option<String>,
        truncated: bool,
    },
}

/// Compute the picker shape for a job's partitioned assets.
///
/// `SingleDim` keys = intersection of `partition_def.keys` across all
/// partitioned assets in encounter order, dropping unpartitioned assets.
/// `Multi` dimensions = per-dimension intersection across every
/// Multi-partitioned asset (resolve-time validator guarantees the
/// dimension name sets match). `Dynamic` when every partitioned asset shares one
/// Dynamic namespace. Mixed kinds are rejected at resolve time so the helper
/// picks whichever kind the partitioned assets share.
pub fn partition_picker_for_assets(
    assets: &[String],
    asset_info_by_key: &std::collections::HashMap<String, AssetDefinitionInfo>,
) -> JobPartitionPicker {
    let defs: Vec<&crate::types::PartitionDefinitionInfo> = assets
        .iter()
        .filter_map(|asset| {
            asset_info_by_key
                .get(asset)
                .and_then(|info| info.partition_def.as_ref())
        })
        .collect();
    if defs.is_empty() {
        return JobPartitionPicker::None;
    }
    let multi_count = defs.iter().filter(|d| !d.dimensions.is_empty()).count();
    if multi_count > 0 {
        // Multi: per-dimension intersection. Use the first def's
        // dimension order; later defs are guaranteed to carry the same
        // dimension names by the resolve-time validator.
        let first = defs[0];
        let mut dims: Vec<crate::types::PartitionDimensionInfo> = first.dimensions.clone();
        for other in defs.iter().skip(1) {
            // Dimension-name sets must match exactly — a key carrying a dim
            // the other asset lacks (or missing one it has) can never
            // validate for both, so there is nothing to offer. Jobs are
            // guarded at resolve time; ad-hoc selections reach here.
            let dims_match = other.dimensions.len() == dims.len()
                && dims
                    .iter()
                    .all(|d| other.dimensions.iter().any(|od| od.name == d.name));
            if !dims_match {
                return JobPartitionPicker::None;
            }
            for dim in dims.iter_mut() {
                let Some(other_dim) = other.dimensions.iter().find(|d| d.name == dim.name) else {
                    continue;
                };
                let next: std::collections::HashSet<&str> =
                    other_dim.keys.iter().map(String::as_str).collect();
                dim.keys.retain(|k| next.contains(k.as_str()));
            }
        }
        if dims.iter().all(|d| d.keys.is_empty()) {
            return JobPartitionPicker::None;
        }
        // Page dimensions only for a single Multi asset (no cross-asset
        // intersection to honor); else `asset_key` None → inline windows.
        let asset_key = (multi_count == 1)
            .then(|| {
                assets.iter().find(|a| {
                    asset_info_by_key
                        .get(*a)
                        .and_then(|i| i.partition_def.as_ref())
                        .is_some_and(|pd| !pd.dimensions.is_empty())
                })
            })
            .flatten()
            .cloned();
        // Intersecting bounded dimension windows can only see the shared keys
        // inside them — surface that so the dialog doesn't present the lists
        // as exhaustive. A dimension that pages (single Multi asset with a
        // bounded window) exposes every key on demand and doesn't count.
        let truncated = defs.iter().any(|d| {
            d.dimensions.iter().any(|dim| {
                let bounded = dim.keys_truncated || dim.total_count as usize > dim.keys.len();
                let paged = asset_key.is_some() && dim.keys_truncated;
                bounded && !paged
            })
        });
        return JobPartitionPicker::Multi {
            dimensions: dims,
            asset_key,
            truncated,
        };
    }
    // Only when every partitioned asset shares one Dynamic namespace — a mixed
    // job can't share a key (validator rejects it), so it falls to single-dim below.
    let dynamic_names: Vec<&str> = defs.iter().filter_map(|d| d.dynamic_namespace()).collect();
    if !dynamic_names.is_empty() {
        let first = dynamic_names[0];
        let all_dynamic_one_namespace =
            dynamic_names.len() == defs.len() && dynamic_names.iter().all(|n| *n == first);
        if all_dynamic_one_namespace {
            // Emit even at 0 so the dialog shows an empty state instead of the
            // button silently firing a keyless (rejected) materialize.
            let total = defs.iter().map(|d| d.total_count).max().unwrap_or(0);
            return JobPartitionPicker::Dynamic {
                dynamic_name: first.to_string(),
                total,
            };
        }
    }
    // Single-dim (Static / TimeWindow). If the driving asset has more
    // partitions than fit in the inline key window, page it on demand;
    // otherwise show the intersection across the job's single-dim assets.
    // Dynamic defs excluded here: `dynamic_namespace()` is `Some`, `keys` empty.
    let first_single = assets.iter().find_map(|a| {
        let pd = asset_info_by_key.get(a)?.partition_def.as_ref()?;
        (pd.dimensions.is_empty()
            && pd.dynamic_namespace().is_none()
            && (pd.total_count > 0 || !pd.keys.is_empty()))
        .then(|| (a.clone(), pd))
    });
    let Some((asset_key, first_def)) = first_single else {
        return JobPartitionPicker::None;
    };
    let needs_paging =
        first_def.keys_truncated || first_def.total_count as usize > first_def.keys.len();
    // Page only when every single-dim asset shares the same key space (same
    // total + window). The paged endpoint serves one asset's keys, so paging a
    // merely-overlapping job (the validator only requires a non-empty
    // intersection) could offer a key invalid for another; differing defs fall
    // through to the intersection path, which only shows shared keys.
    let same_key_space = defs.iter().all(|d| {
        d.dimensions.is_empty()
            && d.total_count == first_def.total_count
            && d.keys == first_def.keys
    });
    if needs_paging && same_key_space {
        return JobPartitionPicker::SingleDimPaged {
            asset_key,
            total: first_def.total_count,
        };
    }
    let contributing: Vec<&&crate::types::PartitionDefinitionInfo> =
        defs.iter().filter(|d| !d.keys.is_empty()).collect();
    let Some((first, rest)) = contributing.split_first() else {
        return JobPartitionPicker::None;
    };
    let mut intersection: Vec<String> = first.keys.to_vec();
    for d in rest {
        let next: std::collections::HashSet<&str> = d.keys.iter().map(String::as_str).collect();
        intersection.retain(|k| next.contains(k.as_str()));
    }
    // Intersecting bounded windows can only see the shared keys inside them
    // — surface that so the dialog doesn't present the list as exhaustive.
    let truncated = contributing
        .iter()
        .any(|d| d.keys_truncated || d.total_count as usize > d.keys.len());
    if intersection.is_empty() {
        JobPartitionPicker::None
    } else {
        JobPartitionPicker::SingleDim {
            keys: intersection,
            truncated,
        }
    }
}

/// Picker shape for launching a job: a whole-asset verb takes no key, so its
/// job gets no picker; everything else picks from the assets' partitions.
pub fn job_partition_picker(
    verb: Option<&crate::types::AssetActionInfo>,
    assets: &[String],
    asset_info_by_key: &std::collections::HashMap<String, AssetDefinitionInfo>,
) -> JobPartitionPicker {
    if verb.is_some_and(|v| v.is_keyless()) {
        return JobPartitionPicker::None;
    }
    partition_picker_for_assets(assets, asset_info_by_key)
}

/// Cartesian product of per-dimension selections — used by the Multi
/// dialog flow to expand the user's `{color: [r,g], size: [s]}` choices
/// into individual partition keys, one per concrete combination. Each
/// output carries `(dim_name, value)` pairs sorted alphabetically by
/// dimension name so the wire form is deterministic.
///
/// The server-side enumerator that produces the same shape lives in
/// `python/src/partitions/definition/mod.rs::cartesian_product`. Both must
/// stay aligned on dimension ordering since the partition key shows up
/// in display strings (`py_partition_key_display`) and equality checks
/// downstream.
pub fn cartesian_partition_keys(
    selections: &[(String, Vec<String>)],
) -> Vec<crate::types::SubmitPartitionKey> {
    if selections.is_empty() || selections.iter().any(|(_, vs)| vs.is_empty()) {
        return Vec::new();
    }
    let mut sorted = selections.to_vec();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    let mut out: Vec<Vec<(String, String)>> = vec![vec![]];
    for (dim, vals) in &sorted {
        let mut next: Vec<Vec<(String, String)>> = Vec::with_capacity(out.len() * vals.len());
        for combo in &out {
            for val in vals {
                let mut extended = combo.clone();
                extended.push((dim.clone(), val.clone()));
                next.push(extended);
            }
        }
        out = next;
    }
    out.into_iter()
        .map(crate::types::SubmitPartitionKey::Multi)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::helpers::fixtures::{make_info, make_map, make_multi, partitioned_with};
    use crate::helpers::job_verb;
    use crate::types::PartitionDefinitionInfo;

    #[test]
    fn a_whole_asset_verb_job_gets_no_partition_picker() {
        let infos = make_map(vec![partitioned_with(
            "events",
            "vacuum",
            "unchanged",
            "keyless",
        )]);
        let assets = vec!["events".to_string()];
        let vacuum = job_verb(Some("vacuum"), &assets, &infos);
        assert!(matches!(
            job_partition_picker(vacuum.as_ref(), &assets, &infos),
            JobPartitionPicker::None
        ));
        // A materialize job over the same asset still picks keys.
        assert!(!matches!(
            job_partition_picker(None, &assets, &infos),
            JobPartitionPicker::None
        ));
    }

    fn picker(assets: &[&str], infos: &HashMap<String, AssetDefinitionInfo>) -> JobPartitionPicker {
        let assets_owned: Vec<String> = assets.iter().map(|s| s.to_string()).collect();
        partition_picker_for_assets(&assets_owned, infos)
    }

    #[test]
    fn picker_none_when_no_partitioned_assets() {
        let infos = make_map(vec![make_info("a", None), make_info("b", None)]);
        assert_eq!(picker(&["a", "b"], &infos), JobPartitionPicker::None);
    }

    #[test]
    fn picker_singledim_intersects_overlapping_keys() {
        // Common key is "y"; "x" only in a, "z" only in b — neither
        // belongs in the intersection.
        let infos = make_map(vec![
            make_info("a", Some(&["x", "y"])),
            make_info("b", Some(&["y", "z"])),
        ]);
        assert_eq!(
            picker(&["a", "b"], &infos),
            JobPartitionPicker::SingleDim {
                keys: vec!["y".into()],
                truncated: false,
            }
        );
    }

    #[test]
    fn picker_none_when_assets_have_no_overlap() {
        // Defended against by the resolve-time validator, but pin the
        // helper's collapse-to-None behaviour.
        let infos = make_map(vec![
            make_info("a", Some(&["x"])),
            make_info("b", Some(&["y"])),
        ]);
        assert_eq!(picker(&["a", "b"], &infos), JobPartitionPicker::None);
    }

    #[test]
    fn picker_preserves_first_asset_order() {
        // Iteration order follows the first partitioned asset's keys.
        let infos = make_map(vec![
            make_info("first", Some(&["b", "a"])),
            make_info("second", Some(&["a", "b"])),
        ]);
        assert_eq!(
            picker(&["first", "second"], &infos),
            JobPartitionPicker::SingleDim {
                keys: vec!["b".into(), "a".into()],
                truncated: false,
            }
        );
    }

    #[test]
    fn picker_skips_assets_missing_from_map() {
        // The jobs list page may render before `assets_info` resolves;
        // the helper must yield the known asset's keys instead of
        // collapsing to None.
        let infos = make_map(vec![make_info("known", Some(&["x", "y"]))]);
        assert_eq!(
            picker(&["known", "missing"], &infos),
            JobPartitionPicker::SingleDim {
                keys: vec!["x".into(), "y".into()],
                truncated: false,
            }
        );
    }

    #[test]
    fn picker_skips_unpartitioned_assets() {
        // Mixed partitioned + unpartitioned: unpartitioned doesn't
        // constrain the intersection.
        let infos = make_map(vec![
            make_info("part", Some(&["x", "y"])),
            make_info("plain", None),
        ]);
        assert_eq!(
            picker(&["part", "plain"], &infos),
            JobPartitionPicker::SingleDim {
                keys: vec!["x".into(), "y".into()],
                truncated: false,
            }
        );
    }

    /// A Dynamic asset: empty `keys`/`dimensions`, storage-sourced `total_count`.
    fn make_dynamic(asset_key: &str, namespace: &str, total: u64) -> AssetDefinitionInfo {
        let mut info = make_info(asset_key, None);
        info.partition_def = Some(PartitionDefinitionInfo {
            kind: "Dynamic".to_string(),
            keys: vec![],
            dimensions: vec![],
            total_count: total,
            keys_truncated: false,
            dynamic_name: namespace.to_string(),
        });
        info
    }

    #[test]
    fn picker_dynamic_single_asset_pages_from_namespace() {
        let infos = make_map(vec![make_dynamic("dyn", "customers", 42)]);
        assert_eq!(
            picker(&["dyn"], &infos),
            JobPartitionPicker::Dynamic {
                dynamic_name: "customers".into(),
                total: 42,
            }
        );
    }

    #[test]
    fn picker_dynamic_zero_total_still_emits_dynamic() {
        // Zero keys → still emit (empty state), not None.
        let infos = make_map(vec![make_dynamic("dyn", "customers", 0)]);
        assert_eq!(
            picker(&["dyn"], &infos),
            JobPartitionPicker::Dynamic {
                dynamic_name: "customers".into(),
                total: 0,
            }
        );
    }

    #[test]
    fn picker_dynamic_multiple_assets_same_namespace() {
        // Two assets backed by the same Dynamic definition share one key space.
        let infos = make_map(vec![
            make_dynamic("a", "customers", 42),
            make_dynamic("b", "customers", 42),
        ]);
        assert_eq!(
            picker(&["a", "b"], &infos),
            JobPartitionPicker::Dynamic {
                dynamic_name: "customers".into(),
                total: 42,
            }
        );
    }

    #[test]
    fn picker_dynamic_mixed_with_static_falls_through_to_singledim() {
        // Mixed kinds can't share a key; helper skips Dynamic, uses the static keys.
        let infos = make_map(vec![
            make_dynamic("dyn", "customers", 42),
            make_info("stat", Some(&["x", "y"])),
        ]);
        assert_eq!(
            picker(&["dyn", "stat"], &infos),
            JobPartitionPicker::SingleDim {
                keys: vec!["x".into(), "y".into()],
                truncated: false,
            }
        );
    }

    #[test]
    fn picker_dynamic_distinct_namespaces_do_not_merge() {
        // Distinct namespaces share no keys → None (don't page one for both).
        let infos = make_map(vec![
            make_dynamic("a", "customers", 42),
            make_dynamic("b", "orders", 7),
        ]);
        assert_eq!(picker(&["a", "b"], &infos), JobPartitionPicker::None);
    }

    #[test]
    fn picker_none_for_empty_asset_selection() {
        let infos = make_map(vec![make_info("a", Some(&["x"]))]);
        assert_eq!(picker(&[], &infos), JobPartitionPicker::None);
    }

    #[test]
    fn picker_multi_returns_per_dimension_keys() {
        let infos = make_map(vec![make_multi(
            "m",
            &[("color", &["r", "g"]), ("size", &["s", "m"])],
        )]);
        let picker = picker(&["m"], &infos);
        let JobPartitionPicker::Multi { dimensions, .. } = picker else {
            panic!("expected Multi, got {picker:?}");
        };
        assert_eq!(dimensions.len(), 2);
        assert_eq!(dimensions[0].name, "color");
        assert_eq!(dimensions[0].keys, vec!["r", "g"]);
        assert_eq!(dimensions[1].name, "size");
        assert_eq!(dimensions[1].keys, vec!["s", "m"]);
    }

    #[test]
    fn picker_multi_intersects_per_dimension_across_assets() {
        // Two Multi-partitioned assets in the same job — per-dim
        // intersection follows the first asset's order.
        let infos = make_map(vec![
            make_multi("a", &[("color", &["r", "g", "b"]), ("size", &["s", "m"])]),
            make_multi("b", &[("color", &["g", "b", "y"]), ("size", &["m", "l"])]),
        ]);
        let picker = picker(&["a", "b"], &infos);
        let JobPartitionPicker::Multi { dimensions, .. } = picker else {
            panic!("expected Multi");
        };
        assert_eq!(dimensions[0].keys, vec!["g", "b"]);
        assert_eq!(dimensions[1].keys, vec!["m"]);
    }

    #[test]
    fn picker_multi_inline_windows_surface_truncation() {
        // Several Multi assets — windows stay inline (no paging), so a
        // bounded dimension window on either asset must flag the picker:
        // shared keys beyond the windows exist but can't be shown.
        let mut a = make_multi("a", &[("color", &["r", "g"]), ("size", &["s", "m"])]);
        if let Some(pd) = a.partition_def.as_mut() {
            pd.dimensions[1].keys_truncated = true;
            pd.dimensions[1].total_count = 5000;
        }
        let b = make_multi("b", &[("color", &["r", "g"]), ("size", &["s", "m"])]);
        let infos = make_map(vec![a, b]);
        let picker = picker(&["a", "b"], &infos);
        let JobPartitionPicker::Multi {
            truncated,
            asset_key,
            ..
        } = picker
        else {
            panic!("expected Multi, got {picker:?}");
        };
        assert_eq!(asset_key, None);
        assert!(truncated);
    }

    #[test]
    fn picker_multi_paged_dimension_is_not_flagged_truncated() {
        // Single Multi asset — a bounded dimension pages on demand instead,
        // so the picker must not warn about hidden keys.
        let mut m = make_multi("m", &[("color", &["r", "g"]), ("size", &["s", "m"])]);
        if let Some(pd) = m.partition_def.as_mut() {
            pd.dimensions[1].keys_truncated = true;
            pd.dimensions[1].total_count = 5000;
        }
        let infos = make_map(vec![m]);
        let picker = picker(&["m"], &infos);
        let JobPartitionPicker::Multi {
            truncated,
            asset_key,
            ..
        } = picker
        else {
            panic!("expected Multi, got {picker:?}");
        };
        assert_eq!(asset_key.as_deref(), Some("m"));
        assert!(!truncated);
    }

    #[test]
    fn picker_multi_unbounded_windows_not_truncated() {
        let infos = make_map(vec![
            make_multi("a", &[("color", &["r", "g"]), ("size", &["s", "m"])]),
            make_multi("b", &[("color", &["r", "g"]), ("size", &["s", "m"])]),
        ]);
        let picker = picker(&["a", "b"], &infos);
        let JobPartitionPicker::Multi { truncated, .. } = picker else {
            panic!("expected Multi, got {picker:?}");
        };
        assert!(!truncated);
    }

    #[test]
    fn picker_multi_collapses_to_none_when_every_dim_disjoint() {
        // Pathological — resolve-time check should reject this, but pin
        // the helper's behaviour.
        let infos = make_map(vec![
            make_multi("a", &[("color", &["r"]), ("size", &["s"])]),
            make_multi("b", &[("color", &["g"]), ("size", &["m"])]),
        ]);
        assert_eq!(picker(&["a", "b"], &infos), JobPartitionPicker::None);
    }

    /// A single-dim asset with more partitions than fit in the key window — the
    /// `keys` field is a truncated window, `total_count` the true size.
    fn make_paged(asset_key: &str, window: &[&str], total: u64) -> AssetDefinitionInfo {
        let mut info = make_info(asset_key, Some(window));
        if let Some(pd) = info.partition_def.as_mut() {
            pd.total_count = total;
            pd.keys_truncated = true;
        }
        info
    }

    #[test]
    fn picker_single_large_asset_pages() {
        let infos = make_map(vec![make_paged("big", &["k0", "k1", "k2"], 10_000)]);
        assert_eq!(
            picker(&["big"], &infos),
            JobPartitionPicker::SingleDimPaged {
                asset_key: "big".into(),
                total: 10_000,
            }
        );
    }

    #[test]
    fn picker_identical_large_assets_page() {
        // Same key space (same total + same window) → safe to page one asset.
        let infos = make_map(vec![
            make_paged("a", &["k0", "k1", "k2"], 10_000),
            make_paged("b", &["k0", "k1", "k2"], 10_000),
        ]);
        assert_eq!(
            picker(&["a", "b"], &infos),
            JobPartitionPicker::SingleDimPaged {
                asset_key: "a".into(),
                total: 10_000,
            }
        );
    }

    #[test]
    fn picker_divergent_large_assets_do_not_page() {
        // Different key spaces must NOT page one asset's keys (could offer a key
        // invalid for the other). Fall back to intersecting the visible windows
        // — and say so: shared keys beyond the windows exist but can't be shown.
        let infos = make_map(vec![
            make_paged("a", &["k0", "k1", "k2"], 10_000),
            make_paged("b", &["k1", "k2", "k3"], 12_000),
        ]);
        assert_eq!(
            picker(&["a", "b"], &infos),
            JobPartitionPicker::SingleDim {
                keys: vec!["k1".into(), "k2".into()],
                truncated: true,
            }
        );
    }

    #[test]
    fn picker_mismatched_multi_dims_returns_none() {
        // a has {region,date}, b has {region} only: a key with `date` is
        // invalid for b, a key without it is invalid for a — no shared key
        // exists, so the picker must offer nothing instead of a's full
        // `date` keyset.
        let infos = make_map(vec![
            make_multi("a", &[("region", &["us", "eu"]), ("date", &["d1", "d2"])]),
            make_multi("b", &[("region", &["us", "eu"])]),
        ]);
        assert_eq!(picker(&["a", "b"], &infos), JobPartitionPicker::None);
    }

    use crate::types::SubmitPartitionKey;

    fn multi(dims: &[(&str, &str)]) -> SubmitPartitionKey {
        SubmitPartitionKey::Multi(
            dims.iter()
                .map(|(d, v)| (d.to_string(), v.to_string()))
                .collect(),
        )
    }

    #[test]
    fn cartesian_single_dim_single_value() {
        let out = cartesian_partition_keys(&[("color".into(), vec!["r".into()])]);
        assert_eq!(out, vec![multi(&[("color", "r")])]);
    }

    #[test]
    fn cartesian_two_dims_two_values_each_produces_four_combinations() {
        let out = cartesian_partition_keys(&[
            ("color".into(), vec!["r".into(), "g".into()]),
            ("size".into(), vec!["s".into(), "m".into()]),
        ]);
        // Dims are sorted alphabetically; values iterate in order.
        assert_eq!(
            out,
            vec![
                multi(&[("color", "r"), ("size", "s")]),
                multi(&[("color", "r"), ("size", "m")]),
                multi(&[("color", "g"), ("size", "s")]),
                multi(&[("color", "g"), ("size", "m")]),
            ]
        );
    }

    #[test]
    fn cartesian_empty_when_any_dim_has_no_selection() {
        // User must pick at least one value per dimension; the helper
        // returns nothing if any dim is empty.
        let out = cartesian_partition_keys(&[
            ("color".into(), vec!["r".into()]),
            ("size".into(), vec![]),
        ]);
        assert!(out.is_empty());
    }

    #[test]
    fn cartesian_empty_when_no_dims_supplied() {
        assert!(cartesian_partition_keys(&[]).is_empty());
    }
}
