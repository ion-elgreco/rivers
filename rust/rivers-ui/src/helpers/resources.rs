use leptos::prelude::*;

/// A resource's value, copied into a signal by an effect. Read this instead
/// of the resource where no `<Transition>` wraps the read (top bars, dialogs,
/// event handlers, memos). The server and the browser's first render both see
/// `None`, so hydration matches; the browser fills in the value right after.
pub fn resource_value<T>(resource: Resource<T>) -> ReadSignal<Option<T>>
where
    T: Clone + Send + Sync + 'static,
{
    let (value, set_value) = signal(None);
    Effect::new(move |_| set_value.set(resource.get()));
    value
}

/// Asset records keyed by asset_key from a page's `get_assets` resource,
/// plus whether the latest fetch failed. Keeps the last good map on `Err`
/// so a transient failure doesn't blank every consumer — the flag is what
/// surfaces the failure (the materialize dialog's staleness warning).
pub fn records_by_key(
    all_assets: Resource<Result<Vec<crate::types::AssetRecord>, ServerFnError>>,
) -> (
    Memo<std::collections::HashMap<String, crate::types::AssetRecord>>,
    Memo<bool>,
) {
    let latest = resource_value(all_assets);
    let records =
        Memo::new(
            move |prev: Option<&std::collections::HashMap<_, _>>| match latest.get() {
                Some(Ok(assets)) => assets
                    .into_iter()
                    .map(|a| (a.asset_key.clone(), a))
                    .collect(),
                _ => prev.cloned().unwrap_or_default(),
            },
        );
    let failed = Memo::new(move |_| matches!(latest.get(), Some(Err(_))));
    (records, failed)
}

/// Asset definitions by key from a page's `get_assets_info` value; empty
/// until it loads or when it failed.
pub fn definitions_by_key(
    assets_info: ReadSignal<Option<Result<Vec<crate::types::AssetDefinitionInfo>, ServerFnError>>>,
) -> Memo<std::collections::HashMap<String, crate::types::AssetDefinitionInfo>> {
    Memo::new(move |_| {
        assets_info.with(|info| match info {
            Some(Ok(infos)) => infos
                .iter()
                .map(|i| (i.asset_key.clone(), i.clone()))
                .collect(),
            _ => Default::default(),
        })
    })
}

/// The assets the code location defines, each with its storage record.
/// Storage keeps records the code location no longer defines and gets a new
/// asset's record only after the definitions change, so the definitions say
/// which assets exist. A defined asset without a record yet is `Missing`.
/// Without definitions (still loading, or the code location is down), the
/// records are all there is.
pub fn defined_assets(
    records: Vec<crate::types::AssetRecord>,
    definitions: Option<&[crate::types::AssetDefinitionInfo]>,
) -> Vec<crate::types::AssetRecord> {
    let Some(definitions) = definitions else {
        return records;
    };
    let mut by_key: std::collections::HashMap<String, crate::types::AssetRecord> = records
        .into_iter()
        .map(|r| (r.asset_key.clone(), r))
        .collect();

    definitions
        .iter()
        .map(|d| {
            by_key
                .remove(&d.asset_key)
                .unwrap_or_else(|| crate::types::AssetRecord {
                    asset_key: d.asset_key.clone(),
                    tags: d.tags.clone(),
                    kinds: d.kinds.clone(),
                    asset_group: d.group.clone(),
                    code_version: d.code_version.clone(),
                    last_event_id: None,
                    last_run_id: None,
                    last_timestamp: None,
                    last_data_version: None,
                    pool: Vec::new(),
                    stale_status: crate::types::StaleStatus::Missing,
                })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::fixtures::make_info;
    use crate::types::{AssetRecord, StaleStatus};

    fn record(key: &str, status: StaleStatus) -> AssetRecord {
        AssetRecord {
            asset_key: key.to_string(),
            tags: vec![],
            kinds: vec![],
            asset_group: None,
            code_version: None,
            last_event_id: None,
            last_run_id: None,
            last_timestamp: Some(1),
            last_data_version: None,
            pool: vec![],
            stale_status: status,
        }
    }

    fn keys(records: &[AssetRecord]) -> Vec<&str> {
        records.iter().map(|r| r.asset_key.as_str()).collect()
    }

    #[test]
    fn defined_assets_lists_the_definitions() {
        let records = vec![
            record("orders", StaleStatus::UpToDate),
            record("old_report", StaleStatus::Stale),
        ];
        let mut weekly = make_info("weekly_revenue", None);
        weekly.group = Some("sales".to_string());
        let definitions = vec![make_info("orders", None), weekly];

        let listed = defined_assets(records, Some(&definitions));

        assert_eq!(keys(&listed), ["orders", "weekly_revenue"]);
        assert_eq!(listed[0].stale_status, StaleStatus::UpToDate);
        assert_eq!(listed[0].last_timestamp, Some(1));
        assert_eq!(listed[1].stale_status, StaleStatus::Missing);
        assert_eq!(listed[1].asset_group.as_deref(), Some("sales"));
        assert_eq!(listed[1].last_timestamp, None);
    }

    #[test]
    fn defined_assets_without_definitions_keeps_the_records() {
        let records = vec![record("orders", StaleStatus::UpToDate)];

        assert_eq!(keys(&defined_assets(records, None)), ["orders"]);
    }
}
