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
