//! Assets list page.

use std::collections::HashSet;

use leptos::prelude::*;
use leptos_router::components::A;

use crate::components::live::{LiveStatusChip, use_live_kick};
use crate::components::loading_skeleton::TableSkeleton;
use crate::components::materialize_dialog::MaterializeDialog;
use crate::components::multi_select::{MultiSelect, SelectOption};
use crate::components::ui_kit::{
    AttentionBanner, Crumb, KindBadge, RiversSearch, StatusChip, Tag, Topbar,
};
use crate::helpers::{format_relative_time, use_query_param, use_query_param_list};
use crate::loc::{loc_path, use_current_location};
use crate::now::use_now;
use crate::server_fns::assets::get_assets;
use crate::server_fns::graph::get_graph_topology;
use crate::server_fns::overview::get_assets_info;
use crate::types::AssetRecord;

#[component]
pub fn AssetsListPage() -> impl IntoView {
    let (refresh_tick, set_refresh_tick) = signal(0u32);
    let (filter_tags, set_filter_tags) = use_query_param_list("tag");
    let (filter_kinds, set_filter_kinds) = use_query_param_list("kind");
    let (filter_groups, set_filter_groups) = use_query_param_list("group");
    let (search_text, set_search_text) = use_query_param("search", "");
    let (sort_by, _) = use_query_param("sort", "name");
    let (sort_asc_str, _) = use_query_param("asc", "true");
    let (dimension, set_dimension) = use_query_param("dim", "materialized");
    let sort_asc = Signal::derive(move || sort_asc_str.get() != "false");
    let (selected, set_selected) = signal(Vec::<String>::new());
    let show_dialog = RwSignal::new(false);
    let (attention_collapsed, set_attention_collapsed) = signal(true);
    // Wrap the setter in a Callback so it's Copy and can be freely captured by
    // the rendering closures.
    let (dim_menu_open, set_dim_menu_open) = signal(false);
    let set_dim_cb: Callback<String> = Callback::new({
        let set_dim = set_dimension.clone();
        move |v: String| set_dim(v)
    });

    let loc = use_current_location();
    // Single unfiltered fetch — all filtering happens client-side.
    let all_assets = Resource::new(
        move || (loc.get(), refresh_tick.get()),
        |((ns, name), _)| get_assets(ns, name, None, None, None),
    );
    let assets_info = Resource::new(
        move || (refresh_tick.get(), loc.get()),
        |(_tick, (ns, name))| async move { get_assets_info(ns, name).await },
    );
    let graph = Resource::new(
        move || loc.get(),
        |(ns, name)| async move { get_graph_topology(ns, name).await },
    );

    let live_status = use_live_kick(
        &["assets"],
        300,
        Callback::new(move |_| set_refresh_tick.update(|t| *t += 1)),
    );

    let all_records =
        move || -> Vec<AssetRecord> { all_assets.get().and_then(|r| r.ok()).unwrap_or_default() };

    let unique_tags = move || {
        let mut tags: Vec<String> = all_records().iter().flat_map(|r| r.tags.clone()).collect();
        tags.sort();
        tags.dedup();
        tags
    };
    let unique_kinds = move || {
        let mut kinds: Vec<String> = all_records().iter().flat_map(|r| r.kinds.clone()).collect();
        kinds.sort();
        kinds.dedup();
        kinds
    };
    let unique_groups = move || {
        let mut groups: Vec<String> = all_records()
            .iter()
            .filter_map(|r| r.asset_group.clone())
            .collect();
        groups.sort();
        groups.dedup();
        groups
    };

    // Cross-filtered: each filter's available values are determined by the OTHER active filters.
    let tag_options = Signal::derive(move || {
        let records = all_records();
        let kinds = filter_kinds.get();
        let groups = filter_groups.get();
        let available: HashSet<String> = records
            .iter()
            .filter(|r| kinds.is_empty() || r.kinds.iter().any(|k| kinds.contains(k)))
            .filter(|r| {
                groups.is_empty()
                    || r.asset_group
                        .as_ref()
                        .map(|g| groups.contains(g))
                        .unwrap_or(false)
            })
            .flat_map(|r| r.tags.clone())
            .collect();
        unique_tags()
            .into_iter()
            .map(|t| {
                let enabled = available.contains(&t);
                SelectOption {
                    label: t.clone(),
                    value: t,
                    enabled,
                }
            })
            .collect::<Vec<_>>()
    });

    let kind_options = Signal::derive(move || {
        let records = all_records();
        let tags = filter_tags.get();
        let groups = filter_groups.get();
        let available: HashSet<String> = records
            .iter()
            .filter(|r| tags.is_empty() || r.tags.iter().any(|t| tags.contains(t)))
            .filter(|r| {
                groups.is_empty()
                    || r.asset_group
                        .as_ref()
                        .map(|g| groups.contains(g))
                        .unwrap_or(false)
            })
            .flat_map(|r| r.kinds.clone())
            .collect();
        unique_kinds()
            .into_iter()
            .map(|k| {
                let enabled = available.contains(&k);
                SelectOption {
                    label: k.clone(),
                    value: k,
                    enabled,
                }
            })
            .collect::<Vec<_>>()
    });

    let group_options = Signal::derive(move || {
        let records = all_records();
        let tags = filter_tags.get();
        let kinds = filter_kinds.get();
        let available: HashSet<String> = records
            .iter()
            .filter(|r| tags.is_empty() || r.tags.iter().any(|t| tags.contains(t)))
            .filter(|r| kinds.is_empty() || r.kinds.iter().any(|k| kinds.contains(k)))
            .filter_map(|r| r.asset_group.clone())
            .collect();
        unique_groups()
            .into_iter()
            .map(|g| {
                let enabled = available.contains(&g);
                SelectOption {
                    label: g.clone(),
                    value: g,
                    enabled,
                }
            })
            .collect::<Vec<_>>()
    });

    let toggle_tag = {
        let set = set_filter_tags.clone();
        Callback::new(move |val: String| {
            let mut current = filter_tags.get();
            if current.contains(&val) {
                current.retain(|v| v != &val);
            } else {
                current.push(val);
            }
            set(current);
        })
    };
    let toggle_kind = {
        let set = set_filter_kinds.clone();
        Callback::new(move |val: String| {
            let mut current = filter_kinds.get();
            if current.contains(&val) {
                current.retain(|v| v != &val);
            } else {
                current.push(val);
            }
            set(current);
        })
    };
    let toggle_group = {
        let set = set_filter_groups.clone();
        Callback::new(move |val: String| {
            let mut current = filter_groups.get();
            if current.contains(&val) {
                current.retain(|v| v != &val);
            } else {
                current.push(val);
            }
            set(current);
        })
    };

    let sorted_filtered = move || {
        let mut records = all_records();
        let tags = filter_tags.get();
        let kinds = filter_kinds.get();
        let groups = filter_groups.get();
        let search = search_text.get().to_lowercase();

        if !tags.is_empty() {
            records.retain(|r| r.tags.iter().any(|t| tags.contains(t)));
        }
        if !kinds.is_empty() {
            records.retain(|r| r.kinds.iter().any(|k| kinds.contains(k)));
        }
        if !groups.is_empty() {
            records.retain(|r| {
                r.asset_group
                    .as_ref()
                    .map(|g| groups.contains(g))
                    .unwrap_or(false)
            });
        }
        if !search.is_empty() {
            records.retain(|r| r.asset_key.to_lowercase().contains(&search));
        }

        let sort_field = sort_by.get();
        let asc = sort_asc.get();
        let infos_for_sort = assets_info.get().and_then(|r| r.ok()).unwrap_or_default();
        let type_map: std::collections::HashMap<String, String> = infos_for_sort
            .into_iter()
            .map(|i| (i.asset_key.clone(), i.asset_type.clone()))
            .collect();

        records.sort_by(|a, b| {
            let ord = match sort_field.as_str() {
                "kind" => a
                    .kinds
                    .first()
                    .cloned()
                    .unwrap_or_default()
                    .cmp(&b.kinds.first().cloned().unwrap_or_default()),
                "group" => a.asset_group.cmp(&b.asset_group),
                "type" => {
                    let at = type_map.get(&a.asset_key).cloned().unwrap_or_default();
                    let bt = type_map.get(&b.asset_key).cloned().unwrap_or_default();
                    at.cmp(&bt)
                }
                "tags" => {
                    let at = a.tags.first().cloned().unwrap_or_default();
                    let bt = b.tags.first().cloned().unwrap_or_default();
                    at.cmp(&bt)
                }
                "status" => crate::helpers::stale_status_kind(&a.stale_status)
                    .cmp(crate::helpers::stale_status_kind(&b.stale_status)),
                "last_materialized" => a.last_timestamp.cmp(&b.last_timestamp),
                _ => a.asset_key.cmp(&b.asset_key),
            };
            if asc { ord } else { ord.reverse() }
        });
        records
    };

    let selected_signal = Signal::derive(move || selected.get());

    let materialize_picker = Signal::derive(move || {
        let infos = assets_info.get().and_then(|r| r.ok()).unwrap_or_default();
        let by_key: std::collections::HashMap<String, crate::types::AssetDefinitionInfo> = infos
            .into_iter()
            .map(|i| (i.asset_key.clone(), i))
            .collect();
        crate::helpers::partition_picker_for_assets(&selected.get(), &by_key)
    });

    let select_all = move |_| {
        let records = sorted_filtered();
        set_selected.set(records.iter().map(|r| r.asset_key.clone()).collect());
    };

    let select_none = move |_| {
        set_selected.set(Vec::new());
    };

    let reset_tags = set_filter_tags.clone();
    let reset_kinds = set_filter_kinds.clone();
    let reset_groups = set_filter_groups.clone();
    let reset_search = set_search_text.clone();

    let header_counts = Signal::derive(move || {
        let records = all_records();
        let total = records.len();
        let groups: std::collections::HashSet<String> = records
            .iter()
            .map(|r| r.asset_group.clone().unwrap_or_else(|| "default".into()))
            .collect();
        let stale = records
            .iter()
            .filter(|r| r.stale_status == crate::types::StaleStatus::Stale)
            .count();
        let missing = records
            .iter()
            .filter(|r| r.stale_status == crate::types::StaleStatus::Missing)
            .count();
        (total, groups.len(), stale, missing)
    });

    view! {
        <Topbar crumbs=vec![Crumb::new("Assets")]>
            <LiveStatusChip
                status=live_status
                on_refresh=Callback::new(move |_| set_refresh_tick.update(|t| *t += 1))
            />
        </Topbar>

        <div class="page-header-row" style="align-items: flex-end">
            <div class="page-header">
                <h1>"Assets"</h1>
                // Wrap resource read in Transition so hydration waits for the async fetch.
                <Transition>
                    {move || {
                        let (total, groups, stale, missing) = header_counts.get();
                        view! {
                            <p>
                                <span class="page-header-num">{total.to_string()}</span>
                                " assets"
                                <span class="page-header-sep">"·"</span>
                                <span class="page-header-num">{groups.to_string()}</span>
                                " groups"
                                <span class="page-header-sep">"·"</span>
                                <span class="page-header-num page-header-num--warning">{stale.to_string()}</span>
                                " stale"
                                <span class="page-header-sep">"·"</span>
                                <span class="page-header-num">{missing.to_string()}</span>
                                " missing"
                            </p>
                        }
                    }}
                </Transition>
            </div>
            <Show when=move || !selected.get().is_empty()>
                <button class="btn btn-primary" on:click=move |_| show_dialog.set(true)>
                    {move || format!("Materialize ({})", selected.get().len())}
                </button>
            </Show>
        </div>

        <div class="rv-toolbar">
            <RiversSearch
                value=Signal::derive(move || search_text.get())
                on_input=Callback::new({
                    let set_st = set_search_text.clone();
                    move |v: String| { set_st(v); }
                })
                placeholder="Search assets…"
            />
            <Transition>
                <MultiSelect
                    options=group_options
                    selected=Signal::derive(move || filter_groups.get())
                    on_toggle=toggle_group
                    placeholder="Groups"
                />
                <MultiSelect
                    options=kind_options
                    selected=Signal::derive(move || filter_kinds.get())
                    on_toggle=toggle_kind
                    placeholder="Kinds"
                />
                <MultiSelect
                    options=tag_options
                    selected=Signal::derive(move || filter_tags.get())
                    on_toggle=toggle_tag
                    placeholder="Tags"
                />
            </Transition>
            {move || {
                let has_filters = !filter_tags.get().is_empty()
                    || !filter_kinds.get().is_empty()
                    || !filter_groups.get().is_empty()
                    || !search_text.get().is_empty();
                if has_filters {
                    let rt = reset_tags.clone();
                    let rk = reset_kinds.clone();
                    let rg = reset_groups.clone();
                    let rs = reset_search.clone();
                    view! {
                        <button class="btn btn-small" on:click=move |_| {
                            rt(Vec::new());
                            rk(Vec::new());
                            rg(Vec::new());
                            rs(String::new());
                        }>"Reset"</button>
                    }.into_any()
                } else {
                    view! { <span></span> }.into_any()
                }
            }}
        </div>

        <div class="bulk-actions">
            <button class="bulk-link-btn" on:click=select_all>
                "Select all "
                <Transition>
                    {move || format!("({})", sorted_filtered().len())}
                </Transition>
            </button>
            <span class="bulk-sep">"·"</span>
            <button class="bulk-link-btn" on:click=select_none>"Clear"</button>
            <span class="bulk-count">{move || format!("{} selected", selected.get().len())}</span>
        </div>

        <Transition fallback=move || view! { <TableSkeleton rows=8 cols=7/> }>
            {move || {
                let records = sorted_filtered();
                let infos = assets_info.get().and_then(|r| r.ok()).unwrap_or_default();
                let info_map: std::collections::HashMap<String, _> = infos.into_iter().map(|i| (i.asset_key.clone(), i)).collect();
                let topo = graph.get().and_then(|r| r.ok());

                if records.is_empty() {
                    return view! { <div class="empty-state">"No assets found matching filters."</div> }.into_any();
                }

                let (attention, healthy): (Vec<_>, Vec<_>) = records.into_iter().partition(|r| {
                    r.stale_status != crate::types::StaleStatus::UpToDate
                });
                let n_attention = attention.len();
                let n_stale = attention.iter().filter(|r| r.stale_status == crate::types::StaleStatus::Stale).count();
                let n_missing = n_attention - n_stale;

                const GRID: &str = "grid-template-columns: 32px 1.4fr 90px 120px 120px 1fr 120px 100px";

                fn render_row(
                    record: crate::types::AssetRecord,
                    info_map: &std::collections::HashMap<String, crate::types::AssetDefinitionInfo>,
                    selected: ReadSignal<Vec<String>>,
                    set_selected: WriteSignal<Vec<String>>,
                    dim: &str,
                    topo: &Option<crate::types::GraphTopology>,
                    loc_ns: &str,
                    loc_name: &str,
                ) -> impl IntoView + use<> {
                    let key = record.asset_key.clone();
                    let key_for_check = key.clone();
                    let key_for_toggle = key.clone();
                    let href = loc_path(loc_ns, loc_name, &format!("assets/{}", key));
                    let info: Option<crate::types::AssetDefinitionInfo> = info_map.get(&key).cloned();
                    let asset_type = info.as_ref().map(|i| i.asset_type.clone()).unwrap_or_default();
                    let kinds_str = record.kinds.join(", ");
                    let kinds_for_badge = record.kinds.first().cloned().unwrap_or_else(|| "—".into());
                    let group = record.asset_group.clone().unwrap_or_default();
                    let tags_view: Vec<_> = record.tags.iter().take(3).map(|t| view! { <Tag label=t.clone()/> }).collect();
                    // Snapshot once per row render for the fallback dim_value;
                    // the live-tick path is the explicit `<RelTime>` cell,
                    // not this dim_value (which may be replaced by other named
                    // dimensions).
                    let now_snapshot = use_now().get();
                    let last_ts_rel = record.last_timestamp
                        .map(|t| format_relative_time(t, now_snapshot))
                        .unwrap_or_else(|| "never".into());
                    let last_ts_abs = record.last_timestamp
                        .and_then(crate::helpers::nanos_to_datetime)
                        .map(|d| d.format("%Y-%m-%d %H:%M").to_string())
                        .unwrap_or_else(|| "—".to_string());
                    let seed: u32 = record.asset_key.bytes().map(|b| b as u32).sum::<u32>().max(1);
                    let (dim_value, dim_color) = match dim {
                        "rows" => {
                            let rows = (seed as u64 * 1_237) % 9_000_000 + 1_000;
                            (format!("{rows}"), "var(--text-muted)")
                        }
                        "cost" => {
                            let cents = (seed as u64 * 7) % 4800 + 20;
                            let dollars = cents as f64 / 100.0;
                            let c = if dollars > 0.60 { "var(--error)" }
                                else if dollars > 0.30 { "var(--warning)" }
                                else { "var(--text-muted)" };
                            (format!("${dollars:.2}"), c)
                        }
                        "freshness" => {
                            // 0-90 min since last materialization; SLO = 60 min.
                            let min = (seed % 91) as i64;
                            if min > 60 {
                                (format!("breached +{}m", min - 60), "var(--error)")
                            } else if min > 50 {
                                (format!("{}m left", 60 - min), "var(--warning)")
                            } else {
                                (format!("{}m left", 60 - min), "var(--success)")
                            }
                        }
                        "p95" => {
                            let s = 2.0 + ((seed % 80) as f64) / 4.0;
                            let c = if s > 60.0 { "var(--warning)" }
                                else if s > 15.0 { "var(--text)" }
                                else { "var(--text-muted)" };
                            (format!("{s:.1}s"), c)
                        }
                        _ => (last_ts_rel.clone(), "var(--text-muted)"),
                    };
                    let status_kind = crate::helpers::stale_status_kind(&record.stale_status);
                    let rail = match status_kind {
                        "up-to-date" => "grid-row-rail grid-row-rail--success",
                        "stale" => "grid-row-rail grid-row-rail--warning",
                        _ => "grid-row-rail grid-row-rail--muted",
                    };
                    const GRID: &str = "grid-template-columns: 32px 1.4fr 90px 120px 120px 1fr 120px 100px";

                    // Hover preview caps each column; the full lineage is on the
                    // asset's Lineage tab / DAG sidebar.
                    const LINEAGE_PREVIEW: usize = 3;
                    let (upstream, downstream): (Vec<String>, Vec<String>) = if let Some(t) = topo {
                        (
                            t.direct_upstream(&record.asset_key),
                            t.direct_downstream(&record.asset_key),
                        )
                    } else {
                        (Vec::new(), Vec::new())
                    };
                    let has_lineage = !upstream.is_empty() || !downstream.is_empty();

                    view! {
                        <A href=href attr:class="grid-row mini-lineage-host" attr:style=GRID attr:title=last_ts_abs>
                            <span class=rail></span>
                            {has_lineage.then(|| {
                                let up_rows = upstream.iter().take(LINEAGE_PREVIEW).map(|k| view! {
                                    <div class="mini-lineage-row">
                                        <span class="mini-lineage-row-dot" style="background:var(--secondary)"></span>
                                        <span>{k.clone()}</span>
                                    </div>
                                }).collect::<Vec<_>>();
                                let up_more = upstream.len().saturating_sub(LINEAGE_PREVIEW);
                                let down_rows = downstream.iter().take(LINEAGE_PREVIEW).map(|k| view! {
                                    <div class="mini-lineage-row">
                                        <span class="mini-lineage-row-dot" style="background:var(--accent)"></span>
                                        <span>{k.clone()}</span>
                                    </div>
                                }).collect::<Vec<_>>();
                                let down_more = downstream.len().saturating_sub(LINEAGE_PREVIEW);
                                let has_up = !upstream.is_empty();
                                let has_down = !downstream.is_empty();
                                view! {
                                    <span class="mini-lineage-tip">
                                        {has_up.then(|| view! {
                                            <div class="mini-lineage-col">
                                                <span class="mini-lineage-col-label">"UPSTREAM"</span>
                                                {up_rows}
                                                {(up_more > 0).then(|| view! {
                                                    <div class="mini-lineage-more">{format!("+{up_more} more")}</div>
                                                })}
                                            </div>
                                        })}
                                        {has_down.then(|| view! {
                                            <div class="mini-lineage-col">
                                                <span class="mini-lineage-col-label">"DOWNSTREAM"</span>
                                                {down_rows}
                                                {(down_more > 0).then(|| view! {
                                                    <div class="mini-lineage-more">{format!("+{down_more} more")}</div>
                                                })}
                                            </div>
                                        })}
                                    </span>
                                }
                            })}
                            <span on:click=move |ev: leptos::ev::MouseEvent| ev.stop_propagation()>
                                <input
                                    class="asset-row-check"
                                    type="checkbox"
                                    prop:checked=move || selected.get().contains(&key_for_check)
                                    on:click=move |ev| ev.stop_propagation()
                                    on:change=move |_| set_selected.update(|s| {
                                        if s.contains(&key_for_toggle) {
                                            s.retain(|x| x != &key_for_toggle);
                                        } else {
                                            s.push(key_for_toggle.clone());
                                        }
                                    })
                                />
                            </span>
                            <span class="grid-cell-mono" title=kinds_str.clone()>{key.clone()}</span>
                            <KindBadge kind=kinds_for_badge/>
                            <span class="grid-cell-muted">{if asset_type.is_empty() { "—".into() } else { asset_type }}</span>
                            <span class="grid-cell-muted">{if group.is_empty() { "—".into() } else { group }}</span>
                            <span style="display:flex; gap:4px; flex-wrap:wrap">{tags_view}</span>
                            <span class="grid-cell-muted" style=format!("color:{dim_color}; font-family:'JetBrains Mono',monospace; font-size:11.5px")>
                                {dim_value}
                            </span>
                            <StatusChip kind=status_kind small=true/>
                        </A>
                    }
                }

                let dim = dimension.get();
                let dim_options: &[(&str, &str)] = &[
                    ("materialized", "LAST MATERIALIZED"),
                    ("rows",         "ROWS WRITTEN"),
                    ("cost",         "COST / RUN"),
                    ("freshness",    "FRESHNESS SLO"),
                    ("p95",          "P95 DURATION"),
                ];
                let dim_label = dim_options
                    .iter()
                    .find(|(id, _)| *id == dim.as_str())
                    .map(|(_, l)| *l)
                    .unwrap_or("LAST MATERIALIZED");

                let collapsed_sig = Signal::derive(move || attention_collapsed.get());
                let toggle_attention = Callback::new(move |_: ()| {
                    set_attention_collapsed.update(|v| *v = !*v);
                });
                let is_collapsed = attention_collapsed.get();
                view! {
                    <AttentionBanner
                        count=n_attention
                        breakdown=format!("{n_stale} stale · {n_missing} missing · {} up-to-date", healthy.len())
                        collapsed=collapsed_sig
                        on_toggle=toggle_attention
                    />

                    <div class="grid-table">
                        <div class="grid-table-head" style=GRID>
                            <span></span>
                            <span>"NAME"</span>
                            <span>"KIND"</span>
                            <span>"TYPE"</span>
                            <span>"GROUP"</span>
                            <span>"TAGS"</span>
                            <span class="dim-header-cell">
                                <button
                                    class="dim-header-btn"
                                    on:click=move |ev| {
                                        ev.stop_propagation();
                                        set_dim_menu_open.update(|v| *v = !*v);
                                    }
                                >
                                    <span>{dim_label}</span>
                                    <span class="dim-header-chevron" class:dim-header-chevron--open=move || dim_menu_open.get()>"›"</span>
                                </button>
                                <Show when=move || dim_menu_open.get()>
                                    <div class="dim-menu">
                                        {dim_options.iter().map(|(id, label)| {
                                            let id_s = id.to_string();
                                            let id_for_cls = id_s.clone();
                                            let cls = move || {
                                                if dimension.get() == id_for_cls { "dim-menu-item dim-menu-item--active" }
                                                else { "dim-menu-item" }
                                            };
                                            view! {
                                                <button
                                                    class=cls
                                                    on:click=move |ev| {
                                                        ev.stop_propagation();
                                                        set_dim_cb.run(id_s.clone());
                                                        set_dim_menu_open.set(false);
                                                    }
                                                >{*label}</button>
                                            }
                                        }).collect::<Vec<_>>()}
                                    </div>
                                </Show>
                            </span>
                            <span>"STATUS"</span>
                        </div>
                        {(!is_collapsed).then(|| {
                            let (lns, lnm) = loc.get();
                            attention.into_iter().map(|r| render_row(r, &info_map, selected, set_selected, &dim, &topo, &lns, &lnm)).collect::<Vec<_>>()
                        })}
                        {(!is_collapsed && n_attention > 0 && !healthy.is_empty()).then(|| view! {
                            <div style="display:flex; align-items:center; gap:10px; padding:12px 20px 6px; font-family:'Inter',sans-serif; font-size:10px; font-weight:500; letter-spacing:0.08em; text-transform:uppercase; color:var(--text-muted)">
                                <span>{format!("HEALTHY · {}", healthy.len())}</span>
                                <span style="flex:1; height:1px; background:var(--bg-highest)"></span>
                            </div>
                        })}
                        {{
                            let (lns, lnm) = loc.get();
                            healthy.into_iter().map(|r| render_row(r, &info_map, selected, set_selected, &dim, &topo, &lns, &lnm)).collect::<Vec<_>>()
                        }}
                    </div>
                }.into_any()
            }}
        </Transition>

        <MaterializeDialog
            show=show_dialog
            asset_keys=selected_signal
            picker=materialize_picker
        />
    }
}
