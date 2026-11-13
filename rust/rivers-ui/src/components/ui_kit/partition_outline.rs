//! The partitions of an asset: tiles with the counts, a strip along the first
//! dimension of the keys, and an outline of the keys down to the partitions.

use core::time::Duration;

use leptos::prelude::*;

use super::RiversSearch;
use crate::helpers::{err_text, format_timestamp, format_with_commas, short_id};
use crate::loc::loc_path;
use crate::now::RelTime;
use crate::server_fns::partitions::get_partition_rows;
use crate::types::{
    OUTLINE_PAGE, OutlineLevel, OutlinePlace, OutlineRow, OutlineRows, PartitionCounts,
    PartitionOverview, PartitionRow, PartitionRowState, PartitionShape, PartitionStrip, StripBar,
};

/// The rows of a level that are shown before the reader asks for more. The
/// minutes of an hour are shown at once.
const FIRST_ROWS: usize = 60;

/// The indent of one level of the outline, in pixels.
const INDENT: usize = 22;

/// A share in percent, rounded down: 99.99% never reads as 100%.
pub fn percent(part: u64, whole: u64) -> String {
    if whole == 0 || part == 0 {
        return "0%".to_string();
    }
    if part >= whole {
        return "100%".to_string();
    }

    let tenths = (part as u128 * 1000 / whole as u128) as u64;
    match (tenths, tenths % 10) {
        (0, _) => "<0.1%".to_string(),
        (_, 0) => format!("{}%", tenths / 10),
        (_, tenth) => format!("{}.{tenth}%", tenths / 10),
    }
}

/// The parts of a stacked bar: a state and its share of the bar in percent.
/// A state with partitions gets `least` percent or more: it is always
/// visible.
pub fn bar_parts(counts: &PartitionCounts, least: f64) -> Vec<(&'static str, f64)> {
    let shares: Vec<(&'static str, f64)> = [
        ("done", counts.materialized),
        ("failed", counts.failed),
        ("missing", counts.missing()),
    ]
    .into_iter()
    .filter(|(_, partitions)| *partitions > 0)
    .map(|(state, partitions)| {
        let share = partitions as f64 / counts.total as f64 * 100.0;
        (state, share.max(least))
    })
    .collect();

    let sum: f64 = shares.iter().map(|(_, share)| share).sum();
    shares
        .into_iter()
        .map(|(state, share)| (state, share * 100.0 / sum))
        .collect()
}

/// The marks of the axis of a strip of `bars` bars that have room for their
/// text: the place of each in percent of the width.
pub fn spaced_marks(marks: &[(u32, String)], bars: usize) -> Vec<(f64, String)> {
    // One character of a mark, in percent of the width of a narrow strip.
    const CHARACTER: f64 = 0.9;

    let mut spaced: Vec<(f64, String)> = Vec::new();
    for (at, text) in marks {
        let left = *at as f64 / bars.max(1) as f64 * 100.0;
        let free_from = spaced
            .last()
            .map(|(left, text)| left + (text.chars().count() + 2) as f64 * CHARACTER)
            .unwrap_or(0.0);
        if left >= free_from {
            spaced.push((left, text.clone()));
        }
    }
    spaced
}

/// The rows that the next click on "Show more" adds to a level: the rows
/// that are read and not shown, and when there are none, the next page of
/// the rows that are not read.
fn next_step(hidden: u64, unread: u64) -> u64 {
    match hidden {
        0 => unread.min(OUTLINE_PAGE),
        hidden => hidden.min(OUTLINE_PAGE),
    }
}

/// The rows of the outline that the reader asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StateFilter {
    #[default]
    All,
    Failed,
    Missing,
}

impl StateFilter {
    fn shows_counts(self, counts: &PartitionCounts) -> bool {
        match self {
            Self::All => true,
            Self::Failed => counts.failed > 0,
            Self::Missing => counts.missing() > 0,
        }
    }

    fn shows_state(self, state: PartitionRowState) -> bool {
        match self {
            Self::All => true,
            Self::Failed => state == PartitionRowState::Failed,
            Self::Missing => state == PartitionRowState::Missing,
        }
    }

    fn nothing(self) -> &'static str {
        match self {
            Self::All => "No partitions.",
            Self::Failed => "No failed partitions here.",
            Self::Missing => "No missing partitions here.",
        }
    }
}

/// Where the rows of the outline are read: the code location and the asset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutlineSource {
    pub loc_ns: String,
    pub loc_name: String,
    pub asset_key: String,
}

impl OutlineSource {
    async fn rows(
        self,
        place: OutlinePlace,
        query: String,
        offset: u64,
    ) -> Result<OutlineLevel, String> {
        get_partition_rows(
            self.loc_ns,
            self.loc_name,
            self.asset_key,
            place,
            query,
            offset,
        )
        .await
        .map_err(|e| err_text(&e))
    }
}

fn rail_of(counts: &PartitionCounts) -> &'static str {
    if counts.failed > 0 {
        "part-rail part-rail--failed"
    } else if counts.missing() > 0 {
        "part-rail part-rail--missing"
    } else {
        "part-rail part-rail--done"
    }
}

fn segments(counts: &PartitionCounts, least: f64, along: &'static str) -> impl IntoView + use<> {
    bar_parts(counts, least)
        .into_iter()
        .map(|(state, share)| {
            view! {
                <span class=format!("part-seg-{state}") style=format!("{along}:{share:.2}%")></span>
            }
        })
        .collect_view()
}

fn dash() -> impl IntoView {
    view! { <span class="part-dash">"–"</span> }
}

/// The four tiles above the outline: the partitions, and each state.
#[component]
pub fn PartitionTiles(
    counts: PartitionCounts,
    shape: PartitionShape,
    /// Shown in the tile of the missing partitions when there are some.
    missing_action: Option<ViewFn>,
) -> impl IntoView {
    let shape = match shape {
        PartitionShape::Dimensions(dimensions) => dimensions
            .into_iter()
            .enumerate()
            .map(|(at, (name, keys))| {
                view! {
                    {(at > 0).then_some(" × ")}
                    {name}
                    " "
                    <b>{format_with_commas(keys)}</b>
                }
            })
            .collect_view()
            .into_any(),
        PartitionShape::Windows { unit, first, last } => view! {
            {(!unit.is_empty()).then(|| format!("{unit} · "))}
            <b>{first}</b>
            " → "
            <b>{last}</b>
        }
        .into_any(),
        PartitionShape::Keys => "\u{a0}".into_any(),
    };

    let value = |count: u64, dot: &'static str| {
        view! {
            <div class="meta-tile-value part-tile-value" class:part-zero=count == 0>
                {(count > 0).then(|| view! { <span class=format!("dot {dot}")></span> })}
                {format_with_commas(count)}
            </div>
        }
    };
    let share = |count: u64| match count {
        0 => "\u{a0}".to_string(),
        count => percent(count, counts.total),
    };
    let missing = counts.missing();

    view! {
        <div class="meta-tile-grid part-tiles">
            <div class="meta-tile">
                <div class="meta-tile-label">"Partitions"</div>
                <div class="meta-tile-value part-tile-value">{format_with_commas(counts.total)}</div>
                <div class="part-tile-sub">{shape}</div>
            </div>
            <div class="meta-tile">
                <div class="meta-tile-label">"Materialized"</div>
                {value(counts.materialized, "dot-success")}
                <div class="part-tile-sub">{percent(counts.materialized, counts.total)}</div>
            </div>
            <div class="meta-tile">
                <div class="meta-tile-label">"Failed"</div>
                {value(counts.failed, "dot-failed")}
                <div class="part-tile-sub">{share(counts.failed)}</div>
            </div>
            <div class="meta-tile">
                <div class="meta-tile-label">"Missing"</div>
                {value(missing, "dot-missing")}
                {match missing_action.filter(|_| missing > 0) {
                    Some(action) => view! { <div class="part-tile-action">{action.run()}</div> }.into_any(),
                    None => view! { <div class="part-tile-sub">{share(missing)}</div> }.into_any(),
                }}
            </div>
        </div>
    }
}

/// One bar for each run of keys of the first dimension, stacked by state.
/// The line beside the head shows the numbers of the bar under the pointer.
#[component]
pub fn PartitionStripView(strip: PartitionStrip) -> impl IntoView {
    let PartitionStrip {
        heading,
        note,
        bars,
        marks,
    } = strip;
    let marks = spaced_marks(&marks, bars.len());
    let bars = StoredValue::new(bars);
    let pointed = RwSignal::new(None::<usize>);

    let readout = move || match pointed.get() {
        None => note.clone().into_any(),
        Some(at) => bars.with_value(|bars| {
            let StripBar { label, counts } = &bars[at];
            let states = [
                (counts.materialized, "materialized"),
                (counts.failed, "failed"),
                (counts.missing(), "missing"),
            ]
            .into_iter()
            .filter(|(partitions, _)| *partitions > 0)
            .map(|(partitions, state)| {
                view! { " · " <b>{format_with_commas(partitions)}</b> " " {state} }
            })
            .collect_view();
            view! { <b>{label.clone()}</b> {states} }.into_any()
        }),
    };

    let columns = bars.with_value(|bars| {
        bars.iter()
            .enumerate()
            .map(|(at, bar)| {
                view! {
                    <div
                        class="part-col"
                        class:part-col--pointed=move || pointed.get() == Some(at)
                        on:mouseenter=move |_| pointed.set(Some(at))
                    >
                        {segments(&bar.counts, 9.0, "height")}
                    </div>
                }
            })
            .collect_view()
    });

    view! {
        <div class="part-strip">
            <div class="part-strip-head">
                <span class="section-header-label">{heading}</span>
                <span class="part-strip-note">{readout}</span>
                <span class="part-legend">
                    <span><i class="part-seg-done"></i>"materialized"</span>
                    <span><i class="part-seg-failed"></i>"failed"</span>
                    <span><i class="part-seg-missing"></i>"missing"</span>
                </span>
            </div>
            <div class="part-bars" on:mouseleave=move |_| pointed.set(None)>{columns}</div>
            <div class="part-axis">
                {marks
                    .into_iter()
                    .map(|(left, text)| view! { <span style=format!("left:{left:.3}%")>{text}</span> })
                    .collect_view()}
            </div>
        </div>
    }
}

/// One row of a level: a run of keys that opens, or a partition.
#[derive(Debug, Clone)]
enum Row {
    Group(OutlineRow),
    Partition(PartitionRow),
}

impl Row {
    fn of(rows: OutlineRows) -> Vec<Self> {
        match rows {
            OutlineRows::Groups(rows) => rows.into_iter().map(Self::Group).collect(),
            OutlineRows::Partitions(rows) => rows.into_iter().map(Self::Partition).collect(),
        }
    }

    fn shown_by(&self, filter: StateFilter) -> bool {
        match self {
            Self::Group(row) => filter.shows_counts(&row.counts),
            Self::Partition(row) => filter.shows_state(row.state),
        }
    }
}

fn group_head(heading: String) -> impl IntoView {
    view! {
        <div class="part-head">
            <span>{heading}</span>
            <span class="part-num">"Partitions"</span>
            <span>"Materialized"</span>
            <span class="part-num">"Failed"</span>
            <span class="part-num">"Missing"</span>
        </div>
    }
}

fn partition_head(heading: String, depth: usize) -> impl IntoView {
    view! {
        <div class="part-head part-head--partitions">
            <span style=format!("padding-left:{}px", depth * INDENT + INDENT)>{heading}</span>
            <span>"State"</span>
            <span>"Materialized"</span>
            <span>"Run"</span>
        </div>
    }
}

fn partition_row(source: &OutlineSource, row: PartitionRow, depth: usize) -> impl IntoView + use<> {
    let (rail, dot) = match row.state {
        PartitionRowState::Materialized => ("part-rail part-rail--done", "dot dot-success"),
        PartitionRowState::Failed => ("part-rail part-rail--failed", "dot dot-failed"),
        PartitionRowState::Missing => ("part-rail part-rail--missing", "dot dot-missing"),
    };
    let failed = row
        .failed_at
        .map(|at| format!("failed {}", format_timestamp(Some(at))));
    let run = row.run_id.map(|run_id| {
        let href = loc_path(&source.loc_ns, &source.loc_name, &format!("runs/{run_id}"));
        view! { <a class="part-run" href=href>{short_id(&run_id, 8)}</a> }
    });

    view! {
        <div class="part-leaf">
            <span class=rail></span>
            <span class="part-key" style=format!("padding-left:{}px", depth * INDENT + INDENT)>
                {row.key}
            </span>
            <span>
                <span class="chip" title=failed>
                    <span class=dot></span>
                    {row.state.label()}
                </span>
            </span>
            <span>
                {match row.materialized_at {
                    Some(at) => view! {
                        <span class="part-time">{format_timestamp(Some(at))}</span>
                        " "
                        <span class="part-ago"><RelTime ts=at/></span>
                    }
                    .into_any(),
                    None => dash().into_any(),
                }}
            </span>
            <span class="part-leaf-run">
                {match run {
                    Some(run) => run.into_any(),
                    None => dash().into_any(),
                }}
            </span>
        </div>
    }
}

/// A row that opens to the rows below it. They are read when it opens first.
#[component]
fn OutlineGroupRow(
    source: OutlineSource,
    row: OutlineRow,
    depth: usize,
    #[prop(into)] filter: Signal<StateFilter>,
) -> impl IntoView {
    let OutlineRow {
        label,
        holds,
        counts,
        place,
    } = row;
    let open = RwSignal::new(false);
    let below = RwSignal::new(None::<Result<OutlineLevel, String>>);
    let asked = StoredValue::new(false);

    let toggle = {
        let source = source.clone();
        let place = place.clone();
        move || {
            open.update(|open| *open = !*open);
            if asked.get_value() {
                return;
            }
            asked.set_value(true);

            let (source, place) = (source.clone(), place.clone());
            leptos::task::spawn_local(async move {
                below.set(Some(source.rows(place, String::new(), 0).await));
            });
        }
    };
    let on_key = {
        let toggle = toggle.clone();
        move |ev: leptos::ev::KeyboardEvent| {
            if matches!(ev.key().as_str(), "Enter" | " ") {
                ev.prevent_default();
                toggle();
            }
        }
    };

    let missing = counts.missing();
    view! {
        <div
            class="part-row"
            class:part-row--open=move || open.get()
            role="button"
            tabindex="0"
            aria-expanded=move || open.get().to_string()
            on:click=move |_| toggle()
            on:keydown=on_key
        >
            <span class=rail_of(&counts)></span>
            <span class="part-name" style=format!("padding-left:{}px", depth * INDENT)>
                <span class="part-caret">{move || if open.get() { "▾" } else { "▸" }}</span>
                <span class="part-key">{label}</span>
                <span class="part-sub">{holds}</span>
            </span>
            <span class="part-num">{format_with_commas(counts.total)}</span>
            <span class="part-share">
                <span class="part-bar">{segments(&counts, 3.0, "width")}</span>
                <span class="part-share-n">{format_with_commas(counts.materialized)}</span>
                <span class="part-share-pct">{percent(counts.materialized, counts.total)}</span>
            </span>
            {match counts.failed {
                0 => view! { <span class="part-num part-dash">"–"</span> }.into_any(),
                failed => view! {
                    <span class="part-num part-num--failed">{format_with_commas(failed)}</span>
                }
                .into_any(),
            }}
            {match missing {
                0 => view! { <span class="part-num part-dash">"–"</span> }.into_any(),
                missing => view! { <span class="part-num">{format_with_commas(missing)}</span> }.into_any(),
            }}
        </div>
        <div class="part-below" style:display=move || if open.get() { "" } else { "none" }>
            {move || {
                let pad = format!("padding-left:{}px", (depth + 1) * INDENT + INDENT);
                match below.get() {
                    None => view! { <div class="part-line" style=pad>"Loading…"</div> }.into_any(),
                    Some(Err(e)) => view! {
                        <div class="part-line part-line--error" style=pad>
                            {format!("Couldn't load the partitions: {e}")}
                        </div>
                    }
                    .into_any(),
                    Some(Ok(level)) => view! {
                        <OutlineLevelView
                            source=source.clone()
                            place=place.clone()
                            level=level
                            depth=depth + 1
                            filter=filter
                        />
                    }
                    .into_any(),
                }
            }}
        </div>
    }
}

/// The rows of one level of the outline. More rows are shown, or read, when
/// the reader asks for them.
#[component]
fn OutlineLevelView(
    source: OutlineSource,
    /// The place of the rows: more of them are read with it.
    place: OutlinePlace,
    /// The text that the keys of the rows hold, for the rows of a search.
    #[prop(optional, into)]
    query: String,
    level: OutlineLevel,
    depth: usize,
    #[prop(into)] filter: Signal<StateFilter>,
) -> AnyView {
    let OutlineLevel {
        heading,
        rows,
        more,
    } = level;
    let head = match &rows {
        OutlineRows::Partitions(_) => Some(partition_head(heading, depth).into_any()),
        OutlineRows::Groups(_) if depth == 0 => Some(group_head(heading).into_any()),
        OutlineRows::Groups(_) => None,
    };

    let rows = RwSignal::new(Row::of(rows));
    let unread = RwSignal::new(more);
    let shown = RwSignal::new(FIRST_ROWS);
    let reading = RwSignal::new(false);
    let failure = RwSignal::new(None::<String>);

    let matching = Memo::new(move |_| {
        let filter = filter.get();
        rows.with(|rows| {
            rows.iter()
                .enumerate()
                .filter(|(_, row)| row.shown_by(filter))
                .map(|(at, _)| at)
                .collect::<Vec<usize>>()
        })
    });
    let hidden = move || matching.with(Vec::len).saturating_sub(shown.get()) as u64;

    let show_more = {
        let source = source.clone();
        move |_| {
            if hidden() > 0 {
                shown.update(|shown| *shown += OUTLINE_PAGE as usize);
                return;
            }
            if reading.get_untracked() {
                return;
            }
            reading.set(true);

            let (source, place, query) = (source.clone(), place.clone(), query.clone());
            let offset = rows.with_untracked(Vec::len) as u64;
            leptos::task::spawn_local(async move {
                match source.rows(place, query, offset).await {
                    Ok(level) => {
                        unread.set(level.more);
                        shown.update(|shown| *shown += level.rows.len());
                        rows.update(|rows| rows.extend(Row::of(level.rows)));
                        failure.set(None);
                    }
                    Err(e) => failure.set(Some(e)),
                }
                reading.set(false);
            });
        }
    };

    let pad = format!("padding-left:{}px", depth * INDENT + INDENT);
    let source = StoredValue::new(source);
    let shown_rows = move || {
        let shown = shown.get();
        matching.with(|matching| matching.iter().copied().take(shown).collect::<Vec<usize>>())
    };
    let row_view = move |at: usize| {
        let source = source.get_value();
        match rows.with_untracked(|rows| rows[at].clone()) {
            Row::Group(row) => view! {
                <OutlineGroupRow source=source row=row depth=depth filter=filter/>
            }
            .into_any(),
            Row::Partition(row) => partition_row(&source, row, depth).into_any(),
        }
    };

    view! {
        <div class="part-level">
            {head}
            <For each=shown_rows key=|at| *at children=row_view/>
            {
                let pad = pad.clone();
                move || {
                    (matching.with(Vec::is_empty) && unread.get() == 0)
                        .then(|| view! { <div class="part-line" style=pad.clone()>{filter.get().nothing()}</div> })
                }
            }
            {
                let pad = pad.clone();
                move || {
                    let (hidden, unread) = (hidden(), unread.get());
                    let left = hidden + unread;
                    (left > 0).then(|| {
                        let step = next_step(hidden, unread);
                        view! {
                            <div class="part-line" style=pad.clone()>
                                <button
                                    class="part-more"
                                    disabled=move || reading.get()
                                    on:click=show_more.clone()
                                >
                                    {format!("Show {} more", format_with_commas(step))}
                                </button>
                                {(left > step).then(|| format!(" of {}", format_with_commas(left)))}
                            </div>
                        }
                    })
                }
            }
            {move || {
                failure.get().map(|e| view! {
                    <div class="part-line part-line--error" style=pad.clone()>
                        {format!("Couldn't load the partitions: {e}")}
                    </div>
                })
            }}
        </div>
    }
    .into_any()
}

/// The Partitions tab of an asset.
#[component]
pub fn PartitionOutline(
    source: OutlineSource,
    overview: PartitionOverview,
    /// Shown in the tile of the missing partitions when there are some.
    #[prop(optional, into)]
    missing_action: Option<ViewFn>,
) -> impl IntoView {
    let PartitionOverview {
        counts,
        shape,
        strip,
        top,
        root,
        from_definition,
    } = overview;

    let filter = RwSignal::new(StateFilter::All);
    let text = RwSignal::new(String::new());
    // The rows of the keys that hold `text`, with that text. `None` while
    // they are read.
    let found = RwSignal::new(None::<(String, Result<OutlineLevel, String>)>);
    let timer = StoredValue::new(None::<TimeoutHandle>);
    on_cleanup(move || {
        if let Some(Some(timer)) = timer.try_get_value() {
            timer.clear();
        }
    });

    let search = {
        let source = source.clone();
        let root = root.clone();
        move |query: String| {
            text.set(query.clone());
            found.set(None);
            if let Some(timer) = timer.get_value() {
                timer.clear();
            }
            if query.is_empty() {
                return;
            }

            let (source, root) = (source.clone(), root.clone());
            let read = move || {
                leptos::task::spawn_local(async move {
                    let level = source.rows(root, query.clone(), 0).await;
                    // The reader typed more, or left the tab: this answer is
                    // for an older text.
                    if text.try_with_untracked(|text| *text == query) == Some(true) {
                        found.set(Some((query, level)));
                    }
                });
            };
            timer.set_value(set_timeout_with_handle(read, Duration::from_millis(250)).ok());
        }
    };

    let pill = move |shows: StateFilter, label: &'static str, count: u64| {
        let count_class = match (shows, count) {
            (StateFilter::Failed, 1..) => "count count--error",
            _ => "count",
        };
        view! {
            <button
                class="filter-pill"
                class:filter-pill--active=move || filter.get() == shows
                aria-pressed=move || (filter.get() == shows).to_string()
                on:click=move |_| filter.set(shows)
            >
                {label}
                <span class=count_class>{format_with_commas(count)}</span>
            </button>
        }
    };

    let found_source = source.clone();
    let found_root = root.clone();
    view! {
        <PartitionTiles counts=counts shape=shape missing_action=missing_action/>
        {strip.map(|strip| view! { <PartitionStripView strip=strip/> })}
        {(!from_definition).then(|| view! {
            <div class="part-notice">
                "The code location does not answer: these are the materialized partitions only."
            </div>
        })}

        {from_definition.then(|| view! {
            <div class="part-toolbar">
                <div class="filter-pill-group">
                    <span class="filter-pill-group-label">"Show"</span>
                    {pill(StateFilter::All, "All", counts.total)}
                    {pill(StateFilter::Failed, "Failed", counts.failed)}
                    {pill(StateFilter::Missing, "Missing", counts.missing())}
                </div>
                <RiversSearch value=text on_input=Callback::new(search) placeholder="find a partition…"/>
            </div>
        })}

        <div class="part-outline">
            <div style:display=move || if text.with(String::is_empty) { "" } else { "none" }>
                <OutlineLevelView source=source place=root level=top depth=0 filter=filter/>
            </div>
            {move || {
                if text.with(String::is_empty) {
                    return None;
                }
                Some(match found.get() {
                    None => view! { <div class="part-line">"Searching…"</div> }.into_any(),
                    Some((_, Err(e))) => view! {
                        <div class="part-line part-line--error">{format!("Couldn't search: {e}")}</div>
                    }
                    .into_any(),
                    Some((query, Ok(level))) if level.rows.is_empty() => view! {
                        <div class="part-line">{format!("No key holds “{query}”.")}</div>
                    }
                    .into_any(),
                    Some((query, Ok(level))) => view! {
                        <OutlineLevelView
                            source=found_source.clone()
                            place=found_root.clone()
                            query=query
                            level=level
                            depth=0
                            filter=filter
                        />
                    }
                    .into_any(),
                })
            }}
        </div>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counts(total: u64, materialized: u64, failed: u64) -> PartitionCounts {
        PartitionCounts {
            total,
            materialized,
            failed,
        }
    }

    #[test]
    fn a_share_is_rounded_down() {
        assert_eq!(percent(1_157_520, 1_157_520), "100%");
        assert_eq!(percent(1_157_519, 1_157_520), "99.9%");
        assert_eq!(percent(86, 90), "95.5%");
        assert_eq!(percent(45, 90), "50%");
        assert_eq!(percent(1, 1_157_520), "<0.1%");
        assert_eq!(percent(0, 90), "0%");
        assert_eq!(percent(0, 0), "0%");
    }

    #[test]
    fn each_state_with_partitions_has_a_visible_part() {
        let parts = bar_parts(&counts(1_000_000, 999_998, 1), 3.0);

        let states: Vec<&str> = parts.iter().map(|(state, _)| *state).collect();
        assert_eq!(states, ["done", "failed", "missing"]);
        assert!(parts[1].1 > 2.5 && parts[2].1 > 2.5);
        let sum: f64 = parts.iter().map(|(_, share)| share).sum();
        assert!((sum - 100.0).abs() < 1e-9);
    }

    #[test]
    fn a_state_with_no_partitions_has_no_part() {
        let parts = bar_parts(&counts(24, 24, 0), 9.0);

        assert_eq!(parts, vec![("done", 100.0)]);
    }

    #[test]
    fn a_mark_with_no_room_for_its_text_is_left_out() {
        let marks = vec![
            (0, "2024-01".to_string()),
            (1, "2024-02".to_string()),
            (30, "2024-03".to_string()),
        ];

        let spaced = spaced_marks(&marks, 60);

        assert_eq!(
            spaced,
            vec![(0.0, "2024-01".to_string()), (50.0, "2024-03".to_string())]
        );
    }

    #[test]
    fn the_next_click_shows_the_hidden_rows_before_it_reads_more() {
        assert_eq!(next_step(40, 300), 40);
        assert_eq!(next_step(250, 300), 100);
        assert_eq!(next_step(0, 300), 100);
        assert_eq!(next_step(0, 7), 7);
        assert_eq!(next_step(0, 0), 0);
    }

    #[test]
    fn the_filter_shows_the_rows_that_hold_the_state() {
        let complete = counts(24, 24, 0);
        let with_gap = counts(24, 20, 1);

        assert!(StateFilter::All.shows_counts(&complete));
        assert!(!StateFilter::Failed.shows_counts(&complete));
        assert!(!StateFilter::Missing.shows_counts(&complete));
        assert!(StateFilter::Failed.shows_counts(&with_gap));
        assert!(StateFilter::Missing.shows_counts(&with_gap));
        assert!(StateFilter::Missing.shows_state(PartitionRowState::Missing));
        assert!(!StateFilter::Missing.shows_state(PartitionRowState::Failed));
    }
}
