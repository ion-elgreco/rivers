# Deferred: Run-detail "ghost: last run" Gantt overlay

Status: deferred (removed from UI; to be reintroduced once real previous-run data is available).

## What it did

On the run-detail page, the Gantt timeline could overlay the previous run's task
durations as dashed "ghost" bars beneath each current bar, with per-task delta
badges (↑Ns slower, ↓Ns faster) and an overall summary at the bottom ("overall Δ
+14s slower than last"). A header toggle chip — `ghost: last run` — turned the
overlay on and off.

This matched the Rivers design for run comparison at a glance.

## Why it was removed

The ghost durations were synthesized deterministically from asset-name byte sums
(seeded fake data), because the storage layer doesn't yet surface "previous run"
timing per asset. Shipping fake comparison data risks misleading users, so the
overlay was cut until a real data source exists.

## What needs to land before reintroducing

1. **Storage query**: given `(job_name, asset_key)`, return the duration of the
   same asset's most recent successful run before the current one. A small API
   surface like `storage::previous_asset_duration(run_id, asset_key) -> Option<Duration>`
   is enough.
2. **Event-to-duration mapping** on the client: the existing `build_gantt_steps`
   already gives `start`/`end` per asset. Feed previous-run durations through a
   sibling map.

## Sketch of the reintroduction

Extend the existing `RunTimelinePanel` (`rust/rivers-ui/src/pages/run_detail.rs`)
with the following additions. Most of this code was live in the repo previously
and can be recovered from git history (`git log --all --follow -p rust/rivers-ui/src/pages/run_detail.rs | grep -C 20 'ghost-toggle\|gantt-lane-ghost\|gantt-lane-delta'`).

```rust
// State (RunDetailPage):
let (show_ghost, set_show_ghost) = signal(true);

// RunTimelinePanel props:
show_ghost: ReadSignal<bool>,
set_show_ghost: WriteSignal<bool>,

// Per-lane extras (LaneData / LaneRow):
g_width_pct: f64,    // % width of the ghost bar
delta_secs: f64,     // current_dur - ghost_dur (positive = slower)
g_dur_secs: f64,     // ghost duration for the ghost-end label

// Ghost toggle chip in panel header (shown only in gantt mode):
<button class="ghost-toggle [--active?]" on:click=toggle>
    <span class="ghost-toggle-line"></span>
    "ghost: last run"
</button>

// Lane rendering (inside <div class="gantt-lane-track">):
<Show when=show_ghost>
    <div class="gantt-lane-ghost" style="left:{g_start}%; width:{g_width_pct}%"/>
    {show_delta.then(|| view!{ <div class="gantt-lane-ghost-label" ...>{ghost_label}</div> })}
</Show>
<div class="gantt-lane-bar ..." style=bar_style>...</div>
<Show when=show_ghost && show_delta>
    <div class="gantt-lane-delta gantt-lane-delta--slower|faster" ...>
        {arrow}{abs(delta_secs)}s
    </div>
</Show>

// Legend (below lanes, when show_ghost):
.gantt-legend with "this run" / "last run" swatches, slower/faster markers,
overall Δ summary.
```

## CSS

The following classes were removed. Recreate them when the overlay returns:

- `.ghost-toggle`, `.ghost-toggle-line`, `.ghost-toggle--active`
- `.gantt-lane-ghost`, `.gantt-lane-ghost-label`
- `.gantt-lane-delta`, `.gantt-lane-delta--slower`, `.gantt-lane-delta--faster`
- `.gantt-legend`, `.gantt-legend-item`, `.gantt-legend-swatch--done`,
  `.gantt-legend-swatch--ghost`, `.gantt-legend-sep`, `.gantt-legend-spacer`

Keyframes to keep:
- `river-flow` — already used by the running-bar stripes animation.

## Related

- Rivers reference design: `/tmp/design/rivers-v2/project/components/run-detail.jsx`,
  `GanttView` function — the exact visual that was ported.
