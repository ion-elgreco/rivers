# AutomationCondition

Declarative conditions that control when assets should be automatically materialized. Conditions compose with boolean operators (`&`, `|`, `~`) to build complex rules.

## High-level presets

```python
import rivers as rs

# Materialize when any dependency updates
@rs.Asset(automation_condition=rs.AutomationCondition.eager())
def eager_asset(upstream: int) -> int:
    return upstream + 1

# Materialize on a cron schedule
@rs.Asset(automation_condition=rs.AutomationCondition.on_cron("0 0 * * *"))
def daily_asset() -> int:
    return 42

# Materialize only when missing
@rs.Asset(automation_condition=rs.AutomationCondition.on_missing())
def fill_gaps() -> int:
    return 1
```

| Method | Description |
|--------|-------------|
| `AutomationCondition.eager()` | Materialize whenever any dependency is updated. Excludes failed partitions/assets, so they aren't auto-retried until re-run. |
| `AutomationCondition.on_cron(cron_schedule, timezone=None)` | Materialize on a cron schedule. |
| `AutomationCondition.on_missing()` | Materialize only when the asset has never been materialized; leaves failed partitions alone. |

---

## Leaf conditions

Fine-grained conditions for building custom rules. All are static methods on `AutomationCondition` (e.g. `AutomationCondition.missing()`).

| Method | Description |
|--------|-------------|
| `.missing()` | Asset has never been materialized. |
| `.in_progress()` | Asset is part of an in-progress run. |
| `.execution_failed()` | Latest execution of this asset failed. |
| `.newly_updated()` | Asset's materialization timestamp changed since the previous tick. |
| `.newly_requested()` | Asset was requested for materialization on the previous tick. |
| `.code_version_changed()` | Code version differs from the last materialized version. |
| `.data_version_changed()` | Data version differs from the previous tick. |
| `.cron_tick_passed(cron_schedule, timezone=None)` | A cron tick has passed since the last evaluation. `cron_schedule` accepts 5 or 6 fields (seconds optional). |
| `.in_latest_time_window(lookback_delta=None)` | Partition falls within the latest time window (`lookback_delta` in seconds). |
| `.initial_evaluation()` | First evaluation tick after daemon startup or a condition tree change. |
| `.backfill_in_progress()` | Asset is part of an active backfill. |
| `.will_be_requested()` | Asset's condition already fired earlier this tick (same-tick cascading). |
| `.last_run_includes_target()` | The dep's latest run also included the root asset being evaluated. |
| `.last_executed_with_tags(tag_keys=None, tag_values=None)` | Latest run that materialized this asset had matching tags. |
| `.has_run_with_tags(tag_keys=None, tag_values=None)` | Any new materialization this tick came from a run with matching tags. |
| `.all_runs_have_tags(tag_keys=None, tag_values=None)` | All new materializations this tick came from runs with matching tags (vacuously true with no materializations). |

### Dep-aggregate conditions

| Method | Description |
|--------|-------------|
| `.any_deps_missing()` | Any upstream dependency is missing and won't be requested this tick. |
| `.any_deps_in_progress()` | Any upstream dependency is currently being materialized. |
| `.any_deps_updated()` | Any dep was updated (and not in a joint run) or will be requested this tick. |
| `.any_deps_match(condition)` | Any upstream dependency satisfies `condition`. |
| `.all_deps_match(condition)` | All upstream dependencies satisfy `condition`. |
| `.all_deps_updated_since_cron(cron_schedule, timezone=None)` | All deps have updated since the last cron tick (used internally by `on_cron()`). |

---

## State-tracking and structural operators

These operators are called on a condition instance and return a transformed condition.

| Method | Description |
|--------|-------------|
| `.newly_true()` | Rising-edge detector: true only on the tick where the condition transitions from false to true. |
| `.since(reset_condition)` | Latch: once the condition fires, stays true until `reset_condition` fires. Reset takes priority if both fire on the same tick. |
| `.since_last_handled()` | Debounce: true while the condition is true and hasn't been handled (materialization requested) yet. Prevents re-firing on the tick immediately after handling. |
| `.on_selected(keys)` | Evaluate the condition against the named asset key(s) instead of the asset wearing it. Useful for cross-asset signaling. |
| `.with_label(label)` | Attach a display label for debugging and UI visualization. |
| `.replace(old, new)` | Recursively replace sub-conditions matching `old` (label string or condition) with `new`. Useful for surgical changes to presets. |
| `.without(condition)` | Drop a child of an `And` condition (matched by label, or by the inner label of a `~cond` child). |

### `.since(reset_condition)` in depth

Without `.since()`, conditions that stay true across ticks (e.g. code version mismatch) would request materialization on **every tick** — dozens of duplicate requests before the first one completes. `.since()` provides fire-once-then-wait semantics:

```python
# "Code changed and we haven't requested materialization yet"
# Fires once when code changes, turns off when materialization is requested,
# won't fire again unless the code version changes again after the reset.
condition = rs.AutomationCondition.code_version_changed().since(
    rs.AutomationCondition.newly_requested()
)

# "A dep updated and we haven't handled it yet"
# This is what .since_last_handled() expands to internally.
condition = rs.AutomationCondition.any_deps_updated().since(
    rs.AutomationCondition.newly_requested()
    | rs.AutomationCondition.newly_updated()
)
```

### `.newly_true()` in depth

Without `.newly_true()`, a condition like `missing()` would fire every tick while the asset remains missing. With `.newly_true()`, it fires only on the tick where the asset first *becomes* missing:

```python
# Fire only when the asset transitions to missing, not while it stays missing
condition = rs.AutomationCondition.missing().newly_true()
```

---

## Boolean operators

Combine conditions using Python operators:

```python
# Materialize when dependencies update AND no failures
condition = (
    rs.AutomationCondition.any_deps_updated()
    & ~rs.AutomationCondition.execution_failed()
)

# Materialize on cron OR when missing
condition = (
    rs.AutomationCondition.on_cron("0 0 * * *")
    | rs.AutomationCondition.missing()
)

# Negate a condition
condition = ~rs.AutomationCondition.in_progress()
```

| Operator | Description |
|----------|-------------|
| `a & b` | Both conditions must be true. |
| `a \| b` | Either condition must be true. |
| `~a` | Condition must be false. |

---

## Properties

| Property | Type | Description |
|----------|------|-------------|
| `label` | `str \| None` | Optional label. |
| `description` | `str` | Human-readable description. |
| `children` | `list[AutomationCondition]` | Child conditions (for composite conditions). |

---

## Examples

### Custom policy

```python
# Only materialize when:
# - All dependencies are up to date
# - A cron tick has passed
# - The asset is not currently in progress
condition = (
    rs.AutomationCondition.all_deps_match(rs.AutomationCondition.newly_updated())
    & rs.AutomationCondition.cron_tick_passed("0 */6 * * *")
    & ~rs.AutomationCondition.in_progress()
).with_label("smart_refresh")

@rs.Asset(automation_condition=condition)
def smart_asset(source: int) -> int:
    return source * 2
```

### Cross-asset signaling

```python
# Trigger downstream when upstream was requested on the previous tick
@rs.Asset(
    automation_condition=rs.AutomationCondition.any_deps_match(
        rs.AutomationCondition.newly_requested()
    ) & ~rs.AutomationCondition.in_progress()
)
def downstream(upstream: int) -> int:
    return upstream * 2
```

### Same-tick cascading with `will_be_requested()`

`will_be_requested()` checks whether a dependency's condition has already fired earlier in the current evaluation tick. This enables same-tick cascading — triggering a downstream asset in the same tick as its upstream, without waiting for the upstream to complete. Conditions are evaluated in topological order (dependencies first).

```python
# Trigger downstream when upstream was updated OR will be requested this tick
condition = rs.AutomationCondition.any_deps_match(
    (rs.AutomationCondition.newly_updated()
     & ~rs.AutomationCondition.last_run_includes_target())
    | rs.AutomationCondition.will_be_requested()
).since_last_handled() & ~rs.AutomationCondition.in_progress()
```

> **Note:** The default `any_deps_updated()` and `any_deps_missing()` composites do not include `will_be_requested()`. Use it in manual compositions when your executor guarantees that dependencies are materialized before their downstreams.

### Code version change with guard

```python
# Re-materialize when code changes, but only once, and not while already running
condition = (
    rs.AutomationCondition.code_version_changed().since(
        rs.AutomationCondition.newly_requested()
    )
    & ~rs.AutomationCondition.in_progress()
)
```

### Surgical edits to a preset

`.replace()` and `.without()` let you tweak the standard presets without rewriting them from scratch:

```python
# Eager, but skip the "any deps missing" guard for this asset
condition = rs.AutomationCondition.eager().without("any_deps_missing")

# on_cron, but use a custom dep-update predicate
condition = rs.AutomationCondition.on_cron("0 * * * *").replace(
    "all_deps_updated_since_cron",
    rs.AutomationCondition.all_deps_match(rs.AutomationCondition.newly_updated()),
)
```

### Cross-asset evaluation with `.on_selected()`

```python
# Fire whenever the named upstream feed updates
condition = (
    rs.AutomationCondition.newly_updated().on_selected("upstream_feed")
    & ~rs.AutomationCondition.in_progress()
)
```
