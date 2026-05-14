# Hooks

Hooks run after an asset step succeeds or fails. Hook errors are logged but never fail the step.

## `Hook.success`

Runs after an asset step completes successfully. Can be used as a bare decorator or with a custom name.

```python
import rivers as rs

@rs.Hook.success
def log_success(context: rs.HookContext):
    print(f"{context.asset_name} succeeded with output: {context.output}")

@rs.Hook.success(name="notify")
def send_notification(context: rs.HookContext):
    notify(f"Asset {context.asset_name} completed in run {context.run_id}")
```

## `Hook.failure`

Runs after an asset step fails.

```python
@rs.Hook.failure
def log_failure(context: rs.HookContext):
    print(f"{context.asset_name} failed: {context.error}")

@rs.Hook.failure(name="alert")
def send_alert(context: rs.HookContext):
    alert(f"Asset {context.asset_name} failed: {context.error}")
```

## Attaching hooks to assets

Pass hooks via the `hooks` parameter on any asset type:

```python
@rs.Asset(hooks=[log_success, send_alert])
def my_asset() -> int:
    return 42

# Also works with multi and graph assets
multi = rs.Asset.from_multi(
    output_defs=[rs.AssetDef(name="a"), rs.AssetDef(name="b")],
    hooks=[log_success],
)(my_multi_fn)
```

---

## `HookContext`

Context object passed to hook functions.

**Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `asset_name` | `str` | Name of the asset that triggered the hook. |
| `run_id` | `str` | ID of the current run. |
| `hook_type` | `str` | Either `"success"` or `"failure"`. |
| `output` | `Any \| None` | The asset's output value (success hooks only). |
| `error` | `str \| None` | Error message (failure hooks only). |
| `metadata` | `dict[str, str] \| None` | Asset metadata if available. |
| `config` | `ConfigT` | Config instance (if the asset uses a config type hint). |

---

## `Hook`

**Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `name` | `str` | Hook name (defaults to function name). |

**Static methods:**

| Method | Description |
|--------|-------------|
| `Hook.success(func=None, *, name=None)` | Create a success hook. |
| `Hook.failure(func=None, *, name=None)` | Create a failure hook. |

---

## Behavior

- Success hooks receive the asset's return value in `context.output`
- Failure hooks receive the error message in `context.error`
- If a hook itself raises an exception, the error is printed to stderr but does **not** fail the asset step
- Hooks run in the order they are listed in the `hooks` parameter
- Only `Asset` nodes support hooks; `Task` and `BashTask` do not
