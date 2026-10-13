# dbt

See the [dbt guide](../guides/dbt.md) for examples.

## `DbtProject`

A dbt project. Its manifest defines the assets of `assets()`.

```python
from rivers.integrations.dbt import DbtProject

project = DbtProject(Path(__file__).parent / "analytics")
```

**Constructor:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `project_dir` | `str \| Path` | required | The directory with `dbt_project.yml`. Made absolute. Raises `ValueError` without `dbt_project.yml`. |
| `target_path` | `Path` | `Path("target")` | The directory with `manifest.json`, relative to `project_dir`. |
| `profiles_dir` | `Path \| None` | `None` | The directory with `profiles.yml`, for `prepare()`, selections, and a `DbtResource` made from the project. `None` uses dbt's search. |
| `profile` | `str \| None` | `None` | The profile to use instead of the project's. |
| `target` | `str \| None` | `None` | The target to use instead of the profile's default. |
| `asset_def_fn` | `(DbtNode) -> AssetDef` | `DbtNode.output_def` | Builds the output of each model, seed, and snapshot of the project, also of the ones that a definition does not select. |
| `source_asset_fn` | `(DbtNode) -> str \| None` | `None` | Called with each dbt source that a selected output reads, directly or through ephemeral models. Returns the name of the rivers asset that loads the source, or `None` for no dep. `None` adds no dep for sources. |

An unknown field raises a pydantic `ValidationError`, e.g. Dagster's `state_path`.

### `manifest_path`

`project_dir / target_path / "manifest.json"`.

### `prepare()`

Runs `dbt parse` and replaces the manifest in one step, so processes that read it meanwhile see the old file or the new one. Also replaces `partial_parse.msgpack`. dbt starts from the old one, so it parses only the files that changed. Raises `DbtError` when dbt cannot parse the project.

### `assets()`

```python
project.assets(
    *,
    select: str = "fqn:*",
    exclude: str = "",
    selector: str = "",
    **kwargs,
) -> Callable[[Callable], MultiAsset]
```

A decorator that builds a multi-asset with one output per selected model, seed, and snapshot. The project's `asset_def_fn` builds each output. rivers adds a lineage-only dep on each model, seed, and snapshot that the output refs, by that resource's output name, and on the asset that `source_asset_fn` names for each source that it reads. The deps that `asset_def_fn` returns stay. The body must be a generator. `kwargs` go to [`Asset.from_multi`](assets.md#assetfrom_multi).

| Parameter | Description |
|-----------|-------------|
| `select` | dbt's `--select`. A selection other than the default runs `dbt ls` once per manifest; the result is cached in the target directory. `dbt ls` needs the profile, like `dbt parse`, but does not connect to the warehouse. See [Select resources](../guides/dbt.md#select-resources). |
| `exclude` | dbt's `--exclude`. |
| `selector` | A selector from `selectors.yml`. Raises `ValueError` with `select` or `exclude`. |

Raises `FileNotFoundError` when the manifest does not exist, and `DbtError` when `dbt ls` fails, e.g. without the profile. Raises `ValueError` when the selection has no model, seed, or snapshot, when an output has no name, or when two resources of the project get the same output name.

When the body runs, each argument that is a `DbtResource` is bound to the outputs, so its `cli(args, context)` selects them and yields their materializations. Its commands start from the `partial_parse.msgpack` in the project's `target_path`. The body must take the resource as a parameter: a body that makes its own `DbtResource` instead raises `TypeError`.

**Definition metadata:**

| Key | Description |
|-----|-------------|
| `dbt/unique_id` | The resource's unique id, e.g. `model.shop.customers`. |
| `dbt/relation` | The relation, e.g. `"shop"."main"."customers"`. |
| `dbt/materialized` | `table`, `view`, `incremental`, `seed`, `snapshot`, ... |
| `dbt/description` | The resource's description, if it has one. |
| `duckdb/schema`, `duckdb/table` | With dbt-duckdb: the table, for [`DuckDBIOHandler`](duckdb.md). rivers adds them to the metadata of the output; keys that `asset_def_fn` sets stay. |

The `dbt/` keys are the defaults of `DbtNode.output_def`.

---

## `DbtNode`

One model, seed, snapshot, or source of the manifest, as `asset_def_fn` and `source_asset_fn` get it. A frozen pydantic model.

| Field | Type | Description |
|-------|------|-------------|
| `unique_id` | `str` | dbt's id, e.g. `model.shop.customers`. |
| `name` | `str` | The resource name. |
| `resource_type` | `str` | `"model"`, `"seed"`, `"snapshot"`, or `"source"`. |
| `props` | `dict` | The resource's entry in `manifest.json`, e.g. `props["config"]`, `props["meta"]`, or `props["source_name"]`. |

### `asset_name`

The default asset name: `name`, with `_v<version>` for a versioned model, e.g. `orders_v2`.

### `output_def(**fields)`

Returns the default output with `fields` set or replaced. The keyword arguments go to [`AssetDef`](assets.md). It has no deps: `assets()` adds them.

| Field | Default |
|-------|---------|
| `name` | `asset_name` |
| `group` | The dbt group. `None` keeps the definition's group. |
| `tags` | The dbt tags, or `None`. |
| `kinds` | `["dbt"]` |
| `code_version` | `props["checksum"]["checksum"]` |
| `metadata` | `dbt/unique_id`, `dbt/relation`, `dbt/materialized`, and `dbt/description`, each when it has a value. |

```python
DbtProject(
    Path(__file__).parent / "analytics",
    asset_def_fn=lambda node: node.output_def(name=f"analytics_{node.asset_name}", pool="warehouse"),
)
```

---

## `DbtResource`

A [resource](../concepts/resources.md) that runs dbt commands.

```python
from rivers.integrations.dbt import DbtResource

dbt = DbtResource(project_dir=project)
```

With a `DbtProject` as `project_dir`, each of `profiles_dir`, `profile`, and `target` that you do not give takes the project's value, when it is not `None`. Fields that still have no value are read from `DBT_PROJECT_DIR`, `DBT_PROFILES_DIR`, `DBT_PROFILE`, `DBT_TARGET`, and `DBT_EXECUTABLE`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `project_dir` | `Path \| DbtProject` | required | The directory with `dbt_project.yml`, or a `DbtProject`. The field holds the directory. |
| `profiles_dir` | `Path \| None` | `None` | The directory with `profiles.yml`. |
| `profile` | `str \| None` | `None` | Passed as `--profile`. |
| `target` | `str \| None` | `None` | Passed as `--target`. |
| `executable` | `str \| None` | `None` | The dbt executable. `None` runs the dbt-core installed with rivers. |

### `cli()`

```python
dbt.cli(
    args: Sequence[str],
    context: AssetExecutionContext | None = None,
    *,
    target_path: str | Path | None = None,
) -> DbtInvocation
```

A dbt command, not started. With the context of a `DbtProject.assets()` body:

- dbt runs the outputs that the run selected: the definition's selection for all of them, or `--select` with the fqn of each and the definition's `--exclude`. A `--select`, `--exclude`, or `--selector` in `args` replaces it.
- Iteration yields a `Materialization` for each output that dbt builds (status `success`, `no-op`, or `reused`).
- In a `build`, an output with tests is yielded when its tests finish, and not at all when one of them has status `fail` or `error`.

dbt's log goes to `context.log`, or to the `rivers.integrations.dbt.resource` logger without a context. `target_path` keeps dbt's artifacts there; by default they go to a temporary directory that is removed.

**Materialization metadata:**

| Key | Type | Description |
|-----|------|-------------|
| `dbt/invocation_id` | `Text` | dbt's id for the command. |
| `dbt/execution_time` | `Duration` | dbt's execution time for the resource. |
| `dbt/message` | `Text` | The adapter's message. |
| `dbt/rows_affected` | `Int` | When the adapter reports it. |
| `dbt/tests` | `Json` | Test name to status (`pass`, `warn`, `skipped`) for the tests on the output. |

---

## `DbtInvocation`

One dbt command, made by `DbtResource.cli()`. It runs once.

| Method | Description |
|--------|-------------|
| `__iter__()` | Runs dbt and yields a `Materialization` per output. After the last one, raises `DbtError` when dbt failed. |
| `wait()` | Runs dbt to the end. Raises `DbtError` when dbt failed. |

---

## `DbtError`

Raised when a dbt command fails. The message names the command and its exit code, then each failed or skipped resource with dbt's message, or dbt's error lines.
