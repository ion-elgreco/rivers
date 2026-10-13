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
    translator: DbtTranslator | None = None,
    **kwargs,
) -> Callable[[Callable], MultiAsset]
```

A decorator that builds a multi-asset with one output per selected model, seed, and snapshot. The outputs have the kind `dbt`. The body must be a generator. `kwargs` go to [`Asset.from_multi`](assets.md#assetfrom_multi).

| Parameter | Description |
|-----------|-------------|
| `select` | dbt's `--select`. A selection other than the default runs `dbt ls` once per manifest; the result is cached in the target directory. |
| `exclude` | dbt's `--exclude`. |
| `selector` | A selector from `selectors.yml`. Raises `ValueError` with `select` or `exclude`. |
| `translator` | A `DbtTranslator`. Default: `DbtTranslator()`. |

Raises `FileNotFoundError` when the manifest does not exist, `ValueError` when the selection has no model, seed, or snapshot, and `DbtError` when `dbt ls` fails.

When the body runs, each argument that is a `DbtResource` is bound to the outputs, so its `cli(args, context)` selects them and yields their materializations. Its commands start from the `partial_parse.msgpack` in the project's `target_path`. The body must take the resource as a parameter: a body that makes its own `DbtResource` instead raises `TypeError`.

**Definition metadata:**

| Key | Description |
|-----|-------------|
| `dbt/unique_id` | The resource's unique id, e.g. `model.shop.customers`. |
| `dbt/relation` | The relation, e.g. `"shop"."main"."customers"`. |
| `dbt/materialized` | `table`, `view`, `incremental`, `seed`, `snapshot`, ... |
| `dbt/description` | The resource's description, if it has one. |
| `duckdb/schema`, `duckdb/table` | With dbt-duckdb: the table, for [`DuckDBIOHandler`](duckdb.md). |

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

## `DbtTranslator`

Maps a dbt resource to the settings of its asset. Each method gets the resource's entry in `manifest.json`.

| Method | Returns | Default |
|--------|---------|---------|
| `asset_name(node)` | `str` | `node["name"]`, with `_v<version>` for a versioned model. |
| `group(node)` | `str \| None` | The dbt group. `None` keeps the definition's group. |
| `tags(node)` | `list[str]` | The dbt tags. |
| `metadata(node)` | `dict[str, str]` | The definition metadata above, without the `duckdb/` keys. |
| `code_version(node)` | `str \| None` | `node["checksum"]["checksum"]`. |
| `upstream_asset(node)` | `str \| None` | For a source or a resource outside the selection: the asset name of its dep, or `None` for no dep. Default: `None` for a source, `asset_name(node)` otherwise. |

---

## `DbtError`

Raised when a dbt command fails. The message names the command and its exit code, then each failed or skipped resource with dbt's message, or dbt's error lines.
