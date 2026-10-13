# dlt

`rivers.integrations.dlt` (`pip install rivers[dlt]`). See the [guide](../guides/dlt.md).

## `DltSourceAssets`

A [dlt](https://dlthub.com/docs) source and the rivers output of each of its selected resources.

```python
from rivers.integrations.dlt import DltSourceAssets

github = DltSourceAssets(github_source(), asset_def_fn=None)
```

**Constructor:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source` | `DltSource` | required | The dlt source, e.g. `github_source()`. Each selected resource becomes an output. `load` loads this source when it gets no other. |
| `asset_def_fn` | `(DltResource) -> AssetDef` | `output_def` | Builds the output of each resource. |

Raises `TypeError` when `source` is not a dlt source, e.g. the source function instead of the source it returns. Raises `ValueError` when the source has no selected resources, when an output has no name, or when two resources get the same output name.

**Attributes:**

| Attribute | Type | Description |
|-----------|------|-------------|
| `source` | `DltSource` | The dlt source. |
| `resources` | `dict[str, DltResource]` | The dlt resource of each output, by output name, in source order. |
| `output_defs` | `list[AssetDef]` | The outputs, for `Asset.from_multi`. |

### `assets(**kwargs)`

Decorator. Builds a [`MultiAsset`](assets.md) with one output per resource. The keyword arguments go to `Asset.from_multi`, e.g. `name`, `group`, `partitions_def`, `io_handler`, `hooks`, or `retry`.

```python
@github.assets(name="github")
def github_assets(context: rs.AssetExecutionContext):
    pipeline = dlt.pipeline("github", destination="duckdb", dataset_name="github")
    yield from github.load(context, pipeline)
```

### `load(context, pipeline, source=None)`

Loads the resources of `context.output_selection` with one `pipeline.run`, and yields a `Materialization` for each. dlt's load summary goes to `context.log`.

| Parameter | Type | Description |
|-----------|------|-------------|
| `context` | `AssetExecutionContext` | The context of the multi-asset. |
| `pipeline` | `dlt.Pipeline` | The pipeline that loads the source. |
| `source` | `DltSource \| None` | The source to load, e.g. built from the partition. Defaults to the source of `DltSourceAssets`. |

A run:

1. Drops the pipeline's pending load packages. With `restore_from_destination` on, it drops all local state, which dlt restores from the destination.
2. Runs `pipeline.run(source.with_resources(...))` with the resources of the selected outputs.
3. Yields a `Materialization` per resource whose tables have no failed load job, then raises dlt's `DestinationHasFailedJobs` when a job failed.

Raises `TypeError` when `pipeline` is not a dlt pipeline or `source` is not a dlt source, and `ValueError` when `source` has no resource for a selected output. dlt's own errors, such as `PipelineStepFailed`, are not changed.

**Output metadata:**

| Key | Type | Description |
|-----|------|-------------|
| `dlt/load_id` | `str` | The load package of the run. Not set when the run extracted nothing. |
| `dlt/dataset` | `str` | The dataset of the tables. |
| `dlt/rows_loaded` | `int` | Rows loaded into the resource's root tables in this run. |
| `dlt/row_counts` | `MetadataValue.json` | Rows loaded per table, nested tables included. |
| `dlt/columns` | `MetadataValue.json` | Column name to dlt data type, per table of the resource. |

---

## `output_def(resource, **fields)`

Returns the default output of a dlt resource, with `fields` set or replaced. The keyword arguments go to [`AssetDef`](assets.md).

| Field | Default |
|-------|---------|
| `name` | `<source>_<resource>`, e.g. `github_issues` |
| `kinds` | `["dlt"]` |
| `metadata` | `{"dlt/resource": <resource name>, "dlt/table": <table name>}`; the table name is the resource name when the resource sets it in the run |

```python
DltSourceAssets(
    github_source(),
    asset_def_fn=lambda resource: output_def(resource, group="raw", pool="github"),
)
```
