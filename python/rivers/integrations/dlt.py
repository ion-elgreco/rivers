"""dlt integration: load a dlt source as a multi-asset with one output per resource.

Install with ``pip install rivers[dlt]``.

Example::

    import dlt
    import rivers as rs
    from rivers.integrations.dlt import DltSourceAssets

    @dlt.source
    def github_source(): ...

    github = DltSourceAssets(github_source())

    @github.assets(name="github")
    def github_assets(context: rs.AssetExecutionContext):
        pipeline = dlt.pipeline("github", destination="duckdb", dataset_name="github")
        yield from github.load(context, pipeline)

    repo = rs.CodeRepository(assets=[github_assets])
"""

import json
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any

from rivers._core import Materialization, MetadataValue
from rivers._core.assets import Asset, AssetDef, AssetExecutionContext, MultiAsset

try:
    import dlt
    from dlt.common.schema.utils import group_tables_by_resource
    from dlt.extract import DltResource, DltSource
except ImportError as e:
    raise ImportError(
        "rivers.integrations.dlt needs dlt: pip install rivers[dlt]"
    ) from e

__all__ = ["DltSourceAssets", "output_def"]


def output_def(resource: DltResource, **fields: Any) -> AssetDef:
    """The default output of a dlt resource, with ``fields`` set or replaced.

    Defaults: ``name`` is ``<source>_<resource>``, e.g. ``"github_issues"``,
    ``kinds`` is ``"dlt"``, and ``metadata`` is ``{"dlt/resource": <resource
    name>, "dlt/table": <table name>}``; the table name is the resource name
    when the resource sets it at run time. Other keyword arguments go to
    :class:`~rivers.AssetDef`, e.g. ``group="raw"``, ``pool="github"``, or
    ``deps=[rs.AssetDef.dep("crm_export")]``.

    Returns:
        The output definition.
    """
    table = resource.table_name
    defaults: dict[str, Any] = {
        "name": f"{resource.source_name}_{resource.name}",
        "kinds": "dlt",
        "metadata": {
            "dlt/resource": resource.name,
            "dlt/table": table if isinstance(table, str) else resource.name,
        },
    }
    return AssetDef(**(defaults | fields))


class DltSourceAssets:
    """A dlt source and the rivers output of each of its selected resources.

    Args:
        source: The dlt source, e.g. ``github_source()``. :meth:`load` loads
            it when it gets no other source.
        asset_def_fn: Builds a resource's output definition. Defaults to
            :func:`output_def`, which declares no deps: return
            ``output_def(resource, deps=[...])`` to link a resource to the
            asset that holds its source data.

    Attributes:
        source: The dlt source.
        resources: Each output's dlt resource, by output name, in source order.
        output_defs: The output definitions, for ``Asset.from_multi``.

    Raises:
        TypeError: If ``source`` is not a dlt source.
        ValueError: If the source has no selected resources, an output has no
            name, or two resources get the same output name.
    """

    def __init__(
        self,
        source: DltSource,
        asset_def_fn: Callable[[DltResource], AssetDef] | None = None,
    ) -> None:
        if not isinstance(source, DltSource):
            raise TypeError(
                f"source must be a dlt source, e.g. github_source(), "
                f"got {type(source).__name__}"
            )

        make_def = asset_def_fn or output_def
        self.source = source
        self.resources: dict[str, DltResource] = {}
        self.output_defs: list[AssetDef] = []
        for resource in source.selected_resources.values():
            output = make_def(resource)
            if not output.name:
                raise ValueError(
                    f"the output of dlt resource {resource.name!r} has no name"
                )
            if output.name in self.resources:
                raise ValueError(
                    f"dlt resources {self.resources[output.name].name!r} and "
                    f"{resource.name!r} both map to the output {output.name!r}; "
                    "pass an asset_def_fn that names them apart"
                )

            self.resources[output.name] = resource
            self.output_defs.append(output)

        if not self.resources:
            raise ValueError(
                f"the dlt source {source.name!r} has no selected resources"
            )

    def assets(self, **kwargs: Any) -> Callable[[Callable[..., Any]], MultiAsset]:
        """Decorator: a multi-asset with one output per resource.

        The function loads the source, normally with
        ``yield from source_assets.load(context, pipeline)``.

        Args:
            **kwargs: Passed to :meth:`~rivers.Asset.from_multi`, e.g. ``name``,
                ``group``, ``partitions_def``, ``io_handler``, ``hooks``, or
                ``retry``.

        Returns:
            A decorator that builds the :class:`~rivers.MultiAsset`.
        """
        return Asset.from_multi(None, output_defs=self.output_defs, **kwargs)

    def load(
        self,
        context: AssetExecutionContext,
        pipeline: dlt.Pipeline,
        source: DltSource | None = None,
    ) -> Iterator[Materialization]:
        """Load the resources of ``context.output_selection`` with one ``pipeline.run``.

        Before the run, the pipeline aborts the load packages that a failed run
        left, and dlt rewinds the pipeline state to before them, so the run
        extracts their data again. dlt's load summary goes to ``context.log``.

        Args:
            context: The multi-asset's context.
            pipeline: The dlt pipeline that loads the source.
            source: The source to load, e.g. built from ``context.partition_key``.
                Defaults to :attr:`source`.

        Yields:
            A :class:`~rivers.Materialization` per selected resource whose
            tables have no failed load job.

        Raises:
            TypeError: If ``pipeline`` is not a dlt pipeline, or ``source`` is
                not a dlt source.
            ValueError: If ``source`` has no resource for a selected output.
            PipelineStepFailed: If dlt's extract, normalize, or load step fails.
            DestinationHasFailedJobs: If a load job failed while dlt's
                ``load.raise_on_failed_jobs`` is off. The resources that loaded
                keep their materialization.
        """
        source = self.source if source is None else source
        if not isinstance(pipeline, dlt.Pipeline):
            raise TypeError(
                f"pipeline must be a dlt pipeline, e.g. dlt.pipeline(...), "
                f"got {type(pipeline).__name__}"
            )
        if not isinstance(source, DltSource):
            raise TypeError(f"source must be a dlt source, got {type(source).__name__}")

        selected = {
            output: self.resources[output].name for output in context.output_selection
        }
        missing = set(selected.values()) - set(source.resources)
        if missing:
            raise ValueError(
                f"the dlt source {source.name!r} has no resources {sorted(missing)}"
            )

        pipeline.abort_packages()
        load_info = pipeline.run(source.with_resources(*selected.values()))
        context.log.info("%s", load_info)

        failed_tables = {
            job.job_file_info.table_name
            for package in load_info.load_packages
            for job in package.jobs["failed_jobs"]
        }
        tables = group_tables_by_resource(pipeline.schemas[source.schema.name].tables)
        trace = pipeline.last_trace
        normalize_info = trace.last_normalize_info if trace else None
        row_counts = normalize_info.row_counts if normalize_info else {}
        common: dict[str, Any] = {"dlt/dataset": pipeline.dataset_name}
        if load_info.loads_ids:
            common["dlt/load_id"] = load_info.loads_ids[-1]

        for output, resource in selected.items():
            resource_tables = tables.get(resource, [])
            if failed_tables.isdisjoint(table.get("name") for table in resource_tables):
                yield Materialization(
                    output_name=output,
                    metadata={
                        **common,
                        **_tables_metadata(resource_tables, row_counts),
                    },
                )

        load_info.raise_on_failed_jobs()


def _tables_metadata(
    tables: Sequence[Mapping[str, Any]], row_counts: Mapping[str, int]
) -> dict[str, Any]:
    """Rows and columns of a resource's tables: its root tables and their nested tables."""
    counts = {
        table["name"]: row_counts[table["name"]]
        for table in tables
        if table["name"] in row_counts
    }
    columns = {
        table["name"]: {
            name: column["data_type"]
            for name, column in table["columns"].items()
            if "data_type" in column
        }
        for table in tables
    }
    roots = [table["name"] for table in tables if not table.get("parent")]

    return {
        "dlt/rows_loaded": sum(counts.get(root, 0) for root in roots),
        "dlt/row_counts": MetadataValue.json(json.dumps(counts)),
        "dlt/columns": MetadataValue.json(json.dumps(columns)),
    }
