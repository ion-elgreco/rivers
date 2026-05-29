"""Delta Lake IO handler — read/write asset outputs as Delta tables.

Optional dependency: requires ``deltalake`` and at least one of ``pyarrow``
or ``polars`` to be installed. Type-specific conversion is delegated to
:class:`DeltaTypeHandler` implementations registered in
:func:`DeltaIOHandler.type_handlers`.
"""

from __future__ import annotations

import json
import time
from typing import Any, Literal

from deltalake import CommitProperties, DeltaTable, WriterProperties, write_deltalake
from deltalake.exceptions import TableNotFoundError
from pydantic_settings import SettingsConfigDict

from rivers._core import InputContext, OutputContext
from rivers.io_handlers.base import BaseIOHandler
from rivers.io_handlers.delta.base import DeltaTypeHandler
from rivers.io_handlers.delta.config import (
    MergeConfig,
    MergeOperationsConfig,
    PartitionExpr,
    WhenMatchedDelete,
    WhenMatchedUpdate,
    WhenMatchedUpdateAll,
    WhenNotMatchedBySourceDelete,
    WhenNotMatchedBySourceUpdate,
    WhenNotMatchedInsert,
    WhenNotMatchedInsertAll,
)
from rivers.io_handlers.delta.merge import _merge_execute
from rivers.io_handlers.delta.predicate import _build_predicate, _resolve_partition_expr

__all__: list[str] = [
    "DeltaIOHandler",
    "DeltaTypeHandler",
    "MergeConfig",
    "MergeOperationsConfig",
    "PartitionExpr",
    "WhenMatchedDelete",
    "WhenMatchedUpdate",
    "WhenMatchedUpdateAll",
    "WhenNotMatchedBySourceDelete",
    "WhenNotMatchedBySourceUpdate",
    "WhenNotMatchedInsert",
    "WhenNotMatchedInsertAll",
]


# Keyed by the type's top-level module rather than its class name: ``DataFrame``
# is shared by pandas, polars and datafusion, so a name-based map can't tell them
# apart when suggesting the right install extra.
_MODULE_TO_EXTRA: dict[str, str] = {
    "pyarrow": "pyarrow",
    "polars": "polars",
    "pandas": "pandas",
    "datafusion": "datafusion",
}


def _build_type_handler_map() -> dict[type, DeltaTypeHandler]:
    """Best-effort discovery of installed type handlers (pyarrow, polars, pandas).

    Returns a mapping from supported Python type to its handler. Missing optional
    dependencies are silently skipped — the handler is only registered when its
    backing library can be imported.
    """
    handlers: dict[type, DeltaTypeHandler] = {}
    for mod_path, cls_name in [
        ("rivers.io_handlers.delta.pyarrow", "PyArrowTypeHandler"),
        ("rivers.io_handlers.delta.polars", "PolarsTypeHandler"),
        ("rivers.io_handlers.delta.datafusion", "DataFusionTypeHandler"),
        ("rivers.io_handlers.delta.pandas", "PandasTypeHandler"),
    ]:
        try:
            mod = __import__(mod_path, fromlist=[cls_name])
            h = getattr(mod, cls_name)()
            for t in h.supported_types:
                handlers[t] = h
        except ImportError:
            pass
    return handlers


DeltaWriteMode = Literal[
    "overwrite", "append", "error", "ignore", "merge", "create_or_replace"
]
DeltaSchemaMode = Literal["overwrite", "merge"]


class DeltaIOHandler(BaseIOHandler):
    """Persist asset outputs as Delta Lake tables.

    Each asset becomes a sub-table at ``{table_uri}/{asset_name}`` (override the
    leaf name with the ``delta/root_name`` asset metadata key). Per-asset
    overrides through ``asset_metadata`` may set ``delta/mode``,
    ``delta/schema_mode``, ``delta/partition_expr``, ``delta/columns``,
    ``delta/writer_properties``, ``delta/commit_properties``,
    ``delta/table_configuration``, ``delta/merge_predicate``, and
    ``delta/version`` (read).
    """

    model_config = SettingsConfigDict(arbitrary_types_allowed=True)

    table_uri: str
    mode: DeltaWriteMode = "overwrite"
    schema_mode: DeltaSchemaMode | None = None
    storage_options: dict[str, str] | None = None
    writer_properties: WriterProperties | None = None
    commit_properties: CommitProperties | None = None
    table_config: dict[str, str] | None = None
    merge_config: MergeConfig | None = None

    @staticmethod
    def type_handlers() -> dict[type, DeltaTypeHandler]:
        """Return the currently registered ``{type: DeltaTypeHandler}`` map."""
        return _build_type_handler_map()

    def _resolve_handler(self, target_type: type) -> DeltaTypeHandler:
        """Look up the type handler for ``target_type`` or raise ``TypeError``.

        Suggests the right install extra (e.g. ``rivers[polars]``) when a known
        type is requested but its handler library is not installed.
        """
        handler = self.type_handlers().get(target_type)
        if handler is not None:
            return handler
        extra = _MODULE_TO_EXTRA.get(target_type.__module__.split(".", 1)[0])
        if extra:
            raise TypeError(
                f"No handler for {target_type.__name__}. "
                f"Install the required extra: pip install rivers[{extra}]"
            )
        raise TypeError(
            f"No type handler found for {target_type!r}. "
            f"Supported types: {list(self.type_handlers())}"
        )

    def _asset_uri(self, asset_name: str) -> str:
        """Compose the table URI used for ``asset_name``."""
        return f"{self.table_uri}/{asset_name}"

    def handle_output(self, context: OutputContext, obj: object) -> None:
        """Write ``obj`` as a Delta commit and record table-level metadata.

        Honors per-asset metadata overrides for write mode, schema evolution,
        partition expression, writer/commit properties and table configuration.
        For partitioned overwrites a partition predicate is built so only the
        targeted partition is replaced; for ``mode='merge'`` the merge is
        executed via :func:`_merge_execute`. Always records ``delta/num_rows``,
        ``delta/size_bytes``, ``delta/version`` and the Arrow schema.
        """
        meta = context.asset_metadata or {}
        mode: DeltaWriteMode = meta.get("delta/mode", self.mode)  # type: ignore[assignment]
        schema_mode: DeltaSchemaMode | None = meta.get(
            "delta/schema_mode", self.schema_mode
        )  # type: ignore[assignment]
        partition_expr = _resolve_partition_expr(meta)

        partition_by = (
            partition_expr.partition_columns
            if partition_expr and context.partition
            else None
        )
        table_name = meta.get("delta/root_name", context.asset_name)
        uri = self._asset_uri(table_name)

        predicate: str | None = None
        if context.partition is not None and mode == "overwrite":
            predicate = _build_predicate(context.partition, partition_expr)
            try:
                DeltaTable(uri, storage_options=self.storage_options)
            except TableNotFoundError:
                predicate = None

        data = self._resolve_handler(type(obj)).to_arrow(obj)

        # Resolve table configuration (IO manager default + per-asset override)
        table_cfg = dict(self.table_config) if self.table_config else {}
        if "delta/table_configuration" in meta:
            table_cfg.update(json.loads(meta["delta/table_configuration"]))
        table_configuration = table_cfg or None

        # Resolve writer/commit properties (per-asset override > handler default)
        writer_properties = self.writer_properties
        if "delta/writer_properties" in meta:
            writer_properties = WriterProperties(
                **json.loads(meta["delta/writer_properties"])
            )

        commit_properties = self.commit_properties
        if "delta/commit_properties" in meta:
            commit_properties = CommitProperties(
                **json.loads(meta["delta/commit_properties"])
            )

        merge_stats: dict[str, Any] | None = None
        start = time.monotonic()
        if mode == "merge":
            merge_predicate_override = meta.get("delta/merge_predicate")
            merge_stats = _merge_execute(
                uri=uri,
                data=data,
                merge_config=self.merge_config,
                partition_predicate=predicate,
                merge_predicate_override=merge_predicate_override,
                storage_options=self.storage_options,
                writer_properties=writer_properties,
                commit_properties=commit_properties,
            )
        elif mode == "create_or_replace":
            DeltaTable.create(
                table_uri=uri,
                schema=data.schema,
                mode="overwrite",
                partition_by=partition_by,
                configuration=table_configuration,
                storage_options=self.storage_options,
                commit_properties=commit_properties,
            )
            write_deltalake(
                uri,
                data,
                mode="append",
                storage_options=self.storage_options,
                writer_properties=writer_properties,  # type: ignore[arg-type]
                commit_properties=commit_properties,
            )
        else:
            write_deltalake(
                uri,
                data,
                mode=mode,  # type: ignore[arg-type]
                schema_mode=schema_mode,
                partition_by=partition_by,
                predicate=predicate,
                storage_options=self.storage_options,
                configuration=table_configuration,
                writer_properties=writer_properties,  # type: ignore[arg-type]
                commit_properties=commit_properties,
            )
        duration = time.monotonic() - start

        dt = DeltaTable(uri, storage_options=self.storage_options)
        actions = dt.get_add_actions(flatten=True)
        num_rows = sum(actions.column("num_records").to_pylist())
        size_bytes = sum(actions.column("size_bytes").to_pylist())

        # Build schema metadata from Delta table schema
        import pyarrow as pa

        from rivers import MetadataValue

        arrow_schema = pa.schema(dt.schema().to_arrow())

        output_meta: dict[str, Any] = {
            "delta/table_uri": uri,
            "delta/mode": mode,
            "delta/num_rows": num_rows,
            "delta/size_bytes": size_bytes,
            "delta/write_duration_s": round(duration, 6),
            "delta/version": dt.version(),
            "rivers/schema": MetadataValue.schema(arrow_schema),
        }
        if merge_stats is not None:
            output_meta["delta/num_output_rows"] = merge_stats.get("num_output_rows", 0)
            output_meta["delta/merge_stats"] = json.dumps(merge_stats)

        context.add_output_metadata(output_meta)

    def load_input(self, context: InputContext) -> Any:
        """Read the upstream Delta table into the requested Python type.

        Resolves the partition predicate, optional column projection and
        optional time-travel ``version`` from ``context.asset_metadata``. The
        actual decode is delegated to the type handler matching
        ``context.type_hint`` (which must be set on the consumer side).
        """
        meta = context.asset_metadata or {}
        partition_expr = _resolve_partition_expr(meta)
        predicate = (
            _build_predicate(context.partition, partition_expr)
            if context.partition is not None
            else None
        )
        table_name = meta.get("delta/root_name", context.asset_name)
        uri = self._asset_uri(table_name)

        columns: list[str] | None = None
        raw_columns = meta.get("delta/columns")
        if raw_columns is not None:
            columns = json.loads(raw_columns)

        version: int | None = None
        raw_version = meta.get("delta/version")
        if raw_version is not None:
            version = int(raw_version)

        if context.type_hint is None:
            raise TypeError(
                "No type_hint provided on InputContext. "
                f"Supported types: {list(self.type_handlers())}"
            )
        handler = self._resolve_handler(context.type_hint)
        return handler.load_input(
            uri,
            table_name,
            self.storage_options,
            predicate,
            context.type_hint,
            columns=columns,
            version=version,
        )
