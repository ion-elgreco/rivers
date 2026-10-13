"""One resource of a dbt manifest, and its default output."""

from typing import Any

from pydantic import BaseModel, ConfigDict

from rivers._core.assets import AssetDef

__all__ = ["DbtNode"]


class DbtNode(BaseModel):
    """A model, seed, snapshot, or source of a dbt project.

    :class:`DbtProject` passes one to its ``asset_def_fn`` and
    ``source_asset_fn``.

    Args:
        unique_id: dbt's id for the resource, e.g. ``"model.shop.customers"``.
        name: The resource name, e.g. ``"customers"``.
        resource_type: ``"model"``, ``"seed"``, ``"snapshot"``, or ``"source"``.
        props: The resource's entry in ``manifest.json``, e.g.
            ``node.props["config"]``, ``node.props["meta"]``, or
            ``node.props["source_name"]``.

    Example::

        project = DbtProject(
            Path(__file__).parent / "analytics",
            asset_def_fn=lambda node: node.output_def(name=f"analytics_{node.asset_name}"),
        )
    """

    model_config = ConfigDict(frozen=True)

    unique_id: str
    name: str
    resource_type: str
    props: dict[str, Any]

    @property
    def asset_name(self) -> str:
        """The default asset name: :attr:`name`, with ``_v<version>`` for a versioned model."""
        version = self.props.get("version")
        return self.name if version is None else f"{self.name}_v{version}"

    def output_def(self, **fields: Any) -> AssetDef:
        """The resource's default output, with ``fields`` set or replaced.

        Defaults: ``name`` is :attr:`asset_name`, ``group`` is the dbt group
        (``None`` keeps the group of the definition), ``tags`` are the dbt
        tags, ``kinds`` is ``"dbt"``, ``code_version`` is the checksum that
        dbt records for the file, and ``metadata`` has ``dbt/unique_id``,
        ``dbt/relation``, ``dbt/materialized``, and ``dbt/description`` when
        they have values. Other keyword arguments go to
        :class:`~rivers.AssetDef`, e.g. ``pool="warehouse"``.

        The default output has no deps: :meth:`DbtProject.assets` adds a dep
        on each resource that the node refs.

        Returns:
            The output definition.
        """
        metadata = {"dbt/unique_id": self.unique_id}
        if relation := self.props.get("relation_name"):
            metadata["dbt/relation"] = relation
        if materialized := (self.props.get("config") or {}).get("materialized"):
            metadata["dbt/materialized"] = materialized
        if description := self.props.get("description"):
            metadata["dbt/description"] = description

        defaults: dict[str, Any] = {
            "name": self.asset_name,
            "group": self.props.get("group"),
            "tags": self.props.get("tags") or None,
            "kinds": "dbt",
            "code_version": (self.props.get("checksum") or {}).get("checksum") or None,
            "metadata": metadata,
        }
        return AssetDef(**(defaults | fields))
