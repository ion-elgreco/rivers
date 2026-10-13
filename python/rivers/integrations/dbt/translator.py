"""How dbt resources become rivers assets."""

from typing import Any

__all__ = ["DbtTranslator"]


class DbtTranslator:
    """Maps a dbt resource to the settings of its asset.

    Override a method in a subclass to change one setting. Each method gets the
    resource's entry in ``manifest.json``, e.g. ``node["name"]``,
    ``node["config"]``, or ``node["meta"]``.

    Example::

        class Translator(DbtTranslator):
            def upstream_asset(self, node):
                if node["resource_type"] == "source":
                    return node["name"]  # the rivers asset that loads the table
                return super().upstream_asset(node)
    """

    def asset_name(self, node: dict[str, Any]) -> str:
        """The asset name: the resource name, with ``_v<version>`` for a versioned model."""
        version = node.get("version")
        return node["name"] if version is None else f"{node['name']}_v{version}"

    def group(self, node: dict[str, Any]) -> str | None:
        """The asset group: the resource's dbt group. ``None`` keeps the group of the definition."""
        return node.get("group")

    def tags(self, node: dict[str, Any]) -> list[str]:
        """The asset tags: the resource's dbt tags."""
        return list(node.get("tags") or [])

    def metadata(self, node: dict[str, Any]) -> dict[str, str]:
        """Definition metadata: ``dbt/unique_id``, ``dbt/relation``, ``dbt/materialized``, and ``dbt/description``."""
        metadata = {"dbt/unique_id": node["unique_id"]}
        if relation := node.get("relation_name"):
            metadata["dbt/relation"] = relation
        if materialized := (node.get("config") or {}).get("materialized"):
            metadata["dbt/materialized"] = materialized
        if description := node.get("description"):
            metadata["dbt/description"] = description
        return metadata

    def code_version(self, node: dict[str, Any]) -> str | None:
        """The code version: the checksum dbt records for the resource's file."""
        return (node.get("checksum") or {}).get("checksum") or None

    def upstream_asset(self, node: dict[str, Any]) -> str | None:
        """The asset that produces an upstream resource these assets do not build.

        Called for dbt sources, and for models, seeds, and snapshots outside the
        selection. ``None`` drops the dependency.

        Returns:
            ``None`` for a source; :meth:`asset_name` for other resources.
        """
        if node["resource_type"] == "source":
            return None
        return self.asset_name(node)
