"""Which resources in ``manifest.json`` become outputs, their deps, and the tests on them."""

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from rivers._core.assets import AssetDef
from rivers.integrations.dbt.translator import DbtTranslator

_ASSET_TYPES = ("model", "seed", "snapshot")


@dataclass(frozen=True)
class DbtOutputs:
    """The dbt resources that one ``DbtProject.assets`` definition builds.

    Args:
        names: Output name of each resource, by unique id.
        selectors: The dbt selector of each output, by output name.
        tests: The outputs each dbt test checks, by the test's unique id.
        selection: dbt arguments that select every output.
        exclude: dbt arguments that a run of some outputs keeps: the
            definition's ``--exclude``, which can exclude tests.
    """

    names: dict[str, str]
    selectors: dict[str, str]
    tests: dict[str, tuple[str, ...]]
    selection: tuple[str, ...]
    exclude: tuple[str, ...]


def selection_args(select: str, exclude: str, selector: str) -> tuple[str, ...]:
    """dbt arguments for a node selection."""
    if selector:
        return ("--selector", selector)

    args = ("--select", select or "fqn:*")
    if exclude:
        args += ("--exclude", exclude)
    return args


def build_outputs(
    manifest: dict[str, Any],
    selected: set[str] | None,
    translator: DbtTranslator,
    selection: tuple[str, ...],
    exclude: tuple[str, ...],
) -> tuple[DbtOutputs, list[AssetDef]]:
    """The outputs of the models, seeds, and snapshots in ``selected`` (``None`` for all).

    Ephemeral models build nothing, so the deps of an output skip through them
    to the resources they read. Only tests in ``selected`` check outputs.
    """
    nodes = manifest["nodes"]
    resources = {**manifest.get("sources", {}), **nodes}
    adapter = manifest["metadata"].get("adapter_type")

    names = {
        uid: translator.asset_name(node)
        for uid, node in sorted(nodes.items())
        if _is_asset(node) and (selected is None or uid in selected)
    }

    output_defs = []
    selectors = {}
    for uid, name in names.items():
        node = nodes[uid]
        deps: list[str] = []
        for parent in _upstream(resources, node):
            dep = names.get(parent) or translator.upstream_asset(resources[parent])
            if dep is not None and dep not in deps:
                deps.append(dep)

        metadata = translator.metadata(node)
        if adapter == "duckdb":
            # The table DuckDBIOHandler loads when a downstream asset reads this one.
            metadata = {
                "duckdb/schema": node["schema"],
                "duckdb/table": node["alias"],
                **metadata,
            }

        output_defs.append(
            AssetDef(
                name,
                deps=[AssetDef.dep(dep) for dep in deps],
                group=translator.group(node),
                tags=translator.tags(node) or None,
                kinds=["dbt"],
                code_version=translator.code_version(node),
                metadata=metadata,
            )
        )
        selectors[name] = "fqn:" + ".".join(node["fqn"])

    tests = {}
    for uid, node in nodes.items():
        if node["resource_type"] != "test" or (
            selected is not None and uid not in selected
        ):
            continue
        attached = node.get("attached_node")
        targets = [attached] if attached else node["depends_on"]["nodes"]
        checked = tuple(names[target] for target in targets if target in names)
        if checked:
            tests[uid] = checked

    return DbtOutputs(names, selectors, tests, selection, exclude), output_defs


def _is_asset(node: dict[str, Any]) -> bool:
    return (
        node["resource_type"] in _ASSET_TYPES
        and node["config"].get("materialized") != "ephemeral"
    )


def _upstream(resources: dict[str, Any], node: dict[str, Any]) -> Iterator[str]:
    """The sources and built resources ``node`` reads, past ephemeral models."""
    seen: set[str] = set()
    stack = list(node.get("depends_on", {}).get("nodes", []))
    while stack:
        uid = stack.pop(0)
        if uid in seen or uid not in resources:
            continue
        seen.add(uid)

        parent = resources[uid]
        if parent["resource_type"] == "source" or _is_asset(parent):
            yield uid
        else:
            stack.extend(parent.get("depends_on", {}).get("nodes", []))
