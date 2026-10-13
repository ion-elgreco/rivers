"""Which resources in ``manifest.json`` become outputs, their deps, and the tests on them."""

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

from rivers._core.assets import AssetDef
from rivers.integrations.dbt.node import DbtNode

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
    asset_def_fn: Callable[[DbtNode], AssetDef],
    source_asset_fn: Callable[[DbtNode], str | None] | None,
    selection: tuple[str, ...],
    exclude: tuple[str, ...],
) -> tuple[DbtOutputs, list[AssetDef]]:
    """The outputs of the models, seeds, and snapshots in ``selected`` (``None`` for all).

    ``asset_def_fn`` builds the output of every model, seed, and snapshot in
    the project, so a dep on one outside the selection has its output name.
    Ephemeral models build nothing, so the deps of an output skip through them
    to the resources they read. Only tests in ``selected`` check outputs.

    Raises:
        ValueError: When an output has no name, or two resources get the same
            output name.
    """
    nodes = manifest["nodes"]
    resources = {**manifest.get("sources", {}), **nodes}
    adapter = manifest["metadata"].get("adapter_type")

    defs: dict[str, AssetDef] = {}
    owners: dict[str, str] = {}
    for uid, node in sorted(nodes.items()):
        if not _is_asset(node):
            continue
        output = asset_def_fn(_node(node))
        if not output.name:
            raise ValueError(f"the output of dbt node {uid!r} has no name")
        if output.name in owners:
            raise ValueError(
                f"dbt nodes {owners[output.name]!r} and {uid!r} both map to the "
                f"output {output.name!r}; pass an asset_def_fn that names them apart"
            )
        owners[output.name] = uid
        defs[uid] = output

    names = {
        uid: name for name, uid in owners.items() if selected is None or uid in selected
    }

    output_defs = []
    selectors = {}
    for uid, name in names.items():
        node = nodes[uid]
        deps: list[str | None] = []
        for parent in _upstream(resources, node):
            if parent in defs:
                deps.append(defs[parent].name)
            elif source_asset_fn is not None:
                deps.append(source_asset_fn(_node(resources[parent])))

        output = defs[uid]
        _add_deps(output, deps)
        if adapter == "duckdb":
            # The table DuckDBIOHandler loads when a downstream asset reads this one.
            output.metadata = {
                "duckdb/schema": node["schema"],
                "duckdb/table": node["alias"],
                **(output.metadata or {}),
            }

        output_defs.append(output)
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


def _node(props: dict[str, Any]) -> DbtNode:
    return DbtNode(
        unique_id=props["unique_id"],
        name=props["name"],
        resource_type=props["resource_type"],
        props=props,
    )


def _add_deps(output: AssetDef, deps: list[str | None]) -> None:
    """Add a lineage-only dep to ``output`` on each name in ``deps`` that it lacks."""
    own = {dep.name for dep in output.deps}
    added = [
        AssetDef.dep(name)
        for name in dict.fromkeys(deps)
        if name is not None and name not in own
    ]

    output.deps = [*output.deps, *added]


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
