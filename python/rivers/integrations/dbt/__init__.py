"""dbt integration: the models, seeds, and snapshots of a dbt project as rivers assets.

Install with ``pip install rivers[dbt]`` and the dbt adapter for your
warehouse, e.g. ``pip install rivers[dbt] dbt-duckdb``.

Example::

    from pathlib import Path

    import rivers as rs
    from rivers.integrations.dbt import DbtProject, DbtResource

    project = DbtProject(Path(__file__).parent / "analytics")

    @project.assets()
    def analytics(context: rs.AssetExecutionContext, dbt: DbtResource):
        yield from dbt.cli(["build"], context)

    repo = rs.CodeRepository(
        assets=[analytics],
        resources={"dbt": DbtResource(project_dir=project)},
    )
"""

try:
    import dbt.version  # noqa: F401
except ImportError as e:
    raise ImportError(
        "rivers.integrations.dbt needs dbt-core: pip install rivers[dbt]"
    ) from e

from rivers.integrations.dbt.project import DbtProject
from rivers.integrations.dbt.resource import DbtError, DbtInvocation, DbtResource
from rivers.integrations.dbt.translator import DbtTranslator

__all__ = [
    "DbtError",
    "DbtInvocation",
    "DbtProject",
    "DbtResource",
    "DbtTranslator",
]
