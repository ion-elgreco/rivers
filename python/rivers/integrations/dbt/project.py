"""A dbt project, its manifest, and the asset definitions built from it."""

import functools
import hashlib
import inspect
import json
import os
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator

from rivers._core.assets import Asset, MultiAsset
from rivers.integrations.dbt._manifest import build_outputs, selection_args
from rivers.integrations.dbt.resource import DbtResource
from rivers.integrations.dbt.translator import DbtTranslator

__all__ = ["DbtProject"]


class DbtProject(BaseModel):
    """A dbt project. Its manifest defines the assets of :meth:`assets`.

    Write the manifest with ``dbt parse`` or :meth:`prepare` before the code
    location loads, e.g. when you build its image.

    Args:
        project_dir: The directory with ``dbt_project.yml``.
        target_path: The directory with ``manifest.json``, relative to
            ``project_dir``.
        profiles_dir: The directory with ``profiles.yml`` for :meth:`prepare`,
            selections, and a :class:`DbtResource` made from the project.
            Default: dbt's search.
        profile: The profile to use instead of the project's.
        target: The target to use instead of the profile's default.

    Example::

        project = DbtProject(Path(__file__).parent / "analytics")
    """

    model_config = ConfigDict(frozen=True)

    project_dir: Path
    target_path: Path = Path("target")
    profiles_dir: Path | None = None
    profile: str | None = None
    target: str | None = None

    def __init__(self, project_dir: str | Path, **kwargs: Any) -> None:
        super().__init__(project_dir=project_dir, **kwargs)

    @field_validator("project_dir")
    @classmethod
    def _absolute_project_dir(cls, project_dir: Path) -> Path:
        project_dir = project_dir.resolve()
        if not (project_dir / "dbt_project.yml").is_file():
            raise ValueError(f"no dbt_project.yml in {project_dir}")
        return project_dir

    @property
    def manifest_path(self) -> Path:
        """``manifest.json`` in the target path."""
        return self.project_dir / self.target_path / "manifest.json"

    def prepare(self) -> None:
        """Parse the project with ``dbt parse`` and write its manifest.

        Processes that read the manifest meanwhile see the old file or the new
        one. Run ``dbt deps`` first when the project uses packages.

        Raises:
            DbtError: When dbt cannot parse the project.
        """
        target = self.manifest_path.parent
        target.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=target, prefix="rivers-parse-") as parsed:
            self._resource().cli(["parse"], target_path=parsed).wait()
            for name in ("partial_parse.msgpack", "manifest.json"):
                if (Path(parsed) / name).exists():
                    os.replace(Path(parsed) / name, target / name)

    def assets(
        self,
        *,
        select: str = "fqn:*",
        exclude: str = "",
        selector: str = "",
        translator: DbtTranslator | None = None,
        **kwargs: Any,
    ) -> Callable[[Callable[..., Any]], MultiAsset]:
        """A decorator that builds a multi-asset of the project's models, seeds, and snapshots.

        Each selected resource is an output, named by the translator. Its deps
        are the resources it refs, so a run can build any subset of them. A
        source, or a resource outside the selection, becomes a dep on the
        asset :meth:`DbtTranslator.upstream_asset` names; by default sources
        add no dep. Tests are not assets: in a ``dbt build`` they gate the
        outputs they test.

        The body is a generator that runs dbt with the :class:`DbtResource` it
        receives; see :meth:`DbtResource.cli`. A body that makes its own
        ``DbtResource`` instead raises ``TypeError``.

        Args:
            select: dbt's ``--select``. Other than the default, the selection
                runs ``dbt ls`` once per manifest; the result is cached in the
                target path.
            exclude: dbt's ``--exclude``.
            selector: A selector from ``selectors.yml``, instead of ``select``
                and ``exclude``.
            translator: Maps resources to asset names, groups, tags, metadata,
                code versions, and deps.
            **kwargs: Passed to :meth:`rivers.Asset.from_multi`, e.g. ``name``,
                ``group``, ``partitions_def``, ``hooks``, or ``retry``. An
                ``io_handler`` that reads tables, like ``DuckDBIOHandler``, loads
                the outputs into downstream assets.

        Returns:
            A decorator for the body.

        Raises:
            FileNotFoundError: When the manifest does not exist.
            ValueError: When the selection matches no model, seed, or snapshot.

        Example::

            @project.assets(exclude="tag:hourly")
            def analytics(context: rs.AssetExecutionContext, dbt: DbtResource):
                yield from dbt.cli(["build"], context)
        """
        if selector and (exclude or select not in ("", "fqn:*")):
            raise ValueError("pass selector, or select and exclude, not both")

        try:
            raw = self.manifest_path.read_bytes()
        except FileNotFoundError:
            raise FileNotFoundError(
                f"no dbt manifest at {self.manifest_path}: run `dbt parse` in "
                f"{self.project_dir} or call DbtProject.prepare()"
            ) from None

        selection = selection_args(select, exclude, selector)
        selected = None
        if select not in ("", "fqn:*") or exclude or selector:
            selected = self._select(raw, selection)

        outputs, output_defs = build_outputs(
            json.loads(raw),
            selected,
            translator or DbtTranslator(),
            selection,
            ("--exclude", exclude) if exclude else (),
        )
        if not output_defs:
            raise ValueError(
                f"dbt selection {' '.join(selection)} matches no model, seed, or snapshot"
            )

        def decorate(fn: Callable[..., Any]) -> MultiAsset:
            if not inspect.isgeneratorfunction(fn):
                raise TypeError(
                    f"'{fn.__name__}' must be a generator: yield from dbt.cli([...], context)"
                )

            @functools.wraps(fn)
            def body(*args: Any, **fn_kwargs: Any) -> Any:
                if not any(isinstance(arg, DbtResource) for arg in args):
                    raise TypeError(
                        f"'{fn.__name__}' must take the DbtResource as a parameter, "
                        "e.g. dbt: DbtResource; a DbtResource made in the body is "
                        "not bound to its outputs"
                    )

                args = tuple(
                    arg._bind(outputs, self.manifest_path.parent)
                    if isinstance(arg, DbtResource)
                    else arg
                    for arg in args
                )
                yield from fn(*args, **fn_kwargs)

            return Asset.from_multi(body, output_defs=output_defs, **kwargs)

        return decorate

    def _select(self, raw: bytes, selection: tuple[str, ...]) -> set[str]:
        """The unique ids ``dbt ls`` lists for a selection, cached per manifest."""
        key = hashlib.sha256(raw + json.dumps(selection).encode()).hexdigest()[:16]
        cache = self.manifest_path.parent / f"rivers-selection-{key}.json"
        try:
            return set(json.loads(cache.read_text()))
        except (OSError, ValueError):
            pass

        selected = self._resource()._list(selection)
        try:
            partial = cache.with_suffix(f".{os.getpid()}.tmp")
            partial.write_text(json.dumps(sorted(selected)))
            os.replace(partial, cache)
        except OSError:
            pass
        return selected

    def _resource(self) -> DbtResource:
        resource = DbtResource(project_dir=self)
        resource._parse_dir = self.manifest_path.parent
        return resource
