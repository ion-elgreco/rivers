"""The release publishes the runtime image that the chart uses by default.

A git CodeLocation without ``spec.image``, ``spec.tag`` or ``spec.digest`` runs
``ghcr.io/ion-elgreco/rivers-runtime:<appVersion>-py<pythonVersion>``. The
release-helm workflow sets ``appVersion`` to the tag without ``v``.

These tests simulate the release of tag ``v1.2.3``. They run the shell steps of
the jobs that build ``Dockerfile.runtime``, and of the jobs that need them, with
a fake ``docker``; actions get stand-ins. Then they compare the pushed tags with
the chart default. The steps run under the host bash (3.2 on macOS), so they
must not use bash-4 syntax such as ``${x,,}``.
"""

from __future__ import annotations

import itertools
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="the release steps are bash scripts for Linux runners",
)

ROOT = Path(__file__).resolve().parents[3]
CHART = ROOT / "deploy" / "helm" / "rivers"
DOCKERFILE = "deploy/docker/Dockerfile.runtime"
RUNTIME_REPO = "ghcr.io/ion-elgreco/rivers-runtime"
PUBLISHED_PYTHONS = ("3.11", "3.12", "3.13")
GLIBC_WHEEL_TARGETS = {
    "amd64": "x86_64-unknown-linux-gnu",
    "arm64": "aarch64-unknown-linux-gnu",
}
MUSL_TARGETS = {
    "amd64": "x86_64-unknown-linux-musl",
    "arm64": "aarch64-unknown-linux-musl",
}
VERSION = "1.2.3"
GITHUB = {
    "ref_name": f"v{VERSION}",
    "repository_owner": "ion-elgreco",
    "actor": "ion-elgreco",
    "sha": "0" * 40,
}
SECRETS = {"GITHUB_TOKEN": "fake-token"}
_EXPRESSION = re.compile(r"\$\{\{\s*([\w.-]+)\s*\}\}")


@dataclass
class Build:
    dockerfile: str
    platforms: list[str]
    tags: list[str]
    build_args: dict[str, str]
    context: set[str]
    push: bool


@dataclass
class JobRun:
    builds: list[Build] = field(default_factory=list)
    docker_calls: list[list[str]] = field(default_factory=list)


@dataclass
class Release:
    builds: list[Build]
    manifests: dict[str, list[str]]
    artifacts: dict[str, dict]
    uploads: dict[str, str]


def _load_workflow(path: Path) -> dict:
    workflow = yaml.safe_load(path.read_text())
    # YAML 1.1 reads the `on:` key as the boolean True.
    workflow["on"] = workflow.pop(True, workflow.get("on"))
    return workflow


def _tag_release_workflows() -> list[dict]:
    workflows = []
    for path in sorted((ROOT / ".github" / "workflows").glob("*.y*ml")):
        workflow = _load_workflow(path)
        trigger = workflow["on"]
        push = trigger.get("push") if isinstance(trigger, dict) else None
        if push and "v*.*.*" in (push.get("tags") or []):
            workflows.append(workflow)
    return workflows


def _needs(job: dict) -> list[str]:
    needs = job.get("needs") or []
    return [needs] if isinstance(needs, str) else list(needs)


def _combos(job: dict) -> list[dict]:
    """Expand a job matrix the way GitHub does (base product, then `include`)."""
    matrix = dict((job.get("strategy") or {}).get("matrix") or {})
    include = matrix.pop("include", [])
    assert not matrix.pop("exclude", None), "matrix exclude is not simulated"
    base = (
        [dict(zip(matrix, values)) for values in itertools.product(*matrix.values())]
        if matrix
        else []
    )
    added = []
    for extra in include:
        fits = [
            c for c in base if all(c[k] == v for k, v in extra.items() if k in matrix)
        ]
        for combo in fits:
            combo.update(extra)
        if not fits:
            added.append(dict(extra))
    return base + added or [{}]


def _render(value: object, ctx: dict) -> str:
    def lookup(match: re.Match) -> str:
        node = ctx
        for part in match.group(1).split("."):
            node = node[part]
        return str(node)

    rendered = _EXPRESSION.sub(lookup, str(value))
    assert "${{" not in rendered, f"unsupported expression in {value!r}"
    return rendered


def _split(value: str) -> list[str]:
    return [item.strip() for item in re.split(r"[,\n]", value) if item.strip()]


def _builds_runtime_image(job: dict) -> bool:
    return any(
        step.get("uses", "").startswith("docker/build-push-action@")
        and (step.get("with") or {}).get("file") == DOCKERFILE
        for step in job.get("steps", [])
    )


def _uploaded_artifacts(job: dict) -> dict[str, tuple[dict, str]]:
    """Artifact name -> (the matrix combination of the job run that uploads it, its path)."""
    uploads = {}
    for combo in _combos(job):
        ctx = {"github": GITHUB, "matrix": combo}
        for step in job.get("steps", []):
            if step.get("uses", "").startswith("actions/upload-artifact@"):
                uploads[_render(step["with"]["name"], ctx)] = (
                    combo,
                    _render(step["with"]["path"], ctx),
                )
    return uploads


def _run_job(
    workflow: dict, job: dict, matrix: dict, workdir: Path, uploads: dict[str, str]
) -> JobRun:
    """Run one matrix combination of a job: shell steps for real, actions as stand-ins.

    A downloaded artifact stands in as one file named after what its job
    uploaded: ``<name>.whl`` for a wheel glob, else the uploaded file's name.
    """
    workspace = workdir / "workspace"
    fake_bin = workdir / "bin"
    workspace.mkdir(parents=True)
    fake_bin.mkdir()
    docker_log = workdir / "docker.log"
    docker = fake_bin / "docker"
    docker.write_text(
        '#!/usr/bin/env bash\nprintf "%s\\n" "$*" >> "$FAKE_DOCKER_LOG"\n'
    )
    docker.chmod(0o755)

    ctx: dict = {
        "github": GITHUB,
        "secrets": SECRETS,
        "matrix": matrix,
        "steps": {},
        "env": {},
    }
    for scope in (workflow, job):
        ctx["env"].update(
            {k: _render(v, ctx) for k, v in (scope.get("env") or {}).items()}
        )

    run = JobRun()
    for step in job["steps"]:
        uses = step.get("uses", "")
        args = step.get("with") or {}
        if "run" in step:
            step_env = {k: _render(v, ctx) for k, v in (step.get("env") or {}).items()}
            output = workdir / "github_output"
            output.write_text("")
            script = workdir / "step.sh"
            script.write_text(step["run"])
            subprocess.run(
                ["bash", "-e", str(script)],
                cwd=workspace,
                env={
                    **ctx["env"],
                    **step_env,
                    "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
                    "GITHUB_OUTPUT": str(output),
                    "FAKE_DOCKER_LOG": str(docker_log),
                },
                stdin=subprocess.DEVNULL,
                check=True,
            )
            if "id" in step:
                outputs = dict(
                    line.split("=", 1) for line in output.read_text().splitlines()
                )
                ctx["steps"][step["id"]] = {"outputs": outputs}
        elif uses.startswith("actions/checkout@"):
            shutil.copytree(ROOT / "deploy" / "docker", workspace / "deploy" / "docker")
        elif uses.startswith("actions/download-artifact@"):
            name = _render(args["name"], ctx)
            dest = workspace / _render(args.get("path", "."), ctx)
            dest.mkdir(parents=True, exist_ok=True)
            uploaded = uploads.get(name, f"{name}.whl")
            file = f"{name}.whl" if "*" in uploaded else Path(uploaded).name
            (dest / file).touch()
        elif uses.startswith("docker/build-push-action@"):
            context = workspace / _render(args["context"], ctx)
            build_args = _render(args.get("build-args", ""), ctx)
            run.builds.append(
                Build(
                    dockerfile=_render(args["file"], ctx),
                    platforms=_split(_render(args["platforms"], ctx)),
                    tags=_split(_render(args["tags"], ctx)),
                    build_args=dict(
                        line.strip().split("=", 1) for line in _split(build_args)
                    ),
                    context={
                        p.relative_to(context).as_posix()
                        for p in context.rglob("*")
                        if p.is_file()
                    },
                    push=_render(args.get("push", False), ctx).lower() == "true",
                )
            )
    if docker_log.exists():
        run.docker_calls = [
            line.split() for line in docker_log.read_text().splitlines()
        ]
    return run


def _imagetools_create(args: list[str]) -> tuple[list[str], list[str]]:
    tags, sources = [], []
    it = iter(args)
    for arg in it:
        if arg in ("--tag", "-t"):
            tags.append(next(it))
        else:
            sources.append(arg)
    return tags, sources


@pytest.fixture
def release(tmp_path) -> Release:
    builds: list[Build] = []
    manifests: dict[str, list[str]] = {}
    artifacts: dict[str, dict] = {}
    uploads: dict[str, str] = {}
    for w, workflow in enumerate(_tag_release_workflows()):
        jobs = workflow["jobs"]
        for job_id, job in jobs.items():
            if not _builds_runtime_image(job):
                continue
            for upstream in _needs(job):
                for name, (combo, path) in _uploaded_artifacts(jobs[upstream]).items():
                    artifacts[name] = combo
                    uploads[name] = path
            for i, combo in enumerate(_combos(job)):
                builds += _run_job(
                    workflow, job, combo, tmp_path / f"{w}-{job_id}-{i}", uploads
                ).builds
            for other_id, other in jobs.items():
                if job_id not in _needs(other):
                    continue
                for i, combo in enumerate(_combos(other)):
                    run = _run_job(
                        workflow,
                        other,
                        combo,
                        tmp_path / f"{w}-{other_id}-{i}",
                        uploads,
                    )
                    for call in run.docker_calls:
                        if call[:3] == ["buildx", "imagetools", "create"]:
                            tags, sources = _imagetools_create(call[3:])
                            manifests.update(dict.fromkeys(tags, sources))
    return Release(
        builds=builds, manifests=manifests, artifacts=artifacts, uploads=uploads
    )


def _chart_values() -> dict:
    return yaml.safe_load((CHART / "values.yaml").read_text())


def _expected_pythons() -> list[str]:
    default = str(_chart_values()["codeLocation"]["runtime"]["pythonVersion"])
    return sorted({*PUBLISHED_PYTHONS, default})


def _chart_default_runtime_image(app_version: str) -> str:
    """Render the `rivers.runtimeImage` branch used when `codeLocation.runtime.image` is empty."""
    helpers = (CHART / "templates" / "_helpers.tpl").read_text()
    define = helpers.split('define "rivers.runtimeImage"', 1)[1].split("define ", 1)[0]
    (line,) = [
        line.strip() for line in define.splitlines() if ".Chart.AppVersion" in line
    ]
    values = _chart_values()

    def lookup(match: re.Match) -> str:
        path = match.group(1)
        if path == ".Chart.AppVersion":
            return app_version
        node = values
        for key in path.removeprefix(".Values.").split("."):
            node = node[key]
        return str(node)

    return re.sub(r"\{\{\s*(\.[\w.]+)\s*\}\}", lookup, line)


def test_release_publishes_the_chart_default_runtime_image(release):
    expected = {
        f"{RUNTIME_REPO}:{VERSION}-py{python}": python for python in _expected_pythons()
    }
    chart_default = _chart_default_runtime_image(VERSION)
    assert chart_default in expected, (
        f"chart default {chart_default} is not a published tag"
    )

    missing = sorted(expected.keys() - release.manifests.keys())
    assert not missing, f"the release never publishes {missing}"

    pushed = {
        tag: build for build in release.builds if build.push for tag in build.tags
    }
    for tag, python in expected.items():
        sources = release.manifests[tag]
        assert set(sources) <= pushed.keys(), (
            f"{tag} combines images no job pushed: {sources}"
        )
        built = sorted(
            (
                pushed[s].dockerfile,
                pushed[s].build_args.get("PYTHON_VERSION"),
                *pushed[s].platforms,
            )
            for s in sources
        )
        assert built == [
            (DOCKERFILE, python, "linux/amd64"),
            (DOCKERFILE, python, "linux/arm64"),
        ]


def test_runtime_images_install_the_release_glibc_wheel_of_their_arch(release):
    for python in _expected_pythons():
        for arch, target in GLIBC_WHEEL_TARGETS.items():
            builds = [
                b
                for b in release.builds
                if b.platforms == [f"linux/{arch}"]
                and b.build_args.get("PYTHON_VERSION") == python
            ]
            assert len(builds) == 1, (
                f"py{python} {arch}: {len(builds)} runtime image builds"
            )
            context = builds[0].context
            wheels = sorted(
                p.removeprefix("wheels/") for p in context if p.startswith("wheels/")
            )
            targets = [
                release.artifacts.get(w.removesuffix(".whl"), {}).get("target")
                for w in wheels
            ]
            assert targets == [target], f"py{python} {arch} context: {sorted(context)}"
            assert context == {f"wheels/{w}" for w in wheels} | {"rivers-runtime"}, (
                sorted(context)
            )


def test_runtime_images_carry_the_static_rivers_runtime_of_their_arch(release):
    for arch, target in MUSL_TARGETS.items():
        name = f"rivers-runtime-{arch}"
        assert release.artifacts.get(name, {}).get("target") == target, (
            f"{name}: {release.artifacts.get(name)}"
        )
        assert release.uploads[name].endswith(f"{target}/release/rivers-runtime")
