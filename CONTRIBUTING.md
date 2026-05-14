# Contributing to rivers

Thanks for your interest in rivers. This document covers how to get a development environment running, what the test matrix looks like, and the conventions PRs are expected to follow.

For larger changes, please open an [issue](https://github.com/ion-elgreco/rivers/issues) or [discussion](https://github.com/ion-elgreco/rivers/discussions) first so we can align on direction before code is written.

## Prerequisites

- **Rust** (stable) — install via [rustup](https://rustup.rs)
- **Python** ≥ 3.10
- **[uv](https://docs.astral.sh/uv/)** — package manager for the Python side
- **[just](https://github.com/casey/just)** — task runner; all dev commands go through the `justfile` at the repo root
- **Docker** + **[k3d](https://k3d.io/)** + **[helmfile](https://helmfile.readthedocs.io/)** — only needed for local Kubernetes development and integration tests

Optional, only for specific recipes:

- `wasm-pack`, `wasm-bindgen` — for `just develop` (release WASM)
- `helm` + the [`helm-unittest`](https://github.com/helm-unittest/helm-unittest) plugin — for `just test-helm`
- `cargo-zigbuild` — for cross-compiling K8s images on macOS

## Setting up the dev environment

Clone, then build the Python extension in editable mode:

```bash
git clone https://github.com/ion-elgreco/rivers.git
cd rivers
just develop-fast
```

`just develop-fast` is the default for day-to-day work — it builds the WASM with the dev cargo profile (preserves panic symbols, larger blob, much faster rebuilds). It is **not** suitable for shipping or for K8s images. For UI release builds or anything you'd publish, use `just develop` instead.

After the first build, the editable wheel is installed into `.venv`. Re-run `just develop-fast` whenever you change Rust code; pure-Python edits don't need a rebuild.

## Running tests

```bash
just test          # Python test suite (pytest)
just test-rust     # Rust workspace tests
just pre-commit    # ruff (lint + format) + pyright
```

For the optional suites:

```bash
just test-helm                   # Helm chart unit tests
just wasm-test                   # rivers-ui browser tests (headless chrome)
just k8s-up && just k8s-test     # K8s integration tests (creates a k3d cluster)
just k8s-down                    # tear it down when you're done
```

When you change anything Python-facing, please cover it in `python/tests/`. Tests should validate actual data, not just lengths or counts, and should parametrize across the dimensions that matter (executor type, asset type, sync vs async) where relevant.

## Trying it out interactively

```bash
just demo                  # boots the example pipeline UI on http://localhost:3000
just demo 1k               # same, but with a 1000-node synthetic graph
```

## Project layout

```
rust/
  rivers-core/      Pure Rust core: graph, execution plan, storage (SurrealDB + RocksDB)
  rivers-api/       gRPC proto + generated code (shared between Python server and UI)
  rivers-ui/        Leptos + Axum web UI (SSR + WASM hydration)
  rivers-operator/  Kubernetes operator
  rivers-k8s/       CRD types and codegen
python/
  src/              PyO3 bindings (Rust)
  rivers/           Python package (Python source + .pyi stubs)
  tests/            pytest suite
proto/              gRPC schema
deploy/helm/        Helm charts (rivers, rivers-crds)
docs/               MkDocs documentation (built with zensical)
examples/           Example pipelines (used by `just demo`)
rfc/                Design RFCs
```

## Code conventions

- **Rust:** use `anyhow` for application error handling. Keep modules focused — split when they pass ~500 lines.
- **Python:** Pydantic `BaseModel` for config and model classes. Google-style docstrings with reStructuredText directives.
- **PyO3:** 0.28.x with `abi3-py310`. Use `Python::try_attach` (not the removed `with_gil`); `Py::clone_ref(py)` rather than bare `.clone()`.
- **Stubs:** when you change a Python-facing API, update the corresponding `.pyi` file under `python/rivers/`.
- **Docs:** when you change a public API, add or update the relevant page under `docs/` (built via `just docs-serve` for live preview).
- **Comments:** default to none. Only write a comment when the *why* is non-obvious — a hidden constraint, an invariant, or a workaround for a specific bug. Don't restate what the code already says.

Run `just pre-commit` before pushing to format the Python tree and run the type checker.

## Pull request workflow

1. Fork the repo and branch off `main`.
2. Make your changes; add tests; run `just test` and `just pre-commit`.
3. If you touched Rust, run `just test-rust`. If you touched the Helm chart, run `just test-helm`.
4. If you touched a public API, update the `.pyi` stubs and the `docs/` pages.
5. Sign off every commit (see [Contributor License Agreement](#contributor-license-agreement) below).
6. Open a PR. The PR template asks for a description and any related issues — please fill it in.
7. CI will run lint, type check, and the full test matrix. Please keep the branch green.

Small, focused PRs are easier to review than large ones. If you're working on something big, splitting it across a few sequential PRs is welcome.

## Contributor License Agreement

rivers uses a lightweight, signoff-based Contributor License Agreement. The full terms are in [`CLA.md`](CLA.md) — please read them before contributing.

In short: by signing off a commit, you certify that you wrote the change (or have the right to submit it), and you grant the Project Owner the license to use, distribute, and **relicense** your contribution under the terms set out in `CLA.md`. The relicensing right is what lets the project change its license in the future without having to track down every past contributor.

To sign off a commit, add the `-s` (or `--signoff`) flag:

```bash
git commit -s -m "your message"
```

That appends a trailer like:

```
Signed-off-by: Your Name <you@example.com>
```

Use a real name and a working email — anonymous or fake sign-offs are not accepted. The name and email must match your `git config user.name` / `user.email`.

If you forget to sign off a commit, amend it:

```bash
git commit --amend --signoff
```

Or, for an entire branch:

```bash
git rebase --signoff main
```

PRs with unsigned commits will be blocked by CI until every commit carries a `Signed-off-by` trailer. By submitting a pull request with signed-off commits, you confirm that you have read and accepted the terms in [`CLA.md`](CLA.md).

If you are contributing on behalf of an employer, please also read Section 4 of `CLA.md` regarding employer authorization.

## Reporting bugs and requesting features

- **Bug reports:** use the [bug report template](https://github.com/ion-elgreco/rivers/issues/new?template=bug_report.yaml). Include a minimal reproduction if possible.
- **Feature requests:** use the [feature request template](https://github.com/ion-elgreco/rivers/issues/new?template=feature_request.yaml).
- **Open-ended questions or design discussion:** start a thread in [Discussions](https://github.com/ion-elgreco/rivers/discussions).

## Security

If you find a security issue, please **do not** open a public issue. Email the maintainer directly or use GitHub's private vulnerability reporting on the repo.

## License

By contributing, you agree that your contributions will be licensed under the same license as the project (see [`LICENSE`](LICENSE)).
