set shell := ["bash", "-uc"]

# Cargo profile for `just develop` / `just test-rust`. Defaults to `dev`
# for fast local iteration; CI sets PROFILE=ci which selects [profile.ci]
# (incremental off so sccache works, deps at opt-level=3).

profile := env_var_or_default("PROFILE", "dev")

# Cargo's `dev` profile outputs to `target/<triple>/debug/`; every other
# profile (release, ci, ...) outputs to `target/<triple>/<profile>/`.

profile_dir := if profile == "dev" { "debug" } else { profile }

_default:
    just --list

# Sync venv with all deps (without building rivers native extension)
venv:
    uv sync --no-install-workspace --all-extras --all-packages

# Build the UI's hydration WASM (release).
wasm:
    cargo build -p rivers-ui --target wasm32-unknown-unknown --release --no-default-features --features hydrate
    wasm-bindgen target/wasm32-unknown-unknown/release/rivers_ui.wasm --out-dir rust/rivers-ui/pkg --target web --no-typescript

# Build WASM in dev mode — preserves panic messages/symbols for debugging.
# Honors $PROFILE so host build-deps (proc-macros, build.rs deps) land in the same
# `target/<profile>/` dir as the follow-up `maturin develop --profile $PROFILE`,
# letting the two steps share artifacts. wasm-pack hardcodes `dev`/`release` and

# can't be pointed at a custom profile like `ci`.
wasm-dev:
    cargo build -p rivers-ui --target wasm32-unknown-unknown --profile {{ profile }} --no-default-features --features hydrate
    wasm-bindgen target/wasm32-unknown-unknown/{{ profile_dir }}/rivers_ui.wasm --out-dir rust/rivers-ui/pkg --target web --no-typescript

# Build and install rivers as editable (release WASM — use for UI work or shipping)
develop: venv wasm
    cd python && VIRTUAL_ENV='{{ justfile_directory() }}/.venv' uvx --from 'maturin[zig]' maturin develop --profile {{ profile }}

# Faster develop for non-UI work — dev-profile WASM build (preserves panic symbols, larger blob; don't use for k8s/release)
develop-fast: venv wasm-dev
    cd python && VIRTUAL_ENV='{{ justfile_directory() }}/.venv' uvx --from 'maturin[zig]' maturin develop --profile {{ profile }}

# Build and install rivers as editable (release mode, stripped WASM)
develop-release: venv wasm
    cd python && VIRTUAL_ENV='{{ justfile_directory() }}/.venv' uvx --from 'maturin[zig]' maturin develop --release

# Run Python tests
test:
    uv run --no-sync pytest python/

# Run all Rust workspace tests.
# - `wasm-dev` is a prerequisite so rivers-ui's `include_bytes!("../pkg/...")`
#   can resolve when the crate is built with `ssr`.
# - `rivers` (PyO3 cdylib) is excluded; its Rust unit tests need a libpython
#   link step bare `cargo test` doesn't provide and are exercised indirectly
#   by the Python test suite.
# - `rivers-ui` is run separately with `--features ssr` to pull in the native

# deps its non-wasm tests need (channel_loop, code_location_registry, ...).
test-rust: wasm-dev
    cargo test --profile {{ profile }} --workspace --exclude rivers --exclude rivers-ui
    cargo test --profile {{ profile }} -p rivers-ui --features ssr

# Run Helm chart unit tests (requires `helm plugin install https://github.com/helm-unittest/helm-unittest`)
test-helm:
    helm dependency update deploy/helm/rivers >/dev/null
    helm unittest deploy/helm/rivers

# Run rivers-ui browser component tests in headless `chrome` (default) or `firefox` — requires matching driver on PATH
wasm-test browser="chrome":
    cd rust/rivers-ui && wasm-pack test --headless --{{ browser }} --no-default-features --features csr

# Run linter, formatter, static type checker
pre-commit:
    uv run --no-sync rumdl check . --fix --flavor mkdocs --fail-on never --disable MD013,MD033,MD041
    uv run --no-sync ruff check python/
    uv run --no-sync ruff format python/
    cd python && uv run --no-sync pyright .

# Run linter, formatter, static type checker in CI
pre-commit-check:
    uv run --no-sync rumdl check . --flavor mkdocs --fail-on never --disable MD013,MD033,MD041
    uv run --no-sync ruff check python/
    uv run --no-sync ruff format --check --diff python/
    uv run --no-sync typos python/
    cd python && uv run --no-sync pyright .

# Start the example project dev UI (optionally with a synthetic graph: just demo 1k)
demo nodes="":
    uv run --no-sync rivers dev examples.demo_project.pipeline {{ if nodes != "" { "--synthetic " + nodes } else { "" } }}

# Build documentation
docs-build:
    uvx zensical build

# Serve documentation with live reload
docs-serve:
    uvx zensical serve

# squidfunk's fork of mike, pinned to a specific commit (zensical's
# versioning provider expects fixes that haven't landed in upstream yet).
# Bump when zensical updates its versioning guidance.

mike_pkg := "git+https://github.com/squidfunk/mike.git@2d4ad799442f4592db8ad53b179bfb33db8c69ac"

# Deploy a versioned docs build. push="--push" is the default for CI;
# pass push="" for a local dry-run that mutates only the local gh-pages

# branch without pushing it. Example: `just docs-deploy 0.1.0 ""`.
docs-deploy version push="--push":
    uvx --from "{{ mike_pkg }}" --with zensical mike deploy {{ push }} --update-aliases {{ version }} latest

# Set the `latest` alias as the default — installs the root redirect

# from `/` to `/latest/`. Idempotent; runs once per release.
docs-set-default push="--push":
    uvx --from "{{ mike_pkg }}" --with zensical mike set-default {{ push }} latest

# === K8s Integration Testing ===

linux_target := "aarch64-unknown-linux-gnu"
cluster_name := env_var_or_default("RIVERS_K3D_CLUSTER", "rivers-test")

# Regenerate CRD YAML from Rust types (source of truth) into the rivers-crds Helm chart
gen-crds:
    cargo run -p rivers-k8s --bin rivers-gen-crd -- codelocation > deploy/helm/rivers-crds/crds/codelocations.rivers.io.yaml
    cargo run -p rivers-k8s --bin rivers-gen-crd -- run > deploy/helm/rivers-crds/crds/runs.rivers.io.yaml
    @echo "Regenerated CodeLocation + Run CRDs from Rust types."

# Cross-compile Rust binaries and Python wheel for Linux, then package as Docker images
k8s-build: _k8s-compile
    mkdir -p deploy/staging
    cp target/{{ linux_target }}/debug/rivers-operator deploy/staging/
    cp target/{{ linux_target }}/debug/rivers-ui deploy/staging/
    cp dist/*.whl deploy/staging/
    docker build -f deploy/docker/Dockerfile.operator -t rivers-operator:latest deploy/staging
    docker build -f deploy/docker/Dockerfile.ui -t rivers-ui:latest deploy/staging
    cp python/pyproject.toml deploy/staging/pyproject.toml
    cp -r dev/k3d/k8s_test_pipeline deploy/staging/k8s_test_pipeline
    docker build -f dev/k3d/Dockerfile.code-location -t rivers-code-location:latest deploy/staging
    cp -r examples/demo_project deploy/staging/demo_project
    docker build -f dev/k3d/Dockerfile.demo -t rivers-demo:latest deploy/staging
    rm -rf deploy/staging

# Cross-compile all artifacts for Linux (no Docker)
_k8s-compile: wasm _k8s-wheel
    cargo zigbuild -p rivers-operator --target {{ linux_target }}
    cargo zigbuild -p rivers-ui --target {{ linux_target }} --features ssr

# Cross-compile just the Python wheel for Linux
_k8s-wheel:
    cd python && CARGO_TARGET_DIR='{{ justfile_directory() }}/target/maturin' VIRTUAL_ENV='{{ justfile_directory() }}/.venv' uvx --from 'maturin[zig]' maturin build --target {{ linux_target }} --zig --out ../dist

# Create k3d cluster, build images, deploy with Helm
k8s-up: k8s-build
    dev/k3d/setup.sh

# Tear down k3d cluster
k8s-down:
    k3d cluster delete {{ cluster_name }}

# Run K8s integration tests (cluster must be running)
k8s-test:
    uv run --no-sync pytest python/tests/integration/kubernetes/ -v

# Build a code-location Docker image (test or demo). Rebuilds only the Python wheel.
k8s-code-location project="test": _k8s-wheel
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p deploy/staging
    cp dist/*.whl deploy/staging/
    cp python/pyproject.toml deploy/staging/pyproject.toml
    if [ "{{ project }}" = "demo" ]; then
        cp -r examples/demo_project deploy/staging/demo_project
        docker build -f dev/k3d/Dockerfile.demo -t rivers-demo:latest deploy/staging
    else
        cp -r dev/k3d/k8s_test_pipeline deploy/staging/k8s_test_pipeline
        docker build -f dev/k3d/Dockerfile.code-location -t rivers-code-location:latest deploy/staging
    fi
    rm -rf deploy/staging

# Push a code-location image to the k3d local registry and re-roll the
# code-location deployment with the new digest (test or demo). The operator
# resolves digests directly (allowInsecureRegistry=true on this cluster), but
# its in-memory cache is keyed by (registry, repo, tag) so we restart the
# operator to drop the cached `latest` digest before reconcile picks up the

# new push.
k8s-deploy project="test": (k8s-code-location project)
    #!/usr/bin/env bash
    set -euo pipefail
    REGISTRY_HOST="localhost:5111"
    if [ "{{ project }}" = "demo" ]; then
        SRC_IMAGE="rivers-demo:latest"
        REPO="rivers-demo"
        CR_NAME="${RIVERS_K8S_DEMO_CODE_LOCATION:-demo}"
    else
        SRC_IMAGE="rivers-code-location:latest"
        REPO="rivers-code-location"
        CR_NAME="${RIVERS_K8S_TEST_CODE_LOCATION:-k8s-test-pipeline}"
    fi
    docker tag "$SRC_IMAGE" "${REGISTRY_HOST}/${REPO}:latest"
    docker push "${REGISTRY_HOST}/${REPO}:latest"
    kubectl -n rivers rollout restart deployment/rivers-operator
    kubectl -n rivers rollout status deployment/rivers-operator --timeout=60s
    kubectl -n rivers rollout status deployment "${CR_NAME}" --timeout=120s

# Full cycle: create cluster, deploy, test, tear down
k8s-integration: k8s-up k8s-test

# Remove environment and caches
[confirm("Are you sure?")]
clean:
    @rm -rf .venv/
    @rm -rf .pytest_cache/
    @rm -rf .ruff_cache/
