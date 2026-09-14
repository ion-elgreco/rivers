#!/usr/bin/env bash
# Build release rivers images for the Kubernetes comparison.
#
# `just k8s-build` compiles debug binaries and produces multi-gigabyte images,
# which would make any image-size comparison meaningless. This mirrors that
# recipe with release profiles instead.
#
# Takes about 25 minutes from cold: SurrealDB is compiled twice, once for the
# operator and UI binaries and once more for the Python wheel, which uses a
# separate cargo target directory.
set -euo pipefail

# Match the cluster's architecture, which on k3d is the host's.
ARCH="$(uname -m)"
case "$ARCH" in
    arm64 | aarch64) TARGET="aarch64-unknown-linux-gnu" ;;
    x86_64 | amd64) TARGET="x86_64-unknown-linux-gnu" ;;
    *) echo "unsupported architecture: $ARCH" >&2; exit 1 ;;
esac
ZIG_VERSION="0.16.0"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

echo "==> Release WASM for the UI"
just wasm

echo "==> Release Linux binaries"
cargo zigbuild -p rivers-operator --target "$TARGET" --release
cargo zigbuild -p rivers-ui --target "$TARGET" --features ssr --release

echo "==> Release wheel"
rm -rf dist-release && mkdir -p dist-release
(cd python && \
  CARGO_TARGET_DIR="$ROOT/target/maturin" \
  VIRTUAL_ENV="$ROOT/.venv" \
  uvx --from 'maturin[zig]' --with "ziglang==${ZIG_VERSION}" \
  maturin build --release --target "$TARGET" --zig --out ../dist-release)

echo "==> Docker images"
rm -rf deploy/staging && mkdir -p deploy/staging
cp "target/${TARGET}/release/rivers-operator" deploy/staging/
cp "target/${TARGET}/release/rivers-ui" deploy/staging/
cp dist-release/*.whl deploy/staging/
cp python/pyproject.toml deploy/staging/pyproject.toml
cp bench/k8s/images/bench_pipeline.py deploy/staging/bench_pipeline.py

docker build -f deploy/docker/Dockerfile.operator -t rivers-operator:release deploy/staging
docker build -f deploy/docker/Dockerfile.ui -t rivers-ui:release deploy/staging
docker build -f bench/k8s/images/Dockerfile.code-location -t rivers-bench-code-location:release deploy/staging
rm -rf deploy/staging

docker images --format "{{.Repository}}:{{.Tag}} {{.Size}}" | grep ":release"
