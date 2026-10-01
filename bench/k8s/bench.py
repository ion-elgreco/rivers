"""Kubernetes startup comparison for rivers, Dagster and Prefect.

Measures three things on one k3d cluster, per orchestrator:

1. **Control plane cold start** — from ``helm install`` to every Deployment and
   StatefulSet in the namespace reporting its full replica count ready.
2. **Code location ready** — from applying a code location to the point where
   the orchestrator will accept work against it.
3. **Footprint** — pod count, container count, image size on disk, and the
   memory the control plane holds at idle.

Every image is cached in the cluster before timing starts, so the numbers
measure orchestrator startup rather than download speed.

Run from the repository root:
    python -m bench.k8s.bench --repeat 3
    python -m bench.k8s.bench --orchestrator rivers --keep
"""

from __future__ import annotations

import argparse

from bench.k8s.cluster import create_cluster, push_images
from bench.k8s.deploy import ORCHESTRATORS, cleanup
from bench.k8s.measure import Measurement, save
from bench.paths import K8S_RESULTS, resolve


def show(measurement: Measurement, footprint: bool) -> None:
    """Print one measurement's phases as they were recorded."""
    for phase in measurement.phases:
        print(
            f"  {phase.name:<22} {phase.seconds:7.1f}s  {phase.status}  "
            f"{phase.detail[:80]}".rstrip(),
            flush=True,
        )
    if footprint:
        print(
            f"  pods={len(measurement.pods)} "
            f"containers={sum(p['containers'] for p in measurement.pods)} "
            f"images={measurement.image_bytes / 1024**2:,.0f}MB",
            flush=True,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--orchestrator", action="append", choices=sorted(ORCHESTRATORS)
    )
    parser.add_argument(
        "--assets", type=int, default=10, help="assets in the code location"
    )
    parser.add_argument(
        "--setup-only", action="store_true", help="create cluster and push images"
    )
    parser.add_argument(
        "--keep", action="store_true", help="leave the namespace up afterwards"
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="measured runs per orchestrator; the report takes the median",
    )
    parser.add_argument(
        "--no-warmup",
        action="store_true",
        help="skip the untimed install that caches public images",
    )
    parser.add_argument(
        "--out", default=None, help="output JSON path, relative to bench/"
    )
    args = parser.parse_args()

    targets = [ORCHESTRATORS[name] for name in (args.orchestrator or ORCHESTRATORS)]
    create_cluster()
    for orch in targets:
        print(f"pushing local images for {orch.name}", flush=True)
        push_images(orch.images)
    if args.setup_only:
        return

    measurements: list[Measurement] = []
    out = resolve(args.out) if args.out else K8S_RESULTS
    for orch in targets:
        if not args.no_warmup:
            # An untimed install pulls every public image into the node cache,
            # so the timed install measures startup and not download speed.
            print(f"\n=== {orch.name}: warm-up install (untimed) ===", flush=True)
            show(orch.deploy(orch.namespace, args.assets, False), footprint=False)
            cleanup(orch)

        for attempt in range(1, args.repeat + 1):
            print(
                f"\n=== {orch.name}: measured install {attempt}/{args.repeat} ===",
                flush=True,
            )
            measurement = orch.deploy(orch.namespace, args.assets)
            measurement.attempt = attempt
            show(measurement, footprint=True)
            measurements.append(measurement)
            save(measurements, out)
            if not args.keep or attempt < args.repeat:
                cleanup(orch)
    print(f"\nwrote {out}", flush=True)


if __name__ == "__main__":
    main()
