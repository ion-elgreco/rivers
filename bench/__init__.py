"""Reproducible comparison of rivers, Dagster and Prefect.

Every entry point runs as a module from the repository root, so the package
imports resolve without any `sys.path` handling:

    .venv/bin/python -m bench.local.sweep --quick
    .venv/bin/python -m bench.k8s.bench --repeat 3
    .venv/bin/python -m bench.report.render --out RESULTS.md
"""
