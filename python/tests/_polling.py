"""Polling helpers — wait for storage to reach a state."""

from __future__ import annotations

import time


def wait_for_runs(storage, min_count: int = 1, timeout: float = 10.0, status=None):
    """Poll storage until at least min_count runs (optionally filtered by status) appear."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        runs = storage.get_runs(limit=100, status=status)
        if len(runs) >= min_count:
            return runs
        time.sleep(0.2)
    return storage.get_runs(limit=100, status=status)


def wait_for_ticks(storage, automation_name, min_count: int = 1, timeout: float = 10.0):
    """Poll storage until at least min_count ticks appear or timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        ticks = storage.get_ticks(automation_name, limit=100)
        if len(ticks) >= min_count:
            return ticks
        time.sleep(0.2)
    return storage.get_ticks(automation_name, limit=100)


def wait_for_asset_materialized(storage, key, timeout: float = 15.0):
    """Poll until asset record has a last_data_version (was materialized) or timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        record = storage.get_asset_record(key)
        if record and record.last_data_version is not None:
            return record
        time.sleep(0.2)
    return storage.get_asset_record(key)


def wait_for_backfill_runs(storage, timeout: float = 15.0):
    """Poll until at least one backfill-launched run appears."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        runs = storage.get_runs(limit=100)
        bf_runs = [r for r in runs if r.launched_by.kind == "backfill"]
        if bf_runs:
            return bf_runs
        time.sleep(0.3)
    runs = storage.get_runs(limit=100)
    return [r for r in runs if r.launched_by.kind == "backfill"]


def wait_for_run_terminal(storage, run_id, timeout: float = 15.0):
    """Poll until a specific run reaches a terminal state."""
    terminal = {"Success", "Failure", "Canceled"}
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        runs = storage.get_runs(limit=100)
        for r in runs:
            if r.run_id == run_id and r.status in terminal:
                return r
        time.sleep(0.2)
    runs = storage.get_runs(limit=100)
    return next((r for r in runs if r.run_id == run_id), None)
