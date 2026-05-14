"""Shared fixtures for the backfill test suite."""

from __future__ import annotations

import pytest


@pytest.fixture(params=[False, True], ids=["sync", "async"])
def is_async(request):
    """Parametrize a backfill test body over sync vs async assets."""
    return request.param
