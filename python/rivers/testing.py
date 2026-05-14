"""Test helpers for rivers.

Storage factories that wrap each `Storage` instance in a dedicated tokio
runtime so the router task lives there for the storage's lifetime.
Dropping the `Storage` then synchronously drains that runtime — releasing
the RocksDB file lock (for embedded backends) and tearing down the
in-memory router state before control returns. Without this, the shared
`io_rt()` fills with fire-and-forget shutdown tasks faster than they
drain across a long test session, and subsequent opens eventually hang.

These factories are a workaround for the lack of an awaitable
`Surreal::shutdown()` upstream. Once that lands (see
https://github.com/ion-elgreco/surrealdb/pull/new/fix/shutdown-awaitable)
and rivers adopts it, the regular :class:`rivers.Storage` constructors
will handle shutdown synchronously themselves and this module can go
away.

**Test-only.** Production code should use the regular
:meth:`rivers.Storage.memory` / :meth:`rivers.Storage.embedded` /
:meth:`rivers.Storage.connect`, which share the global ``io_rt`` and
amortise its cost across the process lifetime.

Example usage in a pytest conftest:

.. code-block:: python

    from rivers.testing import embedded_storage, memory_storage

    @pytest.fixture
    def storage():
        return memory_storage()

    @pytest.fixture
    def embedded_storage_fixture(tmp_path):
        return embedded_storage(str(tmp_path / "db"))
"""

from __future__ import annotations

from rivers._core.storage import Storage

__all__ = ["embedded_storage", "memory_storage"]


def memory_storage() -> Storage:
    """In-memory :class:`Storage` with synchronous-shutdown semantics on drop."""
    # `_test_memory` is deliberately omitted from the type stub so it doesn't
    # surface in IDE autocomplete; this module is its only intended caller.
    return Storage._test_memory()  # type: ignore[attr-defined]


def embedded_storage(path: str) -> Storage:
    """Embedded RocksDB :class:`Storage` with synchronous-shutdown semantics on drop."""
    return Storage._test_embedded(path)  # type: ignore[attr-defined]
