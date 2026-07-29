"""Module-level class-form assets for the loky by-reference tests.

The parallel executor ships `FuncRef` / `IOHandlerRef` (module, qualname)
pairs, so these classes must be importable in the worker subprocess — the
tests put this directory on PYTHONPATH and pass the store dir through
RIVERS_TEST_CLASS_STORE before the module is first imported (parent and
children read the same env).
"""

import os

import obstore.store

import rivers as rs

_store_dir = os.environ.get("RIVERS_TEST_CLASS_STORE")

if _store_dir:
    handler = rs.PickleIOHandler(store=obstore.store.LocalStore(_store_dir, mkdir=True))

    # 2-wide levels so the batch escapes the single-instance InProcess
    # shortcut and actually crosses the loky transport.
    class CA(rs.Asset):
        io_handler = handler

        @classmethod
        def materialize(cls) -> int:
            return 10

    class CB(rs.Asset):
        io_handler = handler

        @classmethod
        def materialize(cls) -> int:
            return 20

    class CC(rs.Asset):
        io_handler = handler

        @classmethod
        def materialize(cls, ca: int) -> int:
            assert ca == 10, f"cc got {ca!r}"
            return ca + 1

    class CD(rs.Asset):
        io_handler = handler

        @classmethod
        def materialize(cls, cb: int) -> int:
            assert cb == 20, f"cd got {cb!r}"
            return cb + 2

    # Decorator-form control group — the pre-existing by-reference path.
    @rs.Asset(io_handler=handler)
    def da() -> int:
        return 10

    @rs.Asset(io_handler=handler)
    def db() -> int:
        return 20

    @rs.Asset(io_handler=handler)
    def dc(da: int) -> int:
        assert da == 10, f"dc got {da!r}"
        return da + 1

    @rs.Asset(io_handler=handler)
    def dd(db: int) -> int:
        assert db == 20, f"dd got {db!r}"
        return db + 2

    # Template base: materialize inherited by both subclasses. The worker must
    # rebind `cls` to the subclass, not the defining base (FuncRef resolves
    # through the bound classmethod's __self__).
    class SeedBase(rs.Asset):
        io_handler = handler
        seed = 0

        @classmethod
        def materialize(cls) -> int:
            return cls.seed

    class SeedOne(SeedBase):
        seed = 100

    class SeedTwo(SeedBase):
        seed = 200

    # Multi class-form: per-output handlers live on the AssetDefs, so the
    # handler ref probe fails parent-side and the raw handler ships instead.
    class MIngest(rs.MultiAsset):
        m_left = rs.AssetDef(io_handler=handler)
        m_right = rs.AssetDef(io_handler=handler)

        @classmethod
        def materialize(cls):
            return {"m_left": 5, "m_right": 6}

    class MSide(rs.Asset):
        io_handler = handler

        @classmethod
        def materialize(cls) -> int:
            return 7

    # PID probes: prove the topology really crossed into a worker process.
    class PidLeft(rs.Asset):
        io_handler = handler

        @classmethod
        def materialize(cls) -> int:
            return os.getpid()

    class PidRight(rs.Asset):
        io_handler = handler

        @classmethod
        def materialize(cls) -> int:
            return os.getpid()
