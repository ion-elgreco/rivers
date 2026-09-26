"""Deliberately mistyped class-form assets — never imported at runtime.

test_stubs asserts pyright flags every line carrying an EXPECT-ERROR marker,
proving the declarable attributes on the bases are really typed (not Any).
"""

from pydantic import BaseModel

import rivers as rs


class TuneConfig(BaseModel):
    target_size_mb: int = 128


class Bad(rs.Asset):
    kinds = 123  # EXPECT-ERROR
    retry = 3.5  # EXPECT-ERROR
    pool_slots = "two"  # EXPECT-ERROR
    metadata = {"owner": 1}  # EXPECT-ERROR
    name = 123  # EXPECT-ERROR
    tags = "raw"  # EXPECT-ERROR
    group = 1.5  # EXPECT-ERROR
    code_version = 7  # EXPECT-ERROR
    io_handler = 42  # EXPECT-ERROR
    partitions_def = 99  # EXPECT-ERROR
    deps = "upstream"  # EXPECT-ERROR
    backfill_strategy = "single"  # EXPECT-ERROR
    compute = 42  # EXPECT-ERROR
    pool = 9  # EXPECT-ERROR
    hooks = "hook"  # EXPECT-ERROR
    automation_condition = "cond"  # EXPECT-ERROR
    actions = "compact"  # EXPECT-ERROR

    @classmethod
    def materialize(cls) -> int:
        return 1

    @rs.action(outcome=rs.Outcome.Unchanged)
    @classmethod
    def tune(cls, ctx: rs.ActionContext[TuneConfig]) -> None:
        _wrong: str = ctx.config.target_size_mb  # EXPECT-ERROR
        _r = ctx.resources  # EXPECT-ERROR
