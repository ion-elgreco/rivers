"""What the suite measures: one declaration per benchmark.

Everything that varies by benchmark is here — the sizes, the per-point budget,
the configurations it is measured in, the field it reports, and the column it
reports under. The sweep and the report both read this, so adding a benchmark
or changing a sweep's shape is a change to this file.

Nothing here knows about a framework. Which frameworks answer a benchmark is
decided by what each driver registers.

The models are strict: a declaration is rejected at import rather than coerced,
because a benchmark that measures the wrong thing is worse than one that does
not start. rivers already depends on pydantic, so this costs no new package.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator

# Reject unknown fields and refuse to coerce, so a typo'd declaration fails
# loudly instead of quietly measuring something else.
STRICT = ConfigDict(frozen=True, strict=True, extra="forbid")


class Mode(BaseModel):
    """One configuration a benchmark is measured in.

    Only the sensor daemon has more than one. Its shipped interval is what a
    framework does out of the box, and the tuned interval is what an operator
    would configure, so both are worth publishing.
    """

    model_config = STRICT

    tuned: bool = False
    # Measurement window, seconds. Overrides the sweep's `--duration` when it
    # is longer, because a 30-second interval needs several intervals to judge.
    window: float = Field(default=0.0, ge=0.0)
    # Restrict this mode to these sizes. Empty means every size.
    sizes: tuple[int, ...] = ()
    # Swept only when the caller asks for it, because it is slow.
    optional: bool = False


class Benchmark(BaseModel):
    """How one benchmark is swept and how its headline number is reported."""

    model_config = STRICT

    quick: tuple[int, ...] = Field(min_length=1)
    full: tuple[int, ...] = Field(min_length=1)
    timeout: int = Field(gt=0)
    metric: str = Field(min_length=1)
    size_header: str = Field(min_length=1)
    # The measured window is part of the cost, so the budget has to cover it.
    windowed: bool = False
    # Appears in the "Where rivers is slower" table. False where a smaller
    # number is not better, or where the metric saturates by design.
    regression: bool = True
    modes: tuple[Mode, ...] = Field(default=(Mode(),), min_length=1)

    @model_validator(mode="after")
    def _measurable(self) -> Benchmark:
        """Reject a declaration that would quietly measure nothing.

        Both of these fail silently otherwise: the sweep runs, the file is
        written, and the missing points only show up as gaps in a table an hour
        later.
        """
        tuned = [mode.tuned for mode in self.modes]
        if len(set(tuned)) != len(tuned):
            # A measurement is identified by (benchmark, framework, n, tuned),
            # so two modes sharing `tuned` overwrite each other in the report.
            raise ValueError("modes must differ in `tuned`, which identifies them")
        swept = set(self.quick) | set(self.full)
        for mode in self.modes:
            unknown = set(mode.sizes) - swept
            if unknown:
                raise ValueError(f"mode restricted to unswept sizes {sorted(unknown)}")
        return self

    def sizes(self, plan: str) -> tuple[int, ...]:
        """The sizes this benchmark is swept at under ``quick`` or ``full``."""
        return self.quick if plan == "quick" else self.full

    def budget(self, window: float) -> int:
        """Wall-clock ceiling for one point.

        A point that blows through it is recorded as a timeout, which is itself
        the answer to "where does this stop working?". A framework that cannot
        load a graph in four minutes has failed for any interactive use, so the
        sweep stops waiting there.
        """
        if not self.windowed:
            return self.timeout
        # Counting ticks at large sensor counts costs real time on top of the
        # window itself.
        return max(self.timeout, int(window * 2 + 120))


# `quick` is a smoke pass that checks the whole suite works. `full` pushes each
# size up until something breaks, because where a framework stops working is
# itself a result.
BENCHMARKS: dict[str, Benchmark] = {
    "cold_start": Benchmark(
        quick=(10, 100, 1_000),
        full=(10, 100, 1_000, 10_000, 50_000),
        timeout=240,
        metric="cold_start_ms",
        size_header="Assets",
    ),
    "graph_load": Benchmark(
        quick=(100, 1_000, 10_000),
        full=(100, 1_000, 10_000, 50_000, 100_000),
        timeout=240,
        metric="total_ms",
        size_header="Assets",
    ),
    "run_latency": Benchmark(
        quick=(1,),
        full=(1,),
        timeout=300,
        metric="latency",
        size_header="Assets",
    ),
    "run_throughput": Benchmark(
        quick=(50,),
        full=(200,),
        timeout=420,
        metric="runs_per_s",
        size_header="Runs",
        regression=False,  # More runs per second is better, not worse.
    ),
    "sensor_pass": Benchmark(
        quick=(10, 50, 100),
        full=(10, 50, 100, 250, 500, 1_000, 2_000),
        timeout=180,
        metric="pass_ms",
        size_header="Sensors",
        windowed=True,
        regression=False,  # A pass cannot beat the interval, so it saturates.
        modes=(
            Mode(tuned=True),
            # The shipped 30-second interval needs a window several intervals
            # long, so it is swept at a few sizes and only when asked for.
            Mode(window=95.0, sizes=(100, 500, 2_000), optional=True),
        ),
    ),
    "partitions": Benchmark(
        quick=(1_000, 10_000),
        full=(1_000, 10_000, 100_000, 500_000, 1_000_000),
        timeout=240,
        metric="total_ms",
        size_header="Partitions",
    ),
    "partition_ops": Benchmark(
        quick=(1_000, 10_000),
        full=(1_000, 10_000, 100_000, 1_000_000),
        timeout=420,
        metric="total_ms",
        size_header="Partitions",
    ),
    "graph_deps": Benchmark(
        quick=(100, 1_000),
        full=(100, 1_000, 10_000, 50_000),
        timeout=300,
        metric="total_ms",
        size_header="Assets",
    ),
    "selection": Benchmark(
        quick=(100, 1_000),
        full=(100, 1_000, 10_000),
        timeout=300,
        metric="total_ms",
        size_header="Assets",
    ),
    "run_steps": Benchmark(
        quick=(10, 100),
        full=(10, 100, 1_000, 5_000),
        timeout=420,
        metric="ms_per_step",
        size_header="Steps",
    ),
    "parallel_scaling": Benchmark(
        quick=(8, 32),
        full=(8, 32, 128, 512),
        timeout=420,
        metric="total_ms",
        size_header="Assets",
    ),
    "read_path": Benchmark(
        quick=(100, 1_000),
        full=(100, 1_000, 10_000),
        # Generous, because the budget has to cover seeding as well as the
        # queries, and seeding is neither timed nor compared. Prefect writes
        # its rows one client call at a time and needs about 20 minutes to
        # reach 10,000 runs; timing out there would report nothing about the
        # read path, which is the only thing this benchmark is asking about.
        timeout=1800,
        metric="query_ms",
        size_header="Runs",
    ),
    "queue_limits": Benchmark(
        quick=(100,),
        full=(100, 1_000, 10_000),
        timeout=300,
        metric="claims_per_s",
        size_header="Claims",
        regression=False,  # More claims per second is better, not worse.
    ),
    "log_capture": Benchmark(
        quick=(100, 1_000),
        full=(100, 1_000, 10_000, 100_000),
        timeout=420,
        metric="total_ms",
        size_header="Log lines",
    ),
    "backfill_drain": Benchmark(
        quick=(10, 50),
        full=(10, 50, 200, 1_000),
        timeout=900,
        metric="partitions_per_s",
        size_header="Partitions",
        regression=False,  # More partitions per second is better, not worse.
    ),
    "condition_pass": Benchmark(
        quick=(10, 100, 500),
        # 20,000 is where the two separate: both hold 10,000, so stopping
        # there reported a tie at the sweep's own edge rather than a ceiling.
        full=(10, 50, 100, 500, 2_000, 10_000, 20_000),
        timeout=900,
        metric="pass_ms",
        size_header="Assets",
        windowed=True,
        regression=False,  # A pass cannot beat the interval, so it saturates.
    ),
    "schedule_tick": Benchmark(
        quick=(10, 50),
        full=(10, 50, 100, 250, 500),
        timeout=300,
        metric="total_ms",
        size_header="Schedules",
    ),
    "cancel_latency": Benchmark(
        quick=(1,),
        full=(1,),
        timeout=300,
        metric="cancel_ms",
        size_header="Runs",
    ),
    "reload": Benchmark(
        quick=(100, 1_000),
        full=(100, 1_000, 10_000),
        timeout=300,
        metric="total_ms",
        size_header="Assets",
    ),
}

# Fields worth seeing scroll past during a sweep: every headline metric, plus
# the breakdowns that say where the time went.
LIVE_KEYS = tuple(
    dict.fromkeys(
        [b.metric for b in BENCHMARKS.values()]
        + ["load_ms", "ms_per_run", "efficiency", "write_ms", "granted", "refused"]
    )
)
