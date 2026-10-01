# Orchestrator benchmark

A reproducible comparison of **rivers**, **Dagster** and **Prefect** on the work
a control plane actually does: starting up, loading a code location, launching
runs, and keeping automations ticking as the workload grows.

Results live in [RESULTS.md](RESULTS.md). Raw measurements are in
[`results/`](results/) as JSON, committed so every published number traces back
to the data that produced it.

## Quick start

You need a rivers build in `.venv` first (`just develop-fast`), plus `uv`.

```bash
bench/setup.sh          # once: builds .venv-bench from its lockfile
bench/run.sh --quick    # about 15 minutes: checks everything works
bench/run.sh            # the full sweep, about two hours
```

Or through `just`, if you have it: `just bench-setup`, `just bench-quick`,
`just bench`, `just bench-k8s`.

For the Kubernetes numbers you also need `docker`, `k3d`, `helm` and `kubectl`:

```bash
bench/run.sh --with-k8s   # about three hours, including a release image build
```

Every run rewrites `RESULTS.md` and the JSON under `results/`.

## What is measured

Measurements are grouped by what they are about. A framework that has no
equivalent feature reports `unsupported` with a reason rather than a number, so
a table can hold two columns or three.

**Loading a code location**

| Benchmark | Question it answers |
|---|---|
| `cold_start` | How long from process launch to a code location that can answer questions about its assets? |
| `graph_load` | How long to build the asset graph in an already warm process? |
| `graph_deps` | The same, for a graph whose assets actually depend on each other. |
| `reload` | How long to load a code location that is already loaded? |
| `selection` | How long to resolve one asset plus its upstream inside a graph of this size? |

**Running work**

| Benchmark | Question it answers |
|---|---|
| `run_latency` | How long does one no-op asset take, end to end? |
| `run_throughput` | How many no-op runs per second? |
| `run_steps` | What does one step cost inside a run of many? |
| `parallel_scaling` | How much of the wall clock is scheduling, when the work itself is fixed? |
| `cancel_latency` | How long from a cancellation request to a terminal run? |
| `log_capture` | What do this many log lines cost to write and to read back? |

**Automation**

| Benchmark | Question it answers |
|---|---|
| `sensor_pass` | How long to tick every sensor once, and does every sensor still fire on time? |
| `condition_pass` | The same question for automation conditions. |
| `schedule_tick` | How long to evaluate every schedule once? |

**Partitions and backfills**

| Benchmark | Question it answers |
|---|---|
| `partitions` | How long to define and load a partitioned asset? |
| `partition_ops` | What do the everyday operations cost once it holds that many keys? |
| `backfill_drain` | How many partitions per second until every one is terminal? |

**Storage**

| Benchmark | Question it answers |
|---|---|
| `read_path` | How long to answer the queries a UI page makes, against a store of this size? |
| `queue_limits` | How many concurrency claims per second, and is the limit ever exceeded? |

The Kubernetes sweep is separate, because it installs charts rather than
driving a framework in-process: how long from `helm install` to a control plane
that accepts work, and what does it cost to run?

`rivers_bench.py` also carries `daemon_shutdown`, which measures what rivers'
deferred per-tick settle costs at `stop()`. The other two have no counterpart,
so it is not swept — run it by hand when that number is wanted.

## Fairness rules

These exist so the numbers survive scrutiny from people who maintain the other
two projects.

1. **Same Python, same machine, same storage class.** Every framework runs on
   CPython 3.13 against local file-backed storage: rivers on embedded
   SurrealDB, Dagster on SQLite, Prefect on SQLite. Each measurement gets a
   fresh temporary store, including Prefect's `PREFECT_HOME`. Dagster and
   Prefect share one virtualenv, which was checked rather than assumed: every
   benchmark was measured in a shared environment and in a separate one, and
   none moved outside run-to-run noise.
2. **Config floors are reported, never sold as performance.** Dagster evaluates
   a sensor at most once per `minimum_interval_seconds`, which defaults to 30.
   rivers defaults to the same 30 seconds (`python/src/daemon/sensors.rs`,
   `unwrap_or(Duration::from_secs(30))`). That is a setting, not a speed limit.
   The sensor benchmark holds every framework to the **same** interval and
   reports each framework's shipped default separately. Comparing rivers
   configured at zero against Dagster at its one-second floor produces a
   four-digit multiplier that means nothing.
3. **Each framework gets its best configuration.** The tuned sensor run gives
   Dagster the thread pool a tuned deployment would configure, and drives its
   iteration loop directly rather than waiting on the sensor daemon's own
   30-second wake cycle.
4. **Every measurement runs in a fresh process.** A crash or an out-of-memory
   kill at one size is recorded as a data point, not propagated to the next.
5. **Missing features are marked, not scored.** Prefect has no sensor, no
   partitioned asset, no backfill, no automation condition and no schedule
   evaluation function, so those cells read "not supported" rather than showing
   a number that implies a comparison. Each `unsupported` entry carries the
   reason, which is what the cell's text comes from.
6. **Every Kubernetes code location holds the same workload.** All three load
   `--assets` no-op assets, so "code location ready" is one measurement rather
   than three different ones sharing a column.
7. **Kubernetes images are cached before timing starts.** An untimed install
   pulls every public image first, so the measured install reflects startup
   rather than download speed. Each orchestrator is installed three times and
   the median is reported.
8. **No emulation.** Dagster publishes amd64-only images. On Apple Silicon they
   would run under emulation, so the benchmark rebuilds the same Dagster version
   natively for the host architecture.
9. **Losses are published too.** `RESULTS.md` ends with a "Where rivers is
   slower" table listing every measurement where rivers takes at least 1.5x the
   best rival's time.
10. **A benchmark that is not like for like says so.** Several are not, and

    each is listed under "Known caveats" below with the direction of the skew.
    Where the skew favours a rival, that is the direction to prefer: a
    conservative number for rivers is one nobody has to take on trust.
11. **Correctness is measured, not assumed.** `queue_limits` fails a framework
   that ever holds more slots than its own limit, however fast it was. A
   limiter that hands out slots it should not is wrong, and reporting only its
   rate would hide that.

## How a failure is classified

| Status | Meaning |
|---|---|
| `ok` | Completed inside its budget. |
| `error` | The framework raised, or the process exited non-zero. |
| `oom` | Resident memory passed 8 GiB, or the OS killed the process. |
| `timeout` | The workload exceeded its per-step wall-clock budget. |
| `starved` | A daemon delivered under 90% of the passes its own interval promised. Some sensor did not fire on time. |
| `unsupported` | The framework has no equivalent feature. |
| `missing_env` | The framework's virtualenv is not built. Run `setup.sh`. |

`starved` is the one to understand. It is not a crash. The system keeps running
and reports itself healthy while automations silently stop firing on schedule.

## Running one piece at a time

Every entry point is a module under `bench`, so run it from the repository
root. Nothing needs installing into the Dagster and Prefect environments: the
working directory is what makes the `bench` package importable from all three.

```bash
# One benchmark, one framework
.venv/bin/python -m bench.local.sweep --only sensor_pass --framework rivers

# Sensors at each framework's shipped 30-second default as well as tuned
.venv/bin/python -m bench.local.sweep --full --default-interval

# Kubernetes only, one install each instead of three
.venv/bin/python -m bench.k8s.bench --repeat 1

# Re-render the document from existing JSON, without measuring anything
.venv/bin/python -m bench.report.render --local results/local_full.json \
    --k8s results/k8s.json --out RESULTS.md

# Update the landing-page section in www/index.html. `run.sh` already does
# this at the end of a sweep; run it directly only to re-render from old JSON.
.venv/bin/python -m bench.report.landing

# One driver on its own, in its own environment, for debugging
.venv-bench/bin/python -m bench.drivers.dagster_bench graph_load --n 1000
```

A sweep writes its JSON after every point, into a `.partial.json` file beside
the real one. The real file is only replaced when the sweep finishes, so
stopping a long run early never truncates results already published.

`render.py` and `landing.py` accept `--local` and `--k8s` more than once. Later
files override earlier ones for the same measurement, which is how a single
point gets re-measured without redoing the whole sweep:

```bash
# Re-measure one benchmark into its own file, then fold it over the full sweep
.venv/bin/python -m bench.local.sweep --full --only cold_start \
    --out results/local_optimized.json
.venv/bin/python -m bench.report.render \
    --local results/local_full.json \
    --local results/local_optimized.json \
    --out RESULTS.md
```

## Layout

One directory per job. A change usually lands in exactly one of them.

```text
bench/
├── setup.sh          build the environments
├── run.sh            run everything, regenerate RESULTS.md
├── paths.py          where every other file lives
├── benchmarks.py     what the suite measures, one declaration each
│
├── harness/          shared measurement code, used by all three drivers
├── drivers/          one script per framework, each in its own virtualenv
├── local/            the local sweep: drive each point in its own process
├── k8s/              the Kubernetes sweep, plus its charts and images
├── report/           turn results JSON into RESULTS.md and the landing page
├── envs/             the pinned Dagster and Prefect environment
└── results/          raw JSON, committed
```

**`benchmarks.py`** — one `Benchmark` per measurement, holding its sizes for
the quick and full sweeps, its per-point budget, the configurations it is
measured in, the field it reports and the column it reports under. The sweep
and the report both read it, so the two cannot disagree about what a benchmark
is.

The models are strict pydantic: a declaration with an unknown field, a wrong
type, two modes that collide in the report, or a mode pinned to a size the
benchmark never sweeps is rejected at import. Each of those otherwise costs a
sweep — the run finishes and the gap only shows up in a table an hour later.

**`harness/`** — everything that must be identical across frameworks, so a
difference in a number comes from the framework and never from how it was
measured.

| File | What it is |
|---|---|
| `result.py` | The `@@RESULT@@` line drivers print and the sweep parses |
| `measure.py` | Timing, memory sampling, and the reported statistics |
| `coldstart.py` | Launching a fresh interpreter and timing it to useful |
| `process.py` | Memory watchdog and child reaping, so one run cannot leak into the next |
| `machine.py` | Hardware and versions, recorded with every result |
| `runner.py` | The entry point every driver shares |

**`drivers/`** — the only code that knows what a framework looks like.

| File | What it is |
|---|---|
| `rivers_bench.py` | rivers measurements, runs in `.venv` |
| `dagster_bench.py` | Dagster measurements, runs in `.venv-bench` |
| `prefect_bench.py` | Prefect measurements, runs in `.venv-bench` |
| `dagster_defs.py` | Dagster code location, loaded over a `ModuleTarget` |

**`local/`** — the sweep that runs on this machine.

| File | What it is |
|---|---|
| `sweep.py` | Runs each point as its own subprocess and collects the results |

`sweep.py` names no benchmark. Which sizes, which budget and which
configurations a benchmark is measured in all come from `benchmarks.py`, so
changing a sweep's shape is a change to that file alone.

**`k8s/`** — the sweep that runs on a k3d cluster.

| File | What it is |
|---|---|
| `bench.py` | Entry point: create the cluster, install, measure, clean up |
| `cluster.py` | The k3d cluster, its registry, and `kubectl`/`helm` wrappers |
| `measure.py` | Readiness polling, pod inventory, image size, memory at idle |
| `deploy.py` | One install function per orchestrator, plus the `ORCHESTRATORS` table |
| `build_images.sh` | The Dagster and Prefect images |
| `build_rivers_images.sh` | Release rivers images — `just k8s-build` only makes debug ones |
| `values/` | Helm values for Dagster and Prefect |
| `images/` | The three code locations, and the Dockerfiles that bake them |

Each orchestrator loads the same code location — `--assets` no-op assets — so
"code location ready" measures the same job three times:

| File | Serves |
|---|---|
| `bench_pipeline.py` | rivers, through a `CodeLocation` resource |
| `bench_defs.py` | Dagster, through its user-code gRPC server |
| `bench_flows.py` | Prefect, registered as deployments after the worker is up |

Prefect has no code location object, so there is nothing for its chart to
serve. A flow becomes servable through a client call instead, which is why its
image runs one in-cluster rather than sitting behind a server.

**`report/`** — nothing here measures anything; it only reads JSON.

| File | What it is |
|---|---|
| `data.py` | Load result files, merge them, look measurements up |
| `summary.py` | The headline rows, which `RESULTS.md` and the landing page both render |
| `tables.py` | One function per markdown table, all on one `markdown_table` primitive |
| `document.py` | Assembles `RESULTS.md` from those tables |
| `render.py` | Command line for the document |
| `landing.py` | Renders the `www/index.html` section and inserts it |

## Adding a benchmark

Each driver is a plain script that hands `harness.main()` a dict of named
functions. A function receives parsed arguments and returns a dict of numbers.
Everything else — timing, the memory watchdog, the failure taxonomy, the result
line the sweep parses — is handled for you.

```python
from bench.harness import main


def bench_example(args):
    """One sentence on what question this answers."""
    started = time.perf_counter()
    do_the_work(args.n)
    return {"total_ms": (time.perf_counter() - started) * 1000}


if __name__ == "__main__":
    main("rivers", {"example": bench_example})
```

Then, in order:

1. Add the same name to the other two drivers in `drivers/`.
2. Declare it in `benchmarks.py`. That one entry gives it sizes, a budget, the
   field it reports and its table column, for both the sweep and the report.
3. Place `table(local, "<name>")` in `report/document.py`, with a sentence
   saying what the reader is looking at.
4. Add a row to `report/summary.py`, under the topic it belongs to. That is the
   one list `RESULTS.md` and the landing page both render, so a benchmark left
   out of it has per-size tables but no headline anywhere.

A benchmark measured in more than one configuration declares a `Mode` for each
— that is how the sensor daemon is swept both tuned and at the shipped
interval, without the sweep knowing which benchmark that is.

If a framework has no equivalent feature, register
`unsupported("why")` from `bench.harness` rather than inventing a number.

## Reproducing the published numbers

Dependencies are declared and locked, never installed ad hoc:

| Environment | Declared in | Installed into |
|---|---|---|
| rivers | `pyproject.toml`, dev group | `.venv` |
| Dagster and Prefect | `bench/envs/pyproject.toml` + `uv.lock` | `.venv-bench` |

rivers has its own environment because maturin builds it there. Dagster and
Prefect share one: they resolve together cleanly, so a second environment would
have bought nothing.

`setup.sh` runs `uv sync --frozen` against the lockfile, so it installs exactly
the resolved tree that produced the published figures — not whatever resolves
today. To refresh a framework, bump its version in `bench/envs/pyproject.toml`,
run `uv lock --project bench/envs`, and commit the new lock.

One lockfile covers both frameworks, so bumping either one can move a shared
pin under the other. Rerun the whole sweep after a bump, never half of it.

Bumping Dagster or Prefect means bumping `bench/k8s/deploy.py` too, so the
local and in-cluster measurements run the same version. `DAGSTER_VERSION` and
`PREFECT_VERSION` there pin every chart version and image tag. The same two
numbers appear in `bench/k8s/build_images.sh`, which builds those images.

The machine is recorded in every result and printed at the top of `RESULTS.md`.
Absolute timings depend on hardware; the ratios between frameworks are what
transfer.

## Known caveats

- **rivers persists its graph at load; the other two do not.** rivers'
  `cold_start`, `graph_load`, `graph_deps`, `partitions` and `reload` figures
  include writing topology to storage. `RESULTS.md` reports the pure in-memory graph build separately,
  which is the like-for-like row.
- **Prefect's run model always crosses an API boundary.** Its run latency
  includes a round trip to its server, which rivers and Dagster do not pay in
  these measurements. That is architecture, not overhead in the usual sense.
- **`condition_pass` does not compare like for like, and reports only the
  steady state.** Both evaluators are held to the same 1-second interval and
  both evaluate every asset every pass. But rivers' daemon also dispatches, and
  Dagster's evaluation returns its run request without launching it, because
  Dagster exposes no way to include the dispatch without running its whole asset
  daemon. Everything else about the two passes is now the same. Both are
  counted on durable records — rivers on tick records, Dagster on the
  evaluations its asset daemon persists — so neither is judged on a number held
  in memory. Both hold one asset missing, a canary whose step always fails, so
  each pass requests exactly one run rather than none or `n`; Dagster gets
  there with a runless materialization event because it launches nothing.
  Separately, both are measured after the first pass's fan-out has been
  absorbed, so neither column includes what reaching the steady state costs.
- **`backfill_drain` compares two architectures, not two tunings.** rivers
  executes each partition in the calling process. Dagster submits a run per
  partition, which a second daemon dequeues and launches as its own process.
  Both are what the framework does by default; neither can be configured into
  the other without leaving the supported path.
- **`graph_deps` is the same measurement as `graph_load` for Prefect.** Prefect
  resolves no dependencies before a run, so there is no edge resolution to
  time. Its column is reported rather than left empty, because the work it does
  do — defining the tasks and building the flow — is real.
- **`log_capture` waits for Prefect's logs to land.** Prefect ships logs to its
  API from a background worker, so a line is not readable when the run ends.
  Its `read_ms` covers waiting for every line to become readable, which is the
  state the other two reach before their run returns.
- **`queue_limits` holds all three to one contract.** Prefect queues a
  claimant it cannot serve, so rivers and Dagster retry until the slot frees
  rather than reporting a refusal the moment the pool is full. A claim that
  never lands inside 30 seconds is the refusal. Without this the rate compared
  a grant against a refusal, and refusing is nearly free: Dagster turned away
  9,995 of 10,000 attempts and scored well for it.
- **`parallel_scaling` runs Prefect on threads.** Its `ThreadPoolTaskRunner`
  is the shipped default; rivers and Dagster both use processes, which costs
  more to start. rivers also writes a pickled return value per asset through
  its IO handler, which the other two do not. Both differences favour Prefect,
  and rivers still leads the row.
- **Every Prefect run benchmark waits for Prefect to record the run.** Prefect
  writes task-run state from a background worker, so a flow call returns before
  any of it exists — at 5,000 tasks, none of the 5,000 runs had reached the API
  when the call returned. `run_latency`, `run_throughput` and `run_steps` all
  wait, because rivers and Dagster have finished writing before their own call
  returns.
- **`reload` rebuilds the definitions in all three.** A reload re-imports the
  code location, so the units are rebuilt inside the timer. Calling Prefect's
  `to_deployment` twice on an already-built flow is not a reload: it costs the
  same at any size, which is why that column used to read 0.2 ms whether it
  held 100 assets or 10,000.
- **Seeding is never timed.** `read_path` fills each store by whatever path is
  cheapest for that framework — rivers by running runs, the other two by
  writing records straight in. What is compared is reading at equal data
  volume, never the writing.
- **The Kubernetes stacks are not the same shape.** rivers deploys an operator,
  a UI and SurrealDB; Dagster a webserver, a daemon, PostgreSQL and a user-code
  server; Prefect a server, PostgreSQL and a worker. `RESULTS.md` lists the pod
  inventory for each so the comparison is legible rather than implied.
- **`k3d image import` fails under Docker Desktop's containerd image store.**
  The benchmark works around this by creating the cluster with its own registry
  and pushing to it.
