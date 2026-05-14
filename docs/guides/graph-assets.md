# Graph Assets

Graph assets let you compose multiple `Task` operations into a sub-DAG that is treated as a single asset in the outer dependency graph. Internal tasks are namespaced as `{graph_name}/{task_name}` and execute as independent steps in the execution plan.

## When to use graph assets

- You have a multi-step transformation that shouldn't be split into separate assets
- You want to reuse `Task` functions across different graph assets
- You need fine-grained control over the execution order within an asset
- You want internal tasks to use lightweight IO (InMemory) while the graph output uses production IO (Delta Lake)

## Defining tasks

A `Task` is a unit of computation inside a graph asset:

```python
import rivers as rs

@rs.Task
def fetch_data(url: str):
    import requests
    return requests.get(url).json()

@rs.Task
def clean(raw: list):
    return [r for r in raw if r.get("valid")]

@rs.Task(name="enrich", tags=["slow"])
def enrich_data(records: list):
    return [{**r, "enriched": True} for r in records]
```

## Building a graph asset

Use `Asset.from_graph()` to create a graph asset. Inside the function body, calling tasks records them into a composition graph:

```python
@rs.Asset.from_graph()
def user_pipeline():
    raw = fetch_data("https://api.example.com/users")
    cleaned = clean(raw)
    return enrich_data(cleaned)
```

The `return` value determines the graph asset's **final node** — its output becomes the graph asset's output. Internal tasks are namespaced: `user_pipeline/fetch_data`, `user_pipeline/clean`, `user_pipeline/enrich`.

## Input wiring

Task inputs can be wired in two ways:

**Positional** — pass the output of one task as an argument to the next:

```python
@rs.Asset.from_graph(name="chain")
def chain():
    x = step_a()
    y = step_b(x)       # step_b receives step_a's output
    return step_c(y)
```

**Keyword** — explicitly name which parameter receives the value:

```python
@rs.Asset.from_graph(name="chain")
def chain():
    x = step_a()
    y = step_b(x)
    return step_c(value=y)  # only 'value' param is wired from composition
```

**Implicit resolution** — task parameters not wired through the composition graph are resolved by name from the outer dependency graph (assets, other tasks):

```python
@rs.Asset
def config_data() -> dict:
    return {"threshold": 0.5}

@rs.Task
def process(value: int, config_data: dict) -> int:
    return value if value > config_data["threshold"] else 0

@rs.Asset.from_graph(name="pipeline")
def pipeline():
    x = compute()
    return process(x)  # 'config_data' is implicitly resolved from the asset
```

## External dependencies

Graph asset functions can accept parameters from the outer dependency graph:

```python
@rs.Asset
def source() -> int:
    return 5

@rs.Asset.from_graph(name="pipeline")
def pipeline(source: int):
    return transform(source)
```

Assets called inside the graph body keep their bare name (not namespaced) — they are external dependencies, not internal tasks.

## IO handler configuration

By default, internal tasks use the same IO handler as the graph asset (or the repository default). Use `node_io_handler` to give internal tasks a different handler:

```python
@rs.Asset.from_graph(
    name="pipeline",
    node_io_handler=rs.InMemoryIOHandler(),  # internal tasks: fast in-memory
    io_handler=pickle_handler,                # graph output: persisted to storage
)
def pipeline():
    x = step_a()
    return step_b(x)
```

The final task's output is written through **both** handlers: `node_io_handler` for the task itself and `io_handler` for the graph asset (dual IO write).

**Resolution hierarchy** for internal task IO handlers:

1. `node_io_handler` if set
2. `io_handler` if set
3. Default IO handler from CodeRepository (InMemoryIOHandler)

Both `io_handler` and `node_io_handler` accept resource references:

```python
@rs.Asset.from_graph(
    name="pipeline",
    node_io_handler="fast_handler",   # resolved from resources
    io_handler="prod_handler",
)
def pipeline():
    ...

repo = rs.CodeRepository(
    assets=[pipeline],
    resources={
        "fast_handler": rs.InMemoryIOHandler(),
        "prod_handler": pickle_handler,
    },
)
```

## Executor configuration

Use `rivers/node/executor` metadata to control which executor internal tasks use:

```python
@rs.Asset.from_graph(
    name="pipeline",
    metadata={
        "rivers/node/executor": "in_process",  # internal tasks: in-process
        "rivers/executor": "parallel",         # graph asset step: subprocess pool
    },
)
def pipeline():
    ...
```

### Why split the executor for the graph and its internals?

The two-key split lets you pick **isolation/scale at the graph boundary** while keeping **the internals cheap and traceable**. The most useful pattern in production:

- `rivers/executor: "kubernetes"` — the graph asset runs as its own pod (its own image, its own CPU/memory request, its own retry boundary).
- `rivers/node/executor: "in_process"` — every internal task in that graph runs inside the same step pod, in-process.

You get:

- **Lowest per-task overhead** — internal tasks share one Python interpreter, one IO handler instance, one set of resource connections. No fresh pod startup, no container scheduling latency, no extra network round-trips between tasks.
- **Per-graph isolation preserved** — the *outer* pipeline still gets process / pod isolation between graphs; just the inside of each graph collapses into a single execution.

Without `rivers/node/executor`, internal tasks would inherit the outer executor — under `kubernetes`, that means each internal task spawns its own step pod. That's right when individual tasks need different images or resources, but excessive for the common case where all tasks in a graph asset are part of the same logical step.

The same pattern works with `parallel` outside and `in_process` inside: the graph asset step gets a worker subprocess (avoiding interpreter contention with other graphs), but its internals run synchronously in that subprocess.

### Resolution hierarchy

For internal task executor:

1. `rivers/node/executor` metadata if set
2. `rivers/executor` metadata if set
3. Job executor or default executor

## Dynamic fan-out

A task that produces a list (or a list of `DynamicOutput`) can fan a downstream task out across each element. Each element becomes its own step instance — scheduled, retried, and persisted independently.

### Barrier collect

The simplest pattern: produce a list, fan a worker out across it, gather the results back, and pass them to a single downstream task.

```python
@rs.Task
def numbers() -> list[int]:
    return [1, 2, 3, 4, 5]

@rs.Task
def double(x: int) -> int:
    return x * 2

@rs.Task
def sum_all(values: list[int]) -> int:
    return sum(values)

@rs.Asset.from_graph()
def doubled():
    nums = numbers()
    mapped = nums.map(double)        # fan out — one `double` instance per element
    return sum_all(mapped.collect())  # barrier — wait for every instance to finish
```

`mapped.collect()` blocks until every fanned-out instance has materialized, then yields a single list-typed `InvokedNodeOutput` you can wire into any downstream task (here `sum_all`).

### Concurrency cap

Pass `max_concurrency=N` to `map` to throttle how many instances run in parallel — useful when the fanned-out task hits a rate-limited API or expensive resource:

```python
mapped = ids.map(fetch_record, max_concurrency=4)
```

Without `max_concurrency`, the executor schedules instances as eagerly as the outer executor allows.

### Streaming collect

When downstream consumption can start before every instance has finished — and especially when results are large — use `.collect_stream()`. It hands the consumer a generator instead of a list:

```python
@rs.Task
def consume_stream(items: object) -> list:
    return [x for x in items]   # iterate as instances complete

@rs.Asset.from_graph()
def streamed():
    nums = numbers()
    mapped = nums.map(process_item)
    return consume_stream(mapped.collect_stream())
```

By default `collect_stream()` emits in **completion order**; pass `ordered=True` to emit in mapping-key order instead. Streaming is the right choice when downstream is itself streaming-friendly (a writer, an aggregator) — it avoids buffering the full result set.

### Named instances with `DynamicOutput`

By default, fanned-out instances are named with their numeric index (`double[0]`, `double[1]`, ...). When the producer wants stable, human-readable instance names, return a list of `rs.DynamicOutput`:

```python
@rs.Task
def docs() -> list[rs.DynamicOutput]:
    return [
        rs.DynamicOutput(key="report_q1", value="/data/report_q1.pdf"),
        rs.DynamicOutput(key="report_q2", value="/data/report_q2.pdf"),
        rs.DynamicOutput(key="invoice",   value="/data/invoice.pdf"),
    ]

@rs.Asset.from_graph()
def lengths():
    mapped = docs().map(path_length)
    return sum_all(mapped.collect())
```

Each instance now runs as `lengths/path_length[report_q1]`, `lengths/path_length[report_q2]`, etc. The name flows into logs, run-event records, and the IO-handler key for that instance — useful when re-runs need to skip already-completed work, or when you want to look up a specific instance in the UI.

`DynamicOutput` and plain values cannot be mixed in the same producer's list — pick one shape.

### Failure handling

When a single mapped instance fails, downstream steps that consume the collect output are skipped (under `Executor.parallel()`); other instances continue running. The collect step itself ends in failure, propagating to the graph asset's status. This matches normal asset-step failure semantics — fan-out doesn't introduce a new failure mode.

## Sharing tasks across graph assets

Two graph assets can use the same task — each gets its own namespaced copy with independent wiring and IO:

```python
@rs.Task
def transform(value: int) -> int:
    return value * 2

@rs.Asset.from_graph(name="pipeline_a")
def pipeline_a(source_a: int):
    return transform(source_a)

@rs.Asset.from_graph(name="pipeline_b")
def pipeline_b(source_b: int):
    return transform(source_b)
```

This creates `pipeline_a/transform` and `pipeline_b/transform` as independent steps.

## Using BashTask in graph assets

`BashTask` runs shell commands natively in Rust and can be used inside graph assets:

```python
fetch = rs.BashTask(name="fetch", command="curl -s https://api.example.com/data")

@rs.Task
def parse(raw: str):
    import json
    return json.loads(raw)

@rs.Asset.from_graph()
def api_data():
    raw = fetch()
    return parse(raw)
```

## Loading graph asset outputs

Use `repo.load_node()` to read outputs after materialization:

```python
repo.materialize()

# Load the graph asset's output (final node's value)
repo.load_node("pipeline")

# Load an internal task's output
repo.load_node("pipeline/step_a")
```

## Graph assets in jobs

When a graph asset is included in a `Job`, its internal tasks are automatically included:

```python
repo = rs.CodeRepository(
    assets=[pipeline],
    tasks=[step_a, step_b],
    jobs=[
        rs.Job(name="my_job", assets=[pipeline], executor=rs.Executor.in_process())
    ],
)
repo.get_job("my_job").execute()
```
