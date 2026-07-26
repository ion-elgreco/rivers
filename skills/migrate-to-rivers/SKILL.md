---
name: migrate-to-rivers
description: Translate a Dagster or Prefect project to rivers, the Rust-powered asset orchestrator. Use when porting assets, ops, flows, tasks, jobs, schedules, sensors, partitions, IO managers, resources, retries, or automation conditions to rivers — or when asked to migrate/convert a data pipeline to rivers.
---

# Migrate to rivers

Port a Dagster or Prefect project to [rivers](https://ion-elgreco.github.io/rivers/) without inventing API.

rivers is asset-centric like Dagster, so most Dagster concepts map closely — but the spellings differ in ways that look right and fail at import. Prefect maps loosely: its flows are imperative, rivers' graph is declarative, so parts of a Prefect project need design decisions rather than translation.

## Ground rule

**Never write a rivers symbol you have not verified.** The API is small enough to check and niche enough that a plausible guess is usually wrong. Before using any name:

1. Check `references/rivers-api.md` (in this skill) — signature-exact, generated from the type stubs.
2. If it isn't there, read the stubs directly: `python/rivers/_core/**/__init__.pyi` in the rivers source, or `python -c "import rivers; help(rivers.X)"`.
3. Only then, the [docs site](https://ion-elgreco.github.io/rivers/).

Guessing costs more than checking: `PartitionsDefinition.static_` (trailing underscore), `MetadataValue.float_`, `daily(start=datetime(...))` (a `datetime`, not a string) are all things a confident guess gets wrong.

## Workflow

### 1. Inventory the source project

Read the whole source project before porting anything. Produce a written inventory:

- **Assets/flows** — name, upstream deps, partitioning, IO manager, resources used
- **Orchestration** — jobs, schedules, sensors, automation conditions/triggers
- **Infrastructure** — executors, work pools, K8s config, concurrency limits
- **Config & secrets** — `dg.Config` classes, `ConfigurableResource`s, Prefect Blocks, env vars
- **Things with no rivers equivalent** — asset checks, dbt/dlt integrations, Prefect transactions, Cloud-only features

Report the last group to the user early. Do not silently drop or fake them.

### 2. Map concepts

Read the relevant reference file **in full** before writing code:

- Dagster → `references/from-dagster.md`
- Prefect → `references/from-prefect.md`

Both carry a mapping table, before/after code for every concept, and a gotchas section covering the traps that survive a naive port (sensor cursors, config generics, partition key formats).

### 3. Port, in dependency order

Bottom-up. The graph is the skeleton; everything else attaches to it.

1. **Resources and IO handlers** — assets reference them, so they exist first
2. **Assets** — the DAG, without partitions or automation
3. **Partitions** — partition defs, then partition mappings on the edges
4. **Automation** — automation conditions, schedules, sensors
5. **Jobs, executors, concurrency** — the run-shaping layer
6. **`CodeRepository`** — assembled last, since it registers all of the above

Port a slice at a time and validate it (step 4) before moving on. A 40-asset project ported in one shot fails with 40 tangled errors.

Keep the source project's module layout unless the user asks otherwise — a diffable port is easier to review than a reorganized one.

### 4. Validate continuously

Three gates, cheapest first:

```bash
python -c "from my_pipeline import repo; repo.validate()"   # graph only — no storage, no side effects
rivers materialize my_pipeline --memory                     # end-to-end, in-memory storage
rivers dev my_pipeline                                      # UI on :3000 — does the graph look right?
```

`repo.validate()` is the fast inner loop: it catches cycles, missing upstreams, unresolvable resource params, and bad partition defs in milliseconds without touching storage. Run it after every slice; only reach for the other two once a slice validates.

Then port the tests. If the source project has none, write at least one materialization test per asset group — rivers tests are ordinary pytest against `repo.materialize()` and `repo.load_node()`.

### 5. Report

Summarize for the user:

- What ported cleanly
- **What changed semantically** — every place the rivers version behaves differently (see the gotchas sections; sensor cursors and Prefect's imperative control flow are the usual culprits)
- What was dropped, and why
- What needs their decision

Semantic drift is the thing a migration must surface. A port that runs but computes something subtly different is worse than one that fails loudly.

## Scope discipline

Translate what exists. A migration is not the moment to redesign the pipeline, add error handling the original lacked, or "improve" asset boundaries. If the source has a genuine bug, mention it — don't fix it silently in the port.
