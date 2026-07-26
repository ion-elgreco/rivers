# Migrating from Dagster or Prefect

rivers ships an **agent skill** that teaches AI coding agents how to port a Dagster or Prefect project to rivers: the concept mapping, the exact API surface, and the places where a mechanical translation would silently change behavior.

The skill lives in [`skills/migrate-to-rivers/`](https://github.com/ion-elgreco/rivers/tree/main/skills/migrate-to-rivers) in the rivers repository.

## Why a skill

rivers is young enough that models invent its API when asked to write it from memory — plausible names like `PartitionsDefinition.static(...)` or `MetadataValue.float(...)` that do not exist. The skill pins the agent to a signature-exact reference generated from the type stubs, and gives it a porting workflow with validation gates, so a migration fails loudly instead of producing code that looks right.

## Install

=== "Claude Code (plugin)"

    ```
    /plugin marketplace add ion-elgreco/rivers
    /plugin install rivers@rivers
    ```

=== "Any agent (copy)"

    ```bash
    git clone --depth 1 https://github.com/ion-elgreco/rivers /tmp/rivers
    mkdir -p .claude/skills
    cp -r /tmp/rivers/skills/migrate-to-rivers .claude/skills/
    ```

    Agents other than Claude Code can read `SKILL.md` and the files in `references/`
    directly — they are plain markdown with no tool-specific syntax.

## Use

Point your agent at the project you want to port:

```text
Migrate the Dagster project in ./pipelines to rivers
```

The skill takes over from there: it inventories the source project, maps concepts, ports bottom-up (resources and IO handlers, then assets, partitions, automation, jobs), and validates each slice with `repo.validate()` before moving on.

## What it covers

| | Dagster | Prefect |
|---|---|---|
| Assets / ops / tasks | ✅ | ✅ |
| Multi-assets, graph assets | ✅ | ✅ |
| Partitions & partition mappings | ✅ | n/a |
| Automation conditions | ✅ | ✅ (from triggers) |
| Schedules & sensors | ✅ | ✅ (from deployments) |
| IO managers → IO handlers | ✅ | ✅ (from result storage) |
| Resources / config | ✅ | ✅ (from Blocks) |
| Retries, concurrency, executors | ✅ | ✅ |

Dagster maps closely — rivers uses the same asset model, so most definitions have a direct counterpart. Prefect maps loosely: its flows are imperative and rivers' graph is declarative, so the skill treats those ports as design work and asks rather than guessing.

## What it will tell you it cannot do

The skill is written to report gaps instead of approximating them. Expect it to flag:

- **Asset checks** — no rivers equivalent; assertions must move into the asset body
- **Integration packages** — `dagster-dbt`, `dagster-dlt` and friends have no rivers ports
- **Prefect caching** — `cache_policy` / `cache_key_fn` have no direct analog; rivers uses materialization state, `code_version`, and `data_version` instead
- **Dynamic control flow** — Prefect flows whose shape depends on runtime data do not fit a declarative DAG
- **Cloud-only features** — Dagster+ and Prefect Cloud functionality

## Keeping it honest

Every rivers snippet in the skill's reference files is exercised by
`python/tests/test_migration_skill.py`, which runs against the built extension. If an
API is renamed, the test fails and the skill gets updated with it.
