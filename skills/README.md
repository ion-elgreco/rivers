# rivers agent skills

Skills that teach AI coding agents how to work with rivers. Each is a directory containing a `SKILL.md` plus reference files — plain markdown, readable by any agent.

| Skill | What it does |
|---|---|
| [`migrate-to-rivers`](migrate-to-rivers/) | Ports a Dagster or Prefect project to rivers: concept mapping, a signature-exact API reference, and the semantic gaps a mechanical translation would miss. |

## Install

For Claude Code:

```text
/plugin marketplace add ion-elgreco/rivers
/plugin install rivers@rivers
```

For anything else, copy the skill directory into wherever your agent looks for skills (`.claude/skills/` for Claude Code, or point the agent at the files directly):

```bash
git clone --depth 1 https://github.com/ion-elgreco/rivers /tmp/rivers
mkdir -p .claude/skills && cp -r /tmp/rivers/skills/migrate-to-rivers .claude/skills/
```

See the [migration guide](https://ion-elgreco.github.io/rivers/guides/migrating/) for details.

## Accuracy

The rivers API these skills document is guarded by `python/tests/test_migration_skill.py`, which runs every documented example against the built extension. If an API changes, the test fails and the skill gets updated with it — agents reading a stale reference is the failure mode worth engineering against.
