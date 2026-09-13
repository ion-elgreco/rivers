#!/usr/bin/env python3
"""Phase 1 of RFC-046: swap concrete SurrealStorage for AnyStorage at call sites.

Leaves the SurrealDB backend itself and the generated dispatch module alone.
Run from the repo root:  python3 scripts/swap_any_storage.py
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

SKIP = ("storage/surrealdb_backend", "storage/any.rs")

# Constructor renames. Order matters: longest prefix first so that
# `new_embedded_with_capability` is not eaten by `new_embedded`.
CTORS = [
    ("SurrealStorage::new_embedded_with_capability", "AnyStorage::surreal_embedded_with_capability"),
    ("SurrealStorage::new_embedded_blocking", "AnyStorage::surreal_embedded_blocking"),
    ("SurrealStorage::new_memory_blocking", "AnyStorage::surreal_memory_blocking"),
    ("SurrealStorage::connect_with_capability", "AnyStorage::surreal_connect_with_capability"),
    ("SurrealStorage::new_embedded", "AnyStorage::surreal_embedded"),
    ("SurrealStorage::new_memory", "AnyStorage::surreal_memory"),
    ("SurrealStorage::connect", "AnyStorage::surreal_connect"),
]


def targets() -> list[Path]:
    out = subprocess.run(
        ["grep", "-rlE", "SurrealStorage|surrealdb_backend::AnyStorage",
         "--include=*.rs", "rust/", "python/src/"],
        capture_output=True, text=True,
    ).stdout.split()
    return [Path(p) for p in out if not any(s in p for s in SKIP)]


def fix_imports(src: str) -> str:
    """Point SurrealStorage imports at the dispatch module instead."""
    # Inline fully-qualified paths, e.g. `crate::storage::surrealdb_backend::AnyStorage`.
    src = src.replace("::surrealdb_backend::AnyStorage", "::any::AnyStorage")
    # `use <path>::surrealdb_backend::AnyStorage;`  (was the only item)
    src = re.sub(
        r"use ((?:crate|rivers_core)::storage)::surrealdb_backend::AnyStorage;",
        r"use \1::any::AnyStorage;",
        src,
    )
    # `use <path>::surrealdb_backend::{A, AnyStorage, B};` -> split into two lines
    def split_group(m: re.Match) -> str:
        path, items = m.group(1), m.group(2)
        rest = [i.strip() for i in items.split(",") if i.strip() and i.strip() != "AnyStorage"]
        lines = [f"use {path}::any::AnyStorage;"]
        if rest:
            inner = rest[0] if len(rest) == 1 else "{" + ", ".join(rest) + "}"
            lines.append(f"use {path}::surrealdb_backend::{inner};")
        return "\n".join(lines)

    src = re.sub(
        r"use ((?:crate|rivers_core)::storage)::surrealdb_backend::\{([^}]*\bAnyStorage\b[^}]*)\};",
        split_group,
        src,
    )
    return src


def main() -> None:
    files = targets()
    changed = 0
    for f in files:
        src = original = f.read_text()
        for old, new in CTORS:
            src = src.replace(old, new)
        src = re.sub(r"\bSurrealStorage\b", "AnyStorage", src)
        src = fix_imports(src)
        if src != original:
            f.write_text(src)
            changed += 1
    print(f"rewrote {changed} of {len(files)} files", file=sys.stderr)


if __name__ == "__main__":
    main()
