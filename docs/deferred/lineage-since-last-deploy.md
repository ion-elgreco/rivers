# Deferred: Lineage "Since last deploy" view (enterprise candidate)

Status: deferred — removed from UI until real deploy-diff data exists in storage.
Target: **Enterprise feature** when reintroduced.

## What it did

A toggle in the lineage page topbar (`Since last deploy`) that when on:

1. Highlighted nodes whose code version changed in the last deploy with a `NEW`
   chip (top-right of the capsule) + primary-color ring around the node.
2. Highlighted edges touching any changed node with the primary color + flowing
   gradient animation; dimmed all other edges.
3. Dimmed nodes that existed before the deploy.
4. Surfaced a small inline chip next to the toggle showing the deploy sha +
   relative time (e.g. `deploy · 610e0a1 · a few minutes ago`).

It matched Rivers' `LineageGraphScreen` "deployView" toggle + deploy-diff banner
(`rivers-screens.jsx:1181-1205`, `:1242-1259`).

## Why it was removed

The "changed subset" was synthesized — we picked every 3rd node deterministically
because storage doesn't yet track which assets shipped in which deploy. Showing
fake deploy deltas is worse than showing none, so the toggle was cut.

## What needs to land before reintroducing

1. **Deploy recording**: storage needs to persist each materialize of the code
   location with a sha + timestamp + author, and track which asset
   `code_version` values changed since the previous deploy.
2. **Server fn**: `get_last_deploy() -> Option<DeployRecord>` returning
   `{ sha, when, by, added: Vec<key>, changed: Vec<key>, removed: Vec<key> }`.
3. **Feature gate**: since the commercial angle is deploy auditing + drift
   detection, gate the UI toggle + the server fn behind the enterprise feature
   flag (whatever mechanism we pick for that layer — likely a build-time cfg or
   runtime license check).

## Sketch of the reintroduction

```rust
// graph.rs
let (deploy_view, set_deploy_view) = signal(false);
let last_deploy = Resource::new(|| (), |_| get_last_deploy());

// Topbar toggle
<label class="toggle-label" title="Highlight assets changed since the last deploy">
    <input type="checkbox" prop:checked=... on:change=... />
    "Since last deploy"
</label>
<Show when=move || deploy_view.get()>
    {move || last_deploy.get().and_then(|r| r.ok()).flatten().map(|d| view! {
        <span class="deploy-pill">{format!("deploy · {} · {}", d.sha, d.when)}</span>
    })}
</Show>

// DagGraph wiring
let changed = if deploy_view.get() {
    last_deploy.get().and_then(|r| r.ok()).flatten()
        .map(|d| d.changed.into_iter().collect::<HashSet<_>>())
        .unwrap_or_default()
} else {
    HashSet::new()
};
<DagGraph ... changed_keys=changed />
```

The `DagGraph` component **still supports** the `changed_keys` prop and the
`NEW` chip rendering — that plumbing wasn't torn out, so reintroduction is just
about surfacing the toggle and feeding real data into it.

## Related files

- `rust/rivers-ui/src/pages/graph.rs` (deploy_view signal + toggle removed here)
- `rust/rivers-ui/src/components/dag/render.rs` (`changed_keys` prop + NEW chip
    - accent ring rendering — preserved)
- Rivers reference: `/tmp/design/rivers-v2/project/components/rivers-screens.jsx`
  `LineageGraphScreen` deploy-diff banner + node NEW chip
