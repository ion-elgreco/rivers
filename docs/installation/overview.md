# Overview

How rivers' components fit together on a Kubernetes cluster.

## What gets deployed

| Component | Image | Purpose |
| --- | --- | --- |
| `rivers-operator` | `ghcr.io/ion-elgreco/rivers-operator` | Reconciles `CodeLocation` CRs into Deployments; runs admission webhooks |
| `rivers-ui` | `ghcr.io/ion-elgreco/rivers-ui` | Web UI (Leptos SSR + WASM hydration), reads run state from SurrealDB |
| SurrealDB | `surrealdb/surrealdb:v3` (subchart) | Shared state — runs, events, asset materializations |

The `rivers` Helm chart wires all three together; the `rivers-crds` Helm chart ships the cluster-scoped `CustomResourceDefinitions` (`CodeLocation`, `Run`) that the operator watches.

## How it fits together

```
   ┌────────────────┐                                ┌─────────────┐
   │  User Browser  │                                │   kubectl   │
   └────────┬───────┘                                └──────┬──────┘
            │ HTTP + SSE                                    │ apply
            │                                               │ CodeLocation
            │                                               │ CR
            ▼                                               ▼
   ┌────────────────┐                              ┌────────────────┐
   │   rivers-ui    │── gRPC :50052 (registry) ───▶│ rivers-operator│
   │  Leptos SSR    │   discover CodeLocations &   │ • registry     │
   │     :3000      │   their grpc endpoints       │       :50052   │
   └─┬──────────┬───┘                              │ • webhook      │
     │          │                                  │       :9443    │
     │          │                                  │ • watches CL   │
     │          │  gRPC :3001                      └───────┬────────┘
     │          │  (trigger run,                           │ creates
     │          │   materialize asset)                     │ Deployment
     │          │                                          │ + Service
     │          │                                          ▼
     │          │                                 ┌────────────────┐
     │          └────────────────────────────────▶│ CodeLocation   │
     │                                            │ Pod(s)         │
     │                                            │ rivers serve   │
     │                                            │     :3001      │
     │  reads runs /                              │ user pipeline  │
     │  asset state                               │ image          │
     │                                            └───────┬────────┘
     │                                                    │ writes
     │                                                    │ run events
     ▼                                                    ▼
   ┌──────────────────────────────────────────────────────────────┐
   │                       SurrealDB :8000                        │
   └──────────────────────────────────────────────────────────────┘
```

`kubectl apply` lands a `CodeLocation` CR (the mutating webhook stamps `spec.identity`; the validating webhook rejects changes to it). The operator watches the CR, resolves `spec.image:spec.tag` to a digest (or trusts `spec.digest` directly), and reconciles a `Deployment` + `Service` running `rivers serve` on the user's pipeline image; it then registers the resulting gRPC endpoint in its in-process `CodeLocationRegistry`.

The UI, on every page load, queries that registry over gRPC `:50052` to discover what CodeLocations exist and where to reach them. Read paths (run history, asset materializations, event logs) go straight to SurrealDB. Write paths (trigger run, force-materialize an asset, evaluate a sensor) dial the relevant CodeLocation pod's gRPC `:3001` directly — the UI never proxies compute. The CodeLocation pod streams run progress back into SurrealDB, which the UI relays to the browser via Server-Sent Events (`/api/events`, consumed by `EventSource` after page hydration).

## Reconciliation sequence

A `CodeLocation` going from `kubectl apply` to `Ready`:

```
   kubectl     k8s API      operator      img registry    Pod      SurrealDB
      │           │             │              │            │            │
      │           │             │              │            │            │
      │─ apply CodeLocation CR ▶│              │            │            │
      │           │             │              │            │            │
      │           │─ webhook ──▶│              │            │            │
      │           │             │              │            │            │
      │           │◀─ stamp id ─┤              │            │            │
      │           │             │              │            │            │
      │           │─ watch evt ▶│              │            │            │
      │           │             │              │            │            │
      │           │             │── HEAD ─────▶│            │            │
      │           │             │              │            │            │
      │           │             │◀── digest ───┤            │            │
      │           │             │              │            │            │
      │           │◀── create Deployment + Service ─────────┤            │
      │           │             │              │            │            │
      │           │── schedule pod ────────────────────────▶│            │
      │           │             │              │            │            │
      │           │             │  register endpoint in     │            │
      │           │             │  CodeLocationRegistry     │            │
      │           │             │  (in-process, :50052)     │            │
      │           │             │              │            │            │
      │           │◀ patch status: phase=Ready,│            │            │
      │           │  grpcEndpoint=<svc>:3001   │            │            │
      │           │             │              │            │            │
      │           │             │              │            │ start      │
      │           │             │              │            │ rivers     │
      │           │             │              │            │ serve      │
      │           │             │              │            │ :3001      │
      │           │             │              │            │            │
```

The mutating admission webhook stamps `spec.identity` (UUID) on create; the validating webhook rejects changes to it on update. `digestRefreshInterval` causes the operator to re-poll the registry periodically — semver-looking tags are cached after the first resolve since they're treated as immutable.

## Run sequence

A user opens the UI and triggers an asset materialization:

```
   Browser     rivers-ui      operator       Pod       SurrealDB
      │            │             │            │             │
      │            │             │            │             │
      │─ HTTP ────▶│             │            │             │
      │            │             │            │             │
      │            │─ list CLs ─▶│            │             │
      │            │             │            │             │
      │            │◀── CLs ─────┤            │             │
      │            │  (incl. grpcEndpoint per CL)           │
      │            │             │            │             │
      │            │─ read runs / assets ──────────────────▶│
      │            │             │            │             │
      │            │◀───────────────────────────────────────┤
      │            │             │            │             │
      │◀── HTML ───┤             │            │             │
      │            │             │            │             │
      │── open EventSource /api/events ──▶    │             │
      │            │             │            │             │
      │            │             │            │             │
      │── click "materialize" ──▶│            │             │
      │            │             │            │             │
      │            │── gRPC: materialize ────▶│             │
      │            │             │            │             │
      │            │             │            │─ write ────▶│
      │            │             │            │  run events │
      │            │             │            │             │
      │            │◀ stream events from SurrealDB ─────────┤
      │            │             │            │             │
      │◀ SSE event ┤             │            │             │
```

The UI never proxies asset compute — it discovers the CodeLocation's `Service` from the operator's registry, then calls the pod's gRPC server (`rivers serve` on `spec.grpcPort`, default `3001`) directly. Run/event state is read from SurrealDB and pushed to the browser via Server-Sent Events.

## Next steps

Ready to install? See [Kubernetes](kubernetes.md).
