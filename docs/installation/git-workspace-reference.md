# Git workspace reference

How the operator and the pods manage the workspace of a
[git-sourced CodeLocation](git-code-locations.md): trees, the volume,
retention, outages. Read this when you size RWX storage, write
NetworkPolicies, create `Run` objects yourself, or debug a build.

## Trees

A tree is one checkout and its venv, under a key made of the commit, the
runtime image, `path` and the `dependencies` setting (`timeoutSeconds`
excluded). A change to any of these gives a new tree: in shared mode the
code-location pod builds it, in fallback mode every pod does.

`spec.env` is not part of the key, even a variable that the install
uses, such as `UV_INDEX_URL`. After you change it, the code-location pods
roll out on the same tree. There is no way to rebuild a tree on request:
to install with the new value, give the CodeLocation a new commit, for
example an empty commit (`git commit --allow-empty`) on its branch.

In shared mode, run and step pods cannot write Python's compiled files
(`__pycache__`) into the tree, so the build compiles the files that these
pods import from the repository: the project directory (`path`) and
editable installs from the repository, such as uv workspace members. If
your code imports other files of the repository, for example through
`sys.path`, each process compiles them again.

## Workspace size

| Value | Default | Applies to |
| --- | --- | --- |
| `codeLocation.workspace.shared.size` (chart) | `20Gi` | Shared mode: the request of the PVC the operator creates per CodeLocation |
| `codeLocation.workspace.sizeLimit` (chart) | `2Gi` | Fallback mode: the `emptyDir` limit per pod |
| `spec.git.workspaceSize` (CodeLocation) | unset | Replaces either value for one CodeLocation |

All three are Kubernetes quantities above zero, like `10Gi`, `500M` or
`1e9`. `5GB` is not a quantity: write `5G` (10^9 bytes) or `5Gi` (2^30
bytes). The API server and the webhook reject any other `workspaceSize`,
and the operator does not start with any other chart value. The operator
does not resize a PVC that is already there.

In fallback mode the limit must hold the checkout and the venv. During
an install, uv's temporary copy of the wheels is in the container's
`/tmp`, outside this limit, so `/tmp` must be writable: a policy that
sets `readOnlyRootFilesystem` on the pods makes these installs fail.

## Volume permissions

The runtime image runs as user and group 65532, and every git-mode pod
sets `fsGroup: 65532`, so that this user can read the git Secret and
write the tree.

In shared mode, group 65532 must also be able to write to the RWX
volume, and not every storage driver applies `fsGroup` to it: a CSI
driver with the default `fsGroupPolicy`, `ReadWriteOnceWithFSType`,
skips RWX volumes, and in-tree `nfs` volumes ignore `fsGroup`. On such
storage, give the volume's root directory group 65532 and mode `2775`
(setgid), or make it writable for all users. Else the code-location
pod's build fails with `Permission denied`.

On OpenShift, the default `restricted-v2` SCC rejects pods with this
fixed `fsGroup`, as it rejects the chart's operator and UI pods, which
set a fixed user and group too.

## File locks

In shared mode, the code-location pods lock files on the RWX volume
(`flock`), so that two pods do not build the same tree or delete old
trees at the same time, and uv does not prune its cache while another
pod installs. These locks must work across nodes. If your storage keeps
them on one node, two pods on different nodes can build the same tree at
the same time and break it.

## uv cache

In shared mode, uv's cache is on the PVC (`cache/`). After each
successful build, the code-location pod runs `uv cache prune --ci`: only
the wheels that uv built from source stay, and pre-built wheels download
again on the next build. A runtime image you build yourself needs uv
0.10.0 or later, for `uv workspace dir` and for a `uv cache prune` that
waits for the installs of other pods instead of deleting files that they
use.

In fallback mode, pods install with `UV_NO_CACHE=1` and keep no cache.

## Old trees

In shared mode, each time a code-location pod starts, it deletes old
trees from the PVC. It keeps:

- the trees that the CodeLocation and its unfinished runs use,
- the `codeLocation.workspace.keepRevisions` newest trees (default 3),
- all trees younger than `codeLocation.workspace.minTreeAge` (default `1h`),
- its own tree.

Write `minTreeAge` as a whole number with `s`, `m` or `h`, for example
`90m` or `24h`. The operator does not start if it cannot read
`minTreeAge` (for example `1d`, `1h30m` or `1.5h`) or `keepRevisions`.
If the pod cannot read these limits, for example because `spec.env` sets
`RIVERS_WORKSPACE_MIN_AGE_SECONDS` or `RIVERS_WORKSPACE_KEEP_REVISIONS`
to a value that is not a whole number, it deletes no tree, and its init
container log gives the reason.

Before the pod deletes a tree, it renames it to `.deleting-<key>-…`. If
the pod stops during a deletion, no part of the tree stays under its
key, and the next pod start deletes the rest. The pod deletes old trees
and prunes the uv cache in its init container, after its own tree is
ready, so its main container starts only after that. On NFS or EFS, the
deletion of a tree with many files can take minutes. Other pods that
wait for the same tree do not wait for the deletion or the prune.

## Back to image mode

When you remove `spec.git`, the operator rolls the code-location pods
out on `spec.image`, clears `status.runSource`, `resolvedCommit`,
`resolvedRef` and `lastFetchedAt`, and new runs get the image. If the
operator cannot resolve `spec.image`, the CodeLocation is `Failed` but
keeps these fields, because its pods still run the git source.

In shared mode, the operator then deletes the workspace PVC and its keep
ConfigMap, but only when no pod can mount the PVC: every code-location
pod runs the image, and no `Run` of the CodeLocation has a git
`spec.source`. A finished run counts too: its pods stay until you delete
the `Run`, and Kubernetes does not delete a PVC that a pod mounts. Until
then, the `WorkspaceKept` condition gives the reason:

| Reason | Meaning |
| --- | --- |
| `RollingOut` | The code-location pods still roll out on the image. |
| `RunsNotListed` | The operator has not listed the runs yet. |
| `RunsUseWorkspace` | The pods of the named runs mount the PVC. Delete the finished ones to free it. |

The operator checks again every minute. It deletes only a PVC and a
ConfigMap that the CodeLocation owns, not others with the same name. If
you add `spec.git` again before the PVC is deleted, the pods use it
again. If the PVC is being deleted, the operator waits until it is gone,
and then creates a new one.

## Custom run and step images

In git mode, `RunBackendConfig.kubernetes(image=...)` and
`Executor.kubernetes(worker_image=...)` replace only the image of the
main container of run and step pods. These pods still run the tree that
the runtime image built: the checkout and the venv. The run records that
image in `spec.source.runtimeImage`, and in fallback mode the init
container that builds the pod's tree runs it. The main container starts
`/workspace/venv/bin/rivers`, so its image must have the same Python
version at the same path as the runtime image: build it `FROM` the
runtime image.

## Runs you create yourself

A `Run` you create without `image` gets the rolled-out tree: the webhook
copies `status.runSource` into the run's `spec.source`, and its runtime
image into `spec.image`. Just after you add `spec.git` to an image-mode
CodeLocation, `status.runSource` is empty and the webhook rejects such
runs; try again when the first tree has rolled out.

A `Run` with your own `spec.source` follows the rules of a CodeLocation:
`spec.source.git.commit` is a full lowercase SHA, and
`spec.source.git.path` and `spec.source.git.url` pass the
[path](git-code-locations.md#project-directory-and-module) and
[url](git-code-locations.md#url-rules) rules. The webhook accepts the
url and Secret if they are those of `spec.git`, or, while a change of
them rolls out, those of `status.runSource`. A `Run` with a digest
`image` must set `spec.source.runtimeImage` to a digest in the
CodeLocation's runtime image repository: that of `spec.image` (or of the
chart's default runtime image), or, while a change of `spec.image` rolls
out, that of `status.resolvedImage`. The webhook rejects other images.

## Egress

In shared mode only the code-location pod needs outbound access to the
git host and the package index; run and step pods fetch nothing. In
fallback mode every pod does. Adjust NetworkPolicies accordingly. The
operator's requests to git hosts and registries carry
`User-Agent: rivers-operator/<version>`, for firewalls that filter on it.

## Outages

A git host that does not answer (connection error, HTTP 5xx, or no
complete answer within `operator.git.timeoutSeconds`, 30 seconds by
default) does not take the CodeLocation down. It stays `Ready` on the
commit it serves, with `SourceResolved` `False`, reason `GitUnreachable`,
and the error, also in `status.message`. In shared mode, runs keep using
that commit's tree. In fallback mode, each run pod fetches the commit
itself, so a run that starts while the host is down fails.

The operator asks the host again after 1, 2 and 4 minutes, then every 5
minutes, and picks up new commits when the host answers. A host that
rate-limits the operator (HTTP 429, or 503 with `Retry-After`) has the
same result, with reason `GitRateLimited`. The operator then sends no
request to that host, for any CodeLocation, until the host's
`Retry-After` time is over: at most one hour, and not sooner than the
steps above. The message gives the time of the next request.

Before the first commit has rolled out there is nothing to serve, and
the CodeLocation is `Failed` until the host answers.

The registry of the runtime image works the same way. If it does not
answer (connection error or HTTP 5xx) or rate-limits the operator (HTTP
429), a CodeLocation that serves a commit stays `Ready` on it, with
`ImageResolved` `False`, reason `RegistryError` or `RateLimited`, and
the error, also in `status.message`. The operator asks the registry
again later, for a 429 when its `Retry-After` time is over. A tag that
the registry does not have (`TagNotFound`) or a registry login that
fails (`AuthenticationFailed`) sets the phase to `Failed`.

## Apply failures

If the API server refuses an object of a git CodeLocation (its
Deployment, Service, workspace PVC or keep ConfigMap), for example
because of a policy, a quota or a value that it does not take,
`DeploymentAvailable` has reason `ApplyFailed` and the API server's
error, after `applying Deployment 'analytics' failed:` (or the object
that failed). The same error is in `status.message`. The pods that run
keep running, so a CodeLocation that serves a commit stays `Ready` on it
and runs keep using it. Before the first commit has rolled out there is
nothing to serve, and the CodeLocation is `Failed`. The operator tries
again when you change the CodeLocation, else after 5 minutes, or after 1
minute for a server error (HTTP 5xx), a conflict or throttling.

## Status fields

| Field | Meaning |
| --- | --- |
| `status.source` | The `SOURCE` column of `kubectl get rcl`: `main@9f3c1ab` in git mode, `repo@sha256:abc1234` in image mode. |
| `status.resolvedCommit` | The full commit of `runSource`. |
| `status.resolvedRef` | The ref that commit came from, such as `refs/heads/main`. Absent for a pinned commit. |
| `status.lastFetchedAt` | The last fetch of `spec.git.ref` from the git host. Absent for a pinned commit. |
| `status.runSource` | The source runs get: url, commit, path, Secret, dependencies and the runtime image that built the tree. Moves only when a rollout finishes. |
| `status.resolvedImage` | The digest of the runtime image; `runSource.runtimeImage` in git mode. |
| `status.message` | The current error, if any. |
