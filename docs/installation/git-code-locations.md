# Git-sourced CodeLocations

A `CodeLocation` can take its code from a git repository instead of an
image. The operator resolves the ref to a commit, an init container
fetches that commit and installs its dependencies with `uv`, and every
run pins that exact commit. The release pipeline shrinks to `git push`.

This page shows how to set one up. The
[workspace reference](git-workspace-reference.md) covers storage,
retention and outages.

## Deploy from git

```yaml title="analytics-git.yaml"
apiVersion: rivers.io/v1alpha1
kind: CodeLocation
metadata:
  name: analytics
  namespace: rivers
spec:
  git:
    url: https://github.com/acme/pipelines.git
    ref:
      branch: main            # exactly one of branch | tag | commit
    path: analytics           # subdirectory holding the project
    secretRef:
      name: git-creds
  module: analytics.pipeline
```

```sh
kubectl apply -f analytics-git.yaml
kubectl -n rivers get rcl
# NAME        PHASE   SOURCE         REPLICAS   AGE
# analytics   Ready   main@9f3c1ab   1/1        45s
```

The pods run the chart's default [runtime image](#runtime-image); the
code and its dependencies come from the repository.

A fuller spec sets every git field, a runtime image from a private
mirror, resources, env and secrets:

```yaml
apiVersion: rivers.io/v1alpha1
kind: CodeLocation
metadata:
  name: analytics
  namespace: rivers
spec:
  git:
    url: ssh://git@github.com/acme/pipelines.git   # ssh:// names the user
    ref:
      branch: main            # or tag: v1.4.0, or commit: <40-hex sha>
    path: services/analytics
    secretRef:
      name: git-creds         # identity + known_hosts for ssh://
    pollInterval: 2m          # default 5m, minimum 1m
    dependencies:
      mode: uvSync            # auto | uvSync | requirements | none
      extras: [snowflake]     # uvSync: --extra
      groups: [prod]          # uvSync: --group
      # files: [requirements.txt]   # requirements mode only
      timeoutSeconds: 900     # fetch + install budget, default 600
    workspaceSize: 10Gi       # PVC request (shared) or emptyDir limit (fallback)
  module: analytics.pipeline
  image: harbor.internal/rivers/rivers-runtime   # default: the chart's runtime image
  tag: 0.5.0-py3.11
  replicas: 1             # the maximum; code locations do not scale out yet
  digestRefreshInterval: 10m
  resources:
    requests:
      cpu: 500m
      memory: 512Mi
    limits:
      cpu: "2"
      memory: 2Gi
  env:
    - name: UV_INDEX_URL
      value: https://pypi.internal.acme/simple
    - name: SNOWFLAKE_PASSWORD
      valueFrom:
        secretKeyRef:
          name: snowflake-creds
          key: password
  imagePullSecrets:
    - name: harbor-pull-secret
  serviceAccountName: rivers-pipelines
```

The sample in the rivers repository,
[`deploy/examples/codelocation-git-sample.yaml`](https://github.com/ion-elgreco/rivers/blob/main/deploy/examples/codelocation-git-sample.yaml),
is ready to `kubectl apply`.

## Revision

`ref` names exactly one of:

| Field | Example | Operator behaviour |
| --- | --- | --- |
| `branch` | `main` | Fetches the branch every `pollInterval` and follows new commits. |
| `tag` | `v1.2.3` | Fetches the tag every `pollInterval`. A semver-like tag is resolved once. |
| `commit` | full SHA | Never fetched: no polling, no `status.lastFetchedAt`. |

`pollInterval` defaults to `5m`; the minimum is `1m`. CodeLocations with
the same url, ref and credentials share their fetches.
`status.lastFetchedAt` is the time of the last fetch.

`commit` takes the full 40-character SHA in lowercase, as `git rev-parse`
prints it. The API server and the webhook reject a short or uppercase
SHA. A CodeLocation created before these checks that still has one fails
with reason `InvalidRef`, and the operator does not deploy it.

## Project directory and module

`path` is the project directory, relative to the repository root
(`analytics`, `services/analytics`). Leave it out for the repository
root. The webhook rejects a `path` that starts with `/`, or that has a
`..`, `.` or empty segment (`./analytics`, `services//analytics`), so
that each directory has one spelling and one tree. A `/` at the end is
fine.

Code-location, run and step pods start in the project directory,
`/workspace/src/<path>`, and have it on `PYTHONPATH`. So Python finds
`module` also after your code changes the working directory, for example
in workers of `Executor.parallel()` that start after the change. A
`PYTHONPATH` in `spec.env` replaces this value: keep the project
directory in your value.

## Dependencies

Dependencies come from the repository. With `dependencies.mode: auto`
(the default), the init container looks in the project directory and
takes the first match:

| Found in `path` | Install |
| --- | --- |
| `uv.lock` | `uv sync --locked` |
| `requirements.txt` | `uv pip install` |
| `pyproject.toml` of a uv workspace member | `uv sync --locked` with the workspace root's `uv.lock` |
| none of these | nothing |

For a workspace member, the init container asks uv for the workspace
root (`uv workspace dir`). uv writes one `uv.lock` at that root, so a
member has none of its own; uv installs the project and the members it
depends on. A project with no lock of its own, under a `uv.lock` that uv
does not count it in (missing from `members`, `exclude`d, or the lock of
a plain project), fails the build. The message names the remedies: add
the project to `members`, give it its own lock, or set
`dependencies.mode: none`. Run `uv workspace dir --project <path>` in the
repository to see what the init container sees.

Set `mode` yourself to skip detection:

| Field | Default | Meaning |
| --- | --- | --- |
| `mode` | `auto` | `auto`, `uvSync`, `requirements` or `none`. `none` fetches the code and installs nothing, also when a lockfile is present: the runtime image carries the dependencies. |
| `files` | `[requirements.txt]` | `requirements` mode: files relative to `path`. |
| `extras` | `[]` | `uvSync` mode: `--extra` passthrough. |
| `groups` | `[]` | `uvSync` mode: `--group` passthrough. |
| `timeoutSeconds` | `600` | Budget for fetch and install. |

The lockfile should include `rivers`: the venv's `rivers` is what runs.
In `requirements` mode, pin exact versions. In fallback mode each pod
resolves the requirements again, so different pods can get different
versions.

## Credentials

`secretRef` names a Secret in the same namespace. The url's scheme
selects the keys (the names follow Flux):

| Url | Keys | Notes |
| --- | --- | --- |
| `https://`, `http://` | `username`, `password` | A forge token is a password. Both keys or neither: without them the fetch is anonymous, one of them is an error. Newlines at the end of the values are ignored, so `kubectl create secret --from-file` works. |
| `ssh://` | `identity`, `known_hosts` | `identity` is a private key without a passphrase. The url must name the user the key logs in as: `ssh://git@github.com/acme/pipelines.git`. |

```sh
# https://: a forge token is the password
kubectl -n rivers create secret generic git-creds \
  --from-literal=username=rivers-bot \
  --from-literal=password="$GITHUB_TOKEN"

# ssh://: a key without a passphrase, plus the host's key
ssh-keyscan -t ed25519 github.com > known_hosts   # compare with the host's published fingerprints
kubectl -n rivers create secret generic git-creds \
  --from-file=identity=./deploy_key \
  --from-file=known_hosts=./known_hosts
```

The keys of the other scheme are ignored, so one Secret with all four
keys can serve both kinds of url. If the Secret does not exist, or the
operator may not read it, the CodeLocation fails with reason
`GitAuthFailed`.

`known_hosts` takes exact and hashed (`|1|`) entries only: no `*`
wildcards or `@cert-authority` lines. Each line must be an entry or a
comment; the operator refuses a file with any other line and does not
connect. Ed25519, ECDSA and RSA host keys work, not DSA. One key per host
is enough: the operator asks each host only for the key types the file
lists for it.

### Url rules

Put credentials only in the Secret. The url goes into the pods'
environment, into each run's `spec.source` and into the tree's
`.git/config`. The webhook rejects:

- A password in the url (`https://bot:TOKEN@…`). A user alone is fine
  (`https://bot@…`, `ssh://git@…`).
- An `ssh://` url without a user: ssh in the pods would log in as the
  pod's own account. A CodeLocation created before this check that still
  has one fails with reason `GitAuthFailed`.
- An `http://` url, unless the chart sets `operator.git.allowInsecure: true`.
  Over http the code and the credentials are not encrypted; enable it
  only for a git host on a trusted network.
- A url that sends git to another host than the one it seems to name: a
  `\` in an `https://` or `http://` url, or, in an `ssh://` url, `?`,
  `#` or `%2F` before the path or a `%` escape in the host.
- A host outside `operator.git.allowedHosts`, when the chart sets that
  list. The default, an empty list, admits any host.

A CodeLocation created before these checks that still has a rejected url
fails with reason `InvalidUrl`, and the operator does not fetch from it.

## Runtime image

`spec.image` is always the container the pods run. In git mode, leave it
out to run the chart's default runtime image,
`ghcr.io/ion-elgreco/rivers-runtime:<chart version>-py3.12`
(`codeLocation.runtime.*` in the chart). Each release publishes this
image for Python 3.11, 3.12 and 3.13 (tags `<version>-py3.11`,
`-py3.12`, `-py3.13`), for `linux/amd64` and `linux/arm64`.

| To | Set |
| --- | --- |
| Pick another Python | `tag: 0.5.0-py3.11`, or `digest`. This replaces the default image's tag or digest. |
| Pull from a private mirror | `image: harbor.internal/rivers/rivers-runtime` with `tag: 0.5.0-py3.11`. As in image mode, the operator resolves `tag` (default `latest`) or `digest`. |

The operator resolves the runtime image's tag again every
`digestRefreshInterval` (default `5m`), also when `ref.commit` pins the
code, and rolls the pods out on a new digest. A semver-like tag, such as
`0.5.0-py3.11`, is resolved once.

A runtime image you build yourself needs git, an ssh client for `ssh://`
urls, uv 0.10.0 or later, the rivers package of the chart's version, and
the init container's binary, `rivers-runtime`:

```dockerfile
COPY --from=ghcr.io/ion-elgreco/rivers-runtime:<version>-py3.12 \
  /usr/local/bin/rivers-runtime /usr/local/bin/
```

`RunBackendConfig.kubernetes(image=...)` and
`Executor.kubernetes(worker_image=...)` replace only the main container
of run and step pods. These pods still run the tree the runtime image
built and start `/workspace/venv/bin/rivers`, so build such an image
`FROM` the runtime image: same Python version at the same path. See
[custom run and step images](git-workspace-reference.md#custom-run-and-step-images).

## Workspace: shared or fallback

A *tree* is a checkout plus its venv. Where it is built decides how fast
pods start:

| | Fallback (default) | Shared |
| --- | --- | --- |
| Chart value | `codeLocation.workspace.shared.enabled: false` | `codeLocation.workspace.shared.enabled: true` |
| Storage | `emptyDir` per pod | One RWX PVC per CodeLocation |
| Who builds | Every pod, before its main container starts | The code-location pod, once per tree |
| Run and step pods | Fetch from the git host and install from the package index, with no cache | Mount the tree read-only and start as fast as in image mode |
| Egress | Every pod | Code-location pods only |

In fallback mode, each code-location pod builds when it starts, and so
does the run pod of each run, also when the operator replaces that pod.
With `Executor.kubernetes`, the pod of each step, mapped instance and
retry builds too: a run on the default `Executor.parallel()` builds once,
a run of 40 steps on `Executor.kubernetes` builds 41 times, and a
backfill makes one run per partition, each with its own build.

**Choose shared mode** where you have RWX storage and you use
`Executor.kubernetes`, start many runs (schedules, sensors, backfills),
or have a large install. Fallback mode is enough for a few runs at a time
on `Executor.parallel()` or `Executor.in_process()`.

`Executor.kubernetes` starts all ready steps of a level at the same time
unless you set `max_concurrent_steps`. In fallback mode all of these
pods fetch and install at once. To limit this, set
`Executor.kubernetes(max_concurrent_steps=...)` for the step pods and a
run queue, `RunQueueConfig(max_concurrent_runs=...)`, for the runs (see
[Concurrency](../concepts/concurrency.md)).

### Failed builds in fallback mode

Each build must finish within `dependencies.timeoutSeconds` (default
600). When a run pod's build fails, for example because the package
index does not answer, the operator replaces the pod, up to 3 times
without progress (the Run's `spec.maxRestarts`). A step pod is not
replaced: its Job has `backoffLimit: 0`, so the step fails. A failed
build is an `INFRASTRUCTURE` failure: an asset with a
[retry policy](../concepts/retries.md) that retries these, such as
`RetryOn.TRANSIENT` or the default `RetryOn.ALL`, retries in a new pod,
which builds again.

## Rollouts

Runs get the last *fully rolled-out* commit. After a push, the operator
rolls the code-location pods to the new commit. `status.resolvedCommit`
moves only when every pod runs it and is ready, and `status.runSource`,
the full source of that tree (url, commit, path, Secret, dependencies
and the runtime image that built it, also `status.resolvedImage`), moves
with it. Until then runs keep the previous commit, and if the new commit
fails to build they keep it until a later commit rolls out.

The CodeLocation stays `Ready` throughout: the Deployment never takes
the old pod down before its replacement is ready (a rollout needs room
for a second pod). A change of the url, `path`, Secret, `dependencies`
or runtime image reaches runs the same way, when its rollout finishes.

A run launched from a code-location pod (UI, schedules, sensors,
backfills) pins the commit that pod serves, so during a rollout each run
uses the same tree as the pod that launched it. For a `Run` you create
yourself, see
[the reference](git-workspace-reference.md#runs-you-create-yourself).

To wait until a push is live:

```sh
kubectl -n rivers wait rcl/analytics \
  --for=jsonpath='{.status.resolvedCommit}'=<sha>
```

## Writing files

In shared mode the tree is read-only in run and step pods, so that no
run can change the code or the venv that other runs use. These pods
start in `/workspace/src/<path>`, so a write to a relative path fails:
`open("scratch.csv", "w")` raises
`OSError: [Errno 30] Read-only file system`, and
`df.write_parquet("out.parquet")`, `sqlite3.connect("cache.db")` or an
IO handler with a relative path, such as
`DeltaIOHandler(table_uri="lake")`, fail too. The same code can work in
fallback mode (each pod writes to its own tree, within the `emptyDir`
limit) and in image mode (the working directory is the image's
`WORKDIR`). Write to other places instead:

- For scratch files, use `tempfile` or an absolute path outside
  `/workspace`, such as `/tmp`. The container's `/tmp` is writable unless
  a cluster policy sets `readOnlyRootFilesystem`; its files go away with
  the pod.
- For data that later steps or runs read, give IO handlers an
  object-store location, such as
  `DeltaIOHandler(table_uri="s3://acme-lake/analytics")`. A local path
  lives only as long as its pod, and steps can run in other pods.

## Troubleshooting

`kubectl -n rivers describe rcl analytics` carries the answer, and
`status.message` repeats the current error.

| Condition | Reason | Meaning |
| --- | --- | --- |
| `SourceResolved` | `RefNotFound` | The repository has no such branch or tag. |
| | `InvalidRef`, `InvalidUrl` | `ref.commit` or `url` breaks the rules above. |
| | `GitAuthFailed` | The Secret is missing or unreadable, the host did not accept the credentials, or an `ssh://` url has no user. |
| | `GitHostKeyRejected` | The host's key is not in `known_hosts`, or the file has a line the operator refuses. |
| | `GitMalformedResponse` | The host's answer was not a valid git response. |
| | `GitUnreachable`, `GitRateLimited` | The host does not answer, or rate-limits the operator. The CodeLocation stays `Ready` on the commit it serves; see [outages](git-workspace-reference.md#outages). |
| `ImageResolved` | `TagNotFound`, `AuthenticationFailed` | The registry does not have the runtime image's tag, or the login failed. |
| | `RegistryError`, `RateLimited` | The registry does not answer, or rate-limits the operator. The CodeLocation stays `Ready`; see [outages](git-workspace-reference.md#outages). |
| `DeploymentAvailable` | `RollingOut` | A new commit or image rolls out. The message names the commit being rolled out and the one runs still use. |
| | `ProgressDeadlineExceeded` | The rollout is stuck, for example because the new commit does not build. The message ends with the build error. |
| | `ApplyFailed` | The API server refused the Deployment, Service, PVC or ConfigMap; see [apply failures](git-workspace-reference.md#apply-failures). |
| `WorkspaceKept` | `RunsUseWorkspace` | After a switch back to image mode, pods of finished runs still mount the workspace PVC; see [back to image mode](git-workspace-reference.md#back-to-image-mode). |

A resolution problem other than an outage sets the phase to `Failed`,
and new runs are rejected until you fix it. While `SourceResolved` is
`False`, `status.message` shows the resolution error rather than a build
error, because the operator cannot get a commit that repairs the build
until it clears.

### Build errors

When a code-location pod cannot build a commit, `status.message` and the
end of the `DeploymentAvailable` message show the error of the pod's
`workspace` init container, after
`workspace build of main@9f3c1ab failed (Error, exit code 1):`.

| The message is | When |
| --- | --- |
| The init container's own message | A `path` that is not in the repository, a key of the git Secret that the pod cannot read, or a build that did not finish within `dependencies.timeoutSeconds`. |
| `git fetch of commit <sha> failed:` and the last lines of git's error | `Permission denied (publickey)`, `Could not resolve host`, or `not our ref` when the repository does not have the commit. For a branch or tag, the init container then fetches the ref and checks that it points to the commit; if that fails too, the message gives its error. |
| The last lines of the init container's log | Any other error, such as a failed install, with the error of `uv`. |

Kubernetes keeps at most 80 lines or 2048 bytes of the log for this
message. For all of it:

```sh
kubectl -n rivers logs <pod> -c workspace
```
