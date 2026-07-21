# Authentication (OIDC & forward auth)

The rivers UI supports three authentication modes:

| Mode | What it does |
|---|---|
| `none` (default) | No authentication — today's behavior. Fine for `rivers dev` and network-isolated clusters. |
| `oidc` | The UI runs the OpenID Connect authorization-code flow (with PKCE) against your identity provider directly. No proxy required. |
| `forward` | A reverse proxy in front of rivers authenticates (Authelia, Authentik, oauth2-proxy, Envoy Gateway, …) and rivers trusts the identity headers it injects — but only from peers on an explicit trusted-proxy CIDR list. |

In both enabled modes, every route except `/healthz`, `/readyz`, and `/auth/*`
requires an identity: pages, server functions, the live-event stream, and
debug endpoints. Manual launches (materialize, job runs, reruns, backfills)
record *who* triggered them — the run's `launched_by` carries the user's
stable subject plus email/name snapshots, shown in the runs list and detail
pages.

Configuration is env-var driven (`RIVERS_AUTH_*`); on Kubernetes the Helm
chart maps `ui.auth.*` values onto those env vars, with secrets flowing via
`secretKeyRef` only.

## OIDC

Register a client at your IdP with redirect URI
`<publicUrl>/auth/callback`, then:

```yaml
# values.yaml
ui:
  auth:
    mode: oidc
    publicUrl: https://rivers.example.com
    oidc:
      issuer: https://keycloak.example.com/realms/main
      clientId: rivers
      existingSecret: rivers-oidc-client   # key: client-secret
```

```bash
kubectl create secret generic rivers-oidc-client \
  --from-literal=client-secret='<client secret>' -n rivers
helm upgrade rivers ./deploy/helm/rivers -f values.yaml
```

Any spec-compliant IdP works via issuer discovery — Keycloak, Okta, Entra
ID, Google, Dex, Authentik, Zitadel. Knobs:

| Value | Env var | Default |
|---|---|---|
| `ui.auth.publicUrl` | `RIVERS_AUTH_PUBLIC_URL` | — (required) |
| `ui.auth.oidc.issuer` | `RIVERS_AUTH_OIDC_ISSUER` | — (required) |
| `ui.auth.oidc.clientId` | `RIVERS_AUTH_OIDC_CLIENT_ID` | — (required) |
| `ui.auth.oidc.existingSecret` | `RIVERS_AUTH_OIDC_CLIENT_SECRET` | — (required unless `publicClient: true`) |
| `ui.auth.oidc.scopes` | `RIVERS_AUTH_OIDC_SCOPES` | `openid profile email` |
| `ui.auth.oidc.groupsClaim` | `RIVERS_AUTH_OIDC_GROUPS_CLAIM` | `groups` (dotted paths traverse, e.g. `realm_access.roles`) |
| `ui.auth.oidc.rpLogout` | `RIVERS_AUTH_OIDC_RP_LOGOUT` | `false` — when `true`, sign-out also ends the IdP session |
| `ui.auth.sessionTtl` | `RIVERS_AUTH_SESSION_TTL` | `28800` (8h) |

Sessions are stateless encrypted cookies (AES-256-GCM) — no session table,
no sticky sessions, and no OAuth tokens are ever stored. The cookie key is
chart-managed (`rivers-ui-auth` Secret, generated once and preserved across
upgrades) or brought via `ui.auth.cookieSecret.existingSecret`; the value
must be base64 of at least 32 bytes. Rotating it signs every user out at
once. When the session expires the next page load transparently re-runs the
redirect — invisible while the IdP session is alive.

Outside the chart (docker-compose, bare processes), set
`RIVERS_AUTH_COOKIE_SECRET` explicitly — e.g. `openssl rand -base64 48`.
Without it each process generates an ephemeral key at startup (logged as a
warning): sessions die on every restart, and behind a load balancer each
replica mints cookies the others can't decrypt, so logins bounce back to
the IdP indefinitely.

## Forward auth

The proxy owns login; rivers consumes the identity headers it injects.
Requests are only trusted when the **socket peer** is inside
`trustedProxies` (never `X-Forwarded-For`, which is spoofable). A request
from any other peer gets `403` with its identity headers ignored; a request
from a trusted proxy *without* identity headers gets a diagnostic `401` —
that means the proxy authenticated nothing or drops its auth response
headers.

```yaml
ui:
  auth:
    mode: forward
    forward:
      # The proxy pods' addresses ONLY — as narrow as you can make it.
      trustedProxies: ["10.42.3.14/32"]
      logoutUrl: https://auth.example.com/logout
```

!!! danger "Never trust the whole pod network"
    On most clusters all pods share one CIDR (k3s defaults to
    `10.42.0.0/16`) — listing it would let **every pod in the cluster**
    forge identity headers and impersonate any user. Scope the list to the
    proxy's actual addresses: its static egress IPs, a dedicated node pool's
    range, or `/32`s kept current by your deploy tooling. If the proxy's IPs
    aren't pinnable, add a NetworkPolicy that only admits UI traffic from
    the proxy pods and treat `trustedProxies` as defense in depth.

Header names default to the Authelia/Authentik convention and are all
configurable:

| Proxy | Values to set |
|---|---|
| Authelia / Authentik | defaults work (`Remote-User`, `Remote-Email`, `Remote-Groups`, `Remote-Name`) |
| oauth2-proxy (`--set-xauthrequest`) | `userHeader: X-Auth-Request-User`, `emailHeader: X-Auth-Request-Email`, `groupsHeader: X-Auth-Request-Groups` |
| Cloudflare Access | `userHeader: Cf-Access-Authenticated-User-Email`, `emailHeader:` same |
| Envoy Gateway (gateway-native OIDC) | whatever your `claimToHeaders` maps emit — see below |

### Envoy Gateway + Keycloak

The chart already ships an HTTPRoute (`ui.httpRoute.enabled`), so Envoy
Gateway can run the OIDC flow itself and mint identity headers from
validated token claims:

```yaml
apiVersion: gateway.envoyproxy.io/v1alpha1
kind: SecurityPolicy
metadata:
  name: rivers-ui-auth
spec:
  targetRefs:
    - group: gateway.networking.k8s.io
      kind: HTTPRoute
      name: rivers-ui
  oidc:
    provider:
      issuer: https://keycloak.example.com/realms/main
    clientID: rivers
    clientSecret:
      name: rivers-oidc-client
    forwardAccessToken: true
  jwt:
    providers:
      - name: keycloak
        remoteJWKS:
          uri: https://keycloak.example.com/realms/main/protocol/openid-connect/certs
        claimToHeaders:
          - claim: sub          # stable id → UserRef.subject
            header: x-user
          - claim: email
            header: x-user-email
          - claim: preferred_username
            header: x-user-name
```

```yaml
ui:
  auth:
    mode: forward
    forward:
      trustedProxies: ["10.42.7.5/32"]   # the Envoy proxy pods only — never the pod CIDR
      userHeader: x-user
      emailHeader: x-user-email
      nameHeader: x-user-name
```

Map `sub` (not `preferred_username`) to the user header so the stored
subject is the never-reassigned Keycloak UUID. One inherited limit: Envoy's
`claimToHeaders` only extracts primitive claims — array claims like
Keycloak's `groups` cannot become a header, so enforce group-based access
at the gateway (its JWT claim authorization matches arrays via
`valueType: StringArray`) or use rivers' native `oidc` mode instead.

### Traefik + Authelia / ingress-nginx

Any forwardAuth-style setup works unchanged — configure the proxy to copy
the auth service's response headers onto the request:

- Traefik: a `forwardAuth` middleware with
  `authResponseHeaders: [Remote-User, Remote-Email, Remote-Groups, Remote-Name]`.
- ingress-nginx: `nginx.ingress.kubernetes.io/auth-url` +
  `auth-response-headers: Remote-User,Remote-Email,Remote-Groups,Remote-Name`.

Then set `trustedProxies` to the controller pods' addresses (see the warning
above — not the cluster pod CIDR). If the UI Service is reachable without
traversing the proxy, forward mode is an auth bypass — pair it with a
NetworkPolicy.

## Access control

Optional allowlists apply in both modes; empty lists admit any
authenticated user, and a match in any list admits:

```yaml
ui:
  auth:
    allowedDomains: ["example.com"]      # email domain
    allowedGroups: ["data-eng"]          # from the groups claim / header
    allowedUsers: ["ops@example.com"]    # email or subject
```

Denied users get a `403` page naming the identity and the gate. Finer roles
(viewer/launcher/admin, per-code-location grants) are a planned follow-up
on top of the groups already captured in the session.

Allowlists are enforced on every request, but the identity they check —
subject, email, groups — is snapshotted into the session cookie at sign-in.
IdP-side changes (removing a user from a group, disabling the account) only
take hold at the next sign-in, so at most `sessionTtl` (default 8h) later.
Tightening the *allowlist* locks a session out immediately; to evict
everyone at once, rotate the cookie secret. Pick a `sessionTtl` that matches
how fast revocation must propagate.

## Who launched what

With auth enabled, manual actions carry the acting user end-to-end: the UI
stamps the session identity onto the gRPC launch request, the code location
persists it on the run's `launched_by` (and on backfill records), and the
UI renders "manual · \<user\>". From Python:

```python
record = storage.get_run(run_id)
assert record.launched_by.kind == "manual"
if record.launched_by.user is not None:
    print(record.launched_by.user.subject, record.launched_by.user.display)

status = repo.get_backfill(backfill_id)
print(status.launched_by.kind, status.launched_by.user)
```

Runs from schedules, sensors, conditions, and the Python API
(`repo.materialize()`, `Job.execute()`) are unaffected — Python-launched
runs have `user = None` until API tokens land.

## Local development

`rivers dev` defaults to `mode: none`. To exercise OIDC locally, run an IdP
(e.g. Dex or Keycloak in Docker) and export the env vars before starting:

```bash
export RIVERS_AUTH_MODE=oidc
export RIVERS_AUTH_PUBLIC_URL=http://localhost:3000
export RIVERS_AUTH_OIDC_ISSUER=http://localhost:5556
export RIVERS_AUTH_OIDC_CLIENT_ID=rivers
export RIVERS_AUTH_OIDC_CLIENT_SECRET=...
rivers dev my_module
```

Forward mode is testable with plain curl:

```bash
RIVERS_AUTH_MODE=forward \
RIVERS_AUTH_FORWARD_TRUSTED_PROXIES=127.0.0.1/32 \
rivers dev my_module
curl -H 'Remote-User: jdoe' http://127.0.0.1:3000/api/whoami
```
