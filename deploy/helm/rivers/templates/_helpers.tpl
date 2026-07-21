{{- define "rivers.namespace" -}}
{{ .Values.global.namespace | default "rivers" }}
{{- end -}}

{{/*
Default image refs. `.Chart.AppVersion` is overridden by the release-helm
workflow's `helm package --app-version`, so a chart packaged at v0.2.0
resolves these to `:0.2.0` without touching values.yaml. Override at
install time via `operator.image` / `ui.image`.
*/}}
{{- define "rivers.operatorImage" -}}
ghcr.io/ion-elgreco/rivers-operator:{{ .Chart.AppVersion }}
{{- end -}}

{{- define "rivers.uiImage" -}}
ghcr.io/ion-elgreco/rivers-ui:{{ .Chart.AppVersion }}
{{- end -}}

{{- define "rivers.surrealEndpoint" -}}
{{- if .Values.surrealdb.enabled -}}
ws://surrealdb.{{ include "rivers.namespace" . }}.svc:{{ .Values.surrealdb.service.port }}
{{- else if .Values.surrealdb.endpoint -}}
{{ .Values.surrealdb.endpoint }}
{{- else -}}
{{ fail "surrealdb.enabled is false; set surrealdb.endpoint to an external SurrealDB ws:// URL" }}
{{- end -}}
{{- end -}}

{{/*
HTTP form of `rivers.surrealEndpoint` for `surreal import` / `surreal sql`,
which talk HTTP, not WebSocket. Inherits the same `enabled / endpoint /
fail` branching by replacing the scheme on the result.
*/}}
{{- define "rivers.surrealHttpEndpoint" -}}
{{- include "rivers.surrealEndpoint" . | replace "ws://" "http://" | replace "wss://" "https://" -}}
{{- end -}}

{{- define "rivers.labels" -}}
app.kubernetes.io/managed-by: Helm
app.kubernetes.io/part-of: rivers
{{- end -}}

{{- define "rivers.selectorLabels" -}}
app.kubernetes.io/part-of: rivers
{{- end -}}

{{/*
Fixed Secret names — release-prefix-free so the surrealdb subchart's
`podExtraEnv` in values.yaml can reference them statically. Only one rivers
release per namespace is supported (the namespace already isolates
collisions across deployments).
*/}}
{{- define "rivers.surrealAuthSecretName" -}}
{{- if .Values.surrealdb.auth.existingSecret -}}
{{ .Values.surrealdb.auth.existingSecret }}
{{- else -}}
rivers-surrealdb-auth
{{- end -}}
{{- end -}}

{{- define "rivers.surrealBootstrapSecretName" -}}
rivers-surrealdb-bootstrap
{{- end -}}

{{/*
True when rivers pods should sign in to SurrealDB. Always true for the
bundled DB (chart manages everything). True for external DB only when the
user supplied creds. Otherwise rivers connects without `signin`, suitable
for `surreal start --unauthenticated`.
*/}}
{{- define "rivers.surrealAuthRequested" -}}
{{- if or .Values.surrealdb.enabled .Values.surrealdb.auth.existingSecret (and .Values.surrealdb.auth.username .Values.surrealdb.auth.password) -}}
true
{{- end -}}
{{- end -}}

{{/*
Resolve a Secret value: inline override wins; otherwise re-read the
existing Secret (`lookup`) so values don't rotate underneath running pods on
upgrade; otherwise fall back to the caller-supplied `generate` value.

The result is **memoized on `.Values`** by `cacheKey` — multiple includes
within one render must agree (e.g. `rivers-surrealdb-auth` and
`rivers-surrealdb-setup` Secrets both bake in the rivers password). Without
memoization each `randAlphaNum` call would diverge, leaving the auth
Secret and the user-init Job's SurrealQL with different passwords on
first install.

Args (dict): `ctx` (root context), `secret`, `key`, `cacheKey`, `generate`
(fallback value when neither an override nor an existing Secret is found),
and optional `override`.
*/}}
{{- define "rivers.lookupOrGenerate" -}}
{{- $cache := index .ctx.Values "__riversCachedPasswords" | default dict -}}
{{- if not (hasKey $cache .cacheKey) -}}
{{-   $val := "" -}}
{{-   if .override -}}
{{-     $val = .override -}}
{{-   else -}}
{{-     $existing := lookup "v1" "Secret" (include "rivers.namespace" .ctx) .secret -}}
{{-     if and $existing $existing.data (index $existing.data .key) -}}
{{-       $val = (index $existing.data .key | b64dec) -}}
{{-     else -}}
{{-       $val = .generate -}}
{{-     end -}}
{{-   end -}}
{{-   $_ := set $cache .cacheKey $val -}}
{{-   $_ := set .ctx.Values "__riversCachedPasswords" $cache -}}
{{- end -}}
{{- index $cache .cacheKey -}}
{{- end -}}

{{- define "rivers.surrealBootstrapPassword" -}}
{{- include "rivers.lookupOrGenerate" (dict "ctx" . "cacheKey" "bootstrap" "secret" (include "rivers.surrealBootstrapSecretName" .) "key" "password" "generate" (randAlphaNum 32) "override" .Values.surrealdb.auth.bootstrap.password) -}}
{{- end -}}

{{- define "rivers.surrealRiversUsername" -}}
{{- .Values.surrealdb.auth.username | default "rivers" -}}
{{- end -}}

{{- define "rivers.surrealRiversPassword" -}}
{{- include "rivers.lookupOrGenerate" (dict "ctx" . "cacheKey" "rivers" "secret" (include "rivers.surrealAuthSecretName" .) "key" .Values.surrealdb.auth.secretKeys.password "generate" (randAlphaNum 32) "override" .Values.surrealdb.auth.password) -}}
{{- end -}}

{{/*
Fail install loudly when `existingSecret` and inline `username`/`password`
are both set — ambiguous which one the chart should use. Other states are
all valid (bundled DB auto-generates; external DB without creds connects
unauthenticated).
*/}}
{{- define "rivers.validateSurrealAuth" -}}
{{- if and .Values.surrealdb.auth.existingSecret (or .Values.surrealdb.auth.username .Values.surrealdb.auth.password) -}}
{{ fail "surrealdb.auth.existingSecret and inline auth.username/auth.password are mutually exclusive — pick one." }}
{{- end -}}
{{- end -}}

{{/*
Blocks the main container from starting until the bundled SurrealDB pod
accepts connections. Bundled-DB only — external DBs are already up at
install time. `/surreal isready` instead of a probe script because the
image is distroless (no shell).
*/}}
{{- define "rivers.surrealReadinessInitContainer" -}}
- name: wait-for-surreal
  image: {{ .Values.surrealdb.image.repository }}:{{ .Values.surrealdb.image.tag }}
  imagePullPolicy: IfNotPresent
  securityContext:
    allowPrivilegeEscalation: false
    readOnlyRootFilesystem: true
    capabilities:
      drop: ["ALL"]
  command: ["/surreal"]
  args:
    - "isready"
    - "--endpoint"
    - {{ include "rivers.surrealEndpoint" . | quote }}
{{- end -}}

{{/*
SurrealDB connection env block stamped on every rivers pod (operator, UI,
code-location daemon — and re-emitted by the run pod onto step pods).
Always emits endpoint/namespace/database; the username/password
secretKeyRefs and the auth-secret coordinates the run pod needs to re-emit
downstream are only added when auth is requested.
*/}}
{{- define "rivers.surrealEnv" -}}
- name: RIVERS_SURREAL_ENDPOINT
  value: {{ include "rivers.surrealEndpoint" . | quote }}
- name: RIVERS_SURREAL_NAMESPACE
  value: {{ .Values.surrealdb.auth.namespace | quote }}
- name: RIVERS_SURREAL_DATABASE
  value: {{ .Values.surrealdb.auth.database | quote }}
{{- if include "rivers.surrealAuthRequested" . }}
- name: RIVERS_SURREAL_USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ include "rivers.surrealAuthSecretName" . }}
      key: {{ .Values.surrealdb.auth.secretKeys.username | quote }}
- name: RIVERS_SURREAL_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "rivers.surrealAuthSecretName" . }}
      key: {{ .Values.surrealdb.auth.secretKeys.password | quote }}
- name: RIVERS_SURREAL_AUTH_SECRET_NAME
  value: {{ include "rivers.surrealAuthSecretName" . | quote }}
- name: RIVERS_SURREAL_AUTH_USERNAME_KEY
  value: {{ .Values.surrealdb.auth.secretKeys.username | quote }}
- name: RIVERS_SURREAL_AUTH_PASSWORD_KEY
  value: {{ .Values.surrealdb.auth.secretKeys.password | quote }}
{{- end }}
{{- end -}}

{{- define "rivers.uiAuthCookieSecretName" -}}
rivers-ui-auth
{{- end -}}

{{/*
Session-cookie key value: base64 of 48 random bytes (the binary expects
base64 of >= 32). Preserved across upgrades via the shared
`rivers.lookupOrGenerate` (the same lookup/memoize path as the SurrealDB
secrets), differing only in the generator.
*/}}
{{- define "rivers.uiAuthCookieSecretValue" -}}
{{- include "rivers.lookupOrGenerate" (dict "ctx" . "cacheKey" "uiCookie" "secret" (include "rivers.uiAuthCookieSecretName" .) "key" .Values.ui.auth.cookieSecret.secretKey "generate" (randAlphaNum 48 | b64enc)) -}}
{{- end -}}

{{/*
Fail the install loudly on unusable ui.auth combinations.
*/}}
{{- define "rivers.validateUiAuth" -}}
{{- $auth := .Values.ui.auth -}}
{{- if not (has $auth.mode (list "none" "oidc" "forward")) -}}
{{ fail (printf "ui.auth.mode must be none|oidc|forward, got %q" $auth.mode) }}
{{- end -}}
{{- if eq $auth.mode "oidc" -}}
{{- if not $auth.oidc.issuer -}}
{{ fail "ui.auth.mode=oidc requires ui.auth.oidc.issuer" }}
{{- end -}}
{{- if not $auth.oidc.clientId -}}
{{ fail "ui.auth.mode=oidc requires ui.auth.oidc.clientId" }}
{{- end -}}
{{- if not $auth.publicUrl -}}
{{ fail "ui.auth.mode=oidc requires ui.auth.publicUrl" }}
{{- end -}}
{{- if and (not $auth.oidc.existingSecret) (not $auth.oidc.publicClient) -}}
{{ fail "ui.auth.mode=oidc requires ui.auth.oidc.existingSecret (or publicClient: true for PKCE-only IdP registrations)" }}
{{- end -}}
{{- end -}}
{{- if and (eq $auth.mode "forward") (not $auth.forward.trustedProxies) -}}
{{ fail "ui.auth.mode=forward requires ui.auth.forward.trustedProxies (0.0.0.0/0 must be typed deliberately)" }}
{{- end -}}
{{- end -}}

{{/*
RIVERS_AUTH_* env for the UI container. Secrets flow via secretKeyRef
only; nothing is emitted in mode "none".
*/}}
{{- define "rivers.uiAuthEnv" -}}
{{- $auth := .Values.ui.auth -}}
{{- if ne $auth.mode "none" }}
- name: RIVERS_AUTH_MODE
  value: {{ $auth.mode | quote }}
{{- with $auth.allowedDomains }}
- name: RIVERS_AUTH_ALLOWED_DOMAINS
  value: {{ join "," . | quote }}
{{- end }}
{{- with $auth.allowedGroups }}
- name: RIVERS_AUTH_ALLOWED_GROUPS
  value: {{ join "," . | quote }}
{{- end }}
{{- with $auth.allowedUsers }}
- name: RIVERS_AUTH_ALLOWED_USERS
  value: {{ join "," . | quote }}
{{- end }}
{{- end }}
{{- if eq $auth.mode "oidc" }}
- name: RIVERS_AUTH_PUBLIC_URL
  value: {{ $auth.publicUrl | quote }}
- name: RIVERS_AUTH_SESSION_TTL
  value: {{ $auth.sessionTtl | quote }}
- name: RIVERS_AUTH_COOKIE_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ $auth.cookieSecret.existingSecret | default (include "rivers.uiAuthCookieSecretName" .) }}
      key: {{ $auth.cookieSecret.secretKey }}
- name: RIVERS_AUTH_OIDC_ISSUER
  value: {{ $auth.oidc.issuer | quote }}
- name: RIVERS_AUTH_OIDC_CLIENT_ID
  value: {{ $auth.oidc.clientId | quote }}
{{- if not $auth.oidc.publicClient }}
- name: RIVERS_AUTH_OIDC_CLIENT_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ $auth.oidc.existingSecret }}
      key: {{ $auth.oidc.clientSecretKey }}
{{- end }}
- name: RIVERS_AUTH_OIDC_SCOPES
  value: {{ $auth.oidc.scopes | quote }}
- name: RIVERS_AUTH_OIDC_GROUPS_CLAIM
  value: {{ $auth.oidc.groupsClaim | quote }}
{{- if $auth.oidc.rpLogout }}
- name: RIVERS_AUTH_OIDC_RP_LOGOUT
  value: "true"
{{- end }}
{{- end }}
{{- if eq $auth.mode "forward" }}
- name: RIVERS_AUTH_FORWARD_TRUSTED_PROXIES
  value: {{ join "," $auth.forward.trustedProxies | quote }}
- name: RIVERS_AUTH_FORWARD_USER_HEADER
  value: {{ $auth.forward.userHeader | quote }}
- name: RIVERS_AUTH_FORWARD_EMAIL_HEADER
  value: {{ $auth.forward.emailHeader | quote }}
- name: RIVERS_AUTH_FORWARD_GROUPS_HEADER
  value: {{ $auth.forward.groupsHeader | quote }}
- name: RIVERS_AUTH_FORWARD_NAME_HEADER
  value: {{ $auth.forward.nameHeader | quote }}
{{- with $auth.forward.logoutUrl }}
- name: RIVERS_AUTH_FORWARD_LOGOUT_URL
  value: {{ . | quote }}
{{- end }}
{{- end }}
{{- end -}}
