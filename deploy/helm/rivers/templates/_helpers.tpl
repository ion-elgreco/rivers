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
existing Secret (`lookup`) so passwords don't rotate underneath running
pods on upgrade; otherwise generate fresh `randAlphaNum 32`.

The result is **memoized on `.Values`** by `cacheKey` — multiple includes
within one render must agree (e.g. `rivers-surrealdb-auth` and
`rivers-surrealdb-setup` Secrets both bake in the rivers password). Without
memoization each `randAlphaNum` call would diverge, leaving the auth
Secret and the user-init Job's SurrealQL with different passwords on
first install.

Args (dict): `ctx` (root context), `secret`, `key`, `override`, `cacheKey`.
*/}}
{{- define "rivers.surrealLookupOrGenerate" -}}
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
{{-       $val = randAlphaNum 32 -}}
{{-     end -}}
{{-   end -}}
{{-   $_ := set $cache .cacheKey $val -}}
{{-   $_ := set .ctx.Values "__riversCachedPasswords" $cache -}}
{{- end -}}
{{- index $cache .cacheKey -}}
{{- end -}}

{{- define "rivers.surrealBootstrapPassword" -}}
{{- include "rivers.surrealLookupOrGenerate" (dict "ctx" . "cacheKey" "bootstrap" "secret" (include "rivers.surrealBootstrapSecretName" .) "key" "password" "override" .Values.surrealdb.auth.bootstrap.password) -}}
{{- end -}}

{{- define "rivers.surrealRiversUsername" -}}
{{- .Values.surrealdb.auth.username | default "rivers" -}}
{{- end -}}

{{- define "rivers.surrealRiversPassword" -}}
{{- include "rivers.surrealLookupOrGenerate" (dict "ctx" . "cacheKey" "rivers" "secret" (include "rivers.surrealAuthSecretName" .) "key" .Values.surrealdb.auth.secretKeys.password "override" .Values.surrealdb.auth.password) -}}
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
