{{/* Name used as the prefix of the chart's resources. */}}
{{- define "stevedore.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 50 | trimSuffix "-" -}}
{{- end -}}

{{/* Labels common to every resource. */}}
{{- define "stevedore.labels" -}}
app.kubernetes.io/name: {{ include "stevedore.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end -}}

{{/*
Selector labels of one component. Call with (dict "root" . "component" "csi").
*/}}
{{- define "stevedore.selectorLabels" -}}
app.kubernetes.io/name: {{ include "stevedore.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/*
An image reference: by digest when one is given, else by tag, else by the
chart's appVersion. Call with (dict "root" . "image" .Values.csi.image).
*/}}
{{- define "stevedore.image" -}}
{{- if .image.digest -}}
{{ .image.repository }}@{{ .image.digest }}
{{- else -}}
{{ .image.repository }}:{{ .image.tag | default .root.Chart.AppVersion }}
{{- end -}}
{{- end -}}

{{/*
The security context of a container that only speaks gRPC: no root, no
capabilities, no writable filesystem.
*/}}
{{- define "stevedore.restrictedSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
runAsNonRoot: true
capabilities:
  drop:
    - ALL
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{/*
The identity a component's certificate carries. Call with
(dict "root" . "component" "server"); components are csi, server,
provider-file and provider-s3.
*/}}
{{- define "stevedore.identity" -}}
{{- $key := .component | replace "provider-file" "providerFile" | replace "provider-s3" "providerS3" -}}
{{- $override := index .root.Values.tls.identities $key -}}
{{- if $override -}}
{{ $override }}
{{- else if eq .root.Values.tls.mode "spiffe" -}}
{{ fail (printf "tls.identities.%s is required with tls.mode spiffe: the component's SPIFFE ID" $key) }}
{{- else -}}
{{ include "stevedore.name" .root }}-{{ .component }}.{{ .root.Release.Namespace }}.svc
{{- end -}}
{{- end -}}

{{/* The Secret holding a component's certificate. Same arguments. */}}
{{- define "stevedore.tlsSecret" -}}
{{- $key := .component | replace "provider-file" "providerFile" | replace "provider-s3" "providerS3" -}}
{{- if eq .root.Values.tls.mode "existing" -}}
{{ required (printf "tls.existingSecrets.%s is required with tls.mode existing" $key) (index .root.Values.tls.existingSecrets $key) }}
{{- else -}}
{{ include "stevedore.name" .root }}-{{ .component }}-tls
{{- end -}}
{{- end -}}

{{/* The TLS arguments of a component's container. Same arguments. */}}
{{- define "stevedore.tlsArgs" -}}
{{- if eq .root.Values.tls.mode "disabled" -}}
- --insecure
{{- else if eq .root.Values.tls.mode "spiffe" -}}
- --spiffe-socket=unix:///spiffe-workload-api/{{ .root.Values.tls.spiffe.socket }}
{{- else -}}
- --tls-cert=/etc/stevedore/tls/tls.crt
- --tls-key=/etc/stevedore/tls/tls.key
- --tls-ca=/etc/stevedore/tls/ca.crt
{{- end -}}
{{- end -}}

{{/* The volume mount and volume of a component's certificate. */}}
{{- define "stevedore.tlsVolumeMount" -}}
{{- if eq .root.Values.tls.mode "spiffe" -}}
- name: tls
  mountPath: /spiffe-workload-api
  readOnly: true
{{- else if ne .root.Values.tls.mode "disabled" -}}
- name: tls
  mountPath: /etc/stevedore/tls
  readOnly: true
{{- end -}}
{{- end -}}

{{- define "stevedore.tlsVolume" -}}
{{- if eq .root.Values.tls.mode "spiffe" -}}
- name: tls
  {{- required "tls.spiffe.volume is required with tls.mode spiffe: the volume holding the Workload API socket" .root.Values.tls.spiffe.volume | toYaml | nindent 2 }}
{{- else if ne .root.Values.tls.mode "disabled" -}}
- name: tls
  secret:
    secretName: {{ include "stevedore.tlsSecret" . }}
{{- end -}}
{{- end -}}

{{/*
The environment that points a component's metrics at the collector, in the
standard OpenTelemetry variables. Call with the root context.
*/}}
{{- define "stevedore.telemetryEnv" -}}
{{- with .Values.telemetry.otlpEndpoint -}}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ . | quote }}
- name: OTEL_METRIC_EXPORT_INTERVAL
  value: {{ mul (int $.Values.telemetry.exportIntervalSeconds) 1000 | quote }}
{{- $attributes := list -}}
{{- range $key, $value := $.Values.telemetry.resourceAttributes -}}
{{- $attributes = append $attributes (printf "%s=%s" $key $value) -}}
{{- end }}
{{- if $attributes }}
- name: OTEL_RESOURCE_ATTRIBUTES
  value: {{ join "," $attributes | quote }}
{{- end }}
{{- end -}}
{{- end -}}
