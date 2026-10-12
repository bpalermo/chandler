{{/* Name used as the prefix of the chart's resources. */}}
{{- define "chandler.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 50 | trimSuffix "-" -}}
{{- end -}}

{{/* Labels common to every resource. */}}
{{- define "chandler.labels" -}}
app.kubernetes.io/name: {{ include "chandler.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end -}}

{{/*
Selector labels of one component. Call with (dict "root" . "component" "csi").
*/}}
{{- define "chandler.selectorLabels" -}}
app.kubernetes.io/name: {{ include "chandler.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/*
An image reference: by digest when one is given, else by tag, else by the
chart's appVersion. Call with (dict "root" . "image" .Values.csi.image).
*/}}
{{- define "chandler.image" -}}
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
{{- define "chandler.restrictedSecurityContext" -}}
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
{{- define "chandler.identity" -}}
{{- $key := .component | replace "provider-file" "providerFile" | replace "provider-s3" "providerS3" -}}
{{- $override := index .root.Values.tls.identities $key -}}
{{- if $override -}}
{{ $override }}
{{- else -}}
{{ include "chandler.name" .root }}-{{ .component }}.{{ .root.Release.Namespace }}.svc
{{- end -}}
{{- end -}}

{{/* The Secret holding a component's certificate. Same arguments. */}}
{{- define "chandler.tlsSecret" -}}
{{- $key := .component | replace "provider-file" "providerFile" | replace "provider-s3" "providerS3" -}}
{{- if eq .root.Values.tls.mode "existing" -}}
{{ required (printf "tls.existingSecrets.%s is required with tls.mode existing" $key) (index .root.Values.tls.existingSecrets $key) }}
{{- else -}}
{{ include "chandler.name" .root }}-{{ .component }}-tls
{{- end -}}
{{- end -}}

{{/* The TLS arguments of a component's container. Same arguments. */}}
{{- define "chandler.tlsArgs" -}}
{{- if eq .root.Values.tls.mode "disabled" -}}
- --insecure
{{- else -}}
- --tls-cert=/etc/chandler/tls/tls.crt
- --tls-key=/etc/chandler/tls/tls.key
- --tls-ca=/etc/chandler/tls/ca.crt
{{- end -}}
{{- end -}}

{{/* The volume mount and volume of a component's certificate. */}}
{{- define "chandler.tlsVolumeMount" -}}
{{- if ne .root.Values.tls.mode "disabled" -}}
- name: tls
  mountPath: /etc/chandler/tls
  readOnly: true
{{- end -}}
{{- end -}}

{{- define "chandler.tlsVolume" -}}
{{- if ne .root.Values.tls.mode "disabled" -}}
- name: tls
  secret:
    secretName: {{ include "chandler.tlsSecret" . }}
{{- end -}}
{{- end -}}
