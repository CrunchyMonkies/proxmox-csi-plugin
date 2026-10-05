{{/*
Expand the name of the chart.
*/}}
{{- define "proxmox-csi-plugin.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "proxmox-csi-plugin.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "proxmox-csi-plugin.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "proxmox-csi-plugin.labels" -}}
helm.sh/chart: {{ include "proxmox-csi-plugin.chart" . }}
app.kubernetes.io/name: {{ include "proxmox-csi-plugin.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "proxmox-csi-plugin.selectorLabels" -}}
app.kubernetes.io/name: {{ include "proxmox-csi-plugin.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: controller
{{- end }}

{{- define "proxmox-csi-plugin-node.selectorLabels" -}}
app.kubernetes.io/name: {{ include "proxmox-csi-plugin.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: node
{{- end }}

{{- define "proxmox-csi-plugin-migrator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "proxmox-csi-plugin.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: migrator
{{- end }}


{{/*
Create the name of the service account to use
*/}}
{{- define "proxmox-csi-plugin.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "proxmox-csi-plugin.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Operator selector labels
*/}}
{{- define "proxmox-csi-plugin-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "proxmox-csi-plugin.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: operator
{{- end }}

{{/*
Create the name of the operator service account to use
*/}}
{{- define "proxmox-csi-plugin-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- printf "%s-operator" (include "proxmox-csi-plugin.fullname" .) }}
{{- else }}
{{- "default" }}
{{- end }}
{{- end }}

{{/*
Name of the Secret holding the operator cloud config, whether this chart
renders it or an existing one was supplied.
*/}}
{{- define "proxmox-csi-plugin-operator.configSecretName" -}}
{{- default (printf "%s-operator" (include "proxmox-csi-plugin.fullname" .)) .Values.operator.existingConfigSecret }}
{{- end }}

{{/*
Key inside the operator config Secret.
*/}}
{{- define "proxmox-csi-plugin-operator.configSecretKey" -}}
{{- default "config.yaml" .Values.operator.existingConfigSecretKey }}
{{- end }}

{{/*
Refuse to render a Proxmox credential into a manifest.
Inline token_secret or password are refused at template time.
*/}}
{{- define "proxmox-csi-plugin-operator.assertNoInlineCredentials" -}}
{{- range $i, $cluster := .Values.operator.config.clusters }}
{{- if $cluster.token_secret }}
{{- fail (printf "operator.config.clusters[%d] (region %q) sets token_secret inline. Use token_ref to name a Secret instead." $i (default "" $cluster.region)) }}
{{- end }}
{{- if $cluster.password }}
{{- fail (printf "operator.config.clusters[%d] (region %q) sets password inline. The operator authenticates with a scoped API token via token_ref." $i (default "" $cluster.region)) }}
{{- end }}
{{- if not (or $cluster.token_ref $cluster.token_id_file $cluster.token_secret_file) }}
{{- fail (printf "operator.config.clusters[%d] (region %q) has no token_ref. The operator resolves its credential from a Secret at startup." $i (default "" $cluster.region)) }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Namespace in the management cluster holding one tenant's volume objects.
*/}}
{{- define "proxmox-csi-plugin-operator.tenantNamespace" -}}
{{- default (printf "tenant-%s" .name) .namespace }}
{{- end }}
