{{/*
Expand the name of the chart.
*/}}
{{- define "metadata-environment.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "metadata-environment.fullname" -}}
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
{{- define "metadata-environment.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "metadata-environment.labels" -}}
helm.sh/chart: {{ include "metadata-environment.chart" . }}
{{ include "metadata-environment.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "metadata-environment.selectorLabels" -}}
app.kubernetes.io/name: {{ include "metadata-environment.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "metadata-environment.serviceAccountName" -}}
{{- if .Values.ingestTaskOperator.serviceAccount.create }}
{{- default (include "metadata-environment.fullname" .) .Values.ingestTaskOperator.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.ingestTaskOperator.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the flink service account to use
*/}}
{{- define "metadata-environment.flink.serviceAccountName" -}}
{{- default "default" .Values.ingestTaskOperator.flink.serviceAccount.name }}
{{- end }}

{{/*
Create the name of the role to use
*/}}
{{- define "metadata-environment.roleName" -}}
{{ default (include "common.names.fullname" .) .Values.ingestTaskOperator.serviceAccount.role.name }}
{{- end }}

{{/*
Create the name of the role to use
*/}}
{{- define "metadata-environment.clusterRoleName" -}}
{{ default (include "common.names.fullname" .) .Values.ingestTaskOperator.serviceAccount.clusterRole.name }}
{{- end }}


{{/*
Create the name of the rolebinding to use
*/}}
{{- define "metadata-environment.roleBindingName" -}}
{{ default (include "common.names.fullname" .) .Values.ingestTaskOperator.serviceAccount.roleBinding.name }}
{{- end }}

{{/*
Create the name of the rolebinding to use
*/}}
{{- define "metadata-environment.clusterRoleBindingName" -}}
{{ default (include "common.names.fullname" .) .Values.ingestTaskOperator.serviceAccount.clusterRoleBinding.name }}
{{- end }}