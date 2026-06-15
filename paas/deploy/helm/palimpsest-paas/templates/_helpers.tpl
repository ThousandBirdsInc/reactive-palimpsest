{{- define "palimpsest-paas.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "palimpsest-paas.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "palimpsest-paas.labels" -}}
app.kubernetes.io/name: {{ include "palimpsest-paas.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "palimpsest-paas.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (printf "%s-control-plane" (include "palimpsest-paas.fullname" .)) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- /* Connection URL to the control-plane metadata database (CloudNativePG -rw service). */ -}}
{{- define "palimpsest-paas.controlPlaneDatabaseUrl" -}}
{{- $db := .Values.controlPlaneDatabase -}}
postgres://{{ $db.owner }}@{{ include "palimpsest-paas.fullname" . }}-control-db-rw:5432/{{ $db.database }}
{{- end -}}
