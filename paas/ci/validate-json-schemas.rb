#!/usr/bin/env ruby
# frozen_string_literal: true

require "json"
require "time"
require "yaml"

ROOT = File.expand_path("../..", __dir__)
SPECS = File.join(ROOT, "paas", "specs")
EXAMPLES = File.join(ROOT, "paas", "examples")
OBSERVABILITY = File.join(ROOT, "paas", "observability")
DASHBOARD = File.join(OBSERVABILITY, "dashboards", "palimpsest-paas-overview.json")
ALERTS = File.join(OBSERVABILITY, "alerts", "palimpsest-paas.rules.yaml")

EXAMPLE_SCHEMAS = {
  "billing-export.sync-egress.json" => ["billing-export.schema.json"],
  "certificate-authority-provider.local-dev.json" => ["certificate-authority-provider.schema.json"],
  "cluster.allocating-storage.json" => ["managed-postgres-cluster.schema.json"],
  "cluster.requested.json" => ["managed-postgres-cluster.schema.json"],
  "clone-redaction-policy.prod-to-dev.json" => ["clone-redaction-policy.schema.json"],
  "database-proxy-route.json" => ["database-proxy-route.schema.json"],
  "domain.env.json" => ["domain.schema.json"],
  "deployment.config-version-1.json" => ["deployment-spec.schema.json", "/properties/spec"],
  "deployment.config-version-2.json" => ["deployment-spec.schema.json", "/properties/spec"],
  "deployment.signed.json" => ["deployment-spec.schema.json"],
  "environment-health.degraded.json" => ["environment-health.schema.json"],
  "environment.managed.json" => ["environment.schema.json"],
  "gateway-route.json" => ["gateway-route.schema.json"],
  "incident.env.json" => ["incident.schema.json"],
  "ip-allowlist-rule.env.json" => ["ip-allowlist-rule.schema.json"],
  "jwt-issuer.env.json" => ["jwt-issuer.schema.json"],
  "maintenance-window.env.json" => ["maintenance-window.schema.json"],
  "managed-postgres-backup-retention-policy.json" => ["managed-postgres-backup-retention-policy.schema.json"],
  "managed-postgres-backup-artifact.local-fs.json" => ["managed-postgres-backup-artifact.schema.json"],
  "managed-postgres-acme-order.pending.json" => ["managed-postgres-acme-order.schema.json"],
  "managed-postgres-major-upgrade.running.json" => ["managed-postgres-major-upgrade.schema.json"],
  "managed-postgres-runtime-check.healthy.json" => ["managed-postgres-runtime-check.schema.json"],
  "managed-postgres-support-access-session.active.json" => ["managed-postgres-support-access-session.schema.json"],
  "node-agent-credential.metadata.json" => ["node-agent-credential-metadata.schema.json"],
  "node-agent-credential.issued.json" => ["node-agent-credential.schema.json"],
  "node-agent.archive-wal-segment.json" => ["node-agent-command.schema.json"],
  "node-agent.check-postgres-standby-lag.json" => ["node-agent-command.schema.json"],
  "node-agent.configure-postgres-access.json" => ["node-agent-command.schema.json"],
  "node-agent.delete-backup-data.json" => ["node-agent-command.schema.json"],
  "node-agent.delete-postgres-data.json" => ["node-agent-command.schema.json"],
  "node-agent.fence-postgres-primary.json" => ["node-agent-command.schema.json"],
  "node-agent.prepare-postgres-standby.json" => ["node-agent-command.schema.json"],
  "node-agent.prepare-postgres.json" => ["node-agent-command.schema.json"],
  "node-agent.prepare-restore.json" => ["node-agent-command.schema.json"],
  "node-agent.promote-postgres-standby.json" => ["node-agent-command.schema.json"],
  "node-agent.resize-postgres-storage.json" => ["node-agent-command.schema.json"],
  "node-agent.run-base-backup.json" => ["node-agent-command.schema.json"],
  "node-agent.update-postgres-minor.json" => ["node-agent-command.schema.json"],
  "node-agent.upgrade-postgres-major.json" => ["node-agent-command.schema.json"],
  "node-host-hardening-check.passing.json" => ["node-host-hardening-check.schema.json"],
  "node-host.local.json" => ["node-host.schema.json"],
  "query-permission-policy.dashboard-probe.json" => ["query-permission-policy.schema.json"],
  "quota-alert.sync-egress.json" => ["quota-alert.schema.json"],
  "quota-policy.sync-egress.json" => ["quota-policy.schema.json"],
  "secret-encryption-key.active.json" => ["secret-encryption-key.schema.json"],
  "secret-rewrap-plan.planned.json" => ["secret-rewrap-plan.schema.json"],
  "sso-provider.org.json" => ["sso-provider.schema.json"],
  "static-egress-ip.env.json" => ["static-egress-ip.schema.json"],
  "sync-deployment.requested.json" => ["sync-deployment.schema.json"],
  "usage-event.sync-egress.json" => ["usage-event.schema.json"],
  "webhook-endpoint.env.json" => ["webhook-endpoint.schema.json"]
}.freeze

def parse_json(path)
  JSON.parse(File.read(path))
rescue JSON::ParserError => error
  abort "#{path}: invalid JSON: #{error.message}"
end

def pointer(root, fragment)
  return root if fragment.nil? || fragment.empty?

  fragment.split("/").drop(1).reduce(root) do |current, token|
    key = token.gsub("~1", "/").gsub("~0", "~")
    current.fetch(key)
  end
end

def type_matches?(value, type)
  case type
  when "null"
    value.nil?
  when "boolean"
    value == true || value == false
  when "integer"
    value.is_a?(Integer) && value != true && value != false
  when "number"
    value.is_a?(Numeric) && value != true && value != false
  when "string"
    value.is_a?(String)
  when "array"
    value.is_a?(Array)
  when "object"
    value.is_a?(Hash)
  else
    false
  end
end

def valid_datetime?(value)
  Time.iso8601(value)
  true
rescue ArgumentError
  false
end

def validate_dashboard(path)
  dashboard = parse_json(path)
  errors = []
  errors << "#{path}: title is required" unless dashboard["title"].is_a?(String) && !dashboard["title"].empty?
  panels = dashboard["panels"]
  unless panels.is_a?(Array) && !panels.empty?
    return errors + ["#{path}: panels must be a non-empty array"]
  end

  ids = []
  expressions = []
  panels.each_with_index do |panel, index|
    panel_path = "#{path}: panel[#{index}]"
    errors << "#{panel_path}: id is required" unless panel["id"].is_a?(Integer)
    ids << panel["id"] if panel["id"].is_a?(Integer)
    errors << "#{panel_path}: title is required" unless panel["title"].is_a?(String) && !panel["title"].empty?
    errors << "#{panel_path}: type is required" unless panel["type"].is_a?(String) && !panel["type"].empty?
    errors << "#{panel_path}: gridPos is required" unless panel["gridPos"].is_a?(Hash)
    targets = panel["targets"]
    unless targets.is_a?(Array) && !targets.empty?
      errors << "#{panel_path}: targets must be a non-empty array"
      next
    end
    targets.each_with_index do |target, target_index|
      expr = target["expr"]
      if expr.is_a?(String) && !expr.empty?
        expressions << expr
      else
        errors << "#{panel_path}.targets[#{target_index}]: expr is required"
      end
    end
  end

  id_counts = Hash.new(0)
  ids.each { |id| id_counts[id] += 1 }
  duplicate_ids = id_counts.select { |_id, count| count > 1 }.keys
  errors << "#{path}: duplicate panel ids #{duplicate_ids.join(", ")}" unless duplicate_ids.empty?

  required_metric_fragments = [
    "palimpsest_gateway_requests_total",
    "palimpsest_gateway_egress_bytes_total",
    "palimpsest_managed_postgres_last_successful_backup_timestamp_seconds",
    "palimpsest_managed_postgres_wal_archives_failed_total",
    "palimpsest_paas_quota_usage_ratio",
    "palimpsest_paas_billing_exports_failed_total",
    "palimpsest_managed_postgres_cluster_lifecycle_state",
    "palimpsest_managed_postgres_storage_allocated_gib",
    "palimpsest_managed_postgres_storage_used_ratio",
    "palimpsest_managed_postgres_last_successful_wal_archive_timestamp_seconds",
    "palimpsest_managed_postgres_last_successful_pitr_check_timestamp_seconds",
    "palimpsest_managed_postgres_last_successful_restore_drill_timestamp_seconds",
    "palimpsest_managed_postgres_last_successful_standby_check_timestamp_seconds",
    "palimpsest_paas_agent_commands_failed_total"
  ]
  required_metric_fragments.each do |fragment|
    next if expressions.any? { |expr| expr.include?(fragment) }

    errors << "#{path}: dashboard does not reference required metric #{fragment}"
  end

  errors
end

def parse_yaml(path)
  YAML.safe_load(File.read(path), aliases: false)
rescue Psych::Exception => error
  abort "#{path}: invalid YAML: #{error.message}"
end

def validate_alerts(path)
  alerts = parse_yaml(path)
  errors = []
  groups = alerts["groups"] if alerts.is_a?(Hash)
  unless groups.is_a?(Array) && !groups.empty?
    return ["#{path}: groups must be a non-empty array"]
  end

  alert_names = []
  groups.each_with_index do |group, group_index|
    group_path = "#{path}: groups[#{group_index}]"
    errors << "#{group_path}: name is required" unless group["name"].is_a?(String) && !group["name"].empty?
    rules = group["rules"]
    unless rules.is_a?(Array) && !rules.empty?
      errors << "#{group_path}: rules must be a non-empty array"
      next
    end
    rules.each_with_index do |rule, rule_index|
      rule_path = "#{group_path}.rules[#{rule_index}]"
      alert = rule["alert"]
      alert_names << alert if alert.is_a?(String)
      errors << "#{rule_path}: alert is required" unless alert.is_a?(String) && !alert.empty?
      errors << "#{rule_path}: expr is required" unless rule["expr"].is_a?(String) && !rule["expr"].empty?
      labels = rule["labels"]
      errors << "#{rule_path}: labels.severity is required" unless labels.is_a?(Hash) && labels["severity"].is_a?(String)
      errors << "#{rule_path}: labels.service is required" unless labels.is_a?(Hash) && labels["service"].is_a?(String)
      annotations = rule["annotations"]
      errors << "#{rule_path}: annotations.summary is required" unless annotations.is_a?(Hash) && annotations["summary"].is_a?(String)
      errors << "#{rule_path}: annotations.description is required" unless annotations.is_a?(Hash) && annotations["description"].is_a?(String)
    end
  end

  required_alerts = [
    "PalimpsestPaaSNodeAgentCommandFailures",
    "PalimpsestPaaSQuotaNearLimit",
    "PalimpsestPaaSBillingExportFailures",
    "PalimpsestManagedPostgresBackupStale",
    "PalimpsestManagedPostgresWalArchiveFailures",
    "PalimpsestManagedPostgresPitrCheckStale",
    "PalimpsestManagedPostgresRestoreDrillStale",
    "PalimpsestManagedPostgresStandbyCheckStale",
    "PalimpsestManagedPostgresStoragePressure",
    "PalimpsestGatewayHighErrorRate",
    "PalimpsestGatewayRateLimited"
  ]
  missing_alerts = required_alerts - alert_names
  errors << "#{path}: missing required alerts #{missing_alerts.join(", ")}" unless missing_alerts.empty?

  errors
end

def validate(schema, value, root, path = "$")
  schema = pointer(root, schema.fetch("$ref").delete_prefix("#")) if schema.key?("$ref")
  errors = []

  if schema.key?("anyOf")
    return [] if schema.fetch("anyOf").any? { |candidate| validate(candidate, value, root, path).empty? }

    return ["#{path}: did not match any allowed schema"]
  end

  if schema.key?("oneOf")
    matches = schema.fetch("oneOf").count { |candidate| validate(candidate, value, root, path).empty? }
    return matches == 1 ? [] : ["#{path}: matched #{matches} oneOf schemas, expected 1"]
  end

  if schema.key?("allOf")
    schema.fetch("allOf").each do |candidate|
      errors.concat(validate(candidate, value, root, path))
    end
  end

  if schema.key?("if") && validate(schema.fetch("if"), value, root, path).empty? && schema.key?("then")
    errors.concat(validate(schema.fetch("then"), value, root, path))
  end

  if schema.key?("const") && value != schema.fetch("const")
    errors << "#{path}: expected #{schema.fetch("const").inspect}, got #{value.inspect}"
  end

  if schema.key?("enum") && !schema.fetch("enum").include?(value)
    errors << "#{path}: expected one of #{schema.fetch("enum").inspect}, got #{value.inspect}"
  end

  if schema.key?("type")
    types = Array(schema.fetch("type"))
    unless types.any? { |type| type_matches?(value, type) }
      errors << "#{path}: expected type #{types.join(" or ")}, got #{value.class}"
      return errors
    end
  end

  if value.is_a?(Hash)
    required = schema.fetch("required", [])
    required.each do |key|
      errors << "#{path}: missing required property #{key.inspect}" unless value.key?(key)
    end

    properties = schema.fetch("properties", {})
    value.each do |key, child|
      child_path = "#{path}.#{key}"
      if properties.key?(key)
        errors.concat(validate(properties.fetch(key), child, root, child_path))
      elsif schema["additionalProperties"] == false
        errors << "#{child_path}: additional property is not allowed"
      elsif schema["additionalProperties"].is_a?(Hash)
        errors.concat(validate(schema.fetch("additionalProperties"), child, root, child_path))
      end
    end
  end

  if value.is_a?(Array)
    if schema.key?("minItems") && value.length < schema.fetch("minItems")
      errors << "#{path}: expected at least #{schema.fetch("minItems")} item(s)"
    end
    if schema.key?("items")
      value.each_with_index do |child, index|
        errors.concat(validate(schema.fetch("items"), child, root, "#{path}[#{index}]"))
      end
    end
  end

  if value.is_a?(String)
    errors << "#{path}: string is shorter than #{schema.fetch("minLength")}" if schema.key?("minLength") && value.length < schema.fetch("minLength")
    errors << "#{path}: string is longer than #{schema.fetch("maxLength")}" if schema.key?("maxLength") && value.length > schema.fetch("maxLength")
    errors << "#{path}: does not match #{schema.fetch("pattern")}" if schema.key?("pattern") && !Regexp.new(schema.fetch("pattern")).match?(value)
    errors << "#{path}: invalid date-time" if schema["format"] == "date-time" && !valid_datetime?(value)
  end

  if value.is_a?(Numeric)
    errors << "#{path}: below minimum #{schema.fetch("minimum")}" if schema.key?("minimum") && value < schema.fetch("minimum")
    errors << "#{path}: above maximum #{schema.fetch("maximum")}" if schema.key?("maximum") && value > schema.fetch("maximum")
  end

  errors
end

schemas = Dir[File.join(SPECS, "*.json")].to_h do |path|
  [File.basename(path), parse_json(path)]
end

example_names = Dir[File.join(EXAMPLES, "*.json")].map { |path| File.basename(path) }.sort
missing_mappings = example_names - EXAMPLE_SCHEMAS.keys
extra_mappings = EXAMPLE_SCHEMAS.keys - example_names
abort "missing schema mappings for examples: #{missing_mappings.join(", ")}" unless missing_mappings.empty?
abort "schema mappings reference missing examples: #{extra_mappings.join(", ")}" unless extra_mappings.empty?

failures = []
EXAMPLE_SCHEMAS.each do |example_name, (schema_name, fragment)|
  schema = schemas.fetch(schema_name) { abort "missing schema #{schema_name}" }
  target_schema = pointer(schema, fragment)
  example = parse_json(File.join(EXAMPLES, example_name))
  errors = validate(target_schema, example, schema)
  failures.concat(errors.map { |error| "#{example_name} -> #{schema_name}: #{error}" })
end

failures.concat(validate_dashboard(DASHBOARD))
failures.concat(validate_alerts(ALERTS))

if failures.empty?
  puts "validated #{EXAMPLE_SCHEMAS.length} PaaS example payloads, dashboard JSON, and alert rules"
else
  warn failures.join("\n")
  exit 1
end
