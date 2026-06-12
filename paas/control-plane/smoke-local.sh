#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CONTROL_PLANE_DIR="$ROOT/paas/control-plane"
API_ADDR="${PALIMPSEST_PAAS_API_ADDR:-127.0.0.1:18088}"
API_URL="http://${API_ADDR}"
DATABASE_URL="${PALIMPSEST_PAAS_DATABASE_URL:-postgres://palimpsest_control:palimpsest_control@127.0.0.1:54330/palimpsest_control}"
export PALIMPSEST_PAAS_AGENT_HOST_ID="${PALIMPSEST_PAAS_AGENT_HOST_ID:-local-dev-host}"
export PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT="${PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT:-/tmp/palimpsest-paas-smoke}"
export PALIMPSEST_PAAS_AGENT_FIRST_PORT="${PALIMPSEST_PAAS_AGENT_FIRST_PORT:-55000}"
export PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_DIR="${PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_DIR:-/tmp/palimpsest-paas-object-store}"
export PALIMPSEST_PAAS_POSTGRES_BIN_DIR="${PALIMPSEST_PAAS_POSTGRES_BIN_DIR:-/usr/local/pgsql-18/bin}"
export PALIMPSEST_PAAS_BILLING_EXPORT_DIR="${PALIMPSEST_PAAS_BILLING_EXPORT_DIR:-/tmp/palimpsest-paas-billing-exports}"
export PALIMPSEST_PAAS_AGENT_SIGNING_KEY_BASE64="${PALIMPSEST_PAAS_AGENT_SIGNING_KEY_BASE64:-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=}"
PALIMPSEST_PAAS_SECRET_PROVIDER="${PALIMPSEST_PAAS_SECRET_PROVIDER:-local-dev}"
export PALIMPSEST_PAAS_SECRET_PROVIDER
PALIMPSEST_PAAS_CLUSTER_PORT="$PALIMPSEST_PAAS_AGENT_FIRST_PORT"

cleanup() {
  if [[ -n "${DB_PROXY_PID:-}" ]]; then
    kill "$DB_PROXY_PID" 2>/dev/null || true
    wait "$DB_PROXY_PID" 2>/dev/null || true
  fi
  if [[ -n "${TCP_UPSTREAM_PID:-}" ]]; then
    kill "$TCP_UPSTREAM_PID" 2>/dev/null || true
    wait "$TCP_UPSTREAM_PID" 2>/dev/null || true
  fi
  if [[ -n "${GATEWAY_PID:-}" ]]; then
    kill "$GATEWAY_PID" 2>/dev/null || true
    wait "$GATEWAY_PID" 2>/dev/null || true
  fi
  if [[ -n "${SERVER_PID:-}" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  docker rm -f palimpsest-pg-cluster_123 >/dev/null 2>&1 || true
  docker rm -f palimpsest-pg-cluster_123_standby >/dev/null 2>&1 || true
  if [[ "${PALIMPSEST_PAAS_KEEP_SMOKE:-0}" != "1" ]]; then
    docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" down -v --remove-orphans >/dev/null 2>&1 || true
    rm -rf "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT"
    rm -rf "$PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_DIR"
    rm -rf "$PALIMPSEST_PAAS_BILLING_EXPORT_DIR"
  fi
}
trap cleanup EXIT

post_json() {
  local path="$1"
  local body="$2"
  local response
  response="$(mktemp)"
  if ! curl --fail-with-body --silent --show-error \
    -H "content-type: application/json" \
    -H "x-actor-id: smoke-local" \
    -X POST \
    --data "$body" \
    --output "$response" \
    "${API_URL}${path}"; then
    cat "$response" >&2 || true
    rm -f "$response"
    return 1
  fi
  cat "$response"
  rm -f "$response"
}

delete_json() {
  local path="$1"
  local response
  response="$(mktemp)"
  if ! curl --fail-with-body --silent --show-error \
    -H "accept: application/json" \
    -H "x-actor-id: smoke-local" \
    -X DELETE \
    --output "$response" \
    "${API_URL}${path}"; then
    cat "$response" >&2 || true
    rm -f "$response"
    return 1
  fi
  cat "$response"
  rm -f "$response"
}

get_json() {
  local path="$1"
  local response
  response="$(mktemp)"
  if ! curl --fail-with-body --silent --show-error \
    -H "accept: application/json" \
    --output "$response" \
    "${API_URL}${path}"; then
    cat "$response" >&2 || true
    rm -f "$response"
    return 1
  fi
  cat "$response"
  rm -f "$response"
}

get_json_bearer() {
  local token="$1"
  local path="$2"
  local response
  response="$(mktemp)"
  if ! curl --fail-with-body --silent --show-error \
    -H "accept: application/json" \
    -H "authorization: Bearer ${token}" \
    --output "$response" \
    "${API_URL}${path}"; then
    cat "$response" >&2 || true
    rm -f "$response"
    return 1
  fi
  cat "$response"
  rm -f "$response"
}

get_text() {
  local path="$1"
  local response
  response="$(mktemp)"
  if ! curl --fail-with-body --silent --show-error \
    --output "$response" \
    "${API_URL}${path}"; then
    cat "$response" >&2 || true
    rm -f "$response"
    return 1
  fi
  cat "$response"
  rm -f "$response"
}

post_json_status() {
  local path="$1"
  local body="$2"
  curl --silent --show-error \
    -H "content-type: application/json" \
    -H "x-actor-id: smoke-local" \
    -X POST \
    --data "$body" \
    --output /tmp/palimpsest-paas-status-response.json \
    --write-out "%{http_code}" \
    "${API_URL}${path}"
}

agent_signature() {
  local host_id="$1"
  local operation="$2"
  local timestamp="$3"
  ruby -ropenssl -rbase64 -e \
    'key = Base64.decode64(ENV.fetch("PALIMPSEST_PAAS_AGENT_SIGNING_KEY_BASE64")); print Base64.strict_encode64(OpenSSL::HMAC.digest("SHA256", key, ARGV.join("\n")))' \
    "$host_id" "$operation" "$timestamp"
}

post_json_agent_signed() {
  local host_id="$1"
  local operation="$2"
  local path="$3"
  local body="$4"
  local timestamp
  local signature
  local response
  timestamp="$(date +%s)"
  signature="$(agent_signature "$host_id" "$operation" "$timestamp")"
  response="$(mktemp)"
  if ! curl --fail-with-body --silent --show-error \
    -H "content-type: application/json" \
    -H "x-palimpsest-agent-host-id: ${host_id}" \
    -H "x-palimpsest-agent-operation: ${operation}" \
    -H "x-palimpsest-agent-timestamp: ${timestamp}" \
    -H "x-palimpsest-agent-key-id: ${PALIMPSEST_PAAS_AGENT_SIGNING_KEY_ID:-}" \
    -H "x-palimpsest-agent-signature: ${signature}" \
    -X POST \
    --data "$body" \
    --output "$response" \
    "${API_URL}${path}"; then
    cat "$response" >&2 || true
    rm -f "$response"
    return 1
  fi
  cat "$response"
  rm -f "$response"
}

post_json_agent_signed_status() {
  local host_id="$1"
  local operation="$2"
  local path="$3"
  local body="$4"
  local timestamp
  local signature
  timestamp="$(date +%s)"
  signature="$(agent_signature "$host_id" "$operation" "$timestamp")"
  curl --silent --show-error \
    -H "content-type: application/json" \
    -H "x-palimpsest-agent-host-id: ${host_id}" \
    -H "x-palimpsest-agent-operation: ${operation}" \
    -H "x-palimpsest-agent-timestamp: ${timestamp}" \
    -H "x-palimpsest-agent-key-id: ${PALIMPSEST_PAAS_AGENT_SIGNING_KEY_ID:-}" \
    -H "x-palimpsest-agent-signature: ${signature}" \
    -X POST \
    --data "$body" \
    --output /tmp/palimpsest-paas-agent-status-response.json \
    --write-out "%{http_code}" \
    "${API_URL}${path}"
}

post_json_bearer() {
  local token="$1"
  local path="$2"
  local body="$3"
  local response
  response="$(mktemp)"
  if ! curl --fail-with-body --silent --show-error \
    -H "content-type: application/json" \
    -H "authorization: Bearer ${token}" \
    -X POST \
    --data "$body" \
    --output "$response" \
    "${API_URL}${path}"; then
    cat "$response" >&2 || true
    rm -f "$response"
    return 1
  fi
  cat "$response"
  rm -f "$response"
}

post_json_bearer_status() {
  local token="$1"
  local path="$2"
  local body="$3"
  curl --silent --show-error \
    -H "content-type: application/json" \
    -H "authorization: Bearer ${token}" \
    -X POST \
    --data "$body" \
    --output /tmp/palimpsest-paas-bearer-status-response.json \
    --write-out "%{http_code}" \
    "${API_URL}${path}"
}

echo "starting control-plane postgres"
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" down -v --remove-orphans
rm -rf "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT"
rm -rf "$PALIMPSEST_PAAS_BILLING_EXPORT_DIR"
mkdir -p "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT"
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" up -d control-plane-postgres

echo "running flyway migrations"
if command -v flyway >/dev/null 2>&1; then
  (
    cd "$CONTROL_PLANE_DIR"
    flyway \
      -configFiles=flyway.conf \
      -url=jdbc:postgresql://127.0.0.1:54330/palimpsest_control \
      -user=palimpsest_control \
      -password=palimpsest_control \
      migrate
  )
else
  docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" run --rm flyway
fi

echo "starting SQL control-plane API on ${API_ADDR}"
cargo run -p palimpsest-paas-control-plane -- serve-sql-api "$API_ADDR" "$DATABASE_URL" &
SERVER_PID="$!"

for _ in $(seq 1 60); do
  if curl --silent --output /dev/null "${API_URL}/v1/configs/diff?old=missing&new=missing"; then
    break
  fi
  sleep 1
done

echo "creating onboarding workspace"
post_json "/v1/onboarding/workspaces" '{"organization":{"organization_id":"org_onboard","name":"Onboard Org"},"project":{"project_id":"project_onboard","organization_id":"org_onboard","name":"Onboard Project"},"environment":{"environment_id":"env_onboard","organization_id":"org_onboard","project_id":"project_onboard","name":"Onboard","region":"local"},"owner_actor_id":"user_onboard_owner"}' \
  | tee /tmp/palimpsest-paas-onboarding-workspace.json >/dev/null
grep -q '"organization_id":"org_onboard"' /tmp/palimpsest-paas-onboarding-workspace.json
grep -q '"project_id":"project_onboard"' /tmp/palimpsest-paas-onboarding-workspace.json
grep -q '"environment_id":"env_onboard"' /tmp/palimpsest-paas-onboarding-workspace.json
grep -q '"actor_id":"user_onboard_owner"' /tmp/palimpsest-paas-onboarding-workspace.json
get_json "/v1/team-memberships?organization_id=org_onboard" | tee /tmp/palimpsest-paas-onboarding-team-memberships.json >/dev/null
grep -q '"role":"owner"' /tmp/palimpsest-paas-onboarding-team-memberships.json
get_json "/v1/audit-events?organization_id=org_onboard&resource_id=org_onboard:user_onboard_owner" | tee /tmp/palimpsest-paas-onboarding-audit.json >/dev/null
grep -q '"action":"team_membership.upsert"' /tmp/palimpsest-paas-onboarding-audit.json

echo "creating control-plane resources"
post_json "/v1/organizations" '{"organization_id":"org_123","name":"Smoke Org"}' >/dev/null
post_json "/v1/projects" '{"project_id":"project_123","organization_id":"org_123","name":"Smoke Project"}' >/dev/null
post_json "/v1/environments" '{"environment_id":"env_123","organization_id":"org_123","project_id":"project_123","name":"Smoke","region":"local"}' >/dev/null
post_json "/v1/environments" '{"environment_id":"env_dev","organization_id":"org_123","project_id":"project_123","name":"Smoke Dev","region":"local"}' >/dev/null
post_json "/v1/environments" '{"environment_id":"env_capacity","organization_id":"org_123","project_id":"project_123","name":"Capacity Guard","region":"local"}' >/dev/null
echo "creating team membership"
post_json "/v1/team-memberships" '{"organization_id":"org_123","actor_id":"user_owner","role":"owner"}' \
  | tee /tmp/palimpsest-paas-team-membership.json >/dev/null
grep -q '"actor_id":"user_owner"' /tmp/palimpsest-paas-team-membership.json
get_json "/v1/team-memberships?organization_id=org_123" | tee /tmp/palimpsest-paas-team-memberships.json >/dev/null
grep -q '"actor_id":"user_owner"' /tmp/palimpsest-paas-team-memberships.json
get_json "/v1/audit-events?organization_id=org_123&action=team_membership.upsert" | tee /tmp/palimpsest-paas-audit-team-membership.json >/dev/null
grep -q '"resource_id":"org_123:user_owner"' /tmp/palimpsest-paas-audit-team-membership.json
get_json "/v1/audit-events?organization_id=org_123&resource_id=org_123" | tee /tmp/palimpsest-paas-audit-organization.json >/dev/null
grep -q '"action":"organization.create"' /tmp/palimpsest-paas-audit-organization.json
echo "registering secret encryption key metadata"
post_json "/v1/secret-encryption-keys" "$(cat "$ROOT/paas/examples/secret-encryption-key.active.json")" \
  | tee /tmp/palimpsest-paas-secret-encryption-key.json >/dev/null
grep -q '"key_ref":"kms:palimpsest:prod:control-plane-key-1"' /tmp/palimpsest-paas-secret-encryption-key.json
grep -q '"status":"active"' /tmp/palimpsest-paas-secret-encryption-key.json
get_json "/v1/secret-encryption-keys?provider=owned-kms&purpose=managed-postgres-secrets&status=active" \
  | tee /tmp/palimpsest-paas-secret-encryption-keys.json >/dev/null
grep -q '"key_ref":"kms:palimpsest:prod:control-plane-key-1"' /tmp/palimpsest-paas-secret-encryption-keys.json
get_json "/v1/secret-encryption-keys/kms%3Apalimpsest%3Aprod%3Acontrol-plane-key-1" \
  | tee /tmp/palimpsest-paas-secret-encryption-key-detail.json >/dev/null
grep -q '"purpose":"managed-postgres-secrets"' /tmp/palimpsest-paas-secret-encryption-key-detail.json
post_json "/v1/secret-encryption-keys" '{"key_ref":"kms:palimpsest:prod:control-plane-key-2","provider":"owned-kms","purpose":"managed-postgres-secrets","status":"active","created_at":"","activated_at":null,"retired_at":null}' \
  | tee /tmp/palimpsest-paas-secret-encryption-key-2.json >/dev/null
grep -q '"key_ref":"kms:palimpsest:prod:control-plane-key-2"' /tmp/palimpsest-paas-secret-encryption-key-2.json
post_json "/v1/secret-rewrap-plans" '{"source_key_ref":"kms:palimpsest:prod:control-plane-key-1","target_key_ref":"kms:palimpsest:prod:control-plane-key-2"}' \
  | tee /tmp/palimpsest-paas-secret-rewrap-plan.json >/dev/null
grep -q '"status":"planned"' /tmp/palimpsest-paas-secret-rewrap-plan.json
grep -q '"matched_secret_count":0' /tmp/palimpsest-paas-secret-rewrap-plan.json
SECRET_REWRAP_PLAN_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("plan_id")' /tmp/palimpsest-paas-secret-rewrap-plan.json)"
get_json "/v1/secret-rewrap-plans?source_key_ref=kms:palimpsest:prod:control-plane-key-1&target_key_ref=kms:palimpsest:prod:control-plane-key-2&status=planned" \
  | tee /tmp/palimpsest-paas-secret-rewrap-plans.json >/dev/null
grep -q "\"plan_id\":\"${SECRET_REWRAP_PLAN_ID}\"" /tmp/palimpsest-paas-secret-rewrap-plans.json
get_json "/v1/secret-rewrap-plans/${SECRET_REWRAP_PLAN_ID}" \
  | tee /tmp/palimpsest-paas-secret-rewrap-plan-detail.json >/dev/null
grep -q '"target_key_ref":"kms:palimpsest:prod:control-plane-key-2"' /tmp/palimpsest-paas-secret-rewrap-plan-detail.json
post_json "/v1/secret-rewrap-plans/${SECRET_REWRAP_PLAN_ID}/run" '{}' \
  | tee /tmp/palimpsest-paas-secret-rewrap-plan-run.json >/dev/null
grep -q '"status":"succeeded"' /tmp/palimpsest-paas-secret-rewrap-plan-run.json
grep -q '"rewrapped_secret_count":0' /tmp/palimpsest-paas-secret-rewrap-plan-run.json
get_json "/v1/audit-events?action=secret_rewrap_plan.run" \
  | tee /tmp/palimpsest-paas-audit-secret-rewrap-run.json >/dev/null
grep -q "\"resource_id\":\"${SECRET_REWRAP_PLAN_ID}\"" /tmp/palimpsest-paas-audit-secret-rewrap-run.json
echo "configuring JWT issuer"
post_json "/v1/jwt-issuers" "$(cat "$ROOT/paas/examples/jwt-issuer.env.json")" \
  | tee /tmp/palimpsest-paas-jwt-issuer.json >/dev/null
grep -q '"issuer_id":"jwt_issuer_env_123"' /tmp/palimpsest-paas-jwt-issuer.json
grep -q '"status":"active"' /tmp/palimpsest-paas-jwt-issuer.json
get_json "/v1/jwt-issuers?environment_id=env_123&status=active" | tee /tmp/palimpsest-paas-jwt-issuers.json >/dev/null
grep -q '"issuer":"https://auth.example.test"' /tmp/palimpsest-paas-jwt-issuers.json
get_json "/v1/jwt-issuers/jwt_issuer_env_123" | tee /tmp/palimpsest-paas-jwt-issuer-detail.json >/dev/null
grep -q '"claim":"sub","field":"user_id"' /tmp/palimpsest-paas-jwt-issuer-detail.json
get_json "/v1/audit-events?organization_id=org_123&action=jwt_issuer.upsert" | tee /tmp/palimpsest-paas-audit-jwt-issuer.json >/dev/null
grep -q '"resource_id":"jwt_issuer_env_123"' /tmp/palimpsest-paas-audit-jwt-issuer.json
echo "configuring webhook endpoint"
post_json "/v1/webhook-endpoints" "$(cat "$ROOT/paas/examples/webhook-endpoint.env.json")" \
  | tee /tmp/palimpsest-paas-webhook-endpoint.json >/dev/null
grep -q '"endpoint_id":"webhook_endpoint_env_123"' /tmp/palimpsest-paas-webhook-endpoint.json
grep -q '"status":"active"' /tmp/palimpsest-paas-webhook-endpoint.json
get_json "/v1/webhook-endpoints?environment_id=env_123&status=active" | tee /tmp/palimpsest-paas-webhook-endpoints.json >/dev/null
grep -q '"url":"https://ops.example.test/palimpsest/webhooks"' /tmp/palimpsest-paas-webhook-endpoints.json
get_json "/v1/webhook-endpoints/webhook_endpoint_env_123" | tee /tmp/palimpsest-paas-webhook-endpoint-detail.json >/dev/null
grep -q '"managed_postgres.backup.failed"' /tmp/palimpsest-paas-webhook-endpoint-detail.json
grep -q '"secret_id":"secret_webhook_endpoint_env_123"' /tmp/palimpsest-paas-webhook-endpoint-detail.json
get_json "/v1/audit-events?organization_id=org_123&action=webhook_endpoint.upsert" | tee /tmp/palimpsest-paas-audit-webhook-endpoint.json >/dev/null
grep -q '"resource_id":"webhook_endpoint_env_123"' /tmp/palimpsest-paas-audit-webhook-endpoint.json
echo "configuring SSO identity provider"
post_json "/v1/sso-providers" "$(cat "$ROOT/paas/examples/sso-provider.org.json")" \
  | tee /tmp/palimpsest-paas-sso-provider.json >/dev/null
grep -q '"provider_id":"sso_provider_org_123"' /tmp/palimpsest-paas-sso-provider.json
grep -q '"kind":"saml"' /tmp/palimpsest-paas-sso-provider.json
grep -q '"status":"active"' /tmp/palimpsest-paas-sso-provider.json
get_json "/v1/sso-providers?organization_id=org_123&kind=saml&status=active" | tee /tmp/palimpsest-paas-sso-providers.json >/dev/null
grep -q '"issuer":"https://idp.example.test/metadata"' /tmp/palimpsest-paas-sso-providers.json
get_json "/v1/sso-providers/sso_provider_org_123" | tee /tmp/palimpsest-paas-sso-provider-detail.json >/dev/null
grep -q '"claim":"email","field":"email"' /tmp/palimpsest-paas-sso-provider-detail.json
grep -q '"secret_id":"secret_sso_provider_org_123_cert"' /tmp/palimpsest-paas-sso-provider-detail.json
get_json "/v1/audit-events?organization_id=org_123&action=sso_identity_provider.upsert" | tee /tmp/palimpsest-paas-audit-sso-provider.json >/dev/null
grep -q '"resource_id":"sso_provider_org_123"' /tmp/palimpsest-paas-audit-sso-provider.json
echo "creating customer-visible incident"
post_json "/v1/incidents" "$(cat "$ROOT/paas/examples/incident.env.json")" \
  | tee /tmp/palimpsest-paas-incident.json >/dev/null
grep -q '"incident_id":"incident_env_123_storage_pressure"' /tmp/palimpsest-paas-incident.json
grep -q '"severity":"warning"' /tmp/palimpsest-paas-incident.json
grep -q '"status":"investigating"' /tmp/palimpsest-paas-incident.json
get_json "/v1/incidents?environment_id=env_123&severity=warning" | tee /tmp/palimpsest-paas-incidents.json >/dev/null
grep -q '"title":"Managed Postgres storage pressure"' /tmp/palimpsest-paas-incidents.json
get_json "/v1/incidents/incident_env_123_storage_pressure" | tee /tmp/palimpsest-paas-incident-detail.json >/dev/null
grep -q '"managed_postgres"' /tmp/palimpsest-paas-incident-detail.json
get_json "/v1/audit-events?organization_id=org_123&action=incident.upsert" | tee /tmp/palimpsest-paas-audit-incident.json >/dev/null
grep -q '"resource_id":"incident_env_123_storage_pressure"' /tmp/palimpsest-paas-audit-incident.json
echo "creating and revoking API key"
post_json "/v1/api-keys" '{"name":"Smoke CI","organization_id":"org_123","project_id":"project_123","environment_id":"env_123","role":"ci"}' \
  | tee /tmp/palimpsest-paas-api-key.json >/dev/null
grep -q '"token":"plmp_' /tmp/palimpsest-paas-api-key.json
API_KEY_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("api_key").fetch("key_id")' /tmp/palimpsest-paas-api-key.json)"
API_KEY_TOKEN="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("token")' /tmp/palimpsest-paas-api-key.json)"
get_json "/v1/api-keys?organization_id=org_123&include_revoked=true" | tee /tmp/palimpsest-paas-api-keys.json >/dev/null
grep -q "\"key_id\":\"${API_KEY_ID}\"" /tmp/palimpsest-paas-api-keys.json
grep -q '"token_prefix":"plmp_' /tmp/palimpsest-paas-api-keys.json
if grep -q 'token_hash\|"token":"' /tmp/palimpsest-paas-api-keys.json; then
  cat /tmp/palimpsest-paas-api-keys.json >&2
  echo "expected API key listing to omit token hash and plaintext token" >&2
  exit 1
fi
get_json_bearer "$API_KEY_TOKEN" "/v1/api-keys" | tee /tmp/palimpsest-paas-api-keys-scoped.json >/dev/null
grep -q "\"key_id\":\"${API_KEY_ID}\"" /tmp/palimpsest-paas-api-keys-scoped.json
post_json_bearer "$API_KEY_TOKEN" "/v1/configs" '{"config_version":"config_api_key","environment_id":"env_123","rendered_hash":"hash_api_key","status":"uploaded"}' >/dev/null
OUT_OF_SCOPE_API_KEY_STATUS="$(post_json_bearer_status "$API_KEY_TOKEN" "/v1/configs" '{"config_version":"config_api_key_out_of_scope","environment_id":"env_capacity","rendered_hash":"hash_out_of_scope","status":"uploaded"}')"
if [[ "$OUT_OF_SCOPE_API_KEY_STATUS" != "401" ]]; then
  cat /tmp/palimpsest-paas-bearer-status-response.json >&2 || true
  echo "expected env-scoped API key to return HTTP 401 for sibling environment, got $OUT_OF_SCOPE_API_KEY_STATUS" >&2
  exit 1
fi
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT count(*) FROM audit_events WHERE actor_id = 'api_key:${API_KEY_ID}' AND action = 'config.upload'" | grep -q 1
post_json "/v1/api-keys/${API_KEY_ID}/revoke" '{}' | tee /tmp/palimpsest-paas-api-key-revoked.json >/dev/null
grep -q '"revoked":true' /tmp/palimpsest-paas-api-key-revoked.json
REVOKED_API_KEY_STATUS="$(post_json_bearer_status "$API_KEY_TOKEN" "/v1/configs" '{"config_version":"config_revoked_key","environment_id":"env_123","rendered_hash":"hash_revoked","status":"uploaded"}')"
if [[ "$REVOKED_API_KEY_STATUS" != "401" ]]; then
  cat /tmp/palimpsest-paas-bearer-status-response.json >&2 || true
  echo "expected revoked API key to return HTTP 401, got $REVOKED_API_KEY_STATUS" >&2
  exit 1
fi
post_json "/v1/api-keys" '{"name":"Smoke Viewer","organization_id":"org_123","project_id":"project_123","environment_id":"env_123","role":"viewer"}' \
  | tee /tmp/palimpsest-paas-viewer-api-key.json >/dev/null
VIEWER_API_KEY_TOKEN="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("token")' /tmp/palimpsest-paas-viewer-api-key.json)"
VIEWER_API_KEY_STATUS="$(post_json_bearer_status "$VIEWER_API_KEY_TOKEN" "/v1/configs" '{"config_version":"config_viewer_key","environment_id":"env_123","rendered_hash":"hash_viewer","status":"uploaded"}')"
if [[ "$VIEWER_API_KEY_STATUS" != "401" ]]; then
  cat /tmp/palimpsest-paas-bearer-status-response.json >&2 || true
  echo "expected viewer API key mutation to return HTTP 401, got $VIEWER_API_KEY_STATUS" >&2
  exit 1
fi
get_json_bearer "$VIEWER_API_KEY_TOKEN" "/v1/audit-events?environment_id=env_123&action=config.upload" | tee /tmp/palimpsest-paas-audit-config-upload-viewer.json >/dev/null
grep -q "\"actor_id\":\"api_key:${API_KEY_ID}\"" /tmp/palimpsest-paas-audit-config-upload-viewer.json
post_json "/v1/api-keys" '{"name":"Smoke Admin","organization_id":"org_123","project_id":"project_123","environment_id":"env_123","role":"admin"}' \
  | tee /tmp/palimpsest-paas-admin-api-key.json >/dev/null
ADMIN_API_KEY_TOKEN="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("token")' /tmp/palimpsest-paas-admin-api-key.json)"
ADMIN_ESCALATION_STATUS="$(post_json_bearer_status "$ADMIN_API_KEY_TOKEN" "/v1/api-keys" '{"name":"Smoke Escalation","organization_id":"org_123","project_id":"project_123","environment_id":"env_123","role":"owner"}')"
if [[ "$ADMIN_ESCALATION_STATUS" != "401" ]]; then
  cat /tmp/palimpsest-paas-bearer-status-response.json >&2 || true
  echo "expected admin API key owner escalation to return HTTP 401, got $ADMIN_ESCALATION_STATUS" >&2
  exit 1
fi
post_json "/v1/api-keys" '{"name":"Smoke Org Admin","organization_id":"org_123","role":"admin"}' \
  | tee /tmp/palimpsest-paas-org-admin-api-key.json >/dev/null
ORG_ADMIN_API_KEY_TOKEN="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("token")' /tmp/palimpsest-paas-org-admin-api-key.json)"
post_json_bearer "$ORG_ADMIN_API_KEY_TOKEN" "/v1/team-memberships" '{"organization_id":"org_123","actor_id":"user_developer","role":"developer"}' \
  | tee /tmp/palimpsest-paas-team-membership-developer.json >/dev/null
grep -q '"actor_id":"user_developer"' /tmp/palimpsest-paas-team-membership-developer.json
ORG_ADMIN_OWNER_MEMBERSHIP_STATUS="$(post_json_bearer_status "$ORG_ADMIN_API_KEY_TOKEN" "/v1/team-memberships" '{"organization_id":"org_123","actor_id":"user_other_owner","role":"owner"}')"
if [[ "$ORG_ADMIN_OWNER_MEMBERSHIP_STATUS" != "401" ]]; then
  cat /tmp/palimpsest-paas-bearer-status-response.json >&2 || true
  echo "expected admin API key owner membership escalation to return HTTP 401, got $ORG_ADMIN_OWNER_MEMBERSHIP_STATUS" >&2
  exit 1
fi
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT count(*) FROM api_keys WHERE id = '${API_KEY_ID}' AND token_hash NOT LIKE 'plmp_%' AND revoked_at IS NOT NULL" | grep -q 1
echo "verifying config history and rollback"
post_json "/v1/configs" '{"config_version":"config_001_previous","environment_id":"env_123","rendered_hash":"hash_previous","status":"deployed"}' >/dev/null
post_json "/v1/configs" '{"config_version":"config_999_current","environment_id":"env_123","rendered_hash":"hash_current","status":"deployed"}' >/dev/null
get_json "/v1/configs?environment_id=env_123" | tee /tmp/palimpsest-paas-config-history.json >/dev/null
grep -q '"config_version":"config_999_current"' /tmp/palimpsest-paas-config-history.json
grep -q '"config_version":"config_001_previous"' /tmp/palimpsest-paas-config-history.json
post_json "/v1/environments/env_123/configs/rollback" '{"config_version":"config_rollback"}' \
  | tee /tmp/palimpsest-paas-config-rollback.json >/dev/null
grep -q '"config_version":"config_rollback"' /tmp/palimpsest-paas-config-rollback.json
grep -q '"rendered_hash":"hash_previous"' /tmp/palimpsest-paas-config-rollback.json
CONFIG_IMMUTABLE_STATUS="$(post_json_status "/v1/configs" '{"config_version":"config_999_current","environment_id":"env_123","rendered_hash":"hash_b","status":"validated"}')"
if [[ "$CONFIG_IMMUTABLE_STATUS" != "409" ]]; then
  cat /tmp/palimpsest-paas-status-response.json >&2 || true
  echo "expected deployed config update to return HTTP 409, got $CONFIG_IMMUTABLE_STATUS" >&2
  exit 1
fi
cargo run -p palimpsest-paas-node-agent -- register "$API_URL"
post_json "/v1/node-hosts/${PALIMPSEST_PAAS_AGENT_HOST_ID}/agent-credentials" '{}' \
  | tee /tmp/palimpsest-paas-agent-credential-old.json >/dev/null
post_json "/v1/node-hosts/${PALIMPSEST_PAAS_AGENT_HOST_ID}/agent-credentials" '{}' \
  | tee /tmp/palimpsest-paas-agent-credential.json >/dev/null
OLD_AGENT_KEY_ID="$(
  ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("key_id")' /tmp/palimpsest-paas-agent-credential-old.json
)"
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT count(*) FROM node_host_agent_credentials WHERE host_id = '${PALIMPSEST_PAAS_AGENT_HOST_ID}' AND state = 'active'" | grep -q 1
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT count(*) FROM node_host_agent_credentials WHERE host_id = '${PALIMPSEST_PAAS_AGENT_HOST_ID}' AND state = 'rotated'" | grep -q 1
get_json "/v1/node-hosts/${PALIMPSEST_PAAS_AGENT_HOST_ID}/agent-credentials?state=active" \
  | tee /tmp/palimpsest-paas-agent-credentials-active.json >/dev/null
grep -q '"state":"active"' /tmp/palimpsest-paas-agent-credentials-active.json
post_json "/v1/node-hosts/${PALIMPSEST_PAAS_AGENT_HOST_ID}/agent-credentials/${OLD_AGENT_KEY_ID}/revoke" '{}' \
  | tee /tmp/palimpsest-paas-agent-credential-revoked.json >/dev/null
grep -q '"state":"revoked"' /tmp/palimpsest-paas-agent-credential-revoked.json
export PALIMPSEST_PAAS_AGENT_SIGNING_KEY_ID="$(
  ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("key_id")' /tmp/palimpsest-paas-agent-credential.json
)"
export PALIMPSEST_PAAS_AGENT_SIGNING_KEY_BASE64="$(
  ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("signing_key_base64")' /tmp/palimpsest-paas-agent-credential.json
)"
cargo run -p palimpsest-paas-node-agent -- heartbeat "$API_URL"
get_json "/v1/node-hosts/${PALIMPSEST_PAAS_AGENT_HOST_ID}/agent-credentials?state=active" \
  | tee /tmp/palimpsest-paas-agent-credentials-used.json >/dev/null
grep -q '"last_used_operation":"heartbeat"' /tmp/palimpsest-paas-agent-credentials-used.json
get_json "/v1/node-hosts?state=active&region=local&failure_domain=local-dev" \
  | tee /tmp/palimpsest-paas-node-hosts-active.json >/dev/null
grep -q "\"host_id\":\"${PALIMPSEST_PAAS_AGENT_HOST_ID}\"" /tmp/palimpsest-paas-node-hosts-active.json
get_json "/v1/node-hosts/${PALIMPSEST_PAAS_AGENT_HOST_ID}" \
  | tee /tmp/palimpsest-paas-node-host.json >/dev/null
grep -q '"state":"active"' /tmp/palimpsest-paas-node-host.json
grep -q '"storage_gib":1024' /tmp/palimpsest-paas-node-host.json

echo "recording node host hardening check"
PALIMPSEST_PAAS_NODE_IMAGE_REF="palimpsest/postgres-node:2026-05-18" \
  PALIMPSEST_PAAS_OS_RELEASE="Ubuntu 24.04 LTS" \
  PALIMPSEST_PAAS_KERNEL_VERSION="6.8.0" \
  PALIMPSEST_PAAS_CONTAINER_RUNTIME="podman 5.0" \
  PALIMPSEST_PAAS_DISK_ENCRYPTION=true \
  PALIMPSEST_PAAS_FIREWALL_ENABLED=true \
  PALIMPSEST_PAAS_UNATTENDED_UPGRADES=true \
  PALIMPSEST_PAAS_LAST_PATCHED_AT="2026-05-18T09:00:00Z" \
  cargo run -p palimpsest-paas-node-agent -- hardening-check "$API_URL" \
  | tee /tmp/palimpsest-paas-node-host-hardening-check.json >/dev/null
ruby -rjson -e 'abort unless JSON.parse(File.read(ARGV.fetch(0))).fetch("status") == "passing"' /tmp/palimpsest-paas-node-host-hardening-check.json
NODE_HOST_HARDENING_CHECK_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("check_id")' /tmp/palimpsest-paas-node-host-hardening-check.json)"
get_json "/v1/node-hosts/${PALIMPSEST_PAAS_AGENT_HOST_ID}/hardening-checks?status=passing" \
  | tee /tmp/palimpsest-paas-node-host-hardening-checks.json >/dev/null
grep -q "\"check_id\":\"${NODE_HOST_HARDENING_CHECK_ID}\"" /tmp/palimpsest-paas-node-host-hardening-checks.json
get_json "/v1/node-hosts/${PALIMPSEST_PAAS_AGENT_HOST_ID}/hardening-checks/${NODE_HOST_HARDENING_CHECK_ID}" \
  | tee /tmp/palimpsest-paas-node-host-hardening-check-detail.json >/dev/null
grep -q '"postgres_major_min":18' /tmp/palimpsest-paas-node-host-hardening-check-detail.json
get_json "/v1/audit-events?action=node_host.hardening_check.record" \
  | tee /tmp/palimpsest-paas-audit-node-host-hardening-check.json >/dev/null
grep -q "\"resource_id\":\"${NODE_HOST_HARDENING_CHECK_ID}\"" /tmp/palimpsest-paas-audit-node-host-hardening-check.json

echo "verifying host maintenance blocks new placement"
post_json "/v1/node-hosts/${PALIMPSEST_PAAS_AGENT_HOST_ID}/state" '{"state":"maintenance"}' >/dev/null
get_json "/v1/node-hosts?state=maintenance" \
  | tee /tmp/palimpsest-paas-node-hosts-maintenance.json >/dev/null
grep -q "\"host_id\":\"${PALIMPSEST_PAAS_AGENT_HOST_ID}\"" /tmp/palimpsest-paas-node-hosts-maintenance.json
cargo run -p palimpsest-paas-node-agent -- heartbeat "$API_URL"
post_json "/v1/managed-postgres/clusters" '{"cluster_id":"cluster_on_maintenance","organization_id":"org_123","project_id":"project_123","environment_id":"env_capacity","region":"us-east-1","postgres_version":"18","tier":"dev","storage_gib":20,"lifecycle_state":"requested","host_assignment":null}' >/dev/null
MAINTENANCE_PLACEMENT_STATUS="$(post_json_status "/v1/managed-postgres/clusters/cluster_on_maintenance/reconcile" '{}')"
if [[ "$MAINTENANCE_PLACEMENT_STATUS" != "400" ]]; then
  cat /tmp/palimpsest-paas-status-response.json >&2 || true
  echo "expected maintenance host placement to return HTTP 400, got $MAINTENANCE_PLACEMENT_STATUS" >&2
  exit 1
fi
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT state FROM node_hosts WHERE id = '${PALIMPSEST_PAAS_AGENT_HOST_ID}'" | grep -q "maintenance"
post_json "/v1/node-hosts/${PALIMPSEST_PAAS_AGENT_HOST_ID}/state" '{"state":"active"}' >/dev/null
get_json "/v1/node-hosts/${PALIMPSEST_PAAS_AGENT_HOST_ID}" \
  | tee /tmp/palimpsest-paas-node-host-active-again.json >/dev/null
grep -q '"state":"active"' /tmp/palimpsest-paas-node-host-active-again.json

echo "creating managed postgres cluster through palimpsest db create"
PALIMPSEST_PAAS_CONTROL_PLANE_URL="$API_URL" cargo run -p palimpsest-cli -- db create \
  --actor-id smoke-local \
  --cluster-id cluster_123 \
  --organization-id org_123 \
  --project-id project_123 \
  --environment-id env_123 \
  --region us-east-1 \
  --postgres-version 18 \
  --tier dev \
  --storage-gib 20 >/dev/null
get_json "/v1/managed-postgres/clusters?environment_id=env_123" | tee /tmp/palimpsest-paas-clusters.json >/dev/null
grep -q '"cluster_id":"cluster_123"' /tmp/palimpsest-paas-clusters.json
get_json "/v1/managed-postgres/clusters/cluster_123" | tee /tmp/palimpsest-paas-cluster.json >/dev/null
grep -q '"lifecycle_state":"initializing_postgres"' /tmp/palimpsest-paas-cluster.json
get_json "/v1/environments/env_123/managed-postgres-endpoint" | tee /tmp/palimpsest-paas-managed-postgres-endpoint.json >/dev/null
grep -q '"active_cluster_id":"cluster_123"' /tmp/palimpsest-paas-managed-postgres-endpoint.json
grep -q '"updated_by_failover_id":null' /tmp/palimpsest-paas-managed-postgres-endpoint.json
get_json_bearer "$ORG_ADMIN_API_KEY_TOKEN" "/v1/managed-postgres/clusters" | tee /tmp/palimpsest-paas-clusters-scoped.json >/dev/null
grep -q '"cluster_id":"cluster_123"' /tmp/palimpsest-paas-clusters-scoped.json

echo "creating quota policy"
post_json "/v1/quota-policies" "$(cat "$ROOT/paas/examples/quota-policy.sync-egress.json")" >/dev/null
get_json "/v1/quota-policies?environment_id=env_123&metric=sync_egress_bytes" | tee /tmp/palimpsest-paas-quota-policies.json >/dev/null
grep -q '"policy_id":"quota_sync_egress_env_123"' /tmp/palimpsest-paas-quota-policies.json
grep -q '"limit_quantity":10000' /tmp/palimpsest-paas-quota-policies.json
get_json "/v1/quota-policies/quota_sync_egress_env_123" | tee /tmp/palimpsest-paas-quota-policy-detail.json >/dev/null
grep -q '"enforcement":"reject"' /tmp/palimpsest-paas-quota-policy-detail.json
post_json "/v1/quota-alerts" '{"alert_id":"quota_alert_sync_egress_env_123_40pct","policy_id":"quota_sync_egress_env_123","threshold_basis_points":4000}' \
  | tee /tmp/palimpsest-paas-quota-alert-created.json >/dev/null
grep -q '"state":"ok"' /tmp/palimpsest-paas-quota-alert-created.json
grep -q '"threshold_basis_points":4000' /tmp/palimpsest-paas-quota-alert-created.json

echo "recording an idempotent usage event"
post_json "/v1/usage-events" "$(cat "$ROOT/paas/examples/usage-event.sync-egress.json")" >/dev/null
get_json "/v1/usage-events?environment_id=env_123&metric=sync_egress_bytes" | tee /tmp/palimpsest-paas-usage-events.json
grep -q '"event_id":"usage_123"' /tmp/palimpsest-paas-usage-events.json
grep -q '"quantity":4096' /tmp/palimpsest-paas-usage-events.json
post_json "/v1/quota-alerts/evaluate?environment_id=env_123&metric=sync_egress_bytes" '{}' \
  | tee /tmp/palimpsest-paas-quota-alerts-evaluated.json >/dev/null
grep -q '"alert_id":"quota_alert_sync_egress_env_123_40pct"' /tmp/palimpsest-paas-quota-alerts-evaluated.json
grep -q '"state":"firing"' /tmp/palimpsest-paas-quota-alerts-evaluated.json
grep -q '"current_quantity":4096' /tmp/palimpsest-paas-quota-alerts-evaluated.json
get_json "/v1/quota-alerts?environment_id=env_123&state=firing" | tee /tmp/palimpsest-paas-quota-alerts.json >/dev/null
grep -q '"alert_id":"quota_alert_sync_egress_env_123_40pct"' /tmp/palimpsest-paas-quota-alerts.json
get_json "/v1/quota-alerts/quota_alert_sync_egress_env_123_40pct" | tee /tmp/palimpsest-paas-quota-alert-detail.json >/dev/null
grep -q '"fired_at":' /tmp/palimpsest-paas-quota-alert-detail.json
OVER_QUOTA_STATUS="$(post_json_status "/v1/usage-events" '{"event_id":"usage_over_quota","idempotency_key":"env_123:sync_egress_bytes:over-quota","organization_id":"org_123","project_id":"project_123","environment_id":"env_123","metric":"sync_egress_bytes","quantity":7000,"occurred_at":"2026-05-17T00:01:00Z"}')"
if [[ "$OVER_QUOTA_STATUS" != "429" ]]; then
  cat /tmp/palimpsest-paas-status-response.json >&2 || true
  echo "expected over-quota usage event to return HTTP 429, got $OVER_QUOTA_STATUS" >&2
  exit 1
fi
echo "creating billing export"
post_json "/v1/billing-exports" "$(cat "$ROOT/paas/examples/billing-export.sync-egress.json")" | tee /tmp/palimpsest-paas-billing-export.json
grep -q '"export_id":"billing_export_123"' /tmp/palimpsest-paas-billing-export.json
grep -q '"event_count":1' /tmp/palimpsest-paas-billing-export.json
grep -q '"quantity_total":4096' /tmp/palimpsest-paas-billing-export.json
grep -q '"status":"succeeded"' /tmp/palimpsest-paas-billing-export.json
grep -q '"delivery_ref":' /tmp/palimpsest-paas-billing-export.json
test -f "$PALIMPSEST_PAAS_BILLING_EXPORT_DIR/billing_export_123.jsonl"
grep -q '"event_id":"usage_123"' "$PALIMPSEST_PAAS_BILLING_EXPORT_DIR/billing_export_123.jsonl"
get_json "/v1/billing-exports?environment_id=env_123&status=succeeded" | tee /tmp/palimpsest-paas-billing-exports.json >/dev/null
grep -q '"export_id":"billing_export_123"' /tmp/palimpsest-paas-billing-exports.json
grep -q '"quantity_total":4096' /tmp/palimpsest-paas-billing-exports.json
get_json "/v1/billing-exports/billing_export_123" | tee /tmp/palimpsest-paas-billing-export-detail.json >/dev/null
grep -q '"event_id":"usage_123"' /tmp/palimpsest-paas-billing-export-detail.json
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT quantity FROM usage_events WHERE idempotency_key = 'env_123:sync_egress_bytes:2026-05-17T00:00:00Z'" | grep -q 4096
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT event_count, quantity_total FROM billing_exports WHERE id = 'billing_export_123'" | grep -q "1 |[[:space:]]*4096"
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT status, delivery_ref IS NOT NULL, delivered_at IS NOT NULL FROM billing_exports WHERE id = 'billing_export_123'" | grep -q "succeeded | t        | t"

echo "verifying storage-capacity placement guard"
post_json "/v1/managed-postgres/clusters" '{"cluster_id":"cluster_too_large","organization_id":"org_123","project_id":"project_123","environment_id":"env_capacity","region":"us-east-1","postgres_version":"18","tier":"dev","storage_gib":2048,"lifecycle_state":"requested","host_assignment":null}' >/dev/null
NO_CAPACITY_STATUS="$(post_json_status "/v1/managed-postgres/clusters/cluster_too_large/reconcile" '{}')"
if [[ "$NO_CAPACITY_STATUS" != "400" ]]; then
  cat /tmp/palimpsest-paas-status-response.json >&2 || true
  echo "expected over-capacity cluster placement to return HTTP 400, got $NO_CAPACITY_STATUS" >&2
  exit 1
fi

echo "container polling and completing auto-queued prepare command"
cargo run -p palimpsest-paas-node-agent -- poll-once-container "$API_URL" | tee /tmp/palimpsest-paas-completed-command.json

test -f "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123/PG_VERSION"
test -f "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123/postgresql.conf"
grep -q "wal_level = logical" "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123/postgresql.conf"
grep -q "file_copy_method = clone" "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123/postgresql.conf"

echo "container polling auto-queued start command"
cargo run -p palimpsest-paas-node-agent -- poll-once-container "$API_URL" | tee /tmp/palimpsest-paas-start-command.json
grep -q '"status": "succeeded"' /tmp/palimpsest-paas-start-command.json
for _ in $(seq 1 30); do
  if docker ps --filter name=palimpsest-pg-cluster_123 --format '{{.Names}}' | grep -q '^palimpsest-pg-cluster_123$'; then
    break
  fi
  sleep 1
done
docker ps --filter name=palimpsest-pg-cluster_123 --format '{{.Names}}' | grep -q '^palimpsest-pg-cluster_123$'

echo "verifying access configuration command was auto-queued"
get_json "/v1/managed-postgres/clusters/cluster_123" | tee /tmp/palimpsest-paas-auto-configuring-cluster.json >/dev/null
grep -q '"lifecycle_state":"configuring_replication"' /tmp/palimpsest-paas-auto-configuring-cluster.json
get_json "/v1/managed-postgres/clusters/cluster_123/agent-commands?status=pending" | tee /tmp/palimpsest-paas-auto-configure-command.json >/dev/null
grep -q '"kind":"configure_postgres_access"' /tmp/palimpsest-paas-auto-configure-command.json
if grep -q "local_dev_password" /tmp/palimpsest-paas-auto-configure-command.json; then
  echo "control plane emitted placeholder local-dev database passwords" >&2
  exit 1
fi

echo "container polling queued access configuration command"
cargo run -p palimpsest-paas-node-agent -- poll-once-container "$API_URL" | tee /tmp/palimpsest-paas-access-command.json
grep -q '"status": "succeeded"' /tmp/palimpsest-paas-access-command.json
docker exec palimpsest-pg-cluster_123 psql --host 127.0.0.1 --port "$PALIMPSEST_PAAS_CLUSTER_PORT" --username postgres --dbname postgres --tuples-only --command "SELECT 1 FROM pg_roles WHERE rolname = 'cluster_123_app'" | grep -q 1
docker exec palimpsest-pg-cluster_123 psql --host 127.0.0.1 --port "$PALIMPSEST_PAAS_CLUSTER_PORT" --username postgres --dbname postgres --tuples-only --command "SELECT 1 FROM pg_replication_slots WHERE slot_name = 'cluster_123_palimpsest_slot'" | grep -q 1
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT count(*) FROM secret_refs WHERE external_ref LIKE 'managed-postgres/cluster_123/%/password'" | grep -q 4
if [[ "$PALIMPSEST_PAAS_SECRET_PROVIDER" == "env-envelope" ]]; then
  docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
    psql -U palimpsest_control -d palimpsest_control --tuples-only \
    --command "SELECT count(*) FROM secret_refs WHERE external_ref LIKE 'managed-postgres/cluster_123/%/password' AND provider = 'env-envelope' AND secret_material IS NULL AND encrypted_material IS NOT NULL AND key_ref IS NOT NULL" | grep -q 4
else
  docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
    psql -U palimpsest_control -d palimpsest_control --tuples-only \
    --command "SELECT count(*) FROM secret_refs WHERE external_ref LIKE 'managed-postgres/cluster_123/%/password' AND provider = 'local-dev' AND secret_material IS NOT NULL AND encrypted_material IS NULL AND key_ref IS NULL" | grep -q 4
fi
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT app_secret_ref IS NOT NULL AND replication_secret_ref IS NOT NULL FROM managed_postgres_clusters WHERE id = 'cluster_123'" | grep -q t

echo "verifying configured cluster reached ready state"
get_json "/v1/managed-postgres/clusters/cluster_123" | tee /tmp/palimpsest-paas-ready-cluster.json >/dev/null
grep -q '"lifecycle_state":"ready"' /tmp/palimpsest-paas-ready-cluster.json

echo "rotating managed postgres database role credentials"
APP_SECRET_BEFORE_ROTATE="$(docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only --no-align \
  --command "SELECT COALESCE(secret_material, encrypted_material) FROM secret_refs WHERE external_ref = 'managed-postgres/cluster_123/app/password'")"
post_json "/v1/managed-postgres/clusters/cluster_123/roles/rotate" '{}' | tee /tmp/palimpsest-paas-role-credential-rotation.json
grep -q '"lifecycle_state":"configuring_replication"' /tmp/palimpsest-paas-role-credential-rotation.json
grep -q '"kind":"rotate_credentials"' /tmp/palimpsest-paas-role-credential-rotation.json
grep -q '"command_id":"cluster_123:rotate-database-role-credentials:' /tmp/palimpsest-paas-role-credential-rotation.json
APP_SECRET_BEFORE_ROTATION_APPLY="$(docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only --no-align \
  --command "SELECT COALESCE(secret_material, encrypted_material) FROM secret_refs WHERE external_ref = 'managed-postgres/cluster_123/app/password'")"
test "$APP_SECRET_BEFORE_ROTATION_APPLY" = "$APP_SECRET_BEFORE_ROTATE"
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT count(*) FROM secret_refs WHERE external_ref LIKE 'managed-postgres/cluster_123/credential-rotations/%/%/password'" | grep -q 4
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT status FROM managed_postgres_role_credential_rotations WHERE cluster_id = 'cluster_123' ORDER BY created_at DESC LIMIT 1" | grep -q applying
if grep -q "plmp_cluster_123_app_" /tmp/palimpsest-paas-role-credential-rotation.json; then
  echo "role rotation response exposed plaintext database password" >&2
  exit 1
fi
echo "container polling queued role credential rotation command"
cargo run -p palimpsest-paas-node-agent -- poll-once-container "$API_URL" | tee /tmp/palimpsest-paas-role-credential-rotation-command.json
grep -q '"status": "succeeded"' /tmp/palimpsest-paas-role-credential-rotation-command.json
APP_SECRET_AFTER_ROTATE="$(docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only --no-align \
  --command "SELECT COALESCE(secret_material, encrypted_material) FROM secret_refs WHERE external_ref = 'managed-postgres/cluster_123/app/password'")"
test "$APP_SECRET_BEFORE_ROTATE" != "$APP_SECRET_AFTER_ROTATE"
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT count(*) FROM secret_refs WHERE external_ref LIKE 'managed-postgres/cluster_123/credential-rotations/%/%/password'" | grep -q 0
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT status = 'applied' AND applied_at IS NOT NULL AND command_id IS NOT NULL FROM managed_postgres_role_credential_rotations WHERE cluster_id = 'cluster_123' ORDER BY created_at DESC LIMIT 1" | grep -q t
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT lifecycle_state FROM managed_postgres_clusters WHERE id = 'cluster_123'" | grep -q ready

echo "verifying node-agent operation tokens"
post_json "/v1/node-hosts/local-dev-host/commands" '{"operation_id":null,"command":{"command_id":"cluster_123:report-status-token-smoke","cluster_id":"cluster_123","action":{"kind":"report_status"}}}' >/dev/null
post_json_agent_signed "local-dev-host" "lease" "/v1/node-hosts/local-dev-host/commands/lease" 'null' \
  | tee /tmp/palimpsest-paas-agent-token-lease.json >/dev/null
grep -q '"operation_token":"plmp_op_' /tmp/palimpsest-paas-agent-token-lease.json
grep -q '"command_id":"cluster_123:report-status-token-smoke"' /tmp/palimpsest-paas-agent-token-lease.json
AGENT_OPERATION_TOKEN="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("operation_token")' /tmp/palimpsest-paas-agent-token-lease.json)"
MISSING_OPERATION_TOKEN_STATUS="$(post_json_agent_signed_status "local-dev-host" "complete:cluster_123:report-status-token-smoke" "/v1/node-hosts/local-dev-host/commands/cluster_123:report-status-token-smoke/complete" '{"command_id":"cluster_123:report-status-token-smoke","host_id":"local-dev-host","status":"succeeded","detail":"missing token"}')"
test "$MISSING_OPERATION_TOKEN_STATUS" = "401"
post_json_agent_signed "local-dev-host" "complete:cluster_123:report-status-token-smoke" "/v1/node-hosts/local-dev-host/commands/cluster_123:report-status-token-smoke/complete" "{\"command_id\":\"cluster_123:report-status-token-smoke\",\"host_id\":\"local-dev-host\",\"status\":\"succeeded\",\"detail\":\"token accepted\",\"operation_token\":\"${AGENT_OPERATION_TOKEN}\"}" >/dev/null
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT status = 'succeeded' AND operation_token_hash IS NULL AND operation_token_expires_at IS NULL FROM agent_commands WHERE id = 'cluster_123:report-status-token-smoke'" | grep -q t

echo "checking managed postgres schema browser and read-only SQL console"
docker exec palimpsest-pg-cluster_123 \
  psql -h 127.0.0.1 -p "$PALIMPSEST_PAAS_CLUSTER_PORT" -U postgres -d postgres --set ON_ERROR_STOP=1 \
  --command "CREATE TABLE IF NOT EXISTS public.dashboard_probe (id integer PRIMARY KEY, label text NOT NULL)" \
  --command "TRUNCATE public.dashboard_probe" \
  --command "INSERT INTO public.dashboard_probe (id, label) VALUES (1, 'alpha'), (2, 'beta')" \
  --command "CREATE TABLE IF NOT EXISTS public.customers (id integer PRIMARY KEY, email text, phone text)" \
  --command "TRUNCATE public.customers" \
  --command "INSERT INTO public.customers (id, email, phone) VALUES (1, 'alice@example.test', '+15550100'), (2, 'bob@example.test', '+15550101')" \
  --command "GRANT SELECT ON public.dashboard_probe TO cluster_123_app"
get_json "/v1/managed-postgres/clusters/cluster_123/schema" | tee /tmp/palimpsest-paas-schema-browser.json >/dev/null
grep -q '"name":"dashboard_probe"' /tmp/palimpsest-paas-schema-browser.json
grep -q '"name":"label","data_type":"text"' /tmp/palimpsest-paas-schema-browser.json
post_json "/v1/managed-postgres/clusters/cluster_123/sql-console/query" '{"sql":"select id, label from public.dashboard_probe order by id","limit":10}' | tee /tmp/palimpsest-paas-sql-console.json >/dev/null
grep -q '"row_count":2' /tmp/palimpsest-paas-sql-console.json
grep -q '"label":"alpha"' /tmp/palimpsest-paas-sql-console.json
SQL_CONSOLE_STATUS="$(post_json_status "/v1/managed-postgres/clusters/cluster_123/sql-console/query" '{"sql":"delete from public.dashboard_probe"}')"
test "$SQL_CONSOLE_STATUS" = "400"
post_json "/v1/managed-postgres/clusters/cluster_123/query-explorer/inspect" '{"sql":"select id, label from public.dashboard_probe order by id","limit":10}' \
  | tee /tmp/palimpsest-paas-query-explorer.json >/dev/null
grep -q '"canonical_sql":"select id, label from public.dashboard_probe order by id"' /tmp/palimpsest-paas-query-explorer.json
grep -q '"sharing_behavior":"read_only_snapshot"' /tmp/palimpsest-paas-query-explorer.json
grep -q '"public.dashboard_probe"' /tmp/palimpsest-paas-query-explorer.json
echo "probing managed postgres runtime state"
post_json "/v1/managed-postgres/clusters/cluster_123/runtime-checks/probe" '{}' \
  | tee /tmp/palimpsest-paas-runtime-check.json >/dev/null
grep -q '"status":"healthy"' /tmp/palimpsest-paas-runtime-check.json
grep -q '"connection_count":' /tmp/palimpsest-paas-runtime-check.json
grep -q '"max_connections":' /tmp/palimpsest-paas-runtime-check.json
grep -q '"replication_slot_lag_bytes":' /tmp/palimpsest-paas-runtime-check.json
RUNTIME_CHECK_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("check_id")' /tmp/palimpsest-paas-runtime-check.json)"
get_json "/v1/managed-postgres/clusters/cluster_123/runtime-checks?status=healthy" \
  | tee /tmp/palimpsest-paas-runtime-checks.json >/dev/null
grep -q "\"check_id\":\"${RUNTIME_CHECK_ID}\"" /tmp/palimpsest-paas-runtime-checks.json
get_json "/v1/managed-postgres/clusters/cluster_123/runtime-checks/${RUNTIME_CHECK_ID}" \
  | tee /tmp/palimpsest-paas-runtime-check-detail.json >/dev/null
grep -q '"blocked_lock_count":0' /tmp/palimpsest-paas-runtime-check-detail.json
get_json "/v1/audit-events?organization_id=org_123&action=managed_postgres_runtime_check.probe" \
  | tee /tmp/palimpsest-paas-audit-runtime-check.json >/dev/null
grep -q "\"resource_id\":\"${RUNTIME_CHECK_ID}\"" /tmp/palimpsest-paas-audit-runtime-check.json
echo "requesting audited managed postgres support access"
post_json "/v1/managed-postgres/clusters/cluster_123/support-access-sessions" '{"reason":"Investigate smoke-test production incident","ticket_ref":"SMOKE-123","duration_minutes":60}' \
  | tee /tmp/palimpsest-paas-support-access-request.json >/dev/null
grep -q '"status":"requested"' /tmp/palimpsest-paas-support-access-request.json
grep -q '"requested_by":"smoke-local"' /tmp/palimpsest-paas-support-access-request.json
SUPPORT_ACCESS_SESSION_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("session_id")' /tmp/palimpsest-paas-support-access-request.json)"
post_json "/v1/managed-postgres/clusters/cluster_123/support-access-sessions/${SUPPORT_ACCESS_SESSION_ID}/approve" '{}' \
  | tee /tmp/palimpsest-paas-support-access-approve.json >/dev/null
grep -q '"status":"active"' /tmp/palimpsest-paas-support-access-approve.json
grep -q '"approved_by":"smoke-local"' /tmp/palimpsest-paas-support-access-approve.json
get_json "/v1/managed-postgres/clusters/cluster_123/support-access-sessions?status=active" \
  | tee /tmp/palimpsest-paas-support-access-active.json >/dev/null
grep -q "\"session_id\":\"${SUPPORT_ACCESS_SESSION_ID}\"" /tmp/palimpsest-paas-support-access-active.json
get_json "/v1/managed-postgres/clusters/cluster_123/support-access-sessions/${SUPPORT_ACCESS_SESSION_ID}" \
  | tee /tmp/palimpsest-paas-support-access-detail.json >/dev/null
grep -q '"ticket_ref":"SMOKE-123"' /tmp/palimpsest-paas-support-access-detail.json
get_json "/v1/audit-events?organization_id=org_123&action=managed_postgres_support_access.approve" \
  | tee /tmp/palimpsest-paas-audit-support-access.json >/dev/null
grep -q "\"resource_id\":\"${SUPPORT_ACCESS_SESSION_ID}\"" /tmp/palimpsest-paas-audit-support-access.json
echo "verifying audit events are immutable"
if docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --set ON_ERROR_STOP=1 \
  --command "UPDATE audit_events SET action = 'tampered' WHERE id = (SELECT id FROM audit_events LIMIT 1)" >/tmp/palimpsest-paas-audit-update.txt 2>&1; then
  echo "audit_events accepted an update" >&2
  exit 1
fi
grep -q 'audit_events are immutable' /tmp/palimpsest-paas-audit-update.txt
post_json "/v1/managed-postgres/clusters/cluster_123/support-access-sessions/${SUPPORT_ACCESS_SESSION_ID}/revoke" '{}' \
  | tee /tmp/palimpsest-paas-support-access-revoke.json >/dev/null
grep -q '"status":"revoked"' /tmp/palimpsest-paas-support-access-revoke.json
post_json "/v1/query-permission-policies" "$(cat "$ROOT/paas/examples/query-permission-policy.dashboard-probe.json")" \
  | tee /tmp/palimpsest-paas-query-permission-policy.json >/dev/null
grep -q '"policy_id":"query_policy_dashboard_probe_read"' /tmp/palimpsest-paas-query-permission-policy.json
grep -q '"status":"active"' /tmp/palimpsest-paas-query-permission-policy.json
get_json "/v1/query-permission-policies?environment_id=env_123&table_schema=public&table_name=dashboard_probe&status=active" \
  | tee /tmp/palimpsest-paas-query-permission-policies.json >/dev/null
grep -q '"policy_id":"query_policy_dashboard_probe_read"' /tmp/palimpsest-paas-query-permission-policies.json
post_json "/v1/query-permission-policies/query_policy_dashboard_probe_read/dry-run" '{"sample_context":{"sub":"user_123","role":"developer"}}' \
  | tee /tmp/palimpsest-paas-query-permission-policy-dry-run.json >/dev/null
grep -q '"accepted":true' /tmp/palimpsest-paas-query-permission-policy-dry-run.json
grep -q '"checked_predicate_sql":"true"' /tmp/palimpsest-paas-query-permission-policy-dry-run.json

echo "requesting managed postgres minor update"
post_json "/v1/managed-postgres/clusters/cluster_123/update-minor" '{"target_postgres_version":"18.4"}' | tee /tmp/palimpsest-paas-update-minor-request.json
grep -q '"lifecycle_state":"updating_postgres"' /tmp/palimpsest-paas-update-minor-request.json
grep -q '"postgres_version":"18.4"' /tmp/palimpsest-paas-update-minor-request.json
grep -q '"kind":"update_postgres_minor"' /tmp/palimpsest-paas-update-minor-request.json
UPDATE_OPERATION_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("operation").fetch("operation_id")' /tmp/palimpsest-paas-update-minor-request.json)"
UPDATE_COMMAND_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("command").fetch("command_id")' /tmp/palimpsest-paas-update-minor-request.json)"
get_json "/v1/managed-postgres/clusters/cluster_123/operations?status=running&kind=update_cluster" | tee /tmp/palimpsest-paas-update-operations-running.json >/dev/null
grep -q "\"operation_id\":\"${UPDATE_OPERATION_ID}\"" /tmp/palimpsest-paas-update-operations-running.json
get_json "/v1/managed-postgres/clusters/cluster_123/agent-commands?status=pending" | tee /tmp/palimpsest-paas-update-agent-commands-pending.json >/dev/null
grep -q "\"command_id\":\"${UPDATE_COMMAND_ID}\"" /tmp/palimpsest-paas-update-agent-commands-pending.json
echo "container polling queued minor update command"
cargo run -p palimpsest-paas-node-agent -- poll-once-container "$API_URL"
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT lifecycle_state, postgres_version FROM managed_postgres_clusters WHERE id = 'cluster_123'" | grep -q "ready[[:space:]]*|[[:space:]]*18.4"
get_json "/v1/managed-postgres/clusters/cluster_123/operations?status=succeeded&kind=update_cluster" | tee /tmp/palimpsest-paas-update-operations-succeeded.json >/dev/null
grep -q "\"operation_id\":\"${UPDATE_OPERATION_ID}\"" /tmp/palimpsest-paas-update-operations-succeeded.json
get_json "/v1/managed-postgres/clusters/cluster_123/agent-commands/${UPDATE_COMMAND_ID}" | tee /tmp/palimpsest-paas-update-agent-command-succeeded.json >/dev/null
grep -q '"status":"succeeded"' /tmp/palimpsest-paas-update-agent-command-succeeded.json
grep -q '"target_postgres_version":"18.4"' "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123/palimpsest-postgres-version.json"

echo "requesting managed postgres storage resize"
post_json "/v1/managed-postgres/clusters/cluster_123/resize" '{"storage_gib":32}' | tee /tmp/palimpsest-paas-resize-request.json
grep -q '"lifecycle_state":"resizing"' /tmp/palimpsest-paas-resize-request.json
grep -q '"kind":"resize_postgres_storage"' /tmp/palimpsest-paas-resize-request.json
RESIZE_OPERATION_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("operation").fetch("operation_id")' /tmp/palimpsest-paas-resize-request.json)"
RESIZE_COMMAND_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("command").fetch("command_id")' /tmp/palimpsest-paas-resize-request.json)"
get_json "/v1/managed-postgres/clusters/cluster_123/operations?status=running" | tee /tmp/palimpsest-paas-operations-running.json >/dev/null
grep -q "\"operation_id\":\"${RESIZE_OPERATION_ID}\"" /tmp/palimpsest-paas-operations-running.json
get_json "/v1/managed-postgres/clusters/cluster_123/operations/${RESIZE_OPERATION_ID}" | tee /tmp/palimpsest-paas-operation-detail-running.json >/dev/null
grep -q '"status":"running"' /tmp/palimpsest-paas-operation-detail-running.json
get_json "/v1/managed-postgres/clusters/cluster_123/agent-commands?status=pending" | tee /tmp/palimpsest-paas-agent-commands-pending.json >/dev/null
grep -q "\"command_id\":\"${RESIZE_COMMAND_ID}\"" /tmp/palimpsest-paas-agent-commands-pending.json
get_json "/v1/managed-postgres/clusters/cluster_123/agent-commands/${RESIZE_COMMAND_ID}" | tee /tmp/palimpsest-paas-agent-command-detail-pending.json >/dev/null
grep -q '"status":"pending"' /tmp/palimpsest-paas-agent-command-detail-pending.json
echo "container polling queued storage resize command"
cargo run -p palimpsest-paas-node-agent -- poll-once-container "$API_URL" | tee /tmp/palimpsest-paas-resize-command.json
grep -q '"status": "succeeded"' /tmp/palimpsest-paas-resize-command.json
grep -q '"storage_gib":32' "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123/palimpsest-storage-quota.json"
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT lifecycle_state, storage_gib FROM managed_postgres_clusters WHERE id = 'cluster_123'" | grep -q "ready[[:space:]]*|[[:space:]]*32"
get_json "/v1/managed-postgres/clusters/cluster_123/operations?status=succeeded&kind=resize_cluster" | tee /tmp/palimpsest-paas-operations-succeeded.json >/dev/null
grep -q "\"operation_id\":\"${RESIZE_OPERATION_ID}\"" /tmp/palimpsest-paas-operations-succeeded.json
get_json "/v1/managed-postgres/clusters/cluster_123/operations/${RESIZE_OPERATION_ID}" | tee /tmp/palimpsest-paas-operation-detail-succeeded.json >/dev/null
grep -q '"status":"succeeded"' /tmp/palimpsest-paas-operation-detail-succeeded.json
get_json "/v1/managed-postgres/clusters/cluster_123/agent-commands?status=succeeded" | tee /tmp/palimpsest-paas-agent-commands-succeeded.json >/dev/null
grep -q "\"command_id\":\"${RESIZE_COMMAND_ID}\"" /tmp/palimpsest-paas-agent-commands-succeeded.json
get_json "/v1/managed-postgres/clusters/cluster_123/agent-commands/${RESIZE_COMMAND_ID}" | tee /tmp/palimpsest-paas-agent-command-detail-succeeded.json >/dev/null
grep -q '"status":"succeeded"' /tmp/palimpsest-paas-agent-command-detail-succeeded.json

echo "pausing and resuming managed postgres cluster"
post_json "/v1/managed-postgres/clusters/cluster_123/pause" '{}' | tee /tmp/palimpsest-paas-pause-request.json >/dev/null
grep -q '"lifecycle_state":"stopping"' /tmp/palimpsest-paas-pause-request.json
grep -q '"kind":"stop_postgres"' /tmp/palimpsest-paas-pause-request.json
PAUSE_OPERATION_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("operation").fetch("operation_id")' /tmp/palimpsest-paas-pause-request.json)"
PAUSE_COMMAND_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("command").fetch("command_id")' /tmp/palimpsest-paas-pause-request.json)"
get_json "/v1/managed-postgres/clusters/cluster_123/operations?status=running&kind=stop_cluster" | tee /tmp/palimpsest-paas-pause-operations-running.json >/dev/null
grep -q "\"operation_id\":\"${PAUSE_OPERATION_ID}\"" /tmp/palimpsest-paas-pause-operations-running.json
cargo run -p palimpsest-paas-node-agent -- poll-once-container "$API_URL" | tee /tmp/palimpsest-paas-pause-command.json
grep -q '"status": "succeeded"' /tmp/palimpsest-paas-pause-command.json
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT lifecycle_state FROM managed_postgres_clusters WHERE id = 'cluster_123'" | grep -q stopped
get_json "/v1/managed-postgres/clusters/cluster_123/operations/${PAUSE_OPERATION_ID}" | tee /tmp/palimpsest-paas-pause-operation-detail-succeeded.json >/dev/null
grep -q '"status":"succeeded"' /tmp/palimpsest-paas-pause-operation-detail-succeeded.json
get_json "/v1/managed-postgres/clusters/cluster_123/agent-commands/${PAUSE_COMMAND_ID}" | tee /tmp/palimpsest-paas-pause-agent-command-succeeded.json >/dev/null
grep -q '"status":"succeeded"' /tmp/palimpsest-paas-pause-agent-command-succeeded.json

post_json "/v1/managed-postgres/clusters/cluster_123/resume" '{}' | tee /tmp/palimpsest-paas-resume-request.json >/dev/null
grep -q '"lifecycle_state":"starting"' /tmp/palimpsest-paas-resume-request.json
grep -q '"kind":"start_postgres"' /tmp/palimpsest-paas-resume-request.json
RESUME_OPERATION_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("operation").fetch("operation_id")' /tmp/palimpsest-paas-resume-request.json)"
RESUME_COMMAND_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("command").fetch("command_id")' /tmp/palimpsest-paas-resume-request.json)"
get_json "/v1/managed-postgres/clusters/cluster_123/operations?status=running&kind=start_cluster" | tee /tmp/palimpsest-paas-resume-operations-running.json >/dev/null
grep -q "\"operation_id\":\"${RESUME_OPERATION_ID}\"" /tmp/palimpsest-paas-resume-operations-running.json
cargo run -p palimpsest-paas-node-agent -- poll-once-container "$API_URL" | tee /tmp/palimpsest-paas-resume-command.json
grep -q '"status": "succeeded"' /tmp/palimpsest-paas-resume-command.json
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT lifecycle_state FROM managed_postgres_clusters WHERE id = 'cluster_123'" | grep -q ready
get_json "/v1/managed-postgres/clusters/cluster_123/operations/${RESUME_OPERATION_ID}" | tee /tmp/palimpsest-paas-resume-operation-detail-succeeded.json >/dev/null
grep -q '"status":"succeeded"' /tmp/palimpsest-paas-resume-operation-detail-succeeded.json
get_json "/v1/managed-postgres/clusters/cluster_123/agent-commands/${RESUME_COMMAND_ID}" | tee /tmp/palimpsest-paas-resume-agent-command-succeeded.json >/dev/null
grep -q '"status":"succeeded"' /tmp/palimpsest-paas-resume-agent-command-succeeded.json

echo "running backup scheduler once"
post_json "/v1/scheduler/backups/run-once" '{}' | tee /tmp/palimpsest-paas-backup-scheduler.json
grep -q '"scheduled":' /tmp/palimpsest-paas-backup-scheduler.json
grep -q '"kind":"run_base_backup"' /tmp/palimpsest-paas-backup-scheduler.json

echo "container polling scheduler queued base backup command"
cargo run -p palimpsest-paas-node-agent -- poll-once-container "$API_URL" | tee /tmp/palimpsest-paas-scheduled-backup-command.json
grep -q '"status": "succeeded"' /tmp/palimpsest-paas-scheduled-backup-command.json
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT count(*) FROM managed_postgres_backups WHERE cluster_id = 'cluster_123' AND status = 'succeeded'" | grep -q 1
post_json "/v1/scheduler/backups/run-once" '{}' | tee /tmp/palimpsest-paas-backup-scheduler-empty.json
grep -q '"scheduled":\[\]' /tmp/palimpsest-paas-backup-scheduler-empty.json
post_json "/v1/managed-postgres/clusters/cluster_123/backup-retention-policy" '{"retention_days":0,"keep_min_successful_backups":1,"enabled":true}' \
  | tee /tmp/palimpsest-paas-backup-retention-policy.json >/dev/null
grep -q '"retention_days":0' /tmp/palimpsest-paas-backup-retention-policy.json
grep -q '"keep_min_successful_backups":1' /tmp/palimpsest-paas-backup-retention-policy.json
get_json "/v1/managed-postgres/clusters/cluster_123/backup-retention-policy" \
  | tee /tmp/palimpsest-paas-backup-retention-policy-detail.json >/dev/null
grep -q '"enabled":true' /tmp/palimpsest-paas-backup-retention-policy-detail.json
post_json "/v1/scheduler/backup-retention/run-once" '{}' | tee /tmp/palimpsest-paas-backup-retention-empty.json >/dev/null
grep -q '"expired_backups":\[\]' /tmp/palimpsest-paas-backup-retention-empty.json

echo "creating sync deployment state"
post_json "/v1/sync-deployments" "$(cat "$ROOT/paas/examples/sync-deployment.requested.json")" >/dev/null
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT lifecycle_state FROM sync_deployments WHERE id = 'deployment_123'" | grep -q requested
get_json "/v1/sync-deployments?environment_id=env_123&lifecycle_state=requested" | tee /tmp/palimpsest-paas-sync-deployments-requested.json >/dev/null
grep -q '"deployment_id":"deployment_123"' /tmp/palimpsest-paas-sync-deployments-requested.json
get_json "/v1/sync-deployments/deployment_123" | tee /tmp/palimpsest-paas-sync-deployment-detail.json >/dev/null
grep -q '"lifecycle_state":"requested"' /tmp/palimpsest-paas-sync-deployment-detail.json
get_json "/v1/environments/env_123/overview" | tee /tmp/palimpsest-paas-environment-overview.json >/dev/null
grep -q '"environment_id":"env_123"' /tmp/palimpsest-paas-environment-overview.json
grep -q '"active_cluster_id":"cluster_123"' /tmp/palimpsest-paas-environment-overview.json
grep -q '"cluster_id":"cluster_123"' /tmp/palimpsest-paas-environment-overview.json
grep -q '"deployment_id":"deployment_123"' /tmp/palimpsest-paas-environment-overview.json
grep -q '"quota_alert_sync_egress_env_123_40pct"' /tmp/palimpsest-paas-environment-overview.json
grep -q '"incident_id":"incident_env_123_storage_pressure"' /tmp/palimpsest-paas-environment-overview.json

echo "creating gateway route discovery state"
post_json "/v1/gateway-routes" "$(cat "$ROOT/paas/examples/gateway-route.json")" >/dev/null
get_json "/v1/gateway-routes?environment_id=env_123" | tee /tmp/palimpsest-paas-gateway-routes.json >/dev/null
grep -q '"host":"env-123.palimpsest.dev"' /tmp/palimpsest-paas-gateway-routes.json
grep -q '"sync_endpoint":"http://127.0.0.1:50051"' /tmp/palimpsest-paas-gateway-routes.json
get_json "/v1/gateway-routes/env-123.palimpsest.dev" | tee /tmp/palimpsest-paas-gateway-route-detail.json >/dev/null
grep -q '"environment_id":"env_123"' /tmp/palimpsest-paas-gateway-route-detail.json
get_json_bearer "$ORG_ADMIN_API_KEY_TOKEN" "/v1/gateway-routes?environment_id=env_123" | tee /tmp/palimpsest-paas-gateway-routes-scoped.json >/dev/null
grep -q '"host":"env-123.palimpsest.dev"' /tmp/palimpsest-paas-gateway-routes-scoped.json
echo "configuring custom domain metadata"
post_json "/v1/domains" "$(cat "$ROOT/paas/examples/domain.env.json")" \
  | tee /tmp/palimpsest-paas-domain.json >/dev/null
grep -q '"hostname":"app.example.test"' /tmp/palimpsest-paas-domain.json
grep -q '"verification_status":"pending"' /tmp/palimpsest-paas-domain.json
grep -q '"tls_status":"pending"' /tmp/palimpsest-paas-domain.json
get_json "/v1/domains?environment_id=env_123&verification_status=pending" | tee /tmp/palimpsest-paas-domains.json >/dev/null
grep -q '"route_host":"env-123.palimpsest.dev"' /tmp/palimpsest-paas-domains.json
get_json "/v1/domains/app.example.test" | tee /tmp/palimpsest-paas-domain-detail.json >/dev/null
grep -q '"verification_token":"palimpsest-domain-verification=env_123"' /tmp/palimpsest-paas-domain-detail.json
get_json "/v1/audit-events?organization_id=org_123&action=domain.upsert" | tee /tmp/palimpsest-paas-audit-domain.json >/dev/null
grep -q '"resource_id":"app.example.test"' /tmp/palimpsest-paas-audit-domain.json
get_json "/v1/environments/env_123/overview" | tee /tmp/palimpsest-paas-environment-overview-with-domain.json >/dev/null
grep -q '"hostname":"app.example.test"' /tmp/palimpsest-paas-environment-overview-with-domain.json
echo "configuring network access metadata"
post_json "/v1/ip-allowlist-rules" "$(cat "$ROOT/paas/examples/ip-allowlist-rule.env.json")" \
  | tee /tmp/palimpsest-paas-ip-allowlist-rule.json >/dev/null
grep -q '"rule_id":"ip_allowlist_env_123_migration"' /tmp/palimpsest-paas-ip-allowlist-rule.json
grep -q '"cidr":"203.0.113.10/32"' /tmp/palimpsest-paas-ip-allowlist-rule.json
grep -q '"purpose":"migration"' /tmp/palimpsest-paas-ip-allowlist-rule.json
get_json "/v1/ip-allowlist-rules?environment_id=env_123&purpose=migration&status=active" | tee /tmp/palimpsest-paas-ip-allowlist-rules.json >/dev/null
grep -q '"name":"Migration CI"' /tmp/palimpsest-paas-ip-allowlist-rules.json
get_json "/v1/ip-allowlist-rules/ip_allowlist_env_123_migration" | tee /tmp/palimpsest-paas-ip-allowlist-rule-detail.json >/dev/null
grep -q '"status":"active"' /tmp/palimpsest-paas-ip-allowlist-rule-detail.json
post_json "/v1/static-egress-ips" "$(cat "$ROOT/paas/examples/static-egress-ip.env.json")" \
  | tee /tmp/palimpsest-paas-static-egress-ip.json >/dev/null
grep -q '"egress_ip_id":"static_egress_env_123_us_east_1a"' /tmp/palimpsest-paas-static-egress-ip.json
grep -q '"ip_address":"198.51.100.23"' /tmp/palimpsest-paas-static-egress-ip.json
grep -q '"status":"active"' /tmp/palimpsest-paas-static-egress-ip.json
get_json "/v1/static-egress-ips?environment_id=env_123&region=us-east-1&status=active" | tee /tmp/palimpsest-paas-static-egress-ips.json >/dev/null
grep -q '"provider_ref":"owned-nat/us-east-1/a"' /tmp/palimpsest-paas-static-egress-ips.json
get_json "/v1/static-egress-ips/static_egress_env_123_us_east_1a" | tee /tmp/palimpsest-paas-static-egress-ip-detail.json >/dev/null
grep -q '"region":"us-east-1"' /tmp/palimpsest-paas-static-egress-ip-detail.json
get_json "/v1/audit-events?organization_id=org_123&action=ip_allowlist_rule.upsert" | tee /tmp/palimpsest-paas-audit-ip-allowlist-rule.json >/dev/null
grep -q '"resource_id":"ip_allowlist_env_123_migration"' /tmp/palimpsest-paas-audit-ip-allowlist-rule.json
get_json "/v1/audit-events?organization_id=org_123&action=static_egress_ip.upsert" | tee /tmp/palimpsest-paas-audit-static-egress-ip.json >/dev/null
grep -q '"resource_id":"static_egress_env_123_us_east_1a"' /tmp/palimpsest-paas-audit-static-egress-ip.json
get_json "/v1/environments/env_123/overview" | tee /tmp/palimpsest-paas-environment-overview-with-network.json >/dev/null
grep -q '"rule_id":"ip_allowlist_env_123_migration"' /tmp/palimpsest-paas-environment-overview-with-network.json
grep -q '"egress_ip_id":"static_egress_env_123_us_east_1a"' /tmp/palimpsest-paas-environment-overview-with-network.json
echo "configuring maintenance window metadata"
post_json "/v1/maintenance-windows" "$(cat "$ROOT/paas/examples/maintenance-window.env.json")" \
  | tee /tmp/palimpsest-paas-maintenance-window.json >/dev/null
grep -q '"window_id":"maintenance_window_env_123_weekly"' /tmp/palimpsest-paas-maintenance-window.json
grep -q '"day_of_week":"sunday"' /tmp/palimpsest-paas-maintenance-window.json
grep -q '"auto_minor_upgrades":true' /tmp/palimpsest-paas-maintenance-window.json
get_json "/v1/maintenance-windows?environment_id=env_123&status=active" | tee /tmp/palimpsest-paas-maintenance-windows.json >/dev/null
grep -q '"name":"Weekly maintenance"' /tmp/palimpsest-paas-maintenance-windows.json
get_json "/v1/maintenance-windows/maintenance_window_env_123_weekly" | tee /tmp/palimpsest-paas-maintenance-window-detail.json >/dev/null
grep -q '"duration_minutes":120' /tmp/palimpsest-paas-maintenance-window-detail.json
get_json "/v1/audit-events?organization_id=org_123&action=maintenance_window.upsert" | tee /tmp/palimpsest-paas-audit-maintenance-window.json >/dev/null
grep -q '"resource_id":"maintenance_window_env_123_weekly"' /tmp/palimpsest-paas-audit-maintenance-window.json
get_json "/v1/environments/env_123/overview" | tee /tmp/palimpsest-paas-environment-overview-with-maintenance.json >/dev/null
grep -q '"window_id":"maintenance_window_env_123_weekly"' /tmp/palimpsest-paas-environment-overview-with-maintenance.json
echo "running maintenance scheduler inside customer window"
post_json "/v1/scheduler/maintenance/run-once" '{"target_postgres_version":"18.5","day_of_week":"sunday","current_time":"03:30"}' \
  | tee /tmp/palimpsest-paas-maintenance-scheduler.json
grep -q '"scheduled":' /tmp/palimpsest-paas-maintenance-scheduler.json
grep -q '"window_id":"maintenance_window_env_123_weekly"' /tmp/palimpsest-paas-maintenance-scheduler.json
grep -q '"kind":"update_postgres_minor"' /tmp/palimpsest-paas-maintenance-scheduler.json
grep -q '"target_postgres_version":"18.5"' /tmp/palimpsest-paas-maintenance-scheduler.json
echo "container polling queued maintenance minor update command"
cargo run -p palimpsest-paas-node-agent -- poll-once-container "$API_URL"
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only --no-align \
  --command "SELECT lifecycle_state, postgres_version FROM managed_postgres_clusters WHERE id = 'cluster_123'" | grep -q "ready|18.5"
get_json "/v1/audit-events?organization_id=org_123&action=managed_postgres_cluster.maintenance_update_minor" \
  | tee /tmp/palimpsest-paas-audit-maintenance-update.json >/dev/null
grep -q '"resource_id":"cluster_123"' /tmp/palimpsest-paas-audit-maintenance-update.json
post_json "/v1/scheduler/maintenance/run-once" '{"target_postgres_version":"18.5","day_of_week":"sunday","current_time":"03:30"}' \
  | tee /tmp/palimpsest-paas-maintenance-scheduler-empty.json
grep -q '"scheduled":\[\]' /tmp/palimpsest-paas-maintenance-scheduler-empty.json
echo "requesting managed postgres major upgrade"
post_json "/v1/managed-postgres/clusters/cluster_123/major-upgrades" '{"target_postgres_version":"19","strategy":"logical_replication_copy"}' \
  | tee /tmp/palimpsest-paas-major-upgrade.json >/dev/null
grep -q '"kind":"upgrade_postgres_major"' /tmp/palimpsest-paas-major-upgrade.json
grep -q '"target_postgres_version":"19"' /tmp/palimpsest-paas-major-upgrade.json
echo "container polling queued major upgrade command"
cargo run -p palimpsest-paas-node-agent -- poll-once-container "$API_URL"
test -f "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123/palimpsest-postgres-major-upgrade-plan.json"
grep -q '"preflight_required":true' "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123/palimpsest-postgres-major-upgrade-plan.json"
test -f "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123/palimpsest-postgres-major-upgrade-preflight.json"
grep -q '"status":"succeeded"' "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123/palimpsest-postgres-major-upgrade-preflight.json"
test -f "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123/palimpsest-postgres-major-upgrade.json"
grep -q '"target_postgres_version":"19"' "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123/palimpsest-postgres-major-upgrade.json"
get_json "/v1/managed-postgres/clusters/cluster_123" \
  | tee /tmp/palimpsest-paas-cluster-after-major-upgrade.json >/dev/null
grep -q '"postgres_version":"19"' /tmp/palimpsest-paas-cluster-after-major-upgrade.json
get_json "/v1/managed-postgres/clusters/cluster_123/major-upgrades?status=succeeded" \
  | tee /tmp/palimpsest-paas-major-upgrades-succeeded.json >/dev/null
grep -q '"status":"succeeded"' /tmp/palimpsest-paas-major-upgrades-succeeded.json

echo "starting gateway from SQL-backed route discovery"
PALIMPSEST_GATEWAY_ADDR=127.0.0.1:8099 \
  PALIMPSEST_GATEWAY_ROUTE_REFRESH_SECS=1 \
  PALIMPSEST_GATEWAY_CONTROL_PLANE_URL="$API_URL" \
  cargo run -p palimpsest-paas-gateway --bin palimpsest-paas-gateway >/tmp/palimpsest-paas-gateway.log 2>&1 &
GATEWAY_PID=$!
for _ in $(seq 1 120); do
  if curl --silent --output /dev/null http://127.0.0.1:8099/healthz; then
    break
  fi
  sleep 0.25
done
curl --fail-with-body --silent --show-error http://127.0.0.1:8099/healthz >/dev/null
UNKNOWN_GATEWAY_STATUS="$(curl --silent --show-error --output /tmp/palimpsest-paas-gateway-unknown.json --write-out "%{http_code}" -H "Host: missing.palimpsest.dev" http://127.0.0.1:8099/)"
test "$UNKNOWN_GATEWAY_STATUS" = "404"
ROUTED_GATEWAY_STATUS="$(curl --silent --show-error --output /tmp/palimpsest-paas-gateway-routed.json --write-out "%{http_code}" -H "Host: env-123.palimpsest.dev" http://127.0.0.1:8099/)"
test "$ROUTED_GATEWAY_STATUS" = "502"
post_json "/v1/gateway-routes" '{
  "host": "env-123-refresh.palimpsest.dev",
  "organization_id": "org_123",
  "project_id": "project_123",
  "environment_id": "env_123",
  "sync_endpoint": "http://127.0.0.1:50051",
  "tls_policy": "terminate_at_gateway",
  "rate_limit": {
    "max_connections": 100,
    "max_requests_per_minute": 1000
  }
}' >/dev/null
for _ in $(seq 1 40); do
  REFRESHED_GATEWAY_STATUS="$(curl --silent --show-error --output /tmp/palimpsest-paas-gateway-refreshed.json --write-out "%{http_code}" -H "Host: env-123-refresh.palimpsest.dev" http://127.0.0.1:8099/)"
  if test "$REFRESHED_GATEWAY_STATUS" = "502"; then
    break
  fi
  sleep 0.25
done
test "$REFRESHED_GATEWAY_STATUS" = "502"
delete_json "/v1/gateway-routes/env-123-refresh.palimpsest.dev" | tee /tmp/palimpsest-paas-gateway-route-delete.json >/dev/null
grep -q '"status":"gateway_route.deleted"' /tmp/palimpsest-paas-gateway-route-delete.json
for _ in $(seq 1 40); do
  DELETED_GATEWAY_STATUS="$(curl --silent --show-error --output /tmp/palimpsest-paas-gateway-deleted.json --write-out "%{http_code}" -H "Host: env-123-refresh.palimpsest.dev" http://127.0.0.1:8099/)"
  if test "$DELETED_GATEWAY_STATUS" = "404"; then
    break
  fi
  sleep 0.25
done
test "$DELETED_GATEWAY_STATUS" = "404"
kill "$GATEWAY_PID" 2>/dev/null || true
wait "$GATEWAY_PID" 2>/dev/null || true
GATEWAY_PID=""

echo "starting owned database TCP proxy"
post_json "/v1/environments/env_123/managed-postgres-endpoint/database-proxy-route" '{"listen_addr":"127.0.0.1:55430"}' \
  | tee /tmp/palimpsest-paas-managed-db-proxy-route.json >/dev/null
grep -q '"database_proxy_listen_addr":"127.0.0.1:55430"' /tmp/palimpsest-paas-managed-db-proxy-route.json
grep -q '"listen_addr":"127.0.0.1:55430"' /tmp/palimpsest-paas-managed-db-proxy-route.json
grep -q "\"upstream_addr\":\"127.0.0.1:${PALIMPSEST_PAAS_CLUSTER_PORT}\"" /tmp/palimpsest-paas-managed-db-proxy-route.json
get_json "/v1/database-proxy-routes/127.0.0.1:55430" | tee /tmp/palimpsest-paas-managed-db-proxy-route-detail.json >/dev/null
grep -q '"cluster_id":"cluster_123"' /tmp/palimpsest-paas-managed-db-proxy-route-detail.json
get_json "/v1/managed-postgres/certificate-authority-providers?default_for_managed_postgres=true" \
  | tee /tmp/palimpsest-paas-ca-providers-default.json >/dev/null
grep -q '"ca_provider_id":"ca_local_dev"' /tmp/palimpsest-paas-ca-providers-default.json
post_json "/v1/managed-postgres/certificate-authority-providers" '{"ca_provider_id":"ca_smoke_local","name":"Smoke local CA","kind":"local_dev","issuer_ref":"palimpsest-smoke-local-ca","status":"active","default_for_managed_postgres":true}' \
  | tee /tmp/palimpsest-paas-ca-provider.json >/dev/null
grep -q '"ca_provider_id":"ca_smoke_local"' /tmp/palimpsest-paas-ca-provider.json
grep -q '"default_for_managed_postgres":true' /tmp/palimpsest-paas-ca-provider.json
get_json "/v1/managed-postgres/certificate-authority-providers/ca_smoke_local" \
  | tee /tmp/palimpsest-paas-ca-provider-detail.json >/dev/null
grep -q '"issuer_ref":"palimpsest-smoke-local-ca"' /tmp/palimpsest-paas-ca-provider-detail.json
post_json "/v1/environments/env_123/managed-postgres-endpoint/certificates" '{"common_name":"db.env-123.palimpsest.local","validity_days":30,"ca_provider_id":"ca_smoke_local"}' \
  | tee /tmp/palimpsest-paas-managed-db-certificate.json >/dev/null
grep -q '"status":"active"' /tmp/palimpsest-paas-managed-db-certificate.json
grep -q '"common_name":"db.env-123.palimpsest.local"' /tmp/palimpsest-paas-managed-db-certificate.json
grep -q '"issued_by":"palimpsest-smoke-local-ca"' /tmp/palimpsest-paas-managed-db-certificate.json
grep -q '"certificate_secret_ref"' /tmp/palimpsest-paas-managed-db-certificate.json
grep -q '"private_key_secret_ref"' /tmp/palimpsest-paas-managed-db-certificate.json
MANAGED_DB_CERTIFICATE_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("certificate_id")' /tmp/palimpsest-paas-managed-db-certificate.json)"
get_json "/v1/environments/env_123/managed-postgres-endpoint/certificates?status=active" \
  | tee /tmp/palimpsest-paas-managed-db-certificates.json >/dev/null
grep -q "\"certificate_id\":\"${MANAGED_DB_CERTIFICATE_ID}\"" /tmp/palimpsest-paas-managed-db-certificates.json
get_json "/v1/environments/env_123/managed-postgres-endpoint/certificates/${MANAGED_DB_CERTIFICATE_ID}" \
  | tee /tmp/palimpsest-paas-managed-db-certificate-detail.json >/dev/null
grep -q "\"certificate_id\":\"${MANAGED_DB_CERTIFICATE_ID}\"" /tmp/palimpsest-paas-managed-db-certificate-detail.json
get_json "/v1/environments/env_123/managed-postgres-endpoint/certificates/${MANAGED_DB_CERTIFICATE_ID}/bundle" \
  | tee /tmp/palimpsest-paas-managed-db-certificate-bundle.json >/dev/null
grep -q '"certificate_pem":"-----BEGIN CERTIFICATE' /tmp/palimpsest-paas-managed-db-certificate-bundle.json
grep -q '"private_key_pem":"-----BEGIN' /tmp/palimpsest-paas-managed-db-certificate-bundle.json
GATEWAY_MTLS_ROUTE_REQUEST="$(ruby -rjson -e 'cert = JSON.parse(File.read(ARGV.fetch(0))); cert_ref = cert.fetch("certificate_secret_ref").fetch("secret_id"); key_ref = cert.fetch("private_key_secret_ref").fetch("secret_id"); print JSON.generate({"host"=>"env-123-mtls.palimpsest.dev","organization_id"=>"org_123","project_id"=>"project_123","environment_id"=>"env_123","sync_endpoint"=>"https://127.0.0.1:50052","tls_policy"=>{"mutual_tls_to_sync"=>{"ca_secret_ref"=>cert_ref,"client_certificate_secret_ref"=>cert_ref,"client_private_key_secret_ref"=>key_ref,"server_name"=>"sync.env-123.palimpsest.local"}},"rate_limit"=>{"max_connections"=>100,"max_requests_per_minute"=>1000}})' /tmp/palimpsest-paas-managed-db-certificate.json)"
post_json "/v1/gateway-routes" "$GATEWAY_MTLS_ROUTE_REQUEST" \
  | tee /tmp/palimpsest-paas-gateway-mtls-route-upsert.json >/dev/null
grep -q '"status":"gateway_route.upserted"' /tmp/palimpsest-paas-gateway-mtls-route-upsert.json
get_json "/v1/gateway-routes/env-123-mtls.palimpsest.dev" \
  | tee /tmp/palimpsest-paas-gateway-mtls-route.json >/dev/null
grep -q '"mutual_tls_to_sync"' /tmp/palimpsest-paas-gateway-mtls-route.json
grep -q '"client_private_key_secret_ref"' /tmp/palimpsest-paas-gateway-mtls-route.json
get_json "/v1/gateway-routes/env-123-mtls.palimpsest.dev/mtls-bundle" \
  | tee /tmp/palimpsest-paas-gateway-mtls-bundle.json >/dev/null
grep -q '"ca_pem":"-----BEGIN CERTIFICATE' /tmp/palimpsest-paas-gateway-mtls-bundle.json
grep -q '"client_certificate_pem":"-----BEGIN CERTIFICATE' /tmp/palimpsest-paas-gateway-mtls-bundle.json
grep -q '"client_private_key_pem":"-----BEGIN' /tmp/palimpsest-paas-gateway-mtls-bundle.json
RENEWAL_DENIED_STATUS="$(post_json_status "/v1/environments/env_123/managed-postgres-endpoint/certificates/renew" '{"renewal_window_days":1,"validity_days":30,"ca_provider_id":"ca_smoke_local"}')"
test "$RENEWAL_DENIED_STATUS" = "400"
grep -q 'not within the 1-day renewal window' /tmp/palimpsest-paas-status-response.json
post_json "/v1/environments/env_123/managed-postgres-endpoint/certificates/renew" '{"force":true,"validity_days":30,"ca_provider_id":"ca_smoke_local"}' \
  | tee /tmp/palimpsest-paas-managed-db-certificate-renewed.json >/dev/null
grep -q '"status":"active"' /tmp/palimpsest-paas-managed-db-certificate-renewed.json
grep -q '"common_name":"db.env-123.palimpsest.local"' /tmp/palimpsest-paas-managed-db-certificate-renewed.json
grep -q '"issued_by":"palimpsest-smoke-local-ca"' /tmp/palimpsest-paas-managed-db-certificate-renewed.json
RENEWED_MANAGED_DB_CERTIFICATE_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("certificate_id")' /tmp/palimpsest-paas-managed-db-certificate-renewed.json)"
get_json "/v1/environments/env_123/managed-postgres-endpoint/certificates/${MANAGED_DB_CERTIFICATE_ID}" \
  | tee /tmp/palimpsest-paas-managed-db-certificate-original-revoked.json >/dev/null
grep -q '"status":"revoked"' /tmp/palimpsest-paas-managed-db-certificate-original-revoked.json
get_json "/v1/environments/env_123/managed-postgres-endpoint" | tee /tmp/palimpsest-paas-managed-postgres-endpoint-with-cert.json >/dev/null
grep -q "\"active_certificate_id\":\"${RENEWED_MANAGED_DB_CERTIFICATE_ID}\"" /tmp/palimpsest-paas-managed-postgres-endpoint-with-cert.json
get_json "/v1/database-proxy-routes/127.0.0.1:55430" | tee /tmp/palimpsest-paas-managed-db-proxy-route-with-cert.json >/dev/null
grep -q "\"certificate_id\":\"${RENEWED_MANAGED_DB_CERTIFICATE_ID}\"" /tmp/palimpsest-paas-managed-db-proxy-route-with-cert.json
grep -q '"mode":"terminate_at_proxy"' /tmp/palimpsest-paas-managed-db-proxy-route-with-cert.json
ruby -rsocket -e 'server = TCPServer.new("127.0.0.1", 55431); client = server.accept; request = client.readpartial(4); abort "unexpected request #{request.inspect}" unless request == "ping"; client.write("pong"); client.close; server.close' &
TCP_UPSTREAM_PID=$!
DB_PROXY_ROUTE_JSON="$(cat <<JSON
{
  "listen_addr": "127.0.0.1:55432",
  "upstream_addr": "127.0.0.1:55431",
  "organization_id": "org_123",
  "project_id": "project_123",
  "environment_id": "env_123",
  "cluster_id": "cluster_123"
}
JSON
)"
post_json "/v1/database-proxy-routes" "$DB_PROXY_ROUTE_JSON" >/dev/null
get_json "/v1/database-proxy-routes?environment_id=env_123" | tee /tmp/palimpsest-paas-db-proxy-routes.json >/dev/null
grep -q '"listen_addr":"127.0.0.1:55432"' /tmp/palimpsest-paas-db-proxy-routes.json
get_json "/v1/database-proxy-routes/127.0.0.1:55432" | tee /tmp/palimpsest-paas-db-proxy-route-detail.json >/dev/null
grep -q '"upstream_addr":"127.0.0.1:55431"' /tmp/palimpsest-paas-db-proxy-route-detail.json
PALIMPSEST_DB_PROXY_CONTROL_PLANE_URL="$API_URL" \
PALIMPSEST_DB_PROXY_ROUTE_REFRESH_SECS=1 \
RUST_LOG=info \
  cargo run -p palimpsest-paas-gateway --bin palimpsest-paas-db-proxy >/tmp/palimpsest-paas-db-proxy.log 2>&1 &
DB_PROXY_PID=$!
DB_PROXY_READY=0
for _ in $(seq 1 120); do
  if ruby -rsocket -e 'client = TCPSocket.new("127.0.0.1", 55432); client.write("ping"); response = client.read(4); client.close; abort "unexpected response #{response.inspect}" unless response == "pong"' 2>/dev/null; then
    DB_PROXY_READY=1
    break
  fi
  sleep 0.25
done
if [[ "$DB_PROXY_READY" != "1" ]]; then
  cat /tmp/palimpsest-paas-db-proxy.log >&2 || true
  echo "database proxy raw route did not become ready" >&2
  exit 1
fi
DB_PROXY_TLS_READY=0
DB_PROXY_TLS_ERROR="/tmp/palimpsest-paas-db-proxy-tls-error.log"
for _ in $(seq 1 120); do
  if ruby -rsocket -ropenssl -rtimeout -e 'Timeout.timeout(5) do; tcp = TCPSocket.new("127.0.0.1", 55430); tcp.write([8, 80877103].pack("NN")); response = tcp.read(1); abort "unexpected TLS negotiation response #{response.inspect}" unless response == "S"; context = OpenSSL::SSL::SSLContext.new; context.verify_mode = OpenSSL::SSL::VERIFY_NONE; ssl = OpenSSL::SSL::SSLSocket.new(tcp, context); ssl.hostname = "db.env-123.palimpsest.local"; ssl.connect; params = "user\0cluster_123_app\0database\0postgres\0application_name\0palimpsest-smoke\0\0"; startup = [8 + params.bytesize, 196608].pack("NN") + params; ssl.write(startup); upstream_response = ssl.read(1); ssl.close; abort "unexpected upstream response #{upstream_response.inspect}" unless upstream_response == "R" || upstream_response == "E"; end' 2>"$DB_PROXY_TLS_ERROR"; then
    DB_PROXY_TLS_READY=1
    break
  fi
  sleep 0.25
done
if [[ "$DB_PROXY_TLS_READY" != "1" ]]; then
  cat /tmp/palimpsest-paas-db-proxy.log >&2 || true
  cat "$DB_PROXY_TLS_ERROR" >&2 || true
  echo "database proxy TLS route did not become ready" >&2
  exit 1
fi
post_json "/v1/managed-postgres/certificate-authority-providers" '{"ca_provider_id":"ca_smoke_external","name":"Smoke external PKI","kind":"external_pki","issuer_ref":"external-pki/smoke","status":"active","default_for_managed_postgres":false}' \
  | tee /tmp/palimpsest-paas-ca-provider-external.json >/dev/null
grep -q '"kind":"external_pki"' /tmp/palimpsest-paas-ca-provider-external.json
EXTERNAL_CERTIFICATE_REQUEST="$(ruby -rjson -e 'bundle = JSON.parse(File.read(ARGV.fetch(0))); print JSON.generate({"common_name"=>"db.env-123.palimpsest.local","validity_days"=>30,"ca_provider_id"=>"ca_smoke_external","certificate_pem"=>bundle.fetch("certificate_pem"),"private_key_pem"=>bundle.fetch("private_key_pem")})' /tmp/palimpsest-paas-managed-db-certificate-bundle.json)"
post_json "/v1/environments/env_123/managed-postgres-endpoint/certificates" "$EXTERNAL_CERTIFICATE_REQUEST" \
  | tee /tmp/palimpsest-paas-managed-db-certificate-external.json >/dev/null
grep -q '"status":"active"' /tmp/palimpsest-paas-managed-db-certificate-external.json
grep -q '"issued_by":"external-pki/smoke"' /tmp/palimpsest-paas-managed-db-certificate-external.json
EXTERNAL_MANAGED_DB_CERTIFICATE_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("certificate_id")' /tmp/palimpsest-paas-managed-db-certificate-external.json)"
get_json "/v1/environments/env_123/managed-postgres-endpoint" | tee /tmp/palimpsest-paas-managed-postgres-endpoint-with-external-cert.json >/dev/null
grep -q "\"active_certificate_id\":\"${EXTERNAL_MANAGED_DB_CERTIFICATE_ID}\"" /tmp/palimpsest-paas-managed-postgres-endpoint-with-external-cert.json
get_json "/v1/environments/env_123/managed-postgres-endpoint/certificates/${MANAGED_DB_CERTIFICATE_ID}" \
  | tee /tmp/palimpsest-paas-managed-db-certificate-local-revoked.json >/dev/null
grep -q '"status":"revoked"' /tmp/palimpsest-paas-managed-db-certificate-local-revoked.json
get_json "/v1/environments/env_123/managed-postgres-endpoint/certificates/${RENEWED_MANAGED_DB_CERTIFICATE_ID}" \
  | tee /tmp/palimpsest-paas-managed-db-certificate-renewed-revoked.json >/dev/null
grep -q '"status":"revoked"' /tmp/palimpsest-paas-managed-db-certificate-renewed-revoked.json
post_json "/v1/managed-postgres/certificate-authority-providers" '{"ca_provider_id":"ca_smoke_acme","name":"Smoke ACME","kind":"acme","issuer_ref":"https://acme-smoke.palimpsest.local/directory","status":"active","default_for_managed_postgres":false}' \
  | tee /tmp/palimpsest-paas-ca-provider-acme.json >/dev/null
grep -q '"kind":"acme"' /tmp/palimpsest-paas-ca-provider-acme.json
post_json "/v1/environments/env_123/managed-postgres-endpoint/certificates" '{"common_name":"db.env-123.palimpsest.local","validity_days":30,"ca_provider_id":"ca_smoke_acme"}' \
  | tee /tmp/palimpsest-paas-managed-db-certificate-acme-provisioning.json >/dev/null
grep -q '"status":"provisioning"' /tmp/palimpsest-paas-managed-db-certificate-acme-provisioning.json
grep -q '"issued_by":"https://acme-smoke.palimpsest.local/directory"' /tmp/palimpsest-paas-managed-db-certificate-acme-provisioning.json
grep -q '"private_key_secret_ref"' /tmp/palimpsest-paas-managed-db-certificate-acme-provisioning.json
if grep -q '"certificate_secret_ref"' /tmp/palimpsest-paas-managed-db-certificate-acme-provisioning.json; then
  echo "provisioning ACME certificate unexpectedly exposed certificate_secret_ref" >&2
  exit 1
fi
ACME_MANAGED_DB_CERTIFICATE_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("certificate_id")' /tmp/palimpsest-paas-managed-db-certificate-acme-provisioning.json)"
get_json "/v1/environments/env_123/managed-postgres-endpoint/certificates/${ACME_MANAGED_DB_CERTIFICATE_ID}/acme-order" \
  | tee /tmp/palimpsest-paas-managed-db-certificate-acme-order.json >/dev/null
grep -q '"status":"pending_challenge"' /tmp/palimpsest-paas-managed-db-certificate-acme-order.json
grep -q '"challenge_type":"http_01"' /tmp/palimpsest-paas-managed-db-certificate-acme-order.json
grep -q '"key_authorization_secret_ref"' /tmp/palimpsest-paas-managed-db-certificate-acme-order.json
ACME_CHALLENGE_TOKEN="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("challenge_token")' /tmp/palimpsest-paas-managed-db-certificate-acme-order.json)"
ACME_KEY_AUTHORIZATION_SECRET_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("key_authorization_secret_ref").fetch("secret_id")' /tmp/palimpsest-paas-managed-db-certificate-acme-order.json)"
ACME_EXPECTED_KEY_AUTHORIZATION="$(docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only --no-align \
  --command "SELECT secret_material FROM secret_refs WHERE id = '${ACME_KEY_AUTHORIZATION_SECRET_ID}'")"
ACME_SERVED_KEY_AUTHORIZATION="$(get_text "/.well-known/acme-challenge/${ACME_CHALLENGE_TOKEN}")"
test "$ACME_SERVED_KEY_AUTHORIZATION" = "$ACME_EXPECTED_KEY_AUTHORIZATION"
post_json "/v1/scheduler/acme-orders/run-once" '{"limit":10}' \
  | tee /tmp/palimpsest-paas-managed-db-certificate-acme-scheduler.json >/dev/null
grep -q '"status":"ready_to_finalize"' /tmp/palimpsest-paas-managed-db-certificate-acme-scheduler.json
post_json "/v1/environments/env_123/managed-postgres-endpoint/certificates/${ACME_MANAGED_DB_CERTIFICATE_ID}/acme-challenge/validate" '{}' \
  | tee /tmp/palimpsest-paas-managed-db-certificate-acme-ready.json >/dev/null
grep -q '"status":"ready_to_finalize"' /tmp/palimpsest-paas-managed-db-certificate-acme-ready.json
ACME_CSR_SECRET_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("csr_secret_ref").fetch("secret_id")' /tmp/palimpsest-paas-managed-db-certificate-acme-order.json)"
ACME_PRIVATE_KEY_SECRET_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("private_key_secret_ref").fetch("secret_id")' /tmp/palimpsest-paas-managed-db-certificate-acme-provisioning.json)"
ACME_CSR_FILE="$(mktemp)"
ACME_PRIVATE_KEY_FILE="$(mktemp)"
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only --no-align \
  --command "SELECT secret_material FROM secret_refs WHERE id = '${ACME_CSR_SECRET_ID}'" > "$ACME_CSR_FILE"
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only --no-align \
  --command "SELECT secret_material FROM secret_refs WHERE id = '${ACME_PRIVATE_KEY_SECRET_ID}'" > "$ACME_PRIVATE_KEY_FILE"
ACME_FINALIZE_REQUEST="$(ruby -rjson -ropenssl -e 'csr = OpenSSL::X509::Request.new(File.read(ARGV.fetch(0))); key = OpenSSL::PKey.read(File.read(ARGV.fetch(1))); cert = OpenSSL::X509::Certificate.new; cert.version = 2; cert.serial = 1; cert.subject = csr.subject; cert.issuer = csr.subject; cert.public_key = csr.public_key; cert.not_before = Time.now; cert.not_after = Time.now + 30 * 24 * 60 * 60; factory = OpenSSL::X509::ExtensionFactory.new; factory.subject_certificate = cert; factory.issuer_certificate = cert; cert.add_extension(factory.create_extension("basicConstraints", "CA:FALSE", true)); cert.add_extension(factory.create_extension("keyUsage", "digitalSignature,keyEncipherment", true)); cert.add_extension(factory.create_extension("extendedKeyUsage", "serverAuth", false)); cert.add_extension(factory.create_extension("subjectAltName", "DNS:db.env-123.palimpsest.local", false)); cert.sign(key, OpenSSL::Digest::SHA256.new); print JSON.generate({"certificate_pem"=>cert.to_pem})' "$ACME_CSR_FILE" "$ACME_PRIVATE_KEY_FILE")"
post_json "/v1/environments/env_123/managed-postgres-endpoint/certificates/${ACME_MANAGED_DB_CERTIFICATE_ID}/acme-finalize" "$ACME_FINALIZE_REQUEST" \
  | tee /tmp/palimpsest-paas-managed-db-certificate-acme-active.json >/dev/null
grep -q '"status":"active"' /tmp/palimpsest-paas-managed-db-certificate-acme-active.json
grep -q '"certificate_secret_ref"' /tmp/palimpsest-paas-managed-db-certificate-acme-active.json
get_json "/v1/environments/env_123/managed-postgres-endpoint" | tee /tmp/palimpsest-paas-managed-postgres-endpoint-with-acme-cert.json >/dev/null
grep -q "\"active_certificate_id\":\"${ACME_MANAGED_DB_CERTIFICATE_ID}\"" /tmp/palimpsest-paas-managed-postgres-endpoint-with-acme-cert.json
get_json "/v1/environments/env_123/managed-postgres-endpoint/certificates/${EXTERNAL_MANAGED_DB_CERTIFICATE_ID}" \
  | tee /tmp/palimpsest-paas-managed-db-certificate-external-revoked.json >/dev/null
grep -q '"status":"revoked"' /tmp/palimpsest-paas-managed-db-certificate-external-revoked.json
wait "$TCP_UPSTREAM_PID"
TCP_UPSTREAM_PID=""

echo "refreshing owned database TCP proxy route without restart"
ruby -rsocket -e 'server = TCPServer.new("127.0.0.1", 55433); client = server.accept; request = client.readpartial(4); abort "unexpected request #{request.inspect}" unless request == "ping"; client.write("pung"); client.close; server.close' &
TCP_UPSTREAM_PID=$!
DB_PROXY_REFRESHED_ROUTE_JSON="$(cat <<JSON
{
  "listen_addr": "127.0.0.1:55432",
  "upstream_addr": "127.0.0.1:55433",
  "organization_id": "org_123",
  "project_id": "project_123",
  "environment_id": "env_123",
  "cluster_id": "cluster_123"
}
JSON
)"
post_json "/v1/database-proxy-routes" "$DB_PROXY_REFRESHED_ROUTE_JSON" >/dev/null
DB_PROXY_REFRESHED=0
for _ in $(seq 1 50); do
  if ruby -rsocket -e 'client = TCPSocket.new("127.0.0.1", 55432); client.write("ping"); response = client.read(4); client.close; abort "unexpected response #{response.inspect}" unless response == "pung"' 2>/dev/null; then
    DB_PROXY_REFRESHED=1
    break
  fi
  sleep 0.1
done
test "$DB_PROXY_REFRESHED" = "1"
wait "$TCP_UPSTREAM_PID"
TCP_UPSTREAM_PID=""
delete_json "/v1/database-proxy-routes/127.0.0.1:55432" | tee /tmp/palimpsest-paas-db-proxy-route-delete.json >/dev/null
grep -q '"status":"database_proxy_route.deleted"' /tmp/palimpsest-paas-db-proxy-route-delete.json
DB_PROXY_DELETED=0
for _ in $(seq 1 50); do
  if ruby -rsocket -rtimeout -e 'begin; Timeout.timeout(1) { client = TCPSocket.new("127.0.0.1", 55432); client.close }; exit 1; rescue; exit 0; end' 2>/dev/null; then
    DB_PROXY_DELETED=1
    break
  fi
  sleep 0.1
done
test "$DB_PROXY_DELETED" = "1"

echo "requesting managed postgres WAL archive"
WAL_SEGMENT_PATH="$(find "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123/pg_wal" -maxdepth 1 -type f -name '000000*' -print -quit)"
WAL_SEGMENT="$(basename "$WAL_SEGMENT_PATH")"
post_json "/v1/managed-postgres/clusters/cluster_123/wal-archives" "{\"segment_name\":\"$WAL_SEGMENT\"}" | tee /tmp/palimpsest-paas-wal-archive-request.json
grep -q '"status":"running"' /tmp/palimpsest-paas-wal-archive-request.json
grep -q '"kind":"archive_wal_segment"' /tmp/palimpsest-paas-wal-archive-request.json
get_json "/v1/managed-postgres/clusters/cluster_123/wal-archives?status=running" | tee /tmp/palimpsest-paas-wal-archives-running.json >/dev/null
grep -q "\"segment_name\":\"${WAL_SEGMENT}\"" /tmp/palimpsest-paas-wal-archives-running.json
get_json "/v1/managed-postgres/clusters/cluster_123/wal-archives/${WAL_SEGMENT}" | tee /tmp/palimpsest-paas-wal-archive-detail-running.json >/dev/null
grep -q '"status":"running"' /tmp/palimpsest-paas-wal-archive-detail-running.json

echo "container polling queued WAL archive command"
cargo run -p palimpsest-paas-node-agent -- poll-once-container "$API_URL" | tee /tmp/palimpsest-paas-wal-archive-command.json
grep -q '"status": "succeeded"' /tmp/palimpsest-paas-wal-archive-command.json
test -f "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/wal/cluster_123/$WAL_SEGMENT"
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT status FROM managed_postgres_wal_archives WHERE cluster_id = 'cluster_123' AND segment_name = '$WAL_SEGMENT'" | grep -q succeeded
get_json "/v1/managed-postgres/clusters/cluster_123/wal-archives?status=succeeded" | tee /tmp/palimpsest-paas-wal-archives-succeeded.json >/dev/null
grep -q "\"segment_name\":\"${WAL_SEGMENT}\"" /tmp/palimpsest-paas-wal-archives-succeeded.json
get_json "/v1/managed-postgres/clusters/cluster_123/wal-archives/${WAL_SEGMENT}" | tee /tmp/palimpsest-paas-wal-archive-detail-succeeded.json >/dev/null
grep -q '"status":"succeeded"' /tmp/palimpsest-paas-wal-archive-detail-succeeded.json

echo "requesting managed postgres base backup"
post_json "/v1/managed-postgres/clusters/cluster_123/backups" '{}' | tee /tmp/palimpsest-paas-backup-request.json
grep -q '"status":"running"' /tmp/palimpsest-paas-backup-request.json
grep -q '"kind":"run_base_backup"' /tmp/palimpsest-paas-backup-request.json
REQUESTED_BACKUP_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("backup").fetch("backup_id")' /tmp/palimpsest-paas-backup-request.json)"
get_json "/v1/managed-postgres/clusters/cluster_123/backups?status=running" | tee /tmp/palimpsest-paas-backups-running.json >/dev/null
grep -q "\"backup_id\":\"${REQUESTED_BACKUP_ID}\"" /tmp/palimpsest-paas-backups-running.json
get_json "/v1/managed-postgres/clusters/cluster_123/backups/${REQUESTED_BACKUP_ID}" | tee /tmp/palimpsest-paas-backup-detail-running.json >/dev/null
grep -q '"status":"running"' /tmp/palimpsest-paas-backup-detail-running.json

echo "container polling queued base backup command"
cargo run -p palimpsest-paas-node-agent -- poll-once-container "$API_URL" | tee /tmp/palimpsest-paas-backup-command.json
grep -q '"status": "succeeded"' /tmp/palimpsest-paas-backup-command.json
find "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/backups/cluster_123" -name PG_VERSION -print -quit | grep -q PG_VERSION
BACKUP_MANIFEST="$(find "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/backups/cluster_123" -name manifest.json -print -quit)"
ruby -rjson -e 'manifest = JSON.parse(File.read(ARGV.fetch(0))); abort "wrong backup id" unless manifest.fetch("backup_id").start_with?("backup_"); abort "wrong cluster" unless manifest.fetch("cluster_id") == "cluster_123"; abort "wrong version" unless manifest.fetch("manifest_version") == 1' "$BACKUP_MANIFEST"
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT count(*) FROM managed_postgres_backups WHERE cluster_id = 'cluster_123' AND status = 'succeeded'" | grep -q 2
get_json "/v1/managed-postgres/clusters/cluster_123/backups?status=succeeded" | tee /tmp/palimpsest-paas-backups-succeeded.json >/dev/null
grep -q "\"backup_id\":\"${REQUESTED_BACKUP_ID}\"" /tmp/palimpsest-paas-backups-succeeded.json
get_json "/v1/managed-postgres/clusters/cluster_123/backups/${REQUESTED_BACKUP_ID}" | tee /tmp/palimpsest-paas-backup-detail-succeeded.json >/dev/null
grep -q '"status":"succeeded"' /tmp/palimpsest-paas-backup-detail-succeeded.json
echo "verifying automatic managed postgres backup artifact catalog entry"
BACKUP_ARTIFACT_ID="backup_artifact_${REQUESTED_BACKUP_ID}_local_fs"
OBJECT_BACKUP_ARTIFACT_ID="backup_artifact_${REQUESTED_BACKUP_ID}_s3_compatible_fs"
get_json "/v1/managed-postgres/clusters/cluster_123/backups/${REQUESTED_BACKUP_ID}/artifacts?status=available" \
  | tee /tmp/palimpsest-paas-backup-artifacts.json >/dev/null
grep -q "\"artifact_id\":\"${BACKUP_ARTIFACT_ID}\"" /tmp/palimpsest-paas-backup-artifacts.json
grep -q "\"artifact_id\":\"${OBJECT_BACKUP_ARTIFACT_ID}\"" /tmp/palimpsest-paas-backup-artifacts.json
get_json "/v1/managed-postgres/clusters/cluster_123/backups/${REQUESTED_BACKUP_ID}/artifacts/${BACKUP_ARTIFACT_ID}" \
  | tee /tmp/palimpsest-paas-backup-artifact-detail.json >/dev/null
grep -q "\"backup_id\":\"${REQUESTED_BACKUP_ID}\"" /tmp/palimpsest-paas-backup-artifact-detail.json
grep -q '"provider":"local_fs"' /tmp/palimpsest-paas-backup-artifact-detail.json
ruby -rjson -e 'artifact = JSON.parse(File.read(ARGV.fetch(0))); hash = artifact.fetch("manifest_sha256"); abort "missing manifest hash" unless hash.is_a?(String) && hash.match?(/\Asha256:[0-9a-f]{64}\z/); size = artifact.fetch("size_bytes"); abort "missing artifact size" unless size.is_a?(Integer) && size.positive?' /tmp/palimpsest-paas-backup-artifact-detail.json
get_json "/v1/managed-postgres/clusters/cluster_123/backups/${REQUESTED_BACKUP_ID}/artifacts/${OBJECT_BACKUP_ARTIFACT_ID}" \
  | tee /tmp/palimpsest-paas-backup-artifact-object-detail.json >/dev/null
grep -q "\"backup_id\":\"${REQUESTED_BACKUP_ID}\"" /tmp/palimpsest-paas-backup-artifact-object-detail.json
grep -q '"provider":"s3_compatible_fs"' /tmp/palimpsest-paas-backup-artifact-object-detail.json
grep -q '"object_uri":"s3://palimpsest-managed-postgres-backups/managed-postgres/cluster_123/base-backups/' /tmp/palimpsest-paas-backup-artifact-object-detail.json
ruby -rjson -e 'artifact = JSON.parse(File.read(ARGV.fetch(0))); hash = artifact.fetch("manifest_sha256"); abort "missing object manifest hash" unless hash.is_a?(String) && hash.match?(/\Asha256:[0-9a-f]{64}\z/); size = artifact.fetch("size_bytes"); abort "missing object artifact size" unless size.is_a?(Integer) && size.positive?' /tmp/palimpsest-paas-backup-artifact-object-detail.json
test -f "${PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_DIR}/palimpsest-managed-postgres-backups/managed-postgres/cluster_123/base-backups/${REQUESTED_BACKUP_ID}/manifest.json"
get_json "/v1/audit-events?action=managed_postgres_backup_artifact.auto_record" \
  | tee /tmp/palimpsest-paas-audit-backup-artifact.json >/dev/null
grep -q "\"resource_id\":\"${BACKUP_ARTIFACT_ID}\"" /tmp/palimpsest-paas-audit-backup-artifact.json
grep -q "\"resource_id\":\"${OBJECT_BACKUP_ARTIFACT_ID}\"" /tmp/palimpsest-paas-audit-backup-artifact.json

echo "running PITR continuity scheduler once"
post_json "/v1/scheduler/pitr-checks/run-once" '{"max_age_hours":1}' | tee /tmp/palimpsest-paas-pitr-check-scheduler.json
grep -q '"checked":' /tmp/palimpsest-paas-pitr-check-scheduler.json
grep -q '"status":"succeeded"' /tmp/palimpsest-paas-pitr-check-scheduler.json
PITR_CHECK_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("checked").fetch(0).fetch("check").fetch("check_id")' /tmp/palimpsest-paas-pitr-check-scheduler.json)"
get_json "/v1/managed-postgres/clusters/cluster_123/pitr-checks?status=succeeded" | tee /tmp/palimpsest-paas-pitr-checks-succeeded.json >/dev/null
grep -q "\"check_id\":\"${PITR_CHECK_ID}\"" /tmp/palimpsest-paas-pitr-checks-succeeded.json
get_json "/v1/managed-postgres/clusters/cluster_123/pitr-checks/${PITR_CHECK_ID}" | tee /tmp/palimpsest-paas-pitr-check-detail-succeeded.json >/dev/null
grep -q '"segment_count":1' /tmp/palimpsest-paas-pitr-check-detail-succeeded.json
post_json "/v1/scheduler/pitr-checks/run-once" '{"max_age_hours":1}' | tee /tmp/palimpsest-paas-pitr-check-scheduler-empty.json
grep -q '"checked":\[\]' /tmp/palimpsest-paas-pitr-check-scheduler-empty.json

echo "checking control-plane metrics"
get_text "/metrics" | tee /tmp/palimpsest-paas-control-plane-metrics.txt
grep -q "palimpsest_paas_agent_commands_failed_total 0" /tmp/palimpsest-paas-control-plane-metrics.txt
grep -q "palimpsest_paas_quota_alerts_firing_total 1" /tmp/palimpsest-paas-control-plane-metrics.txt
grep -q "palimpsest_paas_quota_usage_ratio" /tmp/palimpsest-paas-control-plane-metrics.txt
grep -q "palimpsest_managed_postgres_last_successful_backup_timestamp_seconds" /tmp/palimpsest-paas-control-plane-metrics.txt
grep -q 'palimpsest_managed_postgres_cluster_lifecycle_state{cluster_id="cluster_123",environment_id="env_123",state="ready"} 1' /tmp/palimpsest-paas-control-plane-metrics.txt
grep -q 'palimpsest_managed_postgres_storage_allocated_gib{cluster_id="cluster_123",environment_id="env_123",host_id="local-dev-host"} 32' /tmp/palimpsest-paas-control-plane-metrics.txt
grep -q 'palimpsest_managed_postgres_last_successful_wal_archive_timestamp_seconds{cluster_id="cluster_123"}' /tmp/palimpsest-paas-control-plane-metrics.txt
grep -q 'palimpsest_managed_postgres_last_successful_pitr_check_timestamp_seconds{cluster_id="cluster_123"}' /tmp/palimpsest-paas-control-plane-metrics.txt
grep -q "palimpsest_managed_postgres_wal_archives_failed_total 0" /tmp/palimpsest-paas-control-plane-metrics.txt

echo "checking customer-facing environment health"
get_json "/v1/environments/env_123/health" | tee /tmp/palimpsest-paas-environment-health.json
grep -q '"environment_id":"env_123"' /tmp/palimpsest-paas-environment-health.json
grep -q '"environment_id":"env_123","state":"degraded"' /tmp/palimpsest-paas-environment-health.json
grep -q '"name":"database","state":"healthy"' /tmp/palimpsest-paas-environment-health.json
grep -q '"name":"backup","state":"healthy"' /tmp/palimpsest-paas-environment-health.json
grep -q '"name":"restore_drill","state":"degraded"' /tmp/palimpsest-paas-environment-health.json
grep -q '"name":"wal_archive","state":"healthy"' /tmp/palimpsest-paas-environment-health.json
grep -q '"name":"pitr","state":"healthy"' /tmp/palimpsest-paas-environment-health.json
grep -q '"name":"sync","state":"degraded"' /tmp/palimpsest-paas-environment-health.json
if grep -q "cluster_too_large" /tmp/palimpsest-paas-environment-health.json; then
  echo "capacity guard cluster leaked into primary environment health" >&2
  exit 1
fi

echo "requesting managed postgres standby preparation"
post_json "/v1/managed-postgres/clusters/cluster_123/standbys" '{"target_cluster_id":"cluster_123_standby"}' | tee /tmp/palimpsest-paas-standby-request.json
grep -q '"status":"running"' /tmp/palimpsest-paas-standby-request.json
grep -q '"target_cluster_id":"cluster_123_standby"' /tmp/palimpsest-paas-standby-request.json
grep -q '"kind":"prepare_postgres_standby"' /tmp/palimpsest-paas-standby-request.json
grep -q '"kind":"prepare_standby"' /tmp/palimpsest-paas-standby-request.json
STANDBY_PORT="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("command").fetch("action").fetch("target_port")' /tmp/palimpsest-paas-standby-request.json)"
grep -q '"primary_slot_name":"cluster_123_standby_slot"' /tmp/palimpsest-paas-standby-request.json
grep -q "user='cluster_123_replication'" /tmp/palimpsest-paas-standby-request.json
grep -q "password='plmp_cluster_123_replication_" /tmp/palimpsest-paas-standby-request.json
if grep -q "user=postgres" /tmp/palimpsest-paas-standby-request.json; then
  echo "standby primary_conninfo unexpectedly uses postgres superuser" >&2
  exit 1
fi
STANDBY_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("standby").fetch("standby_id")' /tmp/palimpsest-paas-standby-request.json)"
get_json "/v1/managed-postgres/clusters/cluster_123/standbys?status=running" | tee /tmp/palimpsest-paas-standbys-running.json >/dev/null
grep -q "\"standby_id\":\"${STANDBY_ID}\"" /tmp/palimpsest-paas-standbys-running.json
get_json "/v1/managed-postgres/clusters/cluster_123/standbys/${STANDBY_ID}" | tee /tmp/palimpsest-paas-standby-detail-running.json >/dev/null
grep -q '"status":"running"' /tmp/palimpsest-paas-standby-detail-running.json

echo "container polling queued standby preparation command"
cargo run -p palimpsest-paas-node-agent -- poll-once-container "$API_URL" | tee /tmp/palimpsest-paas-standby-command.json
grep -q '"status": "succeeded"' /tmp/palimpsest-paas-standby-command.json
test -f "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123_standby/PG_VERSION"
test -f "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123_standby/standby.signal"
grep -q "port = ${STANDBY_PORT}" "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123_standby/postgresql.conf"
grep -q "primary_conninfo" "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123_standby/postgresql.auto.conf"
grep -q "user=''cluster_123_replication''" "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123_standby/postgresql.auto.conf"
grep -q "primary_slot_name = 'cluster_123_standby_slot'" "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123_standby/postgresql.auto.conf"
docker exec palimpsest-pg-cluster_123_standby pg_isready --host 127.0.0.1 --port "$STANDBY_PORT" --username postgres
docker exec palimpsest-pg-cluster_123 psql --host 127.0.0.1 --port "$PALIMPSEST_PAAS_CLUSTER_PORT" --username postgres --dbname postgres --tuples-only --command "SELECT slot_type FROM pg_replication_slots WHERE slot_name = 'cluster_123_standby_slot'" | grep -q physical
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT status FROM managed_postgres_standbys WHERE id = '${STANDBY_ID}'" | grep -q succeeded
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT lifecycle_state FROM managed_postgres_clusters WHERE id = 'cluster_123_standby'" | grep -q ready
get_json "/v1/managed-postgres/clusters/cluster_123/standbys?status=succeeded" | tee /tmp/palimpsest-paas-standbys-succeeded.json >/dev/null
grep -q "\"standby_id\":\"${STANDBY_ID}\"" /tmp/palimpsest-paas-standbys-succeeded.json

echo "requesting managed postgres standby lag check"
post_json "/v1/managed-postgres/clusters/cluster_123/standbys/${STANDBY_ID}/checks" '{"max_lag_bytes":33554432}' | tee /tmp/palimpsest-paas-standby-check-request.json
grep -q '"status":"running"' /tmp/palimpsest-paas-standby-check-request.json
grep -q '"kind":"check_postgres_standby_lag"' /tmp/palimpsest-paas-standby-check-request.json
grep -q '"kind":"check_standby"' /tmp/palimpsest-paas-standby-check-request.json
grep -q '"slot_name":"cluster_123_standby_slot"' /tmp/palimpsest-paas-standby-check-request.json
STANDBY_CHECK_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("check").fetch("check_id")' /tmp/palimpsest-paas-standby-check-request.json)"
get_json "/v1/managed-postgres/clusters/cluster_123/standbys/${STANDBY_ID}/checks?status=running" | tee /tmp/palimpsest-paas-standby-checks-running.json >/dev/null
grep -q "\"check_id\":\"${STANDBY_CHECK_ID}\"" /tmp/palimpsest-paas-standby-checks-running.json
get_json "/v1/managed-postgres/clusters/cluster_123/standbys/${STANDBY_ID}/checks/${STANDBY_CHECK_ID}" | tee /tmp/palimpsest-paas-standby-check-detail-running.json >/dev/null
grep -q '"status":"running"' /tmp/palimpsest-paas-standby-check-detail-running.json

echo "container polling queued standby lag check command"
cargo run -p palimpsest-paas-node-agent -- poll-once-container "$API_URL" | tee /tmp/palimpsest-paas-standby-check-command.json
grep -q '"status": "succeeded"' /tmp/palimpsest-paas-standby-check-command.json
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT status FROM managed_postgres_standby_checks WHERE id = '${STANDBY_CHECK_ID}'" | grep -q succeeded
get_json "/v1/managed-postgres/clusters/cluster_123/standbys/${STANDBY_ID}/checks?status=succeeded" | tee /tmp/palimpsest-paas-standby-checks-succeeded.json >/dev/null
grep -q "\"check_id\":\"${STANDBY_CHECK_ID}\"" /tmp/palimpsest-paas-standby-checks-succeeded.json
get_json "/v1/environments/env_123/health" | tee /tmp/palimpsest-paas-environment-health-after-standby-check.json >/dev/null
grep -q '"name":"standby","state":"healthy"' /tmp/palimpsest-paas-environment-health-after-standby-check.json

echo "requesting managed postgres restore clone"
RESTORE_WITHOUT_REDACTION_STATUS="$(post_json_status "/v1/managed-postgres/clusters/cluster_123/restores" '{"target_cluster_id":"cluster_123_restore_blocked","target_environment_id":"env_dev"}')"
if [[ "$RESTORE_WITHOUT_REDACTION_STATUS" != "400" ]]; then
  cat /tmp/palimpsest-paas-status-response.json >&2 || true
  echo "expected cross-environment restore without redaction policy to return HTTP 400, got $RESTORE_WITHOUT_REDACTION_STATUS" >&2
  exit 1
fi
post_json "/v1/managed-postgres/clone-redaction-policies" '{"policy_id":"redact_prod_to_dev","organization_id":"org_123","project_id":"project_123","environment_id":"env_123","name":"Production to development redaction","status":"active","rules":[{"table_schema":"public","table_name":"customers","column_name":"email","method":"hash_sha256"},{"table_schema":"public","table_name":"customers","column_name":"phone","method":"null"}],"created_at":"","updated_at":""}' \
  | tee /tmp/palimpsest-paas-clone-redaction-policy.json >/dev/null
grep -q '"policy_id":"redact_prod_to_dev"' /tmp/palimpsest-paas-clone-redaction-policy.json
get_json "/v1/managed-postgres/clone-redaction-policies?organization_id=org_123&project_id=project_123&environment_id=env_123&status=active" \
  | tee /tmp/palimpsest-paas-clone-redaction-policies.json >/dev/null
grep -q '"policy_id":"redact_prod_to_dev"' /tmp/palimpsest-paas-clone-redaction-policies.json
post_json "/v1/managed-postgres/clusters/cluster_123/restores" '{"target_cluster_id":"cluster_123_restore_dev","target_environment_id":"env_dev","redaction_policy_id":"redact_prod_to_dev"}' | tee /tmp/palimpsest-paas-redacted-restore-request.json
grep -q '"status":"running"' /tmp/palimpsest-paas-redacted-restore-request.json
grep -q '"target_cluster_id":"cluster_123_restore_dev"' /tmp/palimpsest-paas-redacted-restore-request.json
grep -q '"target_environment_id":"env_dev"' /tmp/palimpsest-paas-redacted-restore-request.json
grep -q '"redaction_policy_id":"redact_prod_to_dev"' /tmp/palimpsest-paas-redacted-restore-request.json
grep -q '"kind":"prepare_restore"' /tmp/palimpsest-paas-redacted-restore-request.json
REDACTED_RESTORE_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("restore").fetch("restore_id")' /tmp/palimpsest-paas-redacted-restore-request.json)"
echo "container polling queued redacted restore command"
cargo run -p palimpsest-paas-node-agent -- poll-once-container "$API_URL" | tee /tmp/palimpsest-paas-redacted-restore-command.json
grep -q '"status": "succeeded"' /tmp/palimpsest-paas-redacted-restore-command.json
test -f "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123_restore_dev/PG_VERSION"
test -f "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123_restore_dev/palimpsest-clone-redaction-policy.json"
grep -q '"policy_id": "redact_prod_to_dev"' "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123_restore_dev/palimpsest-clone-redaction-policy.json"
test -f "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123_restore_dev/palimpsest-clone-redaction.sql"
grep -q 'UPDATE "public"."customers" SET "email" = encode(sha256' "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123_restore_dev/palimpsest-clone-redaction.sql"
grep -q 'UPDATE "public"."customers" SET "phone" = NULL' "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123_restore_dev/palimpsest-clone-redaction.sql"
test -f "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123_restore_dev/palimpsest-clone-redaction-applied.json"
grep -q '"policy_id": "redact_prod_to_dev"' "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123_restore_dev/palimpsest-clone-redaction-applied.json"
get_json "/v1/managed-postgres/clusters/cluster_123/restores/${REDACTED_RESTORE_ID}" | tee /tmp/palimpsest-paas-redacted-restore-detail-succeeded.json >/dev/null
grep -q '"status":"succeeded"' /tmp/palimpsest-paas-redacted-restore-detail-succeeded.json
post_json "/v1/managed-postgres/clusters/cluster_123/restores" '{"target_cluster_id":"cluster_123_restore"}' | tee /tmp/palimpsest-paas-restore-request.json
grep -q '"status":"running"' /tmp/palimpsest-paas-restore-request.json
grep -q '"target_cluster_id":"cluster_123_restore"' /tmp/palimpsest-paas-restore-request.json
grep -q '"target_environment_id":"env_123"' /tmp/palimpsest-paas-restore-request.json
grep -q '"kind":"prepare_restore"' /tmp/palimpsest-paas-restore-request.json
RESTORE_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("restore").fetch("restore_id")' /tmp/palimpsest-paas-restore-request.json)"
get_json "/v1/managed-postgres/clusters/cluster_123/restores?status=running" | tee /tmp/palimpsest-paas-restores-running.json >/dev/null
grep -q "\"restore_id\":\"${RESTORE_ID}\"" /tmp/palimpsest-paas-restores-running.json
get_json "/v1/managed-postgres/clusters/cluster_123/restores/${RESTORE_ID}" | tee /tmp/palimpsest-paas-restore-detail-running.json >/dev/null
grep -q '"status":"running"' /tmp/palimpsest-paas-restore-detail-running.json

echo "container polling queued restore command"
cargo run -p palimpsest-paas-node-agent -- poll-once-container "$API_URL" | tee /tmp/palimpsest-paas-restore-command.json
grep -q '"status": "succeeded"' /tmp/palimpsest-paas-restore-command.json
test -f "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123_restore/PG_VERSION"
test -f "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123_restore/recovery.signal"
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT status FROM managed_postgres_restores WHERE target_cluster_id = 'cluster_123_restore'" | grep -q succeeded
get_json "/v1/managed-postgres/clusters/cluster_123/restores?status=succeeded" | tee /tmp/palimpsest-paas-restores-succeeded.json >/dev/null
grep -q "\"restore_id\":\"${RESTORE_ID}\"" /tmp/palimpsest-paas-restores-succeeded.json
get_json "/v1/managed-postgres/clusters/cluster_123/restores/${RESTORE_ID}" | tee /tmp/palimpsest-paas-restore-detail-succeeded.json >/dev/null
grep -q '"status":"succeeded"' /tmp/palimpsest-paas-restore-detail-succeeded.json
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT lifecycle_state FROM managed_postgres_clusters WHERE id = 'cluster_123_restore'" | grep -q stopped

echo "running restore drill scheduler once"
post_json "/v1/scheduler/restore-drills/run-once" '{"max_age_hours":1}' | tee /tmp/palimpsest-paas-restore-drill-scheduler.json
grep -q '"scheduled":' /tmp/palimpsest-paas-restore-drill-scheduler.json
grep -q '"kind":"prepare_restore"' /tmp/palimpsest-paas-restore-drill-scheduler.json
RESTORE_DRILL_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("scheduled").fetch(0).fetch("drill").fetch("drill_id")' /tmp/palimpsest-paas-restore-drill-scheduler.json)"
RESTORE_DRILL_TARGET_CLUSTER_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("scheduled").fetch(0).fetch("cluster").fetch("cluster_id")' /tmp/palimpsest-paas-restore-drill-scheduler.json)"
get_json "/v1/managed-postgres/clusters/cluster_123/restore-drills?status=running" | tee /tmp/palimpsest-paas-restore-drills-running.json >/dev/null
grep -q "\"drill_id\":\"${RESTORE_DRILL_ID}\"" /tmp/palimpsest-paas-restore-drills-running.json
get_json "/v1/managed-postgres/clusters/cluster_123/restore-drills/${RESTORE_DRILL_ID}" | tee /tmp/palimpsest-paas-restore-drill-detail-running.json >/dev/null
grep -q '"status":"running"' /tmp/palimpsest-paas-restore-drill-detail-running.json

echo "container polling queued restore drill command"
cargo run -p palimpsest-paas-node-agent -- poll-once-container "$API_URL" | tee /tmp/palimpsest-paas-restore-drill-command.json
grep -q '"status": "succeeded"' /tmp/palimpsest-paas-restore-drill-command.json
test -f "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/${RESTORE_DRILL_TARGET_CLUSTER_ID}/PG_VERSION"
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT status FROM managed_postgres_restore_drills WHERE id = '${RESTORE_DRILL_ID}'" | grep -q succeeded
get_json "/v1/managed-postgres/clusters/cluster_123/restore-drills?status=succeeded" | tee /tmp/palimpsest-paas-restore-drills-succeeded.json >/dev/null
grep -q "\"drill_id\":\"${RESTORE_DRILL_ID}\"" /tmp/palimpsest-paas-restore-drills-succeeded.json
get_json "/v1/managed-postgres/clusters/cluster_123/restore-drills/${RESTORE_DRILL_ID}" | tee /tmp/palimpsest-paas-restore-drill-detail-succeeded.json >/dev/null
grep -q '"status":"succeeded"' /tmp/palimpsest-paas-restore-drill-detail-succeeded.json
post_json "/v1/scheduler/restore-drills/run-once" '{"max_age_hours":1}' | tee /tmp/palimpsest-paas-restore-drill-scheduler-empty.json
grep -q '"scheduled":\[\]' /tmp/palimpsest-paas-restore-drill-scheduler-empty.json
get_json "/v1/environments/env_123/health" | tee /tmp/palimpsest-paas-environment-health-after-drill.json >/dev/null
grep -q '"name":"restore_drill","state":"healthy"' /tmp/palimpsest-paas-environment-health-after-drill.json

echo "requesting managed postgres failover between restored clones"
post_json "/v1/managed-postgres/clusters/cluster_123_restore/failovers" "{\"target_cluster_id\":\"${RESTORE_DRILL_TARGET_CLUSTER_ID}\"}" | tee /tmp/palimpsest-paas-failover-request.json
grep -q '"status":"running"' /tmp/palimpsest-paas-failover-request.json
grep -q '"kind":"fence_postgres_primary"' /tmp/palimpsest-paas-failover-request.json
grep -q '"kind":"fence_primary"' /tmp/palimpsest-paas-failover-request.json
grep -q '"kind":"promote_postgres_standby"' /tmp/palimpsest-paas-failover-request.json
FAILOVER_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("failover").fetch("failover_id")' /tmp/palimpsest-paas-failover-request.json)"
get_json "/v1/managed-postgres/clusters/cluster_123_restore/failovers?status=running" | tee /tmp/palimpsest-paas-failovers-running.json >/dev/null
grep -q "\"failover_id\":\"${FAILOVER_ID}\"" /tmp/palimpsest-paas-failovers-running.json
get_json "/v1/managed-postgres/clusters/cluster_123_restore/failovers/${FAILOVER_ID}" | tee /tmp/palimpsest-paas-failover-detail-running.json >/dev/null
grep -q '"status":"running"' /tmp/palimpsest-paas-failover-detail-running.json

echo "container polling queued failover fence command"
cargo run -p palimpsest-paas-node-agent -- poll-once-container "$API_URL" | tee /tmp/palimpsest-paas-failover-fence-command.json
grep -q '"status": "succeeded"' /tmp/palimpsest-paas-failover-fence-command.json
test -f "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123_restore/fence.intent"
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT status FROM operations WHERE target_resource_id = '${FAILOVER_ID}' AND kind = 'fence_primary'" | grep -q succeeded
get_json "/v1/managed-postgres/clusters/cluster_123_restore/failovers/${FAILOVER_ID}" | tee /tmp/palimpsest-paas-failover-detail-after-fence.json >/dev/null
grep -q '"status":"running"' /tmp/palimpsest-paas-failover-detail-after-fence.json

echo "container polling queued failover promote command"
cargo run -p palimpsest-paas-node-agent -- poll-once-container "$API_URL" | tee /tmp/palimpsest-paas-failover-promote-command.json
grep -q '"status": "succeeded"' /tmp/palimpsest-paas-failover-promote-command.json
test -f "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/${RESTORE_DRILL_TARGET_CLUSTER_ID}/promotion.intent"
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT status FROM managed_postgres_failovers WHERE id = '${FAILOVER_ID}'" | grep -q succeeded
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT lifecycle_state FROM managed_postgres_clusters WHERE id = '${RESTORE_DRILL_TARGET_CLUSTER_ID}'" | grep -q ready
get_json "/v1/environments/env_123/managed-postgres-endpoint" | tee /tmp/palimpsest-paas-managed-postgres-endpoint-after-failover.json >/dev/null
grep -q "\"active_cluster_id\":\"${RESTORE_DRILL_TARGET_CLUSTER_ID}\"" /tmp/palimpsest-paas-managed-postgres-endpoint-after-failover.json
grep -q "\"updated_by_failover_id\":\"${FAILOVER_ID}\"" /tmp/palimpsest-paas-managed-postgres-endpoint-after-failover.json
grep -q '"database_proxy_listen_addr":"127.0.0.1:55430"' /tmp/palimpsest-paas-managed-postgres-endpoint-after-failover.json
get_json "/v1/database-proxy-routes/127.0.0.1:55430" | tee /tmp/palimpsest-paas-managed-db-proxy-route-after-failover.json >/dev/null
grep -q "\"cluster_id\":\"${RESTORE_DRILL_TARGET_CLUSTER_ID}\"" /tmp/palimpsest-paas-managed-db-proxy-route-after-failover.json
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT active_cluster_id, updated_by_failover_id FROM managed_postgres_endpoints WHERE environment_id = 'env_123'" | grep -q "${RESTORE_DRILL_TARGET_CLUSTER_ID}[[:space:]]*|[[:space:]]*${FAILOVER_ID}"
get_json "/v1/managed-postgres/clusters/cluster_123_restore/failovers?status=succeeded" | tee /tmp/palimpsest-paas-failovers-succeeded.json >/dev/null
grep -q "\"failover_id\":\"${FAILOVER_ID}\"" /tmp/palimpsest-paas-failovers-succeeded.json
delete_json "/v1/environments/env_123/managed-postgres-endpoint/database-proxy-route" \
  | tee /tmp/palimpsest-paas-managed-db-proxy-route-deconfigure.json >/dev/null
grep -q '"removed_listen_addr":"127.0.0.1:55430"' /tmp/palimpsest-paas-managed-db-proxy-route-deconfigure.json
grep -q "\"${ACME_MANAGED_DB_CERTIFICATE_ID}\"" /tmp/palimpsest-paas-managed-db-proxy-route-deconfigure.json
ruby -rjson -e 'endpoint = JSON.parse(File.read(ARGV.fetch(0))).fetch("endpoint"); abort "database proxy listen addr was not cleared" if endpoint.key?("database_proxy_listen_addr"); abort "active certificate was not cleared" if endpoint.key?("active_certificate_id")' /tmp/palimpsest-paas-managed-db-proxy-route-deconfigure.json
get_json "/v1/environments/env_123/managed-postgres-endpoint/certificates" \
  | tee /tmp/palimpsest-paas-managed-db-certificates-after-deconfigure.json >/dev/null
grep -q "\"certificate_id\":\"${MANAGED_DB_CERTIFICATE_ID}\"" /tmp/palimpsest-paas-managed-db-certificates-after-deconfigure.json
grep -q "\"certificate_id\":\"${RENEWED_MANAGED_DB_CERTIFICATE_ID}\"" /tmp/palimpsest-paas-managed-db-certificates-after-deconfigure.json
grep -q "\"certificate_id\":\"${EXTERNAL_MANAGED_DB_CERTIFICATE_ID}\"" /tmp/palimpsest-paas-managed-db-certificates-after-deconfigure.json
grep -q "\"certificate_id\":\"${ACME_MANAGED_DB_CERTIFICATE_ID}\"" /tmp/palimpsest-paas-managed-db-certificates-after-deconfigure.json
grep -q '"status":"revoked"' /tmp/palimpsest-paas-managed-db-certificates-after-deconfigure.json
MANAGED_DB_ROUTE_AFTER_DECONFIGURE_STATUS="$(curl --silent --show-error --output /tmp/palimpsest-paas-managed-db-proxy-route-after-deconfigure.json --write-out "%{http_code}" -H "accept: application/json" "${API_URL}/v1/database-proxy-routes/127.0.0.1:55430")"
test "$MANAGED_DB_ROUTE_AFTER_DECONFIGURE_STATUS" = "404"
MANAGED_DB_PROXY_DECONFIGURED=0
for _ in $(seq 1 50); do
  if ruby -rsocket -rtimeout -e 'begin; Timeout.timeout(1) { client = TCPSocket.new("127.0.0.1", 55430); client.close }; exit 1; rescue; exit 0; end' 2>/dev/null; then
    MANAGED_DB_PROXY_DECONFIGURED=1
    break
  fi
  sleep 0.1
done
test "$MANAGED_DB_PROXY_DECONFIGURED" = "1"

echo "requesting managed postgres delete with final backup"
post_json "/v1/managed-postgres/clusters/cluster_123/delete" '{"final_backup":true,"tombstone_retention_days":0}' | tee /tmp/palimpsest-paas-delete-request.json
grep -q '"lifecycle_state":"deleting"' /tmp/palimpsest-paas-delete-request.json
grep -q '"final_backup"' /tmp/palimpsest-paas-delete-request.json
grep -q '"kind":"run_base_backup"' /tmp/palimpsest-paas-delete-request.json
grep -q '"kind":"stop_postgres"' /tmp/palimpsest-paas-delete-request.json
grep -q '"kind":"delete_postgres_data"' /tmp/palimpsest-paas-delete-request.json

echo "container polling queued final backup command"
cargo run -p palimpsest-paas-node-agent -- poll-once-container "$API_URL" | tee /tmp/palimpsest-paas-delete-final-backup-command.json
grep -q '"status": "succeeded"' /tmp/palimpsest-paas-delete-final-backup-command.json
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT count(*) FROM managed_postgres_backups WHERE cluster_id = 'cluster_123' AND status = 'succeeded'" | grep -q 3

echo "container polling queued final delete stop command"
cargo run -p palimpsest-paas-node-agent -- poll-once-container "$API_URL" | tee /tmp/palimpsest-paas-delete-stop-command.json
grep -q '"status": "succeeded"' /tmp/palimpsest-paas-delete-stop-command.json
if docker ps --filter name=palimpsest-pg-cluster_123 --format '{{.Names}}' | grep -q '^palimpsest-pg-cluster_123$'; then
  echo "managed postgres container still running after final delete stop" >&2
  exit 1
fi

echo "container polling queued delete command"
cargo run -p palimpsest-paas-node-agent -- poll-once-container "$API_URL" | tee /tmp/palimpsest-paas-delete-command.json
grep -q '"status": "succeeded"' /tmp/palimpsest-paas-delete-command.json
test ! -e "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/postgres/cluster_123"
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT lifecycle_state FROM managed_postgres_clusters WHERE id = 'cluster_123'" | grep -q deleted
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT retained_backup_id IS NOT NULL AND unrecoverable_at IS NOT NULL AND retention_expires_at IS NOT NULL AND expired_at IS NULL FROM managed_postgres_deletion_tombstones WHERE cluster_id = 'cluster_123'" | grep -q t
get_json "/v1/managed-postgres/deletion-tombstones/cluster_123" | tee /tmp/palimpsest-paas-tombstone-detail.json >/dev/null
grep -q '"retained_backup_id"' /tmp/palimpsest-paas-tombstone-detail.json
RETAINED_BACKUP_ID="$(ruby -rjson -e 'print JSON.parse(File.read(ARGV.fetch(0))).fetch("retained_backup_id")' /tmp/palimpsest-paas-tombstone-detail.json)"
get_json "/v1/managed-postgres/deletion-tombstones?environment_id=env_123&expired=false" | tee /tmp/palimpsest-paas-tombstones-active.json >/dev/null
grep -q '"cluster_id":"cluster_123"' /tmp/palimpsest-paas-tombstones-active.json
post_json "/v1/managed-postgres/deletion-tombstones/expire" '{}' | tee /tmp/palimpsest-paas-tombstones-expired.json >/dev/null
grep -q '"cluster_id":"cluster_123"' /tmp/palimpsest-paas-tombstones-expired.json
grep -q '"expired_at"' /tmp/palimpsest-paas-tombstones-expired.json
grep -q '"cleanup_commands"' /tmp/palimpsest-paas-tombstones-expired.json
grep -q '"kind":"delete_backup_data"' /tmp/palimpsest-paas-tombstones-expired.json
RETAINED_BACKUP_ARTIFACT_ID="backup_artifact_${RETAINED_BACKUP_ID}_local_fs"
get_json "/v1/managed-postgres/clusters/cluster_123/backups/${RETAINED_BACKUP_ID}/artifacts/${RETAINED_BACKUP_ARTIFACT_ID}" \
  | tee /tmp/palimpsest-paas-retained-backup-artifact-expired.json >/dev/null
grep -q '"status":"expired"' /tmp/palimpsest-paas-retained-backup-artifact-expired.json
echo "container polling queued expired backup cleanup command"
cargo run -p palimpsest-paas-node-agent -- poll-once-container "$API_URL" | tee /tmp/palimpsest-paas-delete-backup-command.json
grep -q '"status": "succeeded"' /tmp/palimpsest-paas-delete-backup-command.json
test ! -e "$PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT/backups/cluster_123/${RETAINED_BACKUP_ID}"
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT status FROM managed_postgres_backups WHERE id = '${RETAINED_BACKUP_ID}'" | grep -q deleted
get_json "/v1/managed-postgres/clusters/cluster_123/backups/${RETAINED_BACKUP_ID}/artifacts/${RETAINED_BACKUP_ARTIFACT_ID}" \
  | tee /tmp/palimpsest-paas-retained-backup-artifact-deleted.json >/dev/null
grep -q '"status":"deleted"' /tmp/palimpsest-paas-retained-backup-artifact-deleted.json
get_json "/v1/managed-postgres/clusters/cluster_123/backups/${RETAINED_BACKUP_ID}/artifacts?status=deleted" \
  | tee /tmp/palimpsest-paas-retained-backup-artifacts-deleted.json >/dev/null
grep -q "\"artifact_id\":\"${RETAINED_BACKUP_ARTIFACT_ID}\"" /tmp/palimpsest-paas-retained-backup-artifacts-deleted.json
get_json "/v1/managed-postgres/deletion-tombstones?environment_id=env_123&expired=true" | tee /tmp/palimpsest-paas-tombstones-expired-list.json >/dev/null
grep -q '"cluster_id":"cluster_123"' /tmp/palimpsest-paas-tombstones-expired-list.json
TOMBSTONED_CREATE_STATUS="$(post_json_status "/v1/managed-postgres/clusters" '{"cluster_id":"cluster_123","organization_id":"org_123","project_id":"project_123","environment_id":"env_123","region":"us-east-1","postgres_version":"18","tier":"dev","storage_gib":20,"lifecycle_state":"requested","host_assignment":null}')"
if [[ "$TOMBSTONED_CREATE_STATUS" != "400" ]]; then
  cat /tmp/palimpsest-paas-status-response.json >&2 || true
  echo "expected tombstoned cluster id create to return HTTP 400, got $TOMBSTONED_CREATE_STATUS" >&2
  exit 1
fi
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT count(*) >= 10 FROM agent_commands WHERE operation_id IS NOT NULL" | grep -q t
docker compose -f "$CONTROL_PLANE_DIR/docker-compose.yaml" exec -T control-plane-postgres \
  psql -U palimpsest_control -d palimpsest_control --tuples-only \
  --command "SELECT count(*) >= 10 FROM operations WHERE status = 'succeeded' AND current_step = 'completed'" | grep -q t

echo "smoke path completed"
