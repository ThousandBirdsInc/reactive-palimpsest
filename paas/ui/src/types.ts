export type HealthState = "healthy" | "degraded" | "unhealthy" | "unknown";

export interface Scope {
  apiBase: string;
  organizationId: string;
  projectId: string;
  environmentId: string;
  actorId: string;
}

export interface Organization {
  organization_id: string;
  display_name?: string | null;
}

export interface Project {
  project_id: string;
  organization_id: string;
  display_name?: string | null;
}

export interface Environment {
  environment_id: string;
  project_id: string;
  display_name?: string | null;
}

export interface EnvironmentHealth {
  environment_id: string;
  state: HealthState | string;
  summary: string;
  components: Array<{
    name: string;
    state: HealthState | string;
    detail: string;
  }>;
}

export interface ManagedPostgresCluster {
  cluster_id: string;
  organization_id: string;
  project_id: string;
  environment_id: string;
  region: string;
  postgres_version: string;
  tier: string;
  storage_gib: number;
  lifecycle_state: string;
  host_assignment?: {
    host_id: string;
    data_dir: string;
    port: number;
  } | null;
}

export interface NodeHost {
  host_id: string;
  region: string;
  failure_domain?: string | null;
  state: string;
  capacity?: {
    max_clusters?: number;
    storage_gib?: number;
  };
  reported_at?: string | null;
}

export interface SyncDeployment {
  deployment_id: string;
  organization_id: string;
  project_id: string;
  environment_id: string;
  region: string;
  lifecycle_state: string;
  config_version?: string | null;
}

export interface QuotaAlert {
  alert_id: string;
  policy_id: string;
  organization_id: string;
  project_id: string;
  environment_id: string;
  metric: string;
  threshold_basis_points: number;
  current_quantity: number;
  limit_quantity: number;
  state: string;
}

export interface QuotaPolicy {
  policy_id: string;
  organization_id: string;
  metric: string;
  limit_quantity: number;
  scope?: string | null;
}

export interface Incident {
  incident_id: string;
  organization_id: string;
  project_id: string;
  environment_id?: string | null;
  title: string;
  severity: string;
  status: string;
  started_at?: string | null;
  updated_at?: string | null;
}

export interface GatewayRoute {
  host: string;
  environment_id: string;
  sync_endpoint: string;
  tls_policy?: unknown;
  rate_limit?: {
    max_connections?: number;
    max_requests_per_minute?: number;
  };
}

export interface DatabaseProxyRoute {
  listen_addr: string;
  upstream_addr: string;
  environment_id: string;
  cluster_id: string;
  tls?: unknown;
}

export interface Domain {
  hostname: string;
  environment_id: string;
  certificate_id?: string | null;
  state?: string | null;
}

export interface AuditEvent {
  event_id: string;
  occurred_at: string;
  actor_id?: string | null;
  action: string;
  resource?: string | null;
  outcome?: string | null;
  organization_id?: string | null;
  project_id?: string | null;
  environment_id?: string | null;
}

export interface ClusterBackup {
  backup_id: string;
  cluster_id: string;
  started_at?: string | null;
  completed_at?: string | null;
  state: string;
  size_bytes?: number | null;
}

export interface ClusterOperation {
  operation_id: string;
  idempotency_key: string;
  target_resource_id: string;
  kind: string;
  status: string;
  current_step: string;
  lease_owner?: string | null;
}

export interface PitrCheck {
  check_id: string;
  cluster_id: string;
  state: string;
  latest_restorable_lsn?: string | null;
  observed_at?: string | null;
}

export interface ManagedPostgresEndpoint {
  environment_id: string;
  active_cluster_id: string;
  updated_by_failover_id?: string | null;
  database_proxy_listen_addr?: string | null;
  active_certificate_id?: string | null;
}

export interface DatabaseCloneRequest {
  source_database: string;
  target_database: string;
  terminate_source_connections?: boolean;
}

export interface DatabaseCloneResponse {
  cluster: ManagedPostgresCluster;
  source_database: string;
  target_database: string;
  command: unknown;
  operation: ClusterOperation;
}

export interface ClusterSchemaColumn {
  name: string;
  data_type: string;
  nullable: boolean;
  ordinal_position: number;
}

export interface ClusterSchemaTable {
  name: string;
  columns: ClusterSchemaColumn[];
}

export interface ClusterSchema {
  name: string;
  tables: ClusterSchemaTable[];
}

export interface ClusterSchemaResponse {
  schemas: ClusterSchema[];
}

export type SqlCellValue = string | number | boolean | null | unknown;

export interface ClusterSqlResult {
  rows: Array<Record<string, SqlCellValue>>;
  row_count: number;
  truncated: boolean;
}

export interface SampleDataSeedTable {
  name: string;
  rows: number;
}

export interface SampleDataSeedResponse {
  tables: SampleDataSeedTable[];
  total_rows: number;
}

export interface ClusterQueryExplain {
  canonical_sql: string;
  statement_kind: string;
  referenced_tables: string[];
  sharing_behavior: string;
  estimated_startup_cost?: number | null;
  estimated_total_cost?: number | null;
  estimated_rows?: number | null;
  explain_plan: unknown;
}

export interface EnvironmentOverview {
  health?: EnvironmentHealth;
  active_database_endpoint?: unknown;
  managed_postgres_clusters?: ManagedPostgresCluster[];
  sync_deployments?: SyncDeployment[];
  quota_alerts?: QuotaAlert[];
  incidents?: Incident[];
  gateway_routes?: GatewayRoute[];
  database_proxy_routes?: DatabaseProxyRoute[];
  node_hosts?: NodeHost[];
  [key: string]: unknown;
}

// ----- permission rule DSL + verifier -----

export type PermissionRuleMode = "row_visibility" | "subscribe" | "both";

export interface PermissionRuleDocument {
  environment_id: string;
  organization_id: string;
  project_id: string;
  dsl: string;
  updated_at: string | null;
}

export interface PermissionUserContextField {
  name: string;
  type: string;
}

export interface PermissionVerifyRule {
  name: string;
  table: string;
  mode: PermissionRuleMode;
  predicate: string;
  canonical: string | null;
  user_fields: string[];
  tautology: boolean;
}

export interface PermissionVerifyResponse {
  ok: boolean;
  error: string | null;
  catalog_source: string;
  catalog_tables: string[];
  user_context: PermissionUserContextField[];
  rules: PermissionVerifyRule[];
}

export interface PermissionVerifyCatalogColumn {
  name: string;
  type: string;
}

export interface PermissionVerifyCatalogTable {
  name: string;
  columns: PermissionVerifyCatalogColumn[];
}

// ----- query permission policies (per-table row/subscribe predicates) -----

export type QueryPermissionOperation = "read" | "subscribe";
export type QueryPermissionPolicyStatus = "draft" | "active";

export interface QueryPermissionPolicy {
  policy_id: string;
  organization_id: string;
  project_id: string;
  environment_id: string;
  name: string;
  table_schema: string;
  table_name: string;
  operation: QueryPermissionOperation;
  principal_claim: string;
  predicate_sql: string;
  sample_context: Record<string, unknown>;
  status: QueryPermissionPolicyStatus;
}

export interface QueryPermissionPolicyDryRunResponse {
  policy_id: string;
  accepted: boolean;
  table_schema: string;
  table_name: string;
  operation: QueryPermissionOperation;
  checked_predicate_sql: string;
  sample_context: Record<string, unknown>;
  decision_detail: string;
}
