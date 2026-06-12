import type {
  AuditEvent,
  ClusterBackup,
  ClusterOperation,
  ClusterQueryExplain,
  ClusterSchemaResponse,
  ClusterSqlResult,
  DatabaseCloneRequest,
  DatabaseCloneResponse,
  SampleDataSeedResponse,
  DatabaseProxyRoute,
  Domain,
  Environment,
  EnvironmentOverview,
  GatewayRoute,
  Incident,
  ManagedPostgresCluster,
  ManagedPostgresEndpoint,
  NodeHost,
  Organization,
  PermissionRuleDocument,
  PermissionVerifyCatalogTable,
  PermissionVerifyResponse,
  QueryPermissionPolicy,
  QueryPermissionPolicyDryRunResponse,
  PitrCheck,
  Project,
  QuotaAlert,
  QuotaPolicy,
  Scope,
  SyncDeployment,
} from "../types";

export interface CreateClusterInput {
  clusterId: string;
  region: string;
  postgresVersion: string;
  tier: string;
  storageGib: number;
}

export class PaasApi {
  constructor(private readonly scope: Scope) {}

  // ---- scope discovery ----

  listOrganizations(): Promise<Organization[]> {
    return this.list("/v1/organizations", "organizations");
  }
  listProjects(): Promise<Project[]> {
    return this.list(
      `/v1/projects?organization_id=${encodeURIComponent(this.scope.organizationId)}`,
      "projects",
    );
  }
  listEnvironments(): Promise<Environment[]> {
    return this.list(
      `/v1/environments?project_id=${encodeURIComponent(this.scope.projectId)}`,
      "environments",
    );
  }

  // ---- env-scoped reads ----

  getOverview(): Promise<EnvironmentOverview | null> {
    return this.getJson<EnvironmentOverview>(
      `/v1/environments/${encodeURIComponent(this.scope.environmentId)}/overview`,
    ).catch(() => null);
  }
  listClusters(): Promise<ManagedPostgresCluster[]> {
    return this.list(
      `/v1/managed-postgres/clusters?environment_id=${encodeURIComponent(this.scope.environmentId)}`,
      "clusters",
    );
  }
  getCluster(clusterId: string): Promise<ManagedPostgresCluster | null> {
    return this.getJson<ManagedPostgresCluster>(
      `/v1/managed-postgres/clusters/${encodeURIComponent(clusterId)}`,
    ).catch(() => null);
  }
  listClusterBackups(clusterId: string): Promise<ClusterBackup[]> {
    return this.list(
      `/v1/managed-postgres/clusters/${encodeURIComponent(clusterId)}/backups`,
      "backups",
    );
  }
  listClusterOperations(clusterId: string): Promise<ClusterOperation[]> {
    return this.list(
      `/v1/managed-postgres/clusters/${encodeURIComponent(clusterId)}/operations`,
      "operations",
    );
  }
  listClusterPitrChecks(clusterId: string): Promise<PitrCheck[]> {
    return this.list(
      `/v1/managed-postgres/clusters/${encodeURIComponent(clusterId)}/pitr-checks`,
      "checks",
    );
  }
  getClusterSchema(clusterId: string, database?: string): Promise<ClusterSchemaResponse | null> {
    const query = database ? `?database=${encodeURIComponent(database)}` : "";
    return this.getJson<ClusterSchemaResponse>(
      `/v1/managed-postgres/clusters/${encodeURIComponent(clusterId)}/schema${query}`,
    ).catch(() => null);
  }
  runClusterQuery(clusterId: string, sql: string, limit: number, database?: string): Promise<ClusterSqlResult> {
    return this.postJson(
      `/v1/managed-postgres/clusters/${encodeURIComponent(clusterId)}/sql-console/query`,
      { sql, limit, database },
    ) as Promise<ClusterSqlResult>;
  }
  explainClusterQuery(clusterId: string, sql: string, database?: string): Promise<ClusterQueryExplain> {
    return this.postJson(
      `/v1/managed-postgres/clusters/${encodeURIComponent(clusterId)}/query-explorer/inspect`,
      { sql, database },
    ) as Promise<ClusterQueryExplain>;
  }
  seedSampleData(clusterId: string): Promise<SampleDataSeedResponse> {
    return this.postJson(
      `/v1/managed-postgres/clusters/${encodeURIComponent(clusterId)}/sample-data/seed`,
      {},
    ) as Promise<SampleDataSeedResponse>;
  }
  createDatabaseClone(
    clusterId: string,
    request: DatabaseCloneRequest,
  ): Promise<DatabaseCloneResponse> {
    return this.postJson(
      `/v1/managed-postgres/clusters/${encodeURIComponent(clusterId)}/database-clones`,
      request,
    ) as Promise<DatabaseCloneResponse>;
  }
  listNodeHosts(): Promise<NodeHost[]> {
    return this.list("/v1/node-hosts", "hosts");
  }
  listSyncDeployments(): Promise<SyncDeployment[]> {
    return this.list(
      `/v1/sync-deployments?environment_id=${encodeURIComponent(this.scope.environmentId)}`,
      "deployments",
    );
  }
  listIncidents(): Promise<Incident[]> {
    return this.list(
      `/v1/incidents?environment_id=${encodeURIComponent(this.scope.environmentId)}`,
      "incidents",
    );
  }
  listQuotaPolicies(): Promise<QuotaPolicy[]> {
    return this.list(
      `/v1/quota-policies?organization_id=${encodeURIComponent(this.scope.organizationId)}`,
      "policies",
    );
  }
  listQuotaAlerts(): Promise<QuotaAlert[]> {
    return this.list(
      `/v1/quota-alerts?environment_id=${encodeURIComponent(this.scope.environmentId)}`,
      "alerts",
    );
  }
  listGatewayRoutes(): Promise<GatewayRoute[]> {
    return this.list(
      `/v1/gateway-routes?environment_id=${encodeURIComponent(this.scope.environmentId)}`,
      "routes",
    );
  }
  getManagedPostgresEndpoint(): Promise<ManagedPostgresEndpoint | null> {
    return this.getJson<ManagedPostgresEndpoint>(
      `/v1/environments/${encodeURIComponent(this.scope.environmentId)}/managed-postgres-endpoint`,
    ).catch(() => null);
  }
  listDatabaseProxyRoutes(): Promise<DatabaseProxyRoute[]> {
    return this.list(
      `/v1/database-proxy-routes?environment_id=${encodeURIComponent(this.scope.environmentId)}`,
      "routes",
    );
  }
  listDomains(): Promise<Domain[]> {
    return this.list(
      `/v1/domains?environment_id=${encodeURIComponent(this.scope.environmentId)}`,
      "domains",
    );
  }
  listAuditEvents(limit = 100, environmentId?: string): Promise<AuditEvent[]> {
    const params = new URLSearchParams({
      environment_id: environmentId ?? this.scope.environmentId,
      limit: String(limit),
    });
    return this.list(`/v1/audit-events?${params.toString()}`, "events");
  }
  getMetricsText(): Promise<string> {
    return this.getText("/metrics").catch(() => "");
  }

  // ---- permission rule DSL + verifier ----

  getPermissionRuleDocument(): Promise<PermissionRuleDocument | null> {
    return this.getJson<PermissionRuleDocument>(
      `/v1/environments/${encodeURIComponent(this.scope.environmentId)}/permission-rule-document`,
    ).catch(() => null);
  }
  savePermissionRuleDocument(dsl: string): Promise<PermissionRuleDocument> {
    return this.putJson(
      `/v1/environments/${encodeURIComponent(this.scope.environmentId)}/permission-rule-document`,
      { dsl },
    ) as Promise<PermissionRuleDocument>;
  }
  verifyPermissions(
    dsl: string,
    catalog?: PermissionVerifyCatalogTable[],
  ): Promise<PermissionVerifyResponse> {
    return this.postJson("/v1/permissions/verify", {
      dsl,
      catalog: catalog && catalog.length > 0 ? catalog : null,
    }) as Promise<PermissionVerifyResponse>;
  }
  listQueryPermissionPolicies(): Promise<QueryPermissionPolicy[]> {
    return this.list(
      `/v1/query-permission-policies?environment_id=${encodeURIComponent(this.scope.environmentId)}`,
      "policies",
    );
  }
  upsertQueryPermissionPolicy(policy: QueryPermissionPolicy): Promise<QueryPermissionPolicy> {
    return this.postJson("/v1/query-permission-policies", policy) as Promise<QueryPermissionPolicy>;
  }
  dryRunQueryPermissionPolicy(
    policyId: string,
    sampleContext?: Record<string, unknown>,
  ): Promise<QueryPermissionPolicyDryRunResponse> {
    return this.postJson(
      `/v1/query-permission-policies/${encodeURIComponent(policyId)}/dry-run`,
      { sample_context: sampleContext ?? null },
    ) as Promise<QueryPermissionPolicyDryRunResponse>;
  }

  // ---- cluster mutations ----

  createCluster(input: CreateClusterInput): Promise<unknown> {
    return this.postJson("/v1/managed-postgres/clusters", {
      cluster_id: input.clusterId,
      organization_id: this.scope.organizationId,
      project_id: this.scope.projectId,
      environment_id: this.scope.environmentId,
      region: input.region,
      postgres_version: input.postgresVersion,
      tier: input.tier,
      storage_gib: input.storageGib,
      lifecycle_state: "requested",
      host_assignment: null,
    });
  }
  reconcileCluster(id: string) {
    return this.postJson(`/v1/managed-postgres/clusters/${encodeURIComponent(id)}/reconcile`, {});
  }
  pauseCluster(id: string) {
    return this.postJson(`/v1/managed-postgres/clusters/${encodeURIComponent(id)}/pause`, {});
  }
  resumeCluster(id: string) {
    return this.postJson(`/v1/managed-postgres/clusters/${encodeURIComponent(id)}/resume`, {});
  }
  rotateRoles(id: string) {
    return this.postJson(`/v1/managed-postgres/clusters/${encodeURIComponent(id)}/roles/rotate`, {});
  }
  runBackup(id: string) {
    return this.postJson(`/v1/managed-postgres/clusters/${encodeURIComponent(id)}/backups`, {});
  }
  resizeCluster(id: string, storageGib: number) {
    return this.postJson(`/v1/managed-postgres/clusters/${encodeURIComponent(id)}/resize`, {
      storage_gib: storageGib,
    });
  }
  deleteCluster(id: string, finalBackup: boolean) {
    return this.postJson(`/v1/managed-postgres/clusters/${encodeURIComponent(id)}/delete`, {
      final_backup: finalBackup,
    });
  }

  // ---- transport ----

  private async list<T>(path: string, key: string): Promise<T[]> {
    const body = await this.getJson<Record<string, unknown>>(path).catch(
      (): Record<string, unknown> => ({}),
    );
    const value = body[key];
    return Array.isArray(value) ? (value as T[]) : [];
  }
  private async getJson<T>(path: string): Promise<T> {
    const response = await fetch(this.url(path), {
      headers: { accept: "application/json" },
    });
    return this.readJson<T>(response);
  }
  private async getText(path: string): Promise<string> {
    const response = await fetch(this.url(path), {
      headers: { accept: "text/plain" },
    });
    if (!response.ok) {
      throw new Error(`GET ${path} returned HTTP ${response.status}`);
    }
    return response.text();
  }
  private async postJson(path: string, body: unknown): Promise<unknown> {
    const response = await fetch(this.url(path), {
      method: "POST",
      headers: {
        accept: "application/json",
        "content-type": "application/json",
        "x-actor-id": this.scope.actorId,
      },
      body: JSON.stringify(body),
    });
    return this.readJson(response);
  }
  private async putJson(path: string, body: unknown): Promise<unknown> {
    const response = await fetch(this.url(path), {
      method: "PUT",
      headers: {
        accept: "application/json",
        "content-type": "application/json",
        "x-actor-id": this.scope.actorId,
      },
      body: JSON.stringify(body),
    });
    return this.readJson(response);
  }
  private async readJson<T = unknown>(response: Response): Promise<T> {
    const text = await response.text();
    const parsed = text ? JSON.parse(text) : null;
    if (!response.ok) {
      const message =
        parsed && typeof parsed === "object" && "error" in parsed
          ? String((parsed as { error: unknown }).error)
          : text;
      throw new Error(`${response.status} ${response.statusText}: ${message}`);
    }
    return parsed as T;
  }
  private url(path: string): string {
    return `${this.scope.apiBase.replace(/\/$/, "")}${path}`;
  }
}

export function metricValue(metricsText: string, metricName: string): number | null {
  for (const line of metricsText.split("\n")) {
    if (!line.startsWith(metricName)) continue;
    const value = Number(line.trim().split(/\s+/).at(-1));
    return Number.isFinite(value) ? value : null;
  }
  return null;
}
