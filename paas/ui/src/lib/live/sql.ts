// Per-resource SQL for the Palimpsest live source.
//
// When the control plane is fronted by a palimpsest-server and
// VITE_PAAS_PALIMPSEST_URL is set, add an entry here and switch the
// corresponding `usePaasResource` call in a page to the Palimpsest
// variant. Today the map is empty — every resource uses polling.
//
// Variables prefixed with `$` are substituted by the LiveSource at
// subscribe time from the current Scope.

import type { Scope } from "../../types";

export interface ResourceSql {
  sql: string;
  vars?: (scope: Scope) => Record<string, string>;
}

export const RESOURCE_SQL: Partial<Record<string, ResourceSql>> = {
  // Example (commented; uncomment when the control-plane Palimpsest endpoint
  // is online and the view exists):
  //
  // clusters: {
  //   sql: `SELECT cluster_id, lifecycle_state, postgres_version, tier,
  //                storage_gib, region, host_id
  //         FROM managed_postgres_clusters
  //         WHERE environment_id = $env`,
  //   vars: (scope) => ({ env: scope.environmentId }),
  // },
};

export function hasPalimpsestSource(name: string): boolean {
  return Boolean(import.meta.env.VITE_PAAS_PALIMPSEST_URL) && Boolean(RESOURCE_SQL[name]);
}
