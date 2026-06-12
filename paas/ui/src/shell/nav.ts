import {
  Activity,
  AlertTriangle,
  Database,
  Gauge,
  KeyRound,
  LayoutGrid,
  ListTree,
  RefreshCw,
  Server,
  Settings,
  Shield,
} from "lucide-react";
import type { ComponentType } from "react";
import type { LucideProps } from "lucide-react";

export interface NavItem {
  label: string;
  to: string;
  icon: ComponentType<LucideProps>;
  end?: boolean;
}

export const NAV_ITEMS: NavItem[] = [
  { label: "Overview", to: "/", icon: LayoutGrid, end: true },
  { label: "Clusters", to: "/clusters", icon: Database },
  { label: "Hosts", to: "/hosts", icon: Server },
  { label: "Incidents", to: "/incidents", icon: AlertTriangle },
  { label: "Quota", to: "/quota", icon: Gauge },
  { label: "Routes", to: "/routes", icon: Shield },
  { label: "Sync", to: "/sync-deployments", icon: RefreshCw },
  { label: "Permissions", to: "/permissions", icon: KeyRound },
  { label: "Audit", to: "/audit", icon: Activity },
  { label: "Settings", to: "/settings", icon: Settings },
];

// Cluster-detail tabs live here so the command palette and shell can
// share the labels with the page.
export const CLUSTER_TABS: Array<{ label: string; slug: string; icon?: ComponentType<LucideProps> }> = [
  { label: "Overview", slug: "" },
  { label: "SQL", slug: "sql" },
  { label: "Clones", slug: "clones" },
  { label: "Backups", slug: "backups" },
  { label: "PITR", slug: "pitr" },
  { label: "Roles", slug: "roles" },
  { label: "Operations", slug: "operations", icon: ListTree },
  { label: "Audit", slug: "audit" },
];
