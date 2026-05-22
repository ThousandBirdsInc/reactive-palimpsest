import { ReactNode } from "react";
import { RefreshCw } from "lucide-react";

interface Props {
  title: string;
  subtitle?: string;
  actions?: ReactNode;
  onRefresh?: () => void;
  loading?: boolean;
}

export function PageHeader({ title, subtitle, actions, onRefresh, loading }: Props) {
  return (
    <header className="page-header">
      <div>
        <h1>{title}</h1>
        {subtitle && <p className="page-subtitle">{subtitle}</p>}
      </div>
      <div className="page-actions">
        {actions}
        {onRefresh && (
          <button
            className="btn-ghost"
            type="button"
            onClick={onRefresh}
            disabled={loading}
            aria-label="Refresh"
            title="Refresh"
          >
            <RefreshCw size={14} className={loading ? "spin" : undefined} />
          </button>
        )}
      </div>
    </header>
  );
}
