import { stateTone, statusLabel } from "../lib/state";

export function StatePill({ state }: { state: string | null | undefined }) {
  const tone = stateTone(state);
  const label = statusLabel(state);
  return <span className={`state-pill state-${tone}`}>{label}</span>;
}
