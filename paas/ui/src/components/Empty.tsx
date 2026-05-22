import { ReactNode } from "react";

interface Props {
  title: string;
  hint?: ReactNode;
}

export function Empty({ title, hint }: Props) {
  return (
    <div className="empty-state">
      <p className="empty-title">{title}</p>
      {hint && <p className="empty-hint">{hint}</p>}
    </div>
  );
}
