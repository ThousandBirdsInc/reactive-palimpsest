import { ReactNode, useState } from "react";

interface Props {
  label: ReactNode;
  confirmLabel?: string;
  confirmHint?: string;
  danger?: boolean;
  disabled?: boolean;
  onConfirm: () => void;
}

export function ConfirmButton({
  label,
  confirmLabel = "Confirm",
  confirmHint,
  danger,
  disabled,
  onConfirm,
}: Props) {
  const [pending, setPending] = useState(false);
  if (pending) {
    return (
      <div className="confirm-inline">
        {confirmHint && <span className="confirm-hint">{confirmHint}</span>}
        <button
          type="button"
          className={danger ? "btn-danger" : "btn-primary"}
          onClick={() => {
            setPending(false);
            onConfirm();
          }}
        >
          {confirmLabel}
        </button>
        <button type="button" className="btn-ghost" onClick={() => setPending(false)}>
          Cancel
        </button>
      </div>
    );
  }
  return (
    <button
      type="button"
      className={danger ? "btn-danger" : "btn-secondary"}
      disabled={disabled}
      onClick={() => setPending(true)}
    >
      {label}
    </button>
  );
}
