'use client';

interface WorkspaceCreditBadgeProps {
  label: string;
  loadingLabel: string;
  value: string | number | null;
  unit?: string;
  secondary?: string | null;
  loading: boolean;
  level: 'unknown' | 'low' | 'medium' | 'high';
  layout?: 'horizontal' | 'stacked';
}

export function WorkspaceCreditBadge({
  label,
  loadingLabel,
  value,
  unit,
  secondary,
  loading,
  level,
  layout = 'horizontal',
}: WorkspaceCreditBadgeProps) {
  const isStacked = layout === 'stacked';
  return (
    <div
      className={`workspace-credit-badge workspace-credit-badge--${level} ${isStacked ? 'workspace-credit-badge--stacked' : ''}`}
      aria-live="polite"
    >
      <span className="workspace-credit-badge-label">{label}</span>
      <div className="workspace-credit-badge-main">
        <strong className="workspace-credit-badge-value">
          {loading ? loadingLabel : value ?? '-'}
        </strong>
        {unit ? (
          <span className="workspace-credit-badge-unit">{unit}</span>
        ) : null}
      </div>
      {!loading && isStacked && secondary ? (
        <span className="workspace-credit-badge-secondary">{secondary}</span>
      ) : null}
    </div>
  );
}
