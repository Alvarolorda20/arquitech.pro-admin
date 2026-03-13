import type {ReactNode} from 'react';

type ConfidenceLevel = 'high' | 'medium' | 'low' | 'unknown';

interface ConfidenceBadgeProps {
  label: string;
  score: number | null;
  suffix?: ReactNode;
}

function resolveLevel(score: number | null): ConfidenceLevel {
  if (score === null || Number.isNaN(score)) {
    return 'unknown';
  }

  if (score >= 0.8) {
    return 'high';
  }

  if (score >= 0.55) {
    return 'medium';
  }

  return 'low';
}

function formatScore(score: number | null): string {
  if (score === null || Number.isNaN(score)) {
    return 'N/A';
  }

  return `${Math.round(score * 100)}%`;
}

export function ConfidenceBadge({label, score, suffix}: ConfidenceBadgeProps) {
  const level = resolveLevel(score);

  return (
    <span className={`confidence-badge ${level}`}>
      {label}: {formatScore(score)}
      {suffix ? ` ${suffix}` : null}
    </span>
  );
}
