import {resolveApiOriginFromHost} from '@/modules/admin/runtime';
import {assertNonLocalUrlInProduction} from '@/lib/url-safety';

function stripTrailingSlashes(value: string): string {
  return value.replace(/\/+$/, '');
}

function normalizeBackendBaseUrl(value: string): string {
  const trimmed = stripTrailingSlashes(String(value || '').trim());
  if (!trimmed) return '';
  // Accept both ".../api" and base host values to avoid accidental "/api/api/*".
  return trimmed.replace(/\/api$/i, '');
}

function resolveFirstConfiguredBackendBaseUrl(): string {
  return (
    process.env.API_URL?.trim() ||
    process.env.BACKEND_URL?.trim() ||
    process.env.NEXT_PUBLIC_API_URL?.trim() ||
    process.env.NEXT_PUBLIC_BACKEND_URL?.trim() ||
    ''
  );
}

export function getBackendApiBaseUrl(): string {
  const configured = resolveFirstConfiguredBackendBaseUrl();
  if (configured) {
    const normalized = normalizeBackendBaseUrl(configured);
    assertNonLocalUrlInProduction(
      normalized,
      'API_URL / BACKEND_URL / NEXT_PUBLIC_API_URL / NEXT_PUBLIC_BACKEND_URL',
    );
    return normalized;
  }
  if (process.env.NODE_ENV !== 'production') {
    return 'http://localhost:8000';
  }
  return '';
}

export function getPublicBackendApiBaseUrl(): string {
  const configured =
    process.env.NEXT_PUBLIC_API_URL?.trim() ||
    process.env.NEXT_PUBLIC_BACKEND_URL?.trim() ||
    '';
  if (configured) {
    const normalized = normalizeBackendBaseUrl(configured);
    assertNonLocalUrlInProduction(normalized, 'NEXT_PUBLIC_API_URL / NEXT_PUBLIC_BACKEND_URL');
    return normalized;
  }
  if (process.env.NODE_ENV !== 'production') {
    return 'http://localhost:8000';
  }
  if (typeof window !== 'undefined' && window.location.origin) {
    const apiOrigin = resolveApiOriginFromHost(window.location.hostname, window.location.protocol);
    if (apiOrigin) {
      const normalized = stripTrailingSlashes(apiOrigin);
      assertNonLocalUrlInProduction(normalized, 'resolved browser API origin');
      return normalized;
    }
    const sameOrigin = stripTrailingSlashes(window.location.origin);
    assertNonLocalUrlInProduction(sameOrigin, 'window.location.origin');
    return sameOrigin;
  }
  return '';
}
