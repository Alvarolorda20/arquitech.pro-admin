import {resolveApiOriginFromHost} from '@/modules/admin/runtime';

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
    return normalizeBackendBaseUrl(configured);
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
    return normalizeBackendBaseUrl(configured);
  }
  if (process.env.NODE_ENV !== 'production') {
    return 'http://localhost:8000';
  }
  if (typeof window !== 'undefined' && window.location.origin) {
    const apiOrigin = resolveApiOriginFromHost(window.location.hostname, window.location.protocol);
    if (apiOrigin) {
      return stripTrailingSlashes(apiOrigin);
    }
    return stripTrailingSlashes(window.location.origin);
  }
  return '';
}
