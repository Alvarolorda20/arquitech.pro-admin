import {getPublicBackendApiBaseUrl} from '@/lib/backend-api';
import {createSupabaseBrowserClient} from '@/lib/supabase/client';
import {resolveApiOriginFromHost} from '@/modules/admin/runtime';
import type {AdminAuthTokensResponse} from '@/modules/admin/contracts';

const ADMIN_SESSION_KEY = 'admin_session_v1';
const LEGACY_ADMIN_ACCESS_TOKEN_KEY = 'admin_access_token';
const DEFAULT_ADMIN_IDLE_TIMEOUT_SECONDS = 30 * 60;
const DEFAULT_ADMIN_IDLE_WARNING_SECONDS = 60;
const ADMIN_REFRESH_MARGIN_SECONDS = 90;

export interface StoredAdminSession {
  access_token: string;
  refresh_token?: string;
  token_type?: string;
  expires_at_ms?: number;
}

interface SetAdminSessionInput {
  access_token: string;
  refresh_token?: string;
  token_type?: string;
  expires_in?: number | string | null;
  expires_at?: number | string | null;
}

type RefreshResponse = AdminAuthTokensResponse;

function parsePositiveInteger(value: string | undefined): number | null {
  if (!value) return null;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return parsed;
}

function normalizeExpiresAtMs(input: {
  expires_at?: number | string | null;
  expires_in?: number | string | null;
}): number | undefined {
  const rawExpiresAt = Number(input.expires_at);
  if (Number.isFinite(rawExpiresAt) && rawExpiresAt > 0) {
    // Supabase usually returns seconds since epoch in expires_at.
    if (rawExpiresAt > 1_000_000_000_000) {
      return Math.floor(rawExpiresAt);
    }
    return Math.floor(rawExpiresAt * 1000);
  }

  const rawExpiresIn = Number(input.expires_in);
  if (Number.isFinite(rawExpiresIn) && rawExpiresIn > 0) {
    return Date.now() + Math.floor(rawExpiresIn * 1000);
  }
  return undefined;
}

export function getAdminIdleTimeoutMs(): number {
  return (
    parsePositiveInteger(process.env.NEXT_PUBLIC_ADMIN_IDLE_TIMEOUT_SECONDS) ??
    DEFAULT_ADMIN_IDLE_TIMEOUT_SECONDS
  ) * 1000;
}

export function getAdminIdleWarningMs(): number {
  return (
    parsePositiveInteger(process.env.NEXT_PUBLIC_ADMIN_IDLE_WARNING_SECONDS) ??
    DEFAULT_ADMIN_IDLE_WARNING_SECONDS
  ) * 1000;
}

export function getStoredAdminSession(): StoredAdminSession | null {
  if (typeof window === 'undefined') return null;

  const rawSession = window.localStorage.getItem(ADMIN_SESSION_KEY);
  if (rawSession) {
    try {
      const parsed = JSON.parse(rawSession) as Partial<StoredAdminSession>;
      const accessToken = String(parsed.access_token || '').trim();
      if (accessToken) {
        return {
          access_token: accessToken,
          refresh_token: String(parsed.refresh_token || '').trim() || undefined,
          token_type: String(parsed.token_type || '').trim() || 'bearer',
          expires_at_ms:
            Number.isFinite(Number(parsed.expires_at_ms)) && Number(parsed.expires_at_ms) > 0
              ? Number(parsed.expires_at_ms)
              : undefined,
        };
      }
    } catch {
      // Corrupted value; clean up below if we can.
    }
  }

  // Backwards compatibility with previous storage format.
  const legacyToken = String(window.localStorage.getItem(LEGACY_ADMIN_ACCESS_TOKEN_KEY) || '').trim();
  if (!legacyToken) return null;
  return {access_token: legacyToken, token_type: 'bearer'};
}

export function getStoredAdminAccessToken(): string {
  return String(getStoredAdminSession()?.access_token || '').trim();
}

export function setStoredAdminSession(input: SetAdminSessionInput): void {
  if (typeof window === 'undefined') return;

  const accessToken = String(input.access_token || '').trim();
  if (!accessToken) return;

  const payload: StoredAdminSession = {
    access_token: accessToken,
    refresh_token: String(input.refresh_token || '').trim() || undefined,
    token_type: String(input.token_type || '').trim() || 'bearer',
    expires_at_ms: normalizeExpiresAtMs(input),
  };

  window.localStorage.setItem(ADMIN_SESSION_KEY, JSON.stringify(payload));
  window.localStorage.removeItem(LEGACY_ADMIN_ACCESS_TOKEN_KEY);
}

export function setStoredAdminAccessToken(token: string): void {
  setStoredAdminSession({access_token: token});
}

export function clearStoredAdminAccessToken(): void {
  if (typeof window === 'undefined') return;
  window.localStorage.removeItem(ADMIN_SESSION_KEY);
  window.localStorage.removeItem(LEGACY_ADMIN_ACCESS_TOKEN_KEY);
}

export function resolveAdminAccessToken(): string {
  // Security: tokens are NEVER read from URL query parameters.
  // Always resolve from localStorage only (set during login flow).
  return getStoredAdminAccessToken();
}

function isSessionExpiringSoon(session: StoredAdminSession, marginSeconds = ADMIN_REFRESH_MARGIN_SECONDS): boolean {
  if (!session.expires_at_ms) return false;
  return session.expires_at_ms - Date.now() <= marginSeconds * 1000;
}

function normalizeAdminPath(path: string): string {
  const raw = String(path || '').trim();
  if (!raw) return '/api/admin';
  if (raw.startsWith('/api/admin/')) return raw;
  if (raw === '/api/admin') return raw;
  const normalized = raw.startsWith('/') ? raw : `/${raw}`;
  if (normalized.startsWith('/admin/')) {
    return `/api${normalized}`;
  }
  return `/api/admin${normalized}`;
}

function buildAdminApiCandidates(path: string): string[] {
  const normalizedPath = normalizeAdminPath(path);
  const candidates: string[] = [normalizedPath];
  const publicBase = getPublicBackendApiBaseUrl();
  if (publicBase) {
    candidates.push(`${publicBase}${normalizedPath}`);
  }
  if (typeof window !== 'undefined') {
    const host = String(window.location.hostname || '').trim().toLowerCase();
    const protocol = String(window.location.protocol || 'https:').trim();
    const apiOrigin = resolveApiOriginFromHost(host, protocol);
    if (apiOrigin) candidates.push(`${apiOrigin}${normalizedPath}`);
  }
  return Array.from(new Set(candidates));
}

async function fetchAdminApi(path: string, init?: RequestInit): Promise<Response> {
  const candidates = buildAdminApiCandidates(path);
  let lastResponse: Response | null = null;
  let lastError: Error | null = null;

  for (const url of candidates) {
    try {
      const response = await fetch(url, init);
      if (response.status !== 404) {
        return response;
      }
      lastResponse = response;
    } catch (error) {
      lastError = error instanceof Error ? error : new Error('Admin API request failed');
    }
  }

  if (lastResponse) return lastResponse;
  throw lastError || new Error('Admin API is unreachable.');
}

export async function adminApiRequest(path: string, init?: RequestInit): Promise<Response> {
  return fetchAdminApi(path, init);
}

async function requestAdminRefresh(refreshToken: string): Promise<RefreshResponse> {
  const response = await fetchAdminApi('/api/admin/refresh', {
    method: 'POST',
    headers: {'content-type': 'application/json'},
    body: JSON.stringify({refresh_token: refreshToken}),
  });
  const payload = (await response.json().catch(() => ({}))) as RefreshResponse;
  if (!response.ok) {
    throw new Error(payload.detail || `HTTP ${response.status}`);
  }
  return payload;
}

async function syncSupabaseAdminSession(input: {
  access_token: string;
  refresh_token: string;
}): Promise<void> {
  if (typeof window === 'undefined') return;
  const accessToken = String(input.access_token || '').trim();
  const refreshToken = String(input.refresh_token || '').trim();
  if (!accessToken || !refreshToken) return;

  const supabase = createSupabaseBrowserClient();
  const {error} = await supabase.auth.setSession({
    access_token: accessToken,
    refresh_token: refreshToken,
  });
  if (error) {
    throw new Error(`No se pudo sincronizar sesion Supabase: ${error.message}`);
  }
}

export async function getValidAdminAccessToken(options?: {forceRefresh?: boolean}): Promise<string> {
  const session = getStoredAdminSession();
  if (!session?.access_token) {
    throw new Error('No admin session found.');
  }

  const mustRefresh = Boolean(options?.forceRefresh) || isSessionExpiringSoon(session);
  if (!mustRefresh) return session.access_token;

  const refreshToken = String(session.refresh_token || '').trim();
  if (!refreshToken) {
    // No refresh token available, best effort: continue with current token.
    return session.access_token;
  }

  let refreshed: RefreshResponse;
  try {
    refreshed = await requestAdminRefresh(refreshToken);
  } catch (error) {
    clearStoredAdminAccessToken();
    throw error;
  }
  const nextAccessToken = String(refreshed.access_token || '').trim();
  if (!nextAccessToken) {
    throw new Error('Refresh response did not include access_token.');
  }
  const nextRefreshToken = String(refreshed.refresh_token || refreshToken).trim();
  if (!nextRefreshToken) {
    clearStoredAdminAccessToken();
    throw new Error('Refresh response did not include refresh_token.');
  }

  await syncSupabaseAdminSession({
    access_token: nextAccessToken,
    refresh_token: nextRefreshToken,
  });

  setStoredAdminSession({
    access_token: nextAccessToken,
    refresh_token: nextRefreshToken,
    token_type: refreshed.token_type,
    expires_in: refreshed.expires_in,
    expires_at: refreshed.expires_at,
  });
  return nextAccessToken;
}

export async function fetchWithAdminAuth(
  input: RequestInfo | URL,
  init?: RequestInit,
): Promise<Response> {
  const target =
    typeof input === 'string'
      ? input
      : input instanceof URL
        ? input.toString()
        : String(input.url || '');
  const headers = new Headers(init?.headers || {});
  const token = await getValidAdminAccessToken();
  headers.set('authorization', `Bearer ${token}`);

  let response = await fetchAdminApi(target, {...init, headers});
  if (response.status !== 401 && response.status !== 403) return response;

  const retryHeaders = new Headers(init?.headers || {});
  const refreshedToken = await getValidAdminAccessToken({forceRefresh: true});
  retryHeaders.set('authorization', `Bearer ${refreshedToken}`);
  response = await fetchAdminApi(target, {...init, headers: retryHeaders});
  return response;
}

export function isExpiredAdminToken(detail: string | undefined, status: number): boolean {
  if (status !== 401 && status !== 403) return false;
  const normalized = String(detail || '').toLowerCase();
  return (
    normalized.includes('token is expired') ||
    normalized.includes('bad_jwt') ||
    normalized.includes('invalid jwt') ||
    normalized.includes('supabase auth validation failed') ||
    normalized.includes('invalid or expired session token') ||
    normalized.includes('invalid or expired refresh token')
  );
}
