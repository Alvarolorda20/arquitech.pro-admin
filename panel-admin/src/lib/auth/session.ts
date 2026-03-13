export const APP_SESSION_STARTED_AT_COOKIE = 'app_session_started_at';

const DEFAULT_MAX_SESSION_AGE_SECONDS = 60 * 60 * 12;

function parsePositiveInteger(value: string | undefined): number | null {
  if (!value) return null;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return parsed;
}

export function getSessionMaxAgeSeconds(): number {
  return (
    parsePositiveInteger(process.env.APP_SESSION_MAX_AGE_SECONDS) ??
    DEFAULT_MAX_SESSION_AGE_SECONDS
  );
}

export function getSessionCookieOptions() {
  return {
    path: '/',
    sameSite: 'lax' as const,
    httpOnly: true,
    secure: process.env.NODE_ENV === 'production',
    maxAge: getSessionMaxAgeSeconds(),
  };
}

export function isSessionExpired(startedAtRaw: string | null | undefined): boolean {
  const startedAt = Number.parseInt(String(startedAtRaw || ''), 10);
  if (!Number.isFinite(startedAt) || startedAt <= 0) return false;
  const elapsedMs = Date.now() - startedAt;
  return elapsedMs > getSessionMaxAgeSeconds() * 1000;
}
