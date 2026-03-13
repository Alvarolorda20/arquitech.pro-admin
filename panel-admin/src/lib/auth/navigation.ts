export type AuthMode = 'login' | 'signup' | 'reset';

export interface AuthSearchParams {
  error?: string;
  success?: string;
  next?: string;
  mode?: string;
}

export function normalizeNextPath(value: string): string {
  if (!value.startsWith('/') || value.startsWith('//')) {
    return '/';
  }

  return value;
}

export function modeFromLegacyModeQuery(mode: string | undefined): AuthMode {
  if (mode === 'signup') {
    return 'signup';
  }

  if (mode === 'reset') {
    return 'reset';
  }

  return 'login';
}

export function routeForMode(mode: AuthMode): string {
  if (mode === 'signup') {
    return '/register';
  }

  if (mode === 'reset') {
    return '/forgot-password';
  }

  return '/login';
}

export function buildAuthUrl({
  mode,
  error,
  success,
  nextPath,
}: {
  mode: AuthMode;
  error?: string;
  success?: string;
  nextPath?: string;
}): string {
  const base = routeForMode(mode);
  const search = new URLSearchParams();

  if (nextPath && nextPath !== '/') {
    search.set('next', nextPath);
  }

  if (error) {
    search.set('error', error);
  }

  if (success) {
    search.set('success', success);
  }

  const query = search.toString();
  return query ? `${base}?${query}` : base;
}

export function buildLegacyModeRedirect(
  params: AuthSearchParams,
): string | null {
  const mode = modeFromLegacyModeQuery(params.mode);

  if (mode === 'login') {
    return null;
  }

  const search = new URLSearchParams();

  if (params.next && params.next !== '/') {
    search.set('next', params.next);
  }

  if (params.error) {
    search.set('error', params.error);
  }

  if (params.success) {
    search.set('success', params.success);
  }

  const base = routeForMode(mode);
  const query = search.toString();
  return query ? `${base}?${query}` : base;
}
