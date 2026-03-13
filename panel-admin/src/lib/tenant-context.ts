import {cookies} from 'next/headers';

export const CURRENT_TENANT_COOKIE = 'current_tenant_id';
export const CURRENT_TENANT_USER_COOKIE = 'current_tenant_user_id';

interface MutableCookieStoreLike {
  set: (name: string, value: string, options?: Record<string, unknown>) => unknown;
  delete: (name: string) => unknown;
}

interface CurrentTenantSelection {
  tenantId: string | null;
  userId: string | null;
}

export function getTenantCookieOptions() {
  return {
    path: '/',
    sameSite: 'lax' as const,
    httpOnly: true,
    secure: process.env.NODE_ENV === 'production',
    maxAge: 60 * 60 * 24 * 30,
  };
}

export function clearTenantSelectionCookies(cookieStore: MutableCookieStoreLike): void {
  cookieStore.delete(CURRENT_TENANT_COOKIE);
  cookieStore.delete(CURRENT_TENANT_USER_COOKIE);
}

export function setTenantSelectionCookies(
  cookieStore: MutableCookieStoreLike,
  input: {tenantId: string; userId: string},
): void {
  const tenantId = String(input.tenantId || '').trim();
  const userId = String(input.userId || '').trim();
  if (!tenantId || !userId) {
    clearTenantSelectionCookies(cookieStore);
    return;
  }

  const options = getTenantCookieOptions();
  cookieStore.set(CURRENT_TENANT_COOKIE, tenantId, options);
  cookieStore.set(CURRENT_TENANT_USER_COOKIE, userId, options);
}

export function isTenantSelectionBoundToUser(
  selectedUserId: string | null | undefined,
  userId: string | null | undefined,
): boolean {
  const normalizedSelection = String(selectedUserId || '').trim();
  const normalizedUserId = String(userId || '').trim();
  return Boolean(normalizedSelection && normalizedUserId && normalizedSelection === normalizedUserId);
}

export async function getCurrentTenantSelection(): Promise<CurrentTenantSelection> {
  const cookieStore = await cookies();
  const tenantId = String(cookieStore.get(CURRENT_TENANT_COOKIE)?.value || '').trim() || null;
  const userId = String(cookieStore.get(CURRENT_TENANT_USER_COOKIE)?.value || '').trim() || null;
  return {tenantId, userId};
}

/**
 * Reads the currently selected tenant ID from the cookie store.
 * Returns `null` if no tenant has been selected yet.
 */
export async function getCurrentTenantId(): Promise<string | null> {
  const {tenantId} = await getCurrentTenantSelection();
  return tenantId;
}
