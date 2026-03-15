import {NextResponse, type NextRequest} from 'next/server';

import {isAuthPath, isProtectedPath, LOGIN_PAGE} from '@/lib/routing/guards';
import {
  APP_SESSION_STARTED_AT_COOKIE,
  getSessionCookieOptions,
  isSessionExpired,
} from '@/lib/auth/session';
import {syncSessionAndTenant} from '@/lib/supabase/middleware';
import {
  CURRENT_TENANT_COOKIE,
  CURRENT_TENANT_USER_COOKIE,
  clearTenantSelectionCookies,
} from '@/lib/tenant-context';
import {getConfiguredAdminHost, getConfiguredWorkspaceHost} from '@/modules/admin/runtime';

function isAdminScopedPath(pathname: string): boolean {
  return (
    pathname === '/' ||
    pathname === '/memberships' ||
    pathname.startsWith('/memberships/') ||
    pathname.startsWith('/tenants/') ||
    pathname === '/logout' ||
    pathname === '/admin' ||
    pathname.startsWith('/admin/') ||
    pathname === '/api/admin' ||
    pathname.startsWith('/api/admin/')
  );
}

function isAdminProtectedPath(pathname: string): boolean {
  if (pathname === '/memberships' || pathname.startsWith('/memberships/')) {
    return true;
  }
  if (pathname === '/tenants' || pathname.startsWith('/tenants/')) {
    return true;
  }
  if (pathname === '/api/admin' || pathname.startsWith('/api/admin/')) {
    return !pathname.startsWith('/api/admin/login') && !pathname.startsWith('/api/admin/refresh');
  }
  return false;
}

function buildHostRedirect(request: NextRequest, targetHost: string): URL {
  const redirectUrl = request.nextUrl.clone();
  redirectUrl.hostname = targetHost;
  redirectUrl.port = '';
  return redirectUrl;
}

function buildSafeUrl(path: string, request: NextRequest): URL {
  const url = new URL(path, request.nextUrl.clone());
  url.port = '';
  return url;
}

export async function middleware(request: NextRequest) {
  const pathname = request.nextUrl.pathname;
  const fullPathWithQuery = `${pathname}${request.nextUrl.search || ''}`;
  const isAuthPage = isAuthPath(pathname);
  const isProtected = isProtectedPath(pathname);
  const isAdminProtected = isAdminProtectedPath(pathname);
  const requestHost = String(request.nextUrl.hostname || '').trim().toLowerCase().replace(/:\d+$/, '');
  const adminHost = getConfiguredAdminHost();
  const workspaceHost = getConfiguredWorkspaceHost();
  const adminScopedPath = isAdminScopedPath(pathname);

  if (adminHost && requestHost !== adminHost && adminScopedPath) {
    return NextResponse.redirect(buildHostRedirect(request, adminHost));
  }

  if (
    adminHost &&
    workspaceHost &&
    adminHost !== workspaceHost &&
    requestHost === adminHost &&
    !adminScopedPath
  ) {
    return NextResponse.redirect(buildHostRedirect(request, workspaceHost));
  }

  // Admin panel is isolated: keep navigation inside admin routes only.
  if (!adminScopedPath) {
    return NextResponse.redirect(buildSafeUrl('/', request));
  }

  const {response, user, isGlobalAdmin} = await syncSessionAndTenant(request);
  const currentTenantId = String(request.cookies.get(CURRENT_TENANT_COOKIE)?.value || '').trim();
  const currentTenantUserId = String(request.cookies.get(CURRENT_TENANT_USER_COOKIE)?.value || '').trim();
  const hasTenantSelectionCookie = Boolean(currentTenantId || currentTenantUserId);
  const hasValidTenantBinding =
    Boolean(currentTenantId) &&
    Boolean(currentTenantUserId) &&
    Boolean(user?.id) &&
    currentTenantUserId === String(user?.id || '').trim();

  if (hasTenantSelectionCookie && !hasValidTenantBinding) {
    clearTenantSelectionCookies(response.cookies);
    try {
      request.cookies.delete(CURRENT_TENANT_COOKIE);
      request.cookies.delete(CURRENT_TENANT_USER_COOKIE);
    } catch {
      // Request cookies may be immutable in some runtimes.
    }
  }

  if (user) {
    const startedAtRaw = request.cookies.get(APP_SESSION_STARTED_AT_COOKIE)?.value;
    if (isSessionExpired(startedAtRaw)) {
      const logoutUrl = buildSafeUrl('/logout', request);
      logoutUrl.searchParams.set('reason', 'session_expired');
      logoutUrl.searchParams.set('next', fullPathWithQuery);
      return NextResponse.redirect(logoutUrl);
    }

    if (!startedAtRaw) {
      response.cookies.set(
        APP_SESSION_STARTED_AT_COOKIE,
        String(Date.now()),
        getSessionCookieOptions(),
      );
    }
  }

  if (!user && (isProtected || isAdminProtected)) {
    const loginUrl = buildSafeUrl(LOGIN_PAGE, request);
    loginUrl.searchParams.set('next', fullPathWithQuery);

    return NextResponse.redirect(loginUrl);
  }

  if (user && isAdminProtected && !isGlobalAdmin) {
    const logoutUrl = buildSafeUrl('/logout', request);
    logoutUrl.searchParams.set('next', '/');
    return NextResponse.redirect(logoutUrl);
  }

  if (user && isAuthPage) {
    const allowInviteRegister =
      pathname === '/register' && request.nextUrl.searchParams.get('invited') === '1';
    if (allowInviteRegister) {
      return response;
    }

    return NextResponse.redirect(buildSafeUrl('/', request));
  }

  return response;
}

export const config = {
  matcher: ['/((?!_next/static|_next/image|favicon.ico).*)'],
};
