import {NextResponse, type NextRequest} from 'next/server';

import {APP_SESSION_STARTED_AT_COOKIE} from '@/lib/auth/session';
import {createSupabaseServerClient} from '@/lib/supabase/server';
import {clearTenantSelectionCookies} from '@/lib/tenant-context';

function normalizeAdminNextPath(value: string): string {
  const normalized = String(value || '').trim();
  if (normalized.startsWith('//')) {
    return '/memberships';
  }
  if (
    normalized === '/' ||
    normalized === '/memberships' ||
    normalized.startsWith('/tenants/')
  ) {
    return normalized;
  }
  if (normalized.startsWith('/admin')) {
    return normalized.replace(/^\/admin/, '') || '/';
  }
  return '/memberships';
}

export async function GET(request: NextRequest) {
  const supabase = await createSupabaseServerClient();
  await supabase.auth.signOut();

  const reason = String(request.nextUrl.searchParams.get('reason') || '').trim().toLowerCase();
  const nextPath = normalizeAdminNextPath(
    request.nextUrl.searchParams.get('next') || '/memberships',
  );

  const adminLoginUrl = new URL('/', request.url);
  if (reason === 'session_expired') {
    adminLoginUrl.searchParams.set('reason', 'session_expired');
  }
  if (nextPath && nextPath !== '/memberships') {
    adminLoginUrl.searchParams.set('next', nextPath);
  }

  const response = NextResponse.redirect(adminLoginUrl);
  response.cookies.delete(APP_SESSION_STARTED_AT_COOKIE);
  clearTenantSelectionCookies(response.cookies);
  return response;
}
