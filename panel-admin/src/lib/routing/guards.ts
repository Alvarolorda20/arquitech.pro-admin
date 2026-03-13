export const LOGIN_PAGE = '/login';

export const AUTH_PAGES = ['/login', '/register', '/forgot-password'];

export const PROTECTED_PREFIXES = ['/upload', '/products', '/tenants', '/api/tenant-context'];

export function isAuthPath(pathname: string): boolean {
  return AUTH_PAGES.includes(pathname);
}

export function isProtectedPath(pathname: string): boolean {
  return PROTECTED_PREFIXES.some(
    (prefix) => pathname === prefix || pathname.startsWith(`${prefix}/`),
  );
}
