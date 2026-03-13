function stripTrailingSlashes(value: string): string {
  return String(value || '').trim().replace(/\/+$/, '');
}

function normalizeOrigin(value: string | undefined): string {
  const raw = stripTrailingSlashes(String(value || ''));
  if (!raw) return '';
  try {
    return new URL(raw).origin;
  } catch {
    return '';
  }
}

function extractHostFromOrigin(origin: string): string | null {
  const normalized = normalizeOrigin(origin);
  if (!normalized) return null;
  try {
    return new URL(normalized).hostname.toLowerCase();
  } catch {
    return null;
  }
}

function resolveConfiguredWorkspaceOrigin(): string {
  return (
    normalizeOrigin(process.env.NEXT_PUBLIC_WORKSPACE_APP_URL) ||
    normalizeOrigin(process.env.WORKSPACE_APP_URL) ||
    normalizeOrigin(process.env.NEXT_PUBLIC_APP_URL) ||
    normalizeOrigin(process.env.APP_BASE_URL) ||
    normalizeOrigin(process.env.NEXT_PUBLIC_SITE_URL) ||
    ''
  );
}

function resolveConfiguredAdminOrigin(): string {
  return (
    normalizeOrigin(process.env.NEXT_PUBLIC_ADMIN_APP_URL) ||
    normalizeOrigin(process.env.ADMIN_APP_URL) ||
    ''
  );
}

export function getConfiguredWorkspaceHost(): string | null {
  const explicitHost = String(process.env.WORKSPACE_APP_HOST || '').trim().toLowerCase();
  if (explicitHost) return explicitHost;
  return extractHostFromOrigin(resolveConfiguredWorkspaceOrigin());
}

export function getConfiguredAdminHost(): string | null {
  const explicitHost = String(process.env.ADMIN_PANEL_HOST || '').trim().toLowerCase();
  if (explicitHost) return explicitHost;
  return extractHostFromOrigin(resolveConfiguredAdminOrigin());
}

export function resolveApiOriginFromHost(hostname: string, protocol: string): string | null {
  const host = String(hostname || '').trim().toLowerCase();
  const safeProtocol = protocol === 'http:' || protocol === 'https:' ? protocol : 'https:';
  if (host.startsWith('app.') && host.length > 4) {
    return `${safeProtocol}//api.${host.slice(4)}`;
  }
  if (host.startsWith('admin.') && host.length > 6) {
    return `${safeProtocol}//api-admin.${host.slice(6)}`;
  }
  return null;
}

export function resolveWorkspaceOriginFromBrowser(): string {
  const configured = resolveConfiguredWorkspaceOrigin();
  if (configured) return configured;

  const configuredAdminOrigin = resolveConfiguredAdminOrigin();
  if (configuredAdminOrigin) {
    try {
      const adminUrl = new URL(configuredAdminOrigin);
      const host = adminUrl.hostname.toLowerCase();
      if (host.startsWith('admin.') && host.length > 6) {
        return `${adminUrl.protocol}//app.${host.slice(6)}`;
      }
    } catch {
      // Fall through to empty origin for relative links.
    }
  }

  return '';
}

export function buildWorkspaceSwitchHref(input: {
  tenantId: string;
  nextPath?: string;
  adminReturnPath?: string;
}): string {
  const tenantId = String(input.tenantId || '').trim();
  const nextPath = String(input.nextPath || '/products/comparacion-presupuestos').trim();
  const adminReturnPath = String(input.adminReturnPath || '').trim();
  const query = new URLSearchParams({
    tenantId,
    next: nextPath,
  });
  const adminOrigin =
    resolveConfiguredAdminOrigin() ||
    (typeof window !== 'undefined' ? stripTrailingSlashes(window.location.origin) : '');
  if (adminOrigin && adminReturnPath.startsWith('/') && !adminReturnPath.startsWith('//')) {
    query.set('admin_return', `${adminOrigin}${adminReturnPath}`);
  }
  const path = `/tenants/switch?${query.toString()}`;
  const workspaceOrigin = resolveWorkspaceOriginFromBrowser();
  return workspaceOrigin ? `${workspaceOrigin}${path}` : path;
}
