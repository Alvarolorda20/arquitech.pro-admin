import {NextRequest, NextResponse} from 'next/server';

import {getBackendApiBaseUrl} from '@/lib/backend-api';
import {assertNonLocalUrlInProduction} from '@/lib/url-safety';
import {ADMIN_PROXY_ALLOWED_PATHS} from '@/modules/admin/contracts';
import {resolveApiOriginFromHost} from '@/modules/admin/runtime';

const ALLOWED_PATHS: ReadonlySet<string> = new Set<string>(ADMIN_PROXY_ALLOWED_PATHS);

function resolveAdminPath(path: string[] | undefined): string | null {
  if (!path || path.length === 0) {
    return null;
  }

  const normalized = path
    .map((segment) => String(segment || '').trim())
    .filter((segment) => segment.length > 0)
    .join('/');

  if (!normalized || !ALLOWED_PATHS.has(normalized)) {
    return null;
  }
  return normalized;
}

function stripTrailingSlashes(value: string): string {
  return String(value || '').trim().replace(/\/+$/, '');
}

function normalizeBaseUrl(value: string): string {
  const base = stripTrailingSlashes(value);
  if (!base) return '';
  return base.replace(/\/api$/i, '');
}

function resolveBackendCandidates(request: NextRequest): string[] {
  const configured =
    process.env.API_URL?.trim() ||
    process.env.BACKEND_URL?.trim() ||
    process.env.NEXT_PUBLIC_API_URL?.trim() ||
    process.env.NEXT_PUBLIC_BACKEND_URL?.trim() ||
    '';

  const candidates: string[] = [];
  if (configured) {
    const normalized = normalizeBaseUrl(configured);
    assertNonLocalUrlInProduction(
      normalized,
      'API_URL / BACKEND_URL / NEXT_PUBLIC_API_URL / NEXT_PUBLIC_BACKEND_URL',
    );
    candidates.push(normalized);
  } else {
    const defaultBase = normalizeBaseUrl(getBackendApiBaseUrl());
    if (defaultBase) candidates.push(defaultBase);
  }

  const host = String(request.nextUrl.hostname || '').trim().toLowerCase();
  const protocol = String(request.nextUrl.protocol || 'https:').trim();
  const apiOrigin = resolveApiOriginFromHost(host, protocol);
  if (apiOrigin) {
    const normalized = normalizeBaseUrl(apiOrigin);
    assertNonLocalUrlInProduction(normalized, 'resolved api origin from host');
    candidates.push(normalized);
  }

  return Array.from(new Set(candidates.filter(Boolean)));
}

async function proxyRequest(
  request: NextRequest,
  method: 'GET' | 'POST' | 'PATCH' | 'DELETE',
  path: string[] | undefined,
) {
  const resolvedPath = resolveAdminPath(path);
  if (!resolvedPath) {
    return NextResponse.json({detail: 'Not found'}, {status: 404});
  }

  let backendCandidates: string[] = [];
  try {
    backendCandidates = resolveBackendCandidates(request);
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Admin backend configuration error.';
    return NextResponse.json({detail: message}, {status: 503});
  }
  if (backendCandidates.length === 0) {
    return NextResponse.json(
      {
        detail:
          'Admin backend misconfigured: set API_URL (or NEXT_PUBLIC_API_URL / NEXT_PUBLIC_BACKEND_URL).',
      },
      {status: 503},
    );
  }
  const headers = new Headers();
  const authHeader = request.headers.get('authorization');
  if (authHeader) {
    headers.set('authorization', authHeader);
  }

  let body: string | undefined;
  if (method !== 'GET') {
    body = await request.text();
    if (body) {
      headers.set('content-type', 'application/json');
    }
  }

  let upstream: Response | null = null;
  let lastFetchError: Error | null = null;
  const incomingUrl = new URL(request.url);

  for (const backendBase of backendCandidates) {
    const targetUrl = new URL(`${backendBase}/api/admin/${resolvedPath}`);
    for (const [key, value] of incomingUrl.searchParams.entries()) {
      targetUrl.searchParams.append(key, value);
    }
    try {
      const candidate = await fetch(targetUrl, {
        method,
        headers,
        body,
        cache: 'no-store',
      });
      if (candidate.status !== 404) {
        upstream = candidate;
        break;
      }
      upstream = candidate;
    } catch (error) {
      lastFetchError = error instanceof Error ? error : new Error('Admin backend unreachable');
    }
  }

  if (!upstream) {
    return NextResponse.json(
      {
        detail: lastFetchError
          ? `Admin backend unreachable: ${lastFetchError.message}`
          : 'Admin backend unreachable',
      },
      {status: 503},
    );
  }

  const responseBody = await upstream.arrayBuffer();
  const responseHeaders = new Headers();
  const contentType = upstream.headers.get('content-type');
  if (contentType) {
    responseHeaders.set('content-type', contentType);
  }
  const contentDisposition = upstream.headers.get('content-disposition');
  if (contentDisposition) {
    responseHeaders.set('content-disposition', contentDisposition);
  }
  const cacheControl = upstream.headers.get('cache-control');
  if (cacheControl) {
    responseHeaders.set('cache-control', cacheControl);
  }

  return new NextResponse(responseBody, {
    status: upstream.status,
    headers: responseHeaders,
  });
}

export async function GET(
  request: NextRequest,
  context: {params: Promise<{path?: string[]}>},
) {
  const params = await context.params;
  return proxyRequest(request, 'GET', params.path);
}

export async function POST(
  request: NextRequest,
  context: {params: Promise<{path?: string[]}>},
) {
  const params = await context.params;
  return proxyRequest(request, 'POST', params.path);
}

export async function PATCH(
  request: NextRequest,
  context: {params: Promise<{path?: string[]}>},
) {
  const params = await context.params;
  return proxyRequest(request, 'PATCH', params.path);
}

export async function DELETE(
  request: NextRequest,
  context: {params: Promise<{path?: string[]}>},
) {
  const params = await context.params;
  return proxyRequest(request, 'DELETE', params.path);
}
