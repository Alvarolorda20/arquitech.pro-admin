const LOCAL_HOSTS = new Set(['localhost', '0.0.0.0', '::1', '[::1]']);

function normalizeInput(value: string | undefined | null): string {
  return String(value || '').trim();
}

function normalizeHostname(hostname: string): string {
  return String(hostname || '').trim().toLowerCase();
}

function hostFromInput(value: string): string {
  const raw = normalizeInput(value);
  if (!raw) return '';

  try {
    const asUrl = raw.includes('://') ? new URL(raw) : new URL(`https://${raw}`);
    return normalizeHostname(asUrl.hostname);
  } catch {
    return '';
  }
}

export function isProductionRuntime(): boolean {
  const nodeEnv = normalizeInput(process.env.NODE_ENV).toLowerCase();
  const appEnv = normalizeInput(process.env.ENVIRONMENT).toLowerCase();
  return nodeEnv === 'production' || appEnv === 'production' || appEnv === 'prod';
}

export function isLocalHost(hostname: string): boolean {
  const host = normalizeHostname(hostname);
  if (!host) return false;
  if (LOCAL_HOSTS.has(host)) return true;
  if (host.startsWith('127.')) return true;
  return false;
}

export function isLocalUrl(value: string): boolean {
  const host = hostFromInput(value);
  if (!host) return false;
  return isLocalHost(host);
}

export function assertNonLocalUrlInProduction(value: string, configLabel: string): void {
  if (!isProductionRuntime()) return;
  const raw = normalizeInput(value);
  if (!raw) return;
  if (isLocalUrl(raw)) {
    throw new Error(
      `[config] ${configLabel} points to a local URL in production (${raw}). Use a public host such as https://api-admin.arquitech.pro.`,
    );
  }
}
