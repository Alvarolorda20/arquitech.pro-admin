export interface AdminAuthTokensResponse {
  access_token?: string;
  refresh_token?: string;
  token_type?: string;
  expires_in?: number;
  expires_at?: number;
  detail?: string;
}

export const ADMIN_PROXY_ALLOWED_PATHS = [
  'login',
  'refresh',
  'tenant-overview',
  'run-artifacts',
  'run-artifact/download',
  'memberships',
  'memberships/status',
  'memberships/role',
  'tenant-subscriptions/status',
  'tenant-credits/adjust',
  'tenant-billing-config',
] as const;

export type AdminProxyAllowedPath = (typeof ADMIN_PROXY_ALLOWED_PATHS)[number];

