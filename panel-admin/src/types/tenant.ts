export type TenantRole = 'owner' | 'editor' | 'viewer';

export interface Tenant {
  id: string;
  name: string;
  slug: string;
  products: string[];
  metadata: Record<string, unknown>;
}

export interface TenantMembership {
  tenant_id: string;
  role: TenantRole;
  tenants: Tenant | null;
}
