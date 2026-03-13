import {createSupabaseServerClient} from '@/lib/supabase/server';
import {getCurrentTenantSelection, isTenantSelectionBoundToUser} from '@/lib/tenant-context';
import {resolveTenantProducts, type ProductDefinition} from '@/lib/products';
import type {TenantMembership} from '@/types/tenant';

interface WorkspaceUser {
  id: string;
  email?: string;
  displayName?: string;
}

interface WorkspaceSelection {
  activeMembership: TenantMembership | null;
  enabledProducts: ProductDefinition[];
}

export function resolveWorkspaceSelection({
  memberships,
  currentTenantId,
}: {
  memberships: TenantMembership[];
  currentTenantId: string | null;
}): WorkspaceSelection {
  if (memberships.length === 0) {
    return {activeMembership: null, enabledProducts: []};
  }

  const active =
    memberships.find((m) => m.tenant_id === currentTenantId) ?? memberships[0];

  const tenantProducts = active.tenants?.products ?? [];
  const resolved = resolveTenantProducts(tenantProducts);
  return {activeMembership: active, enabledProducts: resolved};
}

interface WorkspaceContext {
  user: WorkspaceUser | null;
  memberships: TenantMembership[];
  activeMembership: TenantMembership | null;
  enabledProducts: ProductDefinition[];
  currentTenantId: string | null;
  activeProjectId: string | null;
  isAdminReviewMode: boolean;
}

function resolveUserDisplayName(user: {
  user_metadata?: Record<string, unknown> | null;
  email?: string | null;
}): string | undefined {
  const metadata = user.user_metadata || {};
  const metadataCandidates = [
    metadata.full_name,
    metadata.fullName,
    metadata.name,
    metadata.display_name,
    metadata.displayName,
  ];
  for (const candidate of metadataCandidates) {
    if (typeof candidate === 'string' && candidate.trim()) {
      return candidate.trim();
    }
  }

  if (typeof user.email === 'string' && user.email.includes('@')) {
    return user.email.split('@')[0];
  }

  return undefined;
}

export async function getWorkspaceContext(): Promise<WorkspaceContext> {
  const supabase = await createSupabaseServerClient();
  const {
    data: {user},
  } = await supabase.auth.getUser();

  if (!user) {
    return {
      user: null,
      memberships: [],
      activeMembership: null,
      enabledProducts: [],
      currentTenantId: null,
      activeProjectId: null,
      isAdminReviewMode: false,
    };
  }

  try {
    const {error: acceptError} = await supabase.rpc('accept_pending_tenant_invites');
    if (acceptError) {
      console.error('[workspace-context] accept_pending_tenant_invites failed:', acceptError.message);
    }
  } catch (error) {
    // The RPC may not exist yet in environments where migrations are pending.
    console.error('[workspace-context] accept_pending_tenant_invites exception:', error);
  }

  const currentTenantSelection = await getCurrentTenantSelection();
  const currentTenantId = currentTenantSelection.tenantId;
  const isTenantSelectionValid = isTenantSelectionBoundToUser(
    currentTenantSelection.userId,
    user.id,
  );
  const tenantIdForResolution = isTenantSelectionValid ? currentTenantId : null;

  const {data: memberships} = await supabase
    .from('memberships')
    .select('tenant_id, role, tenants(id, name, slug, products, metadata)')
    .eq('user_id', user.id)
    .eq('status', 'active');

  const safeMemberships = (memberships ?? []) as unknown as TenantMembership[];

  let {activeMembership, enabledProducts} = resolveWorkspaceSelection({
    memberships: safeMemberships,
    currentTenantId: tenantIdForResolution,
  });
  let isAdminReviewMode = false;

  const needsAdminTenantOverride =
    Boolean(tenantIdForResolution) &&
    (
      !activeMembership ||
      String(activeMembership.tenant_id || '').trim() !== String(tenantIdForResolution || '').trim()
    );

  if (needsAdminTenantOverride && tenantIdForResolution) {
    const [{data: isGlobalAdminRpc}, {data: tenantRow}] = await Promise.all([
      supabase.rpc('is_global_admin'),
      supabase
        .from('tenants')
        .select('id,name,slug,products,metadata')
        .eq('id', tenantIdForResolution)
        .maybeSingle(),
    ]);

    if (Boolean(isGlobalAdminRpc) && tenantRow?.id) {
      const sourceTenant = {
        id: String(tenantRow.id).trim(),
        name: String(tenantRow.name || '').trim(),
        slug: String(tenantRow.slug || '').trim(),
        products: Array.isArray(tenantRow.products)
          ? tenantRow.products.map((item) => String(item || '').trim()).filter(Boolean)
          : [],
        metadata:
          tenantRow.metadata && typeof tenantRow.metadata === 'object'
            ? (tenantRow.metadata as Record<string, unknown>)
            : {},
      };
      const resolvedTenantIdFromRow = String(sourceTenant.id || tenantIdForResolution).trim();
      const tenantProducts = Array.isArray(sourceTenant?.products)
        ? sourceTenant.products.map((item) => String(item || '').trim()).filter(Boolean)
        : [];
      activeMembership = {
        tenant_id: resolvedTenantIdFromRow,
        role: 'viewer',
        tenants: {
          id: resolvedTenantIdFromRow,
          name: String(sourceTenant?.name || '').trim() || `Tenant ${resolvedTenantIdFromRow.slice(0, 8)}`,
          slug: String(sourceTenant?.slug || '').trim() || '',
          products: tenantProducts,
          metadata:
            sourceTenant?.metadata && typeof sourceTenant.metadata === 'object'
              ? (sourceTenant.metadata as Record<string, unknown>)
              : {},
        },
      };
      const resolvedProducts = resolveTenantProducts(activeMembership.tenants?.products ?? []);
      enabledProducts =
        resolvedProducts.length > 0
          ? resolvedProducts
          : resolveTenantProducts(['comparacion_presupuestos']);
      isAdminReviewMode = true;
    }
  }

  const resolvedTenantId = activeMembership?.tenant_id ?? null;

  let activeProjectId: string | null = null;
  if (resolvedTenantId) {
    const {data: projects, error: projectError} = await supabase
      .from('projects')
      .select('id')
      .eq('tenant_id', resolvedTenantId)
      .eq('status', 'active')
      .order('updated_at', {ascending: false})
      .limit(1);

    if (!projectError && projects && projects.length > 0) {
      activeProjectId = projects[0].id as string;
    } else {
      const {data: fallbackProjects} = await supabase
        .from('projects')
        .select('id')
        .eq('tenant_id', resolvedTenantId)
        .order('updated_at', {ascending: false})
        .limit(1);
      activeProjectId = fallbackProjects?.[0]?.id ?? null;
    }
  }

  return {
    user: {
      id: user.id,
      email: user.email,
      displayName: resolveUserDisplayName(user),
    },
    memberships: safeMemberships,
    activeMembership,
    enabledProducts,
    currentTenantId: tenantIdForResolution,
    activeProjectId,
    isAdminReviewMode,
  };
}
