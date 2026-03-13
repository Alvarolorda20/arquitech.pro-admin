import type {ProductDefinition} from '@/lib/products';
import {createSupabaseServerClient} from '@/lib/supabase/server';
import type {TenantMembership, TenantRole} from '@/types/tenant';
import {getBackendApiBaseUrl} from '@/lib/backend-api';

type RawRole = TenantRole | string;

interface TenantPlanRow {
  plan_key: string | null;
  status: string | null;
  membership_plans:
    | {
        display_name: string | null;
        route_path: string | null;
      }
    | Array<{
        display_name: string | null;
        route_path: string | null;
      }>
    | null;
}

interface PlanCatalogRow {
  plan_key: string | null;
  display_name: string | null;
  route_path: string | null;
}

interface ProjectRow {
  id: string | null;
  name: string | null;
  status: string | null;
  updated_at: string | null;
}

interface BudgetRunRow {
  id: string | null;
  project_id: string | null;
  status: string | null;
  started_at: string | null;
  finished_at: string | null;
  pipeline_job_id: string | null;
  error_message?: string | null;
  result_payload:
    | {
        output_excel?: {
          path?: string;
          bucket?: string;
        };
      }
    | null;
}

interface TeamMembershipRow {
  user_id: string | null;
  role: TenantRole;
  status: string | null;
  created_at: string | null;
}

interface TeamProfileRow {
  user_id: string | null;
  full_name: string | null;
  avatar_url: string | null;
}

export type WorkspaceRoleLabel = 'Owner' | 'Admin' | 'Member';

export interface DashboardMembershipChip {
  planKey: string;
  label: string;
  status: 'active' | 'blocked';
  routePath: string | null;
}

export interface DashboardTenantSwitcherItem {
  tenantId: string;
  name: string;
  isActive: boolean;
  href: string;
}

export interface DashboardProjectSummary {
  id: string;
  name: string;
  status: string;
  updatedAt: string | null;
}

export interface DashboardRecentRun {
  id: string;
  projectId: string;
  projectName: string;
  status: string;
  pipelineJobId: string | null;
  startedAt: string | null;
  finishedAt: string | null;
  progress: number | null;
  progressMessage: string | null;
  viewHref: string;
  downloadHref: string | null;
}

interface LiveRunStatus {
  status: string;
  progress: number | null;
  message: string | null;
}

export interface DashboardTeamMember {
  userId: string;
  fullName: string | null;
  avatarUrl: string | null;
  role: TenantRole;
  status: string;
}

export interface DashboardContextData {
  workspaceName: string;
  workspaceAvatarUrl: string | null;
  roleLabel: WorkspaceRoleLabel;
  rawRole: TenantRole;
  canExecuteRuns: boolean;
  canManageTeam: boolean;
  hasComparisonProduct: boolean;
  tenantSwitcherItems: DashboardTenantSwitcherItem[];
  membershipChips: DashboardMembershipChip[];
  membershipDataSource: 'subscriptions' | 'products-fallback';
  projects: DashboardProjectSummary[];
  recentRuns: DashboardRecentRun[];
  lastDownloadHref: string | null;
  teamMembers: DashboardTeamMember[];
  teamDataAvailable: boolean;
}

function normalizedRole(role: RawRole): TenantRole {
  if (role === 'owner' || role === 'editor' || role === 'viewer') {
    return role;
  }
  return 'viewer';
}

export function roleLabelFromMembership(role: RawRole): WorkspaceRoleLabel {
  const safeRole = normalizedRole(role);
  if (safeRole === 'owner') return 'Owner';
  if (safeRole === 'editor') return 'Admin';
  return 'Member';
}

function canRoleExecuteRuns(role: RawRole): boolean {
  const safeRole = normalizedRole(role);
  return safeRole === 'owner' || safeRole === 'editor';
}

function canRoleManageTeam(role: RawRole): boolean {
  const safeRole = normalizedRole(role);
  return safeRole === 'owner' || safeRole === 'editor';
}

function resolveWorkspaceAvatarUrl(metadata: Record<string, unknown> | null | undefined): string | null {
  if (!metadata || typeof metadata !== 'object') {
    return null;
  }

  const keys = ['avatar_url', 'avatarUrl', 'logo_url', 'logoUrl', 'image_url', 'imageUrl'];
  for (const key of keys) {
    const value = metadata[key];
    if (typeof value === 'string' && value.trim()) {
      return value.trim();
    }
  }

  return null;
}

function buildFallbackMembershipChips(enabledProducts: ProductDefinition[]): DashboardMembershipChip[] {
  return enabledProducts.map((product) => ({
    planKey: product.id,
    label: product.title,
    status: 'active',
    routePath: product.href,
  }));
}

function normalizePlanJoin(row: TenantPlanRow): {label: string; routePath: string | null} {
  const plan = Array.isArray(row.membership_plans)
    ? row.membership_plans[0]
    : row.membership_plans;
  const fallbackLabel = String(row.plan_key || '').trim();
  return {
    label: String(plan?.display_name || '').trim() || fallbackLabel || 'Plan',
    routePath: String(plan?.route_path || '').trim() || null,
  };
}

function statusFromSubscription(status: string | null | undefined): 'active' | 'blocked' {
  const normalized = String(status || '').trim().toLowerCase();
  if (normalized === 'active' || normalized === 'trial') {
    return 'active';
  }
  return 'blocked';
}

function downloadHrefForRun(baseUrl: string, pipelineJobId: string | null | undefined): string | null {
  const safeJobId = String(pipelineJobId || '').trim();
  if (!safeJobId) return null;
  return `${baseUrl}/api/download/${encodeURIComponent(safeJobId)}`;
}

function normalizeRunStatus(status: string | null | undefined): string {
  const normalized = String(status || '').trim().toLowerCase();
  if (normalized === 'processing') return 'running';
  if (normalized === 'done') return 'completed';
  if (normalized === 'error') return 'failed';
  return normalized || 'queued';
}

async function fetchLiveRunStatus(
  baseUrl: string,
  pipelineJobId: string,
): Promise<LiveRunStatus | null> {
  try {
    const res = await fetch(`${baseUrl}/api/status/${encodeURIComponent(pipelineJobId)}`, {
      cache: 'no-store',
    });
    if (!res.ok) return null;
    const payload = (await res.json()) as {
      status?: string;
      progress?: number;
      message?: string;
    };
    return {
      status: normalizeRunStatus(payload.status || ''),
      progress: typeof payload.progress === 'number' ? payload.progress : null,
      message: typeof payload.message === 'string' ? payload.message : null,
    };
  } catch {
    return null;
  }
}

export async function getDashboardContext({
  memberships,
  activeMembership,
  enabledProducts,
  activePath,
  currentUser,
}: {
  memberships: TenantMembership[];
  activeMembership: TenantMembership | null;
  enabledProducts: ProductDefinition[];
  activePath: string;
  currentUser?: {id: string; email?: string; displayName?: string} | null;
}): Promise<DashboardContextData> {
  const role = normalizedRole(activeMembership?.role ?? 'viewer');
  const tenantId = activeMembership?.tenant_id ?? null;
  const workspaceName = activeMembership?.tenants?.name?.trim() || 'Workspace';
  const workspaceAvatarUrl = resolveWorkspaceAvatarUrl(activeMembership?.tenants?.metadata);
  const hasComparisonProduct = enabledProducts.some((product) => product.id === 'comparacion_presupuestos');
  const backendBaseUrl = getBackendApiBaseUrl();

  const tenantSwitcherItems = memberships.map((membership) => ({
    tenantId: membership.tenant_id,
    name: membership.tenants?.name || membership.tenant_id,
    isActive: membership.tenant_id === tenantId,
    href: `/tenants/switch?tenantId=${encodeURIComponent(
      membership.tenant_id,
    )}&next=${encodeURIComponent(activePath)}`,
  }));

  const baseContext: DashboardContextData = {
    workspaceName,
    workspaceAvatarUrl,
    roleLabel: roleLabelFromMembership(role),
    rawRole: role,
    canExecuteRuns: canRoleExecuteRuns(role),
    canManageTeam: canRoleManageTeam(role),
    hasComparisonProduct,
    tenantSwitcherItems,
    membershipChips: buildFallbackMembershipChips(enabledProducts),
    membershipDataSource: 'products-fallback',
    projects: [],
    recentRuns: [],
    lastDownloadHref: null,
    teamMembers: [],
    teamDataAvailable: true,
  };

  if (!tenantId) {
    return baseContext;
  }

  const supabase = await createSupabaseServerClient();

  try {
    const [{data: planCatalogData, error: planCatalogError}, {data: planStatusData, error: planStatusError}] =
      await Promise.all([
        supabase
          .from('membership_plans')
          .select('plan_key,display_name,route_path')
          .eq('is_active', true)
          .order('sort_order', {ascending: true}),
        supabase
          .from('tenant_subscriptions')
          .select('plan_key,status,membership_plans(display_name,route_path)')
          .eq('tenant_id', tenantId),
      ]);

    if (!planCatalogError && !planStatusError && planCatalogData && planStatusData) {
      const catalogRows = planCatalogData as PlanCatalogRow[];
      const subscriptionRows = planStatusData as TenantPlanRow[];
      const subscriptionsByPlan = new Map(
        subscriptionRows.map((row) => [
          String(row.plan_key || '').trim().toLowerCase(),
          {
            status: statusFromSubscription(row.status),
            ...normalizePlanJoin(row),
          },
        ]),
      );

      const chips: DashboardMembershipChip[] = catalogRows
        .map((plan) => {
          const planKey = String(plan.plan_key || '').trim().toLowerCase();
          if (!planKey) return null;

          const found = subscriptionsByPlan.get(planKey);
          return {
            planKey,
            label: String(plan.display_name || '').trim() || planKey,
            status: found?.status || 'blocked',
            routePath: found?.routePath || String(plan.route_path || '').trim() || null,
          } satisfies DashboardMembershipChip;
        })
        .filter((chip): chip is DashboardMembershipChip => Boolean(chip));

      if (chips.length > 0) {
        baseContext.membershipChips = chips;
        baseContext.membershipDataSource = 'subscriptions';
      }
    }
  } catch {
    // Soft-fail: keep product-based fallback chips.
  }

  try {
    const {data: projectsData, error: projectsError} = await supabase
      .from('projects')
      .select('id,name,status,updated_at')
      .eq('tenant_id', tenantId)
      .order('updated_at', {ascending: false})
      .limit(100);

    if (!projectsError && projectsData) {
      baseContext.projects = (projectsData as ProjectRow[])
        .filter((row) => row.id)
        .map((row) => ({
          id: String(row.id),
          name: String(row.name || '').trim() || 'Proyecto',
          status: String(row.status || '').trim().toLowerCase() || 'draft',
          updatedAt: row.updated_at || null,
        }));
    }
  } catch {
    // Projects panel supports empty-state when data cannot be loaded.
  }

  try {
    const {data: runsData, error: runsError} = await supabase
      .from('budget_runs')
      .select('id,project_id,status,started_at,finished_at,pipeline_job_id,error_message,result_payload')
      .eq('tenant_id', tenantId)
      .order('started_at', {ascending: false})
      .limit(10);

    if (!runsError && runsData) {
      const projectsById = new Map(baseContext.projects.map((project) => [project.id, project.name]));
      const rawRuns = (runsData as BudgetRunRow[]).filter((row) => row.id && row.project_id);
      const liveStatusByJobId = new Map<string, LiveRunStatus>();

      await Promise.all(
        rawRuns.map(async (row) => {
          const pipelineJobId = String(row.pipeline_job_id || '').trim() || null;
          const dbStatus = normalizeRunStatus(row.status);
          if (!pipelineJobId) return;
          if (dbStatus !== 'running' && dbStatus !== 'queued') return;
          const liveStatus = await fetchLiveRunStatus(backendBaseUrl, pipelineJobId);
          if (liveStatus) {
            liveStatusByJobId.set(pipelineJobId, liveStatus);
          }
        }),
      );

      baseContext.recentRuns = rawRuns.map((row) => {
        const projectId = String(row.project_id);
        const pipelineJobId = String(row.pipeline_job_id || '').trim() || null;
        const dbStatus = normalizeRunStatus(row.status);
        const liveStatus = pipelineJobId ? liveStatusByJobId.get(pipelineJobId) || null : null;
        const resolvedStatus = liveStatus?.status || dbStatus;
        const downloadHref =
          resolvedStatus === 'completed'
            ? downloadHrefForRun(backendBaseUrl, pipelineJobId)
            : null;

        return {
          id: String(row.id),
          projectId,
          projectName: projectsById.get(projectId) || 'Proyecto',
          status: resolvedStatus,
          pipelineJobId,
          startedAt: row.started_at || null,
          finishedAt: row.finished_at || null,
          progress: liveStatus?.progress ?? null,
          progressMessage: liveStatus?.message ?? null,
          viewHref: `/products/comparacion-presupuestos?project=${encodeURIComponent(projectId)}`,
          downloadHref,
        };
      });

      const latestWithDownload = baseContext.recentRuns.find(
        (run) => run.status === 'completed' && run.downloadHref,
      );
      baseContext.lastDownloadHref = latestWithDownload?.downloadHref || null;
    }
  } catch {
    // Keep dashboard available even if budget_runs migration is pending.
  }

  if (baseContext.canManageTeam) {
    try {
      const [{data: membershipsData, error: membershipsError}, {data: profilesData, error: profilesError}] =
        await Promise.all([
          supabase
            .from('memberships')
            .select('user_id,role,status,created_at')
            .eq('tenant_id', tenantId)
            .order('created_at', {ascending: true}),
          supabase
            .from('profiles')
            .select('user_id,full_name,avatar_url')
            .eq('tenant_id', tenantId),
        ]);

      if (!membershipsError && !profilesError && membershipsData) {
        const profilesByUser = new Map(
          ((profilesData || []) as TeamProfileRow[])
            .filter((row) => row.user_id)
            .map((row) => [
              String(row.user_id),
              {fullName: row.full_name || null, avatarUrl: row.avatar_url || null},
            ]),
        );

        baseContext.teamMembers = ((membershipsData || []) as TeamMembershipRow[])
          .filter((row) => row.user_id)
          .filter((row) => String(row.status || '').trim().toLowerCase() !== 'disabled')
          .map((row) => {
            const userId = String(row.user_id);
            const profile = profilesByUser.get(userId);
            const currentUserFallback =
              currentUser && currentUser.id === userId
                ? currentUser.displayName || currentUser.email || null
                : null;
            return {
              userId,
              fullName: profile?.fullName || currentUserFallback,
              avatarUrl: profile?.avatarUrl || null,
              role: normalizedRole(row.role),
              status: String(row.status || '').trim().toLowerCase() || 'active',
            };
          });
      } else {
        baseContext.teamDataAvailable = false;
      }
    } catch {
      baseContext.teamDataAvailable = false;
    }
  }

  return baseContext;
}
