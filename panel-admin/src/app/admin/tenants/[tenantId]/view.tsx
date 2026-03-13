'use client';

import Link from 'next/link';
import {useCallback, useEffect, useMemo, useRef, useState} from 'react';
import {useParams, usePathname, useRouter} from 'next/navigation';
import {
  Activity,
  BadgeCheck,
  ChevronDown,
  ChevronUp,
  Download,
  DollarSign,
  FileJson,
  History,
  ListTree,
  Loader2,
  ShieldUser,
  UserRound,
} from 'lucide-react';

import styles from './tenant-detail.module.css';
import {ThemeToggleButton} from '@/components/theme/theme-toggle-button';
import {
  clearStoredAdminAccessToken,
  fetchWithAdminAuth,
  getAdminIdleTimeoutMs,
  getAdminIdleWarningMs,
  getValidAdminAccessToken,
  isExpiredAdminToken,
} from '@/lib/admin-session';

type StatusKind = 'idle' | 'ok' | 'error';

interface MembershipInfo {
  role: string;
  status: string;
  is_admin: boolean;
  enabled: boolean;
  created_at?: string | null;
  updated_at?: string | null;
}

interface UserMetrics {
  runs_total: number;
  runs_completed: number;
  runs_failed: number;
  last_run_at?: string | null;
}

interface CurrencyAmount {
  currency?: string;
  amount?: number;
}

interface RunCostInfo {
  amount?: number;
  currency?: string;
  source?: string;
}

interface RunCreditInfo {
  amount?: number;
  refunded?: boolean;
  net_amount?: number;
  mode?: string;
}

interface RunFilesInfo {
  pauta_filename?: string | null;
  pdf_count?: number;
  pdf_filenames?: string[];
  output_filename?: string | null;
}

interface ActivityRun {
  run_id?: string;
  pipeline_job_id?: string;
  task_id?: string;
  project_id?: string;
  project_name?: string;
  title?: string;
  status?: string;
  started_at?: string | null;
  finished_at?: string | null;
  duration_seconds?: number | null;
  error_message?: string | null;
  cost?: RunCostInfo | null;
  credits?: RunCreditInfo | null;
  files?: RunFilesInfo;
}

interface RunArtifactItem {
  artifact_id: string;
  extraction_id: string;
  source_pdf: string;
  artifact_key: string;
  artifact_class: string;
  artifact_class_label?: string;
  stage?: string;
  label?: string;
  filename?: string;
  retention_days?: number | null;
}

interface ActivityProject {
  project_id?: string;
  project_name?: string;
  runs_total?: number;
  runs_completed?: number;
  runs_failed?: number;
  runs_running?: number;
  last_run_at?: string | null;
  cost_by_currency?: CurrencyAmount[];
  credits_total?: number;
  credits_refunded?: number;
  credits_net?: number;
  credited_runs?: number;
}

interface ActivityLog {
  at?: string | null;
  level?: string;
  event?: string;
  message?: string;
  run_id?: string;
  project_id?: string;
}

interface UserActivitySummary {
  runs_total?: number;
  runs_completed?: number;
  runs_failed?: number;
  runs_running?: number;
  first_run_at?: string | null;
  last_run_at?: string | null;
  total_duration_seconds?: number;
  avg_duration_seconds?: number | null;
  priced_runs?: number;
  cost_by_currency?: CurrencyAmount[];
  credits_total?: number;
  credits_refunded?: number;
  credits_net?: number;
  credited_runs?: number;
}

interface UserActivity {
  summary?: UserActivitySummary;
  project_breakdown?: ActivityProject[];
  recent_runs?: ActivityRun[];
  logs?: ActivityLog[];
}

interface MembershipBreakdown {
  membership_id?: string;
  role?: string;
  status?: string;
  runs_total?: number;
  runs_completed?: number;
  runs_failed?: number;
  runs_running?: number;
  priced_runs?: number;
  cost_by_currency?: CurrencyAmount[];
  credits_total?: number;
  credits_refunded?: number;
  credits_net?: number;
  credited_runs?: number;
}

interface AdminUser {
  membership_id?: string;
  tenant_id: string;
  user_id: string;
  email?: string;
  display_name?: string;
  membership: MembershipInfo;
  metrics: UserMetrics;
  activity?: UserActivity;
  membership_breakdown?: MembershipBreakdown[];
}

interface TenantPlan {
  plan_key: string;
  display_name?: string;
  route_path?: string;
  status?: string;
}

interface BillingAppQuota {
  app_key: string;
  executions_limit: number;
  reruns_limit: number;
  executions_used?: number;
  reruns_used?: number;
}

interface BillingConfig {
  show_client_badge: boolean;
  use_credit_plan: boolean;
  use_custom_plan: boolean;
  custom_plan: {
    apps: Record<string, BillingAppQuota>;
  };
}

interface TenantOverview {
  tenant_id: string;
  name?: string;
  products?: string[];
  plans?: TenantPlan[];
  metadata?: Record<string, unknown>;
  billing_config?: BillingConfig;
  billing_kind?: string;
  client_badge_visible?: boolean;
  quota?: {
    apps?: Array<{
      app_key?: string;
      executions_remaining?: number;
      reruns_remaining?: number;
      executions_limit?: number;
      reruns_limit?: number;
    }>;
  };
  memberships_total: number;
  active_memberships: number;
  admin_memberships: number;
  runs_total: number;
  runs_completed: number;
  runs_failed: number;
  last_run_at?: string | null;
  credits_balance?: number | null;
  credits_enabled?: boolean;
  credits_monthly_granted_now?: number;
}

interface CreditPolicy {
  avg_run_cost_usd?: number;
  runs_sampled?: number;
  recommended_credits_per_execution?: number;
  recommended_starting_credits?: number;
  recommended_monthly_credits?: number;
  target_runs_per_month?: number;
  recommended_tiers?: Array<{
    key?: string;
    label?: string;
    monthly_credits?: number;
    estimated_runs?: number;
  }>;
}

interface OverviewPayload {
  actor?: {id?: string; email?: string};
  users?: AdminUser[];
  tenants?: TenantOverview[];
  tenant_admin_roles?: string[];
  available_plans?: Array<{
    plan_key: string;
    display_name?: string;
    route_path?: string;
  }>;
  credit_policy?: CreditPolicy | null;
  detail?: string;
}

interface RunArtifactsPayload {
  artifacts?: RunArtifactItem[];
  detail?: string;
}

interface RunArtifactsState {
  loaded: boolean;
  loading: boolean;
  error: string;
  artifacts: RunArtifactItem[];
}

const ACTIVE_PLAN_STATUSES = new Set(['active', 'trial']);
const ACTIVITY_EVENTS = ['mousedown', 'keydown', 'touchstart', 'scroll', 'mousemove'] as const;
const CREDIT_REASON_OPTIONS = [
  {value: 'ajuste_admin_recarga_manual', label: 'Recarga manual'},
  {value: 'ajuste_admin_bono_comercial', label: 'Bono comercial'},
  {value: 'ajuste_admin_regularizacion_facturacion', label: 'Regularizacion de facturacion'},
  {value: 'ajuste_admin_correccion_operativa', label: 'Correccion operativa'},
];

function normalizePlanKey(raw: string | null | undefined): string {
  return String(raw || '').trim().toLowerCase();
}

function normalizeBillingConfig(raw: unknown): BillingConfig {
  const source = raw && typeof raw === 'object' ? (raw as Record<string, unknown>) : {};
  const useCreditPlan = Boolean(source.use_credit_plan);
  const useCustomPlan = Boolean(source.use_custom_plan);
  const customPlanRaw =
    source.custom_plan && typeof source.custom_plan === 'object'
      ? (source.custom_plan as Record<string, unknown>)
      : {};
  const appsRaw =
    customPlanRaw.apps && typeof customPlanRaw.apps === 'object'
      ? (customPlanRaw.apps as Record<string, unknown>)
      : {};
  const normalizedApps: Record<string, BillingAppQuota> = {};
  for (const [rawKey, rawPayload] of Object.entries(appsRaw)) {
    const appKey = normalizePlanKey(rawKey);
    if (!appKey || !rawPayload || typeof rawPayload !== 'object') continue;
    const payload = rawPayload as Record<string, unknown>;
    normalizedApps[appKey] = {
      app_key: appKey,
      executions_limit: Math.max(0, Number(payload.executions_limit || 0)),
      reruns_limit: Math.max(0, Number(payload.reruns_limit || 0)),
      executions_used: Math.max(0, Number(payload.executions_used || 0)),
      reruns_used: Math.max(0, Number(payload.reruns_used || 0)),
    };
  }
  return {
    show_client_badge: source.show_client_badge !== false,
    use_credit_plan: useCustomPlan ? false : useCreditPlan,
    use_custom_plan: useCustomPlan,
    custom_plan: {
      apps: normalizedApps,
    },
  };
}

function formatDate(raw?: string | null): string {
  if (!raw) return '-';
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return raw;
  return parsed.toLocaleString();
}

function formatMembershipRole(raw?: string | null): string {
  const role = String(raw || '').trim().toLowerCase();
  if (!role) return '-';
  if (role === 'owner') return 'Owner';
  if (role === 'editor' || role === 'admin') return 'Admin';
  if (role === 'viewer' || role === 'member') return 'Member';
  return role;
}

function membershipRoleWeight(raw?: string | null): number {
  const role = String(raw || '').trim().toLowerCase();
  if (role === 'owner') return 0;
  if (role === 'editor' || role === 'admin') return 1;
  if (role === 'viewer' || role === 'member') return 2;
  return 9;
}

function getDisplayPlanLabel(plan: {display_name?: string; plan_key?: string} | undefined): string {
  const display = String(plan?.display_name || '').trim();
  return display || 'Plan';
}

function getActiveTenantPlanSet(tenant: TenantOverview | null): Set<string> {
  if (!tenant) return new Set<string>();

  const fromPlans = (tenant.plans || [])
    .filter((plan) => {
      const status = String(plan.status || '').trim().toLowerCase();
      return !status || ACTIVE_PLAN_STATUSES.has(status);
    })
    .map((plan) => normalizePlanKey(plan.plan_key))
    .filter(Boolean);

  const fromLegacyProducts = (tenant.products || [])
    .map((plan) => normalizePlanKey(plan))
    .filter(Boolean);

  return new Set([...fromPlans, ...fromLegacyProducts]);
}

function getRequestedPlanSet(tenant: TenantOverview | null): Set<string> {
  if (!tenant?.metadata || typeof tenant.metadata !== 'object') return new Set<string>();
  const raw = tenant.metadata['requested_plan_keys'];
  if (!Array.isArray(raw)) return new Set<string>();

  return new Set(
    raw
      .map((value) => normalizePlanKey(String(value || '')))
      .filter(Boolean),
  );
}

function formatDuration(rawSeconds?: number | null): string {
  const seconds = Number(rawSeconds);
  if (!Number.isFinite(seconds) || seconds < 0) return '-';
  if (seconds < 60) return `${Math.round(seconds)}s`;
  const whole = Math.round(seconds);
  const hours = Math.floor(whole / 3600);
  const minutes = Math.floor((whole % 3600) / 60);
  const secs = whole % 60;
  if (hours > 0) return `${hours}h ${minutes}m ${secs}s`;
  return `${minutes}m ${secs}s`;
}

function formatCurrencyAmount(amount?: number | null, currency?: string | null): string {
  const parsed = Number(amount);
  const value = Number.isFinite(parsed) ? parsed : 0;
  const normalizedCurrency = String(currency || 'USD').trim().toUpperCase() || 'USD';
  try {
    return new Intl.NumberFormat('es-ES', {
      style: 'currency',
      currency: normalizedCurrency,
      maximumFractionDigits: 4,
    }).format(value);
  } catch {
    return `${value.toFixed(4)} ${normalizedCurrency}`;
  }
}

function formatCredits(raw?: number | null): string {
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) return '-';
  return new Intl.NumberFormat('es-ES', {maximumFractionDigits: 0}).format(Math.round(parsed));
}

function formatCostByCurrency(costs?: CurrencyAmount[]): string {
  if (!Array.isArray(costs) || costs.length === 0) return 'Sin trazabilidad';
  return costs
    .map((entry) => formatCurrencyAmount(entry.amount, entry.currency || 'USD'))
    .join(' | ');
}

function formatRunStatus(raw?: string | null): string {
  const status = String(raw || '').trim().toLowerCase();
  if (!status) return 'Sin estado';
  if (status === 'completed') return 'Completada';
  if (status === 'failed') return 'Fallida';
  if (status === 'cancelled') return 'Cancelada';
  if (status === 'running') return 'En curso';
  if (status === 'queued' || status === 'in_progress') return 'En cola';
  return status;
}

function getRunIdentityKey(run: ActivityRun): string {
  return (
    String(run.run_id || '').trim() ||
    String(run.pipeline_job_id || '').trim() ||
    String(run.task_id || '').trim() ||
    `${String(run.project_id || '').trim()}::${String(run.started_at || '').trim()}`
  );
}

function extractDownloadFilename(rawHeader: string | null): string | null {
  const header = String(rawHeader || '').trim();
  if (!header) return null;
  const utfMatch = header.match(/filename\*=UTF-8''([^;]+)/i);
  if (utfMatch && utfMatch[1]) {
    try {
      return decodeURIComponent(utfMatch[1]).trim();
    } catch {
      return utfMatch[1].trim();
    }
  }
  const simpleMatch = header.match(/filename=\"?([^\";]+)\"?/i);
  if (!simpleMatch || !simpleMatch[1]) return null;
  return simpleMatch[1].trim();
}

export function AdminTenantDetailView() {
  const params = useParams<{tenantId: string}>();
  const router = useRouter();
  const pathname = usePathname();

  const tenantId = String(params?.tenantId || '').trim();

  const [sessionReady, setSessionReady] = useState(false);
  const [tenant, setTenant] = useState<TenantOverview | null>(null);
  const [users, setUsers] = useState<AdminUser[]>([]);
  const [availablePlans, setAvailablePlans] = useState<
    Array<{plan_key: string; display_name?: string; route_path?: string}>
  >([]);
  const [creditPolicy, setCreditPolicy] = useState<CreditPolicy | null>(null);
  const [creditDeltaInput, setCreditDeltaInput] = useState('100');
  const [creditReasonInput, setCreditReasonInput] = useState(CREDIT_REASON_OPTIONS[0].value);
  const [adjustingCredits, setAdjustingCredits] = useState(false);
  const [billingConfigDraft, setBillingConfigDraft] = useState<BillingConfig | null>(null);
  const [savingBillingConfig, setSavingBillingConfig] = useState(false);
  const [actor, setActor] = useState<{id?: string; email?: string}>({});
  const [loading, setLoading] = useState(false);
  const [statusText, setStatusText] = useState('Cargando cliente...');
  const [statusKind, setStatusKind] = useState<StatusKind>('idle');
  const [toast, setToast] = useState('');
  const [selectedUserId, setSelectedUserId] = useState('');
  const [expandedRunKey, setExpandedRunKey] = useState('');
  const [runArtifactsByRunId, setRunArtifactsByRunId] = useState<Record<string, RunArtifactsState>>({});
  const toastTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [idleWarningVisible, setIdleWarningVisible] = useState(false);
  const [idleCountdownSeconds, setIdleCountdownSeconds] = useState(0);
  const lastActivityRef = useRef(Date.now());
  const lastRefreshAttemptRef = useRef(0);

  const showToast = useCallback((text: string) => {
    setToast(text);
    if (toastTimer.current) clearTimeout(toastTimer.current);
    toastTimer.current = setTimeout(() => setToast(''), 2200);
  }, []);

  useEffect(() => {
    return () => {
      if (toastTimer.current) clearTimeout(toastTimer.current);
    };
  }, []);

  const setStatus = useCallback((text: string, kind: StatusKind = 'idle') => {
    setStatusText(text);
    setStatusKind(kind);
  }, []);

  const redirectToAdminLogin = useCallback((reason?: string) => {
    clearStoredAdminAccessToken();
    const nextPath = encodeURIComponent(pathname || '/memberships');
    const reasonQuery = reason ? `&reason=${encodeURIComponent(reason)}` : '';
    const logoutHref = `/logout?next=${nextPath}${reasonQuery}`;
    if (typeof window !== 'undefined') {
      window.location.assign(logoutHref);
      return;
    }
    router.replace(logoutHref);
  }, [pathname, router]);

  useEffect(() => {
    let cancelled = false;
    async function bootstrapSession() {
      try {
        await getValidAdminAccessToken();
        if (!cancelled) setSessionReady(true);
      } catch {
        if (!cancelled) redirectToAdminLogin();
      }
    }
    void bootstrapSession();
    return () => {
      cancelled = true;
    };
  }, [redirectToAdminLogin]);

  const idleTimeoutMs = getAdminIdleTimeoutMs();
  const idleWarningMs = Math.min(getAdminIdleWarningMs(), Math.max(1000, idleTimeoutMs - 1000));

  const markActivity = useCallback(() => {
    if (!sessionReady) return;
    lastActivityRef.current = Date.now();
    setIdleWarningVisible(false);
    setIdleCountdownSeconds(0);

    const now = Date.now();
    if (now - lastRefreshAttemptRef.current < 30_000) return;
    lastRefreshAttemptRef.current = now;
    void getValidAdminAccessToken().catch(() => redirectToAdminLogin('session_expired'));
  }, [redirectToAdminLogin, sessionReady]);

  useEffect(() => {
    if (!sessionReady) return;
    for (const eventName of ACTIVITY_EVENTS) {
      window.addEventListener(eventName, markActivity, {passive: true});
    }
    return () => {
      for (const eventName of ACTIVITY_EVENTS) {
        window.removeEventListener(eventName, markActivity);
      }
    };
  }, [markActivity, sessionReady]);

  useEffect(() => {
    if (!sessionReady) return;
    const timer = window.setInterval(() => {
      const remainingMs = idleTimeoutMs - (Date.now() - lastActivityRef.current);
      if (remainingMs <= 0) {
        redirectToAdminLogin('session_expired');
        return;
      }
      if (remainingMs <= idleWarningMs) {
        setIdleWarningVisible(true);
        setIdleCountdownSeconds(Math.max(1, Math.ceil(remainingMs / 1000)));
      } else {
        setIdleWarningVisible(false);
        setIdleCountdownSeconds(0);
      }
    }, 1000);

    return () => window.clearInterval(timer);
  }, [idleTimeoutMs, idleWarningMs, redirectToAdminLogin, sessionReady]);

  const continueSession = useCallback(() => {
    markActivity();
    void getValidAdminAccessToken({forceRefresh: true}).catch(() =>
      redirectToAdminLogin('session_expired'),
    );
  }, [markActivity, redirectToAdminLogin]);

  const loadTenant = useCallback(async () => {
    if (!sessionReady) return;
    if (!tenantId) {
      setStatus('Falta tenant_id en la ruta.', 'error');
      return;
    }

    setLoading(true);
    setStatus('Cargando detalle del cliente...', 'idle');

    try {
      const response = await fetchWithAdminAuth(
        `/api/admin/tenant-overview?tenant_id=${encodeURIComponent(tenantId)}`,
      );
      const payload = (await response.json().catch(() => ({}))) as OverviewPayload;
      if (!response.ok) {
        if (isExpiredAdminToken(payload.detail, response.status)) {
          setStatus('Sesion admin expirada. Redirigiendo al login...', 'error');
          redirectToAdminLogin('session_expired');
          return;
        }
        throw new Error(payload.detail || `HTTP ${response.status}`);
      }

      const tenantNode =
        (payload.tenants || []).find((item) => item.tenant_id === tenantId) ||
        (payload.tenants || [])[0] ||
        null;
      setTenant(tenantNode);
      setBillingConfigDraft(normalizeBillingConfig(tenantNode?.billing_config));
      setUsers((payload.users || []).filter((user) => user.tenant_id === tenantId));
      setAvailablePlans(Array.isArray(payload.available_plans) ? payload.available_plans : []);
      setCreditPolicy(payload.credit_policy || null);
      setActor(payload.actor || {});
      setStatus('Detalle cargado.', 'ok');
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Error cargando detalle';
      setStatus(message, 'error');
    } finally {
      setLoading(false);
    }
  }, [redirectToAdminLogin, sessionReady, setStatus, tenantId]);

  useEffect(() => {
    void loadTenant();
  }, [loadTenant]);

  const activePlanSet = useMemo(() => getActiveTenantPlanSet(tenant), [tenant]);
  const requestedPlanSet = useMemo(() => getRequestedPlanSet(tenant), [tenant]);
  const billingMode = useMemo<'credits' | 'custom' | 'none'>(() => {
    if (billingConfigDraft?.use_custom_plan) return 'custom';
    if (billingConfigDraft?.use_credit_plan) return 'credits';
    return 'none';
  }, [billingConfigDraft?.use_credit_plan, billingConfigDraft?.use_custom_plan]);
  const billingAppCatalog = useMemo(() => {
    const availablePlanMap = new Map(
      availablePlans.map((plan) => [normalizePlanKey(plan.plan_key), getDisplayPlanLabel(plan)]),
    );
    const configuredKeys = Object.keys(billingConfigDraft?.custom_plan?.apps || {}).map((key) =>
      normalizePlanKey(key),
    );
    const activeKeys = Array.from(activePlanSet.values()).map((key) => normalizePlanKey(key));
    const keys = new Set([...activeKeys, ...configuredKeys].filter(Boolean));
    if (keys.size === 0) {
      for (const key of availablePlanMap.keys()) keys.add(normalizePlanKey(key));
    }
    return Array.from(keys.values())
      .filter(Boolean)
      .sort((a, b) => a.localeCompare(b))
      .map((key) => ({
        key,
        label: availablePlanMap.get(key) || key,
      }));
  }, [activePlanSet, availablePlans, billingConfigDraft?.custom_plan?.apps]);

  useEffect(() => {
    if (!billingConfigDraft) return;
    if (billingAppCatalog.length === 0) return;
    setBillingConfigDraft((previous) => {
      if (!previous) return previous;
      const currentApps = previous.custom_plan?.apps || {};
      const nextApps: Record<string, BillingAppQuota> = {};
      let changed = false;
      for (const app of billingAppCatalog) {
        const existing = currentApps[app.key];
        if (existing) {
          nextApps[app.key] = {
            ...existing,
            app_key: app.key,
            executions_limit: Math.max(0, Number(existing.executions_limit || 0)),
            reruns_limit: Math.max(0, Number(existing.reruns_limit || 0)),
            executions_used: Math.max(0, Number(existing.executions_used || 0)),
            reruns_used: Math.max(0, Number(existing.reruns_used || 0)),
          };
          continue;
        }
        changed = true;
        nextApps[app.key] = {
          app_key: app.key,
          executions_limit: 0,
          reruns_limit: 0,
          executions_used: 0,
          reruns_used: 0,
        };
      }
      if (!changed && Object.keys(currentApps).length === Object.keys(nextApps).length) return previous;
      return {
        ...previous,
        custom_plan: {
          ...previous.custom_plan,
          apps: nextApps,
        },
      };
    });
  }, [billingAppCatalog, billingConfigDraft]);

  const tenantUsers = useMemo(() => {
    return [...users].sort((a, b) => {
      const byRole = membershipRoleWeight(a.membership.role) - membershipRoleWeight(b.membership.role);
      if (byRole !== 0) return byRole;
      const aName = String(a.display_name || a.email || a.user_id || '').toLowerCase();
      const bName = String(b.display_name || b.email || b.user_id || '').toLowerCase();
      return aName.localeCompare(bName);
    });
  }, [users]);

  useEffect(() => {
    if (tenantUsers.length === 0) {
      setSelectedUserId('');
      return;
    }
    setSelectedUserId((previous) => {
      if (previous && tenantUsers.some((user) => user.user_id === previous)) {
        return previous;
      }
      return tenantUsers[0].user_id;
    });
  }, [tenantUsers]);

  useEffect(() => {
    setExpandedRunKey('');
    setRunArtifactsByRunId({});
  }, [selectedUserId]);

  const selectedUser = useMemo(() => {
    if (!selectedUserId) return null;
    return tenantUsers.find((user) => user.user_id === selectedUserId) || null;
  }, [selectedUserId, tenantUsers]);

  const selectedActivity = useMemo<UserActivity | null>(() => {
    if (!selectedUser) return null;
    if (selectedUser.activity) return selectedUser.activity;

    return {
      summary: {
        runs_total: Number(selectedUser.metrics.runs_total || 0),
        runs_completed: Number(selectedUser.metrics.runs_completed || 0),
        runs_failed: Number(selectedUser.metrics.runs_failed || 0),
        runs_running: 0,
        first_run_at: null,
        last_run_at: selectedUser.metrics.last_run_at || null,
        total_duration_seconds: 0,
        avg_duration_seconds: null,
        priced_runs: 0,
        cost_by_currency: [],
        credits_total: 0,
        credits_refunded: 0,
        credits_net: 0,
        credited_runs: 0,
      },
      project_breakdown: [],
      recent_runs: [],
      logs: [],
    };
  }, [selectedUser]);

  const selectedMembershipBreakdown = useMemo<MembershipBreakdown[]>(() => {
    if (!selectedUser || !selectedActivity) return [];

    if (Array.isArray(selectedUser.membership_breakdown) && selectedUser.membership_breakdown.length > 0) {
      return selectedUser.membership_breakdown;
    }

    const summary = selectedActivity.summary || {};
    return [
      {
        membership_id: selectedUser.membership_id,
        role: selectedUser.membership.role,
        status: selectedUser.membership.status,
        runs_total: Number(summary.runs_total || 0),
        runs_completed: Number(summary.runs_completed || 0),
        runs_failed: Number(summary.runs_failed || 0),
        runs_running: Number(summary.runs_running || 0),
        priced_runs: Number(summary.priced_runs || 0),
        credits_total: Number(summary.credits_total || 0),
        credits_refunded: Number(summary.credits_refunded || 0),
        credits_net: Number(summary.credits_net || 0),
        credited_runs: Number(summary.credited_runs || 0),
        cost_by_currency: Array.isArray(summary.cost_by_currency) ? summary.cost_by_currency : [],
      },
    ];
  }, [selectedActivity, selectedUser]);

  const metrics = useMemo(() => {
    const clientMemberships = Math.max(0, Number(tenant?.memberships_total || 0));
    const activeClientMemberships = Math.max(0, Number(tenant?.active_memberships || 0));
    const inactiveClientMemberships = Math.max(0, clientMemberships - activeClientMemberships);
    const usersWithActivity = tenantUsers.filter((user) => Number(user.metrics.runs_total || 0) > 0).length;
    return {
      clientMemberships,
      activeClientMemberships,
      inactiveClientMemberships,
      activePlans: activePlanSet.size,
      runs: Number(tenant?.runs_total || 0),
      runsOk: Number(tenant?.runs_completed || 0),
      runsKo: Number(tenant?.runs_failed || 0),
      usersWithActivity,
      creditBalance: typeof tenant?.credits_balance === 'number' ? tenant.credits_balance : null,
    };
  }, [activePlanSet.size, tenant, tenantUsers]);
  const creditPlanEnabled = billingMode === 'credits';

  const onTogglePlan = useCallback(
    async (planKey: string, enabled: boolean) => {
      if (!tenant) return;

      setStatus(`${enabled ? 'Activando' : 'Desactivando'} plan...`, 'idle');
      const response = await fetchWithAdminAuth('/api/admin/tenant-subscriptions/status', {
        method: 'PATCH',
        headers: {
          'content-type': 'application/json',
        },
        body: JSON.stringify({
          tenant_id: tenant.tenant_id,
          plan_key: planKey,
          enabled,
        }),
      });
      const payload = (await response.json().catch(() => ({}))) as {detail?: string};
      if (!response.ok) {
        if (isExpiredAdminToken(payload.detail, response.status)) {
          setStatus('Sesion admin expirada. Redirigiendo al login...', 'error');
          redirectToAdminLogin('session_expired');
          return;
        }
        throw new Error(payload.detail || `HTTP ${response.status}`);
      }

      showToast(`Plan ${enabled ? 'activado' : 'desactivado'}`);
      await loadTenant();
    },
    [loadTenant, redirectToAdminLogin, setStatus, showToast, tenant],
  );

  const onAdjustCredits = useCallback(async () => {
    if (!tenant) return;
    if (!creditPlanEnabled) {
      setStatus('El plan de creditos esta desactivado para este cliente.', 'error');
      return;
    }
    const delta = Number.parseInt(creditDeltaInput, 10);
    if (!Number.isFinite(delta) || delta === 0) {
      setStatus('Introduce un ajuste de creditos distinto de 0.', 'error');
      return;
    }

    setAdjustingCredits(true);
    setStatus('Aplicando ajuste de creditos...', 'idle');
    try {
      const response = await fetchWithAdminAuth('/api/admin/tenant-credits/adjust', {
        method: 'PATCH',
        headers: {
          'content-type': 'application/json',
        },
        body: JSON.stringify({
          tenant_id: tenant.tenant_id,
          delta_credits: delta,
          reason: creditReasonInput || 'ajuste_admin',
        }),
      });
      const payload = (await response.json().catch(() => ({}))) as {detail?: string};
      if (!response.ok) {
        if (isExpiredAdminToken(payload.detail, response.status)) {
          setStatus('Sesion admin expirada. Redirigiendo al login...', 'error');
          redirectToAdminLogin('session_expired');
          return;
        }
        throw new Error(payload.detail || `HTTP ${response.status}`);
      }

      showToast(`Creditos ${delta > 0 ? 'sumados' : 'descontados'}: ${Math.abs(delta)}`);
      setCreditDeltaInput('100');
      await loadTenant();
    } catch (error) {
      const message = error instanceof Error ? error.message : 'No se pudo ajustar creditos.';
      setStatus(message, 'error');
    } finally {
      setAdjustingCredits(false);
    }
  }, [
    creditDeltaInput,
    creditPlanEnabled,
    creditReasonInput,
    loadTenant,
    redirectToAdminLogin,
    setStatus,
    showToast,
    tenant,
  ]);

  const onSaveBillingConfig = useCallback(async () => {
    if (!tenant || !billingConfigDraft) return;
    setSavingBillingConfig(true);
    setStatus('Guardando configuracion de facturacion...', 'idle');
    try {
      const customPlanApps = billingAppCatalog.map((app) => {
        const source = billingConfigDraft.custom_plan.apps[app.key] || {
          executions_limit: 0,
          reruns_limit: 0,
        };
        return {
          app_key: app.key,
          executions_limit: Math.max(0, Number(source.executions_limit || 0)),
          reruns_limit: Math.max(0, Number(source.reruns_limit || 0)),
        };
      });
      const response = await fetchWithAdminAuth('/api/admin/tenant-billing-config', {
        method: 'PATCH',
        headers: {
          'content-type': 'application/json',
        },
        body: JSON.stringify({
          tenant_id: tenant.tenant_id,
          show_client_badge: Boolean(billingConfigDraft.show_client_badge),
          use_credit_plan: Boolean(billingConfigDraft.use_credit_plan),
          use_custom_plan: Boolean(billingConfigDraft.use_custom_plan),
          custom_plan_apps: customPlanApps,
        }),
      });
      const payload = (await response.json().catch(() => ({}))) as {detail?: string};
      if (!response.ok) {
        if (isExpiredAdminToken(payload.detail, response.status)) {
          setStatus('Sesion admin expirada. Redirigiendo al login...', 'error');
          redirectToAdminLogin('session_expired');
          return;
        }
        throw new Error(payload.detail || `HTTP ${response.status}`);
      }
      showToast('Configuracion de facturacion guardada');
      await loadTenant();
    } catch (error) {
      const message = error instanceof Error ? error.message : 'No se pudo guardar la configuracion.';
      setStatus(message, 'error');
    } finally {
      setSavingBillingConfig(false);
    }
  }, [
    billingAppCatalog,
    billingConfigDraft,
    loadTenant,
    redirectToAdminLogin,
    setStatus,
    showToast,
    tenant,
  ]);

  const loadRunArtifacts = useCallback(
    async (run: ActivityRun) => {
      const runId = String(run.run_id || '').trim();
      if (!runId || !tenantId) return;

      const currentState = runArtifactsByRunId[runId];
      if (currentState?.loading) return;

      setRunArtifactsByRunId((previous) => ({
        ...previous,
        [runId]: {
          loaded: false,
          loading: true,
          error: '',
          artifacts: previous[runId]?.artifacts || [],
        },
      }));

      try {
        const response = await fetchWithAdminAuth(
          `/api/admin/run-artifacts?run_id=${encodeURIComponent(runId)}&tenant_id=${encodeURIComponent(tenantId)}`,
        );
        const payload = (await response.json().catch(() => ({}))) as RunArtifactsPayload;
        if (!response.ok) {
          if (isExpiredAdminToken(payload.detail, response.status)) {
            setStatus('Sesion admin expirada. Redirigiendo al login...', 'error');
            redirectToAdminLogin('session_expired');
            return;
          }
          throw new Error(payload.detail || `HTTP ${response.status}`);
        }

        const artifacts = Array.isArray(payload.artifacts) ? payload.artifacts : [];
        setRunArtifactsByRunId((previous) => ({
          ...previous,
          [runId]: {
            loaded: true,
            loading: false,
            error: '',
            artifacts,
          },
        }));
      } catch (error) {
        const message = error instanceof Error ? error.message : 'No se pudieron cargar los artefactos JSON.';
        setRunArtifactsByRunId((previous) => ({
          ...previous,
          [runId]: {
            loaded: true,
            loading: false,
            error: message,
            artifacts: [],
          },
        }));
      }
    },
    [redirectToAdminLogin, runArtifactsByRunId, setStatus, tenantId],
  );

  const downloadRunArtifact = useCallback(
    async (run: ActivityRun, artifact: RunArtifactItem) => {
      const runId = String(run.run_id || '').trim();
      const extractionId = String(artifact.extraction_id || '').trim();
      const artifactClass = String(artifact.artifact_class || '').trim();
      const artifactKey = String(artifact.artifact_key || '').trim();
      if (!runId || !tenantId || !extractionId || !artifactClass || !artifactKey) {
        showToast('No se pudo identificar el archivo a descargar.');
        return;
      }

      try {
        const query = new URLSearchParams({
          run_id: runId,
          tenant_id: tenantId,
          extraction_id: extractionId,
          artifact_class: artifactClass,
          artifact_key: artifactKey,
        });
        const response = await fetchWithAdminAuth(`/api/admin/run-artifact/download?${query.toString()}`);
        if (!response.ok) {
          const payload = (await response.json().catch(() => ({}))) as {detail?: string};
          if (isExpiredAdminToken(payload.detail, response.status)) {
            setStatus('Sesion admin expirada. Redirigiendo al login...', 'error');
            redirectToAdminLogin('session_expired');
            return;
          }
          throw new Error(payload.detail || `HTTP ${response.status}`);
        }

        const blob = await response.blob();
        const objectUrl = window.URL.createObjectURL(blob);
        const anchor = document.createElement('a');
        const responseFilename = extractDownloadFilename(response.headers.get('content-disposition'));
        anchor.href = objectUrl;
        anchor.download = responseFilename || artifact.filename || `${artifact.artifact_key || 'artifact'}.json`;
        document.body.appendChild(anchor);
        anchor.click();
        anchor.remove();
        window.setTimeout(() => window.URL.revokeObjectURL(objectUrl), 0);
      } catch (error) {
        const message = error instanceof Error ? error.message : 'No se pudo descargar el artefacto.';
        setStatus(message, 'error');
      }
    },
    [redirectToAdminLogin, setStatus, showToast, tenantId],
  );

  const selectedSummary = useMemo(() => selectedActivity?.summary || {}, [selectedActivity]);
  const selectedProjects = useMemo<ActivityProject[]>(
    () =>
      Array.isArray(selectedActivity?.project_breakdown)
        ? selectedActivity?.project_breakdown || []
        : [],
    [selectedActivity],
  );
  const selectedRuns = useMemo<ActivityRun[]>(
    () => (Array.isArray(selectedActivity?.recent_runs) ? selectedActivity?.recent_runs || [] : []),
    [selectedActivity],
  );
  const selectedLogs = useMemo<ActivityLog[]>(
    () => (Array.isArray(selectedActivity?.logs) ? selectedActivity?.logs || [] : []),
    [selectedActivity],
  );

  const creditDeltaOptions = useMemo(() => {
    const recommendedStart = Math.max(100, Number(creditPolicy?.recommended_starting_credits || 0));
    const recommendedExec = Math.max(10, Number(creditPolicy?.recommended_credits_per_execution || 0));
    const options = [
      {value: String(recommendedStart), label: `+${recommendedStart} (Saldo inicial recomendado)`},
      {value: String(Math.max(100, Math.round(recommendedStart / 2))), label: `+${Math.max(100, Math.round(recommendedStart / 2))} (Media carga)`},
      {value: String(Math.max(50, recommendedExec * 5)), label: `+${Math.max(50, recommendedExec * 5)} (Pack 5 ejecuciones)`},
      {value: String(-Math.max(50, recommendedExec * 5)), label: `-${Math.max(50, recommendedExec * 5)} (Descuento 5 ejecuciones)`},
      {value: String(-Math.max(100, Math.round(recommendedStart / 4))), label: `-${Math.max(100, Math.round(recommendedStart / 4))} (Regularizacion)`},
    ];
    return options.filter((option, index, self) => self.findIndex((candidate) => candidate.value === option.value) === index);
  }, [creditPolicy]);

  useEffect(() => {
    if (creditDeltaOptions.some((option) => option.value === creditDeltaInput)) return;
    setCreditDeltaInput(creditDeltaOptions[0]?.value || '100');
  }, [creditDeltaInput, creditDeltaOptions]);

  useEffect(() => {
    if (!expandedRunKey) return;
    const expandedRun = selectedRuns.find((run) => getRunIdentityKey(run) === expandedRunKey);
    if (!expandedRun) return;
    const runId = String(expandedRun.run_id || '').trim();
    if (!runId) return;
    const artifactState = runArtifactsByRunId[runId];
    if (artifactState?.loaded || artifactState?.loading) return;
    void loadRunArtifacts(expandedRun);
  }, [expandedRunKey, loadRunArtifacts, runArtifactsByRunId, selectedRuns]);

  const backHref = '/memberships';

  return (
    <main className={styles.shell}>
      <div className={styles.wrap}>
        <section className={styles.hero}>
          <div style={{display: 'flex', justifyContent: 'flex-end', marginBottom: 8}}>
            <ThemeToggleButton showLabel className='theme-toggle-btn--inline' />
          </div>
          <div className={styles.heroTop}>
            <Link href={backHref} className={styles.backLink}>
              Volver al resumen
            </Link>
            <button type='button' className={styles.reload} onClick={() => void loadTenant()} disabled={loading}>
              {loading ? 'Cargando...' : 'Recargar'}
            </button>
          </div>
          <h1 className={styles.title}>{tenant?.name || 'Cliente sin nombre'}</h1>
          <p className={styles.subtitle}>Panel individual con metricas de usuarios, membresias, ejecuciones y logs.</p>
          <div className={styles.pills}>
            <span className={styles.pill}>
              Planes activos: <strong>{metrics.activePlans}</strong>
            </span>
            <span className={styles.pill}>
              Solicitados: <strong>{requestedPlanSet.size}</strong>
            </span>
            <span className={styles.pill}>
              Ultima actividad: <strong>{formatDate(tenant?.last_run_at)}</strong>
            </span>
            <span className={styles.pill}>
              Creditos restantes: <strong>{formatCredits(metrics.creditBalance)}</strong>
            </span>
            <span className={styles.pill}>
              Administrador: <strong>{actor.email || actor.id || '-'}</strong>
            </span>
          </div>
          <div
            className={
              statusKind === 'error'
                ? `${styles.status} ${styles.statusError}`
                : statusKind === 'ok'
                  ? `${styles.status} ${styles.statusOk}`
                  : styles.status
            }
          >
            {statusText}
          </div>
        </section>

        <section className={styles.kpis}>
          {[
            ['Membresias cliente', metrics.clientMemberships],
            ['Activas cliente', metrics.activeClientMemberships],
            ['Inactivas cliente', metrics.inactiveClientMemberships],
            ['Usuarios con actividad', metrics.usersWithActivity],
            ['Ejecuciones', metrics.runs],
            ['Saldo creditos', formatCredits(metrics.creditBalance)],
            ['OK / KO', `${metrics.runsOk} / ${metrics.runsKo}`],
          ].map(([label, value]) => (
            <article key={String(label)} className={styles.kpiCard}>
              <span className={styles.kpiLabel}>{String(label)}</span>
              <div className={styles.kpiValue}>{String(value)}</div>
            </article>
          ))}
        </section>

        <section className={styles.section}>
          <h2 className={styles.sectionTitle}>Facturacion del cliente</h2>
          <div className={styles.billingSection}>
            <div className={styles.billingCard}>
              <label className={styles.billingToggleRow}>
                <input
                  type='checkbox'
                  checked={Boolean(billingConfigDraft?.show_client_badge)}
                  onChange={(event) =>
                    setBillingConfigDraft((previous) =>
                      previous
                        ? {
                            ...previous,
                            show_client_badge: event.target.checked,
                          }
                        : previous,
                    )
                  }
                  disabled={savingBillingConfig}
                />
                <span>Mostrar indicador de consumo al cliente</span>
              </label>
              <label className={styles.creditField}>
                Modo de consumo
                <select
                  className={styles.creditInput}
                  value={billingMode}
                  onChange={(event) => {
                    const nextMode = String(event.target.value || 'none').trim().toLowerCase();
                    setBillingConfigDraft((previous) =>
                      previous
                        ? {
                            ...previous,
                            use_credit_plan: nextMode === 'credits',
                            use_custom_plan: nextMode === 'custom',
                          }
                        : previous,
                    );
                  }}
                  disabled={savingBillingConfig}
                >
                  <option value='credits'>Plan de creditos</option>
                  <option value='custom'>Plan personalizado (N ejecuciones / M re ejecuciones)</option>
                  <option value='none'>Sin limites</option>
                </select>
              </label>
              <div className={styles.billingHint}>
                {billingMode === 'credits'
                  ? 'Se consumen creditos del saldo.'
                  : billingMode === 'custom'
                    ? 'Se consumen cupos por aplicacion (ejecucion y re ejecucion).'
                    : 'Sin creditos ni cupos: uso ilimitado en apps activas.'}
              </div>
            </div>
            <div className={styles.billingCard}>
              <div className={styles.billingGridTitle}>Limites por aplicacion</div>
              <div className={styles.billingGrid}>
                {billingAppCatalog.length === 0 ? (
                  <div className={styles.emptyInline}>No hay aplicaciones detectadas para configurar.</div>
                ) : (
                  billingAppCatalog.map((app) => {
                    const appQuota = billingConfigDraft?.custom_plan?.apps?.[app.key];
                    const executionsUsed = Math.max(0, Number(appQuota?.executions_used || 0));
                    const rerunsUsed = Math.max(0, Number(appQuota?.reruns_used || 0));
                    return (
                      <div key={app.key} className={styles.billingAppRow}>
                        <div className={styles.billingAppTitle}>{app.label}</div>
                        <div className={styles.billingInputGroup}>
                          <label className={styles.creditField}>
                            N ejecuciones
                            <input
                              type='number'
                              min={0}
                              className={styles.creditInput}
                              value={Math.max(0, Number(appQuota?.executions_limit || 0))}
                              onChange={(event) => {
                                const value = Math.max(0, Number.parseInt(event.target.value || '0', 10) || 0);
                                setBillingConfigDraft((previous) =>
                                  previous
                                    ? {
                                        ...previous,
                                        custom_plan: {
                                          ...previous.custom_plan,
                                          apps: {
                                            ...previous.custom_plan.apps,
                                            [app.key]: {
                                              app_key: app.key,
                                              executions_limit: value,
                                              reruns_limit: Math.max(
                                                0,
                                                Number(previous.custom_plan.apps?.[app.key]?.reruns_limit || 0),
                                              ),
                                              executions_used: Math.max(
                                                0,
                                                Number(previous.custom_plan.apps?.[app.key]?.executions_used || 0),
                                              ),
                                              reruns_used: Math.max(
                                                0,
                                                Number(previous.custom_plan.apps?.[app.key]?.reruns_used || 0),
                                              ),
                                            },
                                          },
                                        },
                                      }
                                    : previous,
                                );
                              }}
                              disabled={savingBillingConfig}
                            />
                          </label>
                          <label className={styles.creditField}>
                            M re ejecuciones
                            <input
                              type='number'
                              min={0}
                              className={styles.creditInput}
                              value={Math.max(0, Number(appQuota?.reruns_limit || 0))}
                              onChange={(event) => {
                                const value = Math.max(0, Number.parseInt(event.target.value || '0', 10) || 0);
                                setBillingConfigDraft((previous) =>
                                  previous
                                    ? {
                                        ...previous,
                                        custom_plan: {
                                          ...previous.custom_plan,
                                          apps: {
                                            ...previous.custom_plan.apps,
                                            [app.key]: {
                                              app_key: app.key,
                                              executions_limit: Math.max(
                                                0,
                                                Number(previous.custom_plan.apps?.[app.key]?.executions_limit || 0),
                                              ),
                                              reruns_limit: value,
                                              executions_used: Math.max(
                                                0,
                                                Number(previous.custom_plan.apps?.[app.key]?.executions_used || 0),
                                              ),
                                              reruns_used: Math.max(
                                                0,
                                                Number(previous.custom_plan.apps?.[app.key]?.reruns_used || 0),
                                              ),
                                            },
                                          },
                                        },
                                      }
                                    : previous,
                                );
                              }}
                              disabled={savingBillingConfig}
                            />
                          </label>
                        </div>
                        <div className={styles.billingUsage}>
                          Usadas: {executionsUsed} ejec. | {rerunsUsed} re ejec.
                        </div>
                      </div>
                    );
                  })
                )}
              </div>
              <button
                type='button'
                className={styles.creditAdjustBtn}
                onClick={() => void onSaveBillingConfig()}
                disabled={!billingConfigDraft || savingBillingConfig}
              >
                {savingBillingConfig ? 'Guardando...' : 'Guardar facturacion'}
              </button>
            </div>
          </div>
        </section>

        <section className={styles.section}>
          <h2 className={styles.sectionTitle}>Creditos del cliente</h2>
          <div className={styles.creditSection}>
            <div className={styles.creditSummary}>
              <div className={styles.creditMain}>
                <span className={styles.creditMainLabel}>Saldo actual</span>
                <strong className={styles.creditMainValue}>{formatCredits(metrics.creditBalance)} creditos</strong>
              </div>
              <div className={styles.creditHints}>
                <span>
                  Recomendado por ejecucion:{' '}
                  <strong>{Number(creditPolicy?.recommended_credits_per_execution || 0)}</strong>
                </span>
                <span>
                  Recomendado inicio cliente:{' '}
                  <strong>{Number(creditPolicy?.recommended_starting_credits || 0)}</strong>
                </span>
                <span>
                  Base coste medio: <strong>{formatCurrencyAmount(creditPolicy?.avg_run_cost_usd || 0, 'USD')}</strong>
                </span>
                {Array.isArray(creditPolicy?.recommended_tiers) && creditPolicy.recommended_tiers.length > 0 ? (
                  <span>
                    Packs sugeridos:{' '}
                    <strong>
                      {creditPolicy.recommended_tiers
                        .map((tier) => `${tier.label}: ${Number(tier.monthly_credits || 0)} (${Number(tier.estimated_runs || 0)} ejec.)`)
                        .join(' | ')}
                    </strong>
                  </span>
                ) : null}
              </div>
            </div>
            <div className={styles.creditAdjust}>
              <label className={styles.creditField}>
                Ajuste de saldo
                <select
                  className={styles.creditInput}
                  value={creditDeltaInput}
                  onChange={(event) => setCreditDeltaInput(event.target.value)}
                  disabled={adjustingCredits}
                >
                  {creditDeltaOptions.map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>
              <label className={styles.creditField}>
                Motivo
                <select
                  className={styles.creditInput}
                  value={creditReasonInput}
                  onChange={(event) => setCreditReasonInput(event.target.value)}
                  disabled={adjustingCredits}
                >
                  {CREDIT_REASON_OPTIONS.map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>
              <button
                type='button'
                className={styles.creditAdjustBtn}
                onClick={() => void onAdjustCredits()}
                disabled={adjustingCredits || !creditPlanEnabled}
              >
                {adjustingCredits ? 'Aplicando...' : 'Aplicar ajuste'}
              </button>
              {!creditPlanEnabled ? (
                <div className={styles.billingHint}>El plan de creditos esta desactivado para este cliente.</div>
              ) : null}
            </div>
          </div>
        </section>

        <section className={styles.section}>
          <h2 className={styles.sectionTitle}>Planes del cliente</h2>
          <div className={styles.planCards}>
            {availablePlans.length === 0 ? (
              <div className={styles.empty}>No hay planes disponibles en catalogo.</div>
            ) : (
              availablePlans.map((plan) => {
                const key = normalizePlanKey(plan.plan_key);
                const enabled = activePlanSet.has(key);
                const requested = requestedPlanSet.has(key);
                return (
                  <article key={key} className={styles.planCard}>
                    <div>
                      <h3 className={styles.planTitle}>{getDisplayPlanLabel(plan)}</h3>
                      {requested ? <span className={styles.requestedBadge}>Solicitado</span> : null}
                    </div>
                    <button
                      type='button'
                      className={enabled ? `${styles.toggle} ${styles.toggleOn}` : `${styles.toggle} ${styles.toggleOff}`}
                      onClick={() => void onTogglePlan(key, !enabled)}
                      disabled={loading}
                    >
                      {enabled ? 'Activo' : 'Inactivo'}
                    </button>
                  </article>
                );
              })
            )}
          </div>
        </section>

        <section className={styles.section}>
          <div className={styles.userSectionHeader}>
            <h2 className={styles.sectionTitle}>Usuarios del cliente</h2>
            <span className={styles.userSectionHint}>Selecciona un usuario para ver su actividad detallada.</span>
          </div>
          <div className={styles.userTableWrap}>
            <table className={styles.table}>
              <thead>
                <tr>
                  <th>Usuario</th>
                  <th>Rol</th>
                  <th>Estado</th>
                  <th>Ejecuciones</th>
                  <th>Ultima actividad</th>
                  <th>Detalle</th>
                </tr>
              </thead>
              <tbody>
                {tenantUsers.length === 0 ? (
                  <tr>
                    <td colSpan={6} className={styles.empty}>
                      No hay usuarios cliente para mostrar.
                    </td>
                  </tr>
                ) : (
                  tenantUsers.map((user) => {
                    const isSelected = user.user_id === selectedUserId;
                    return (
                      <tr
                        key={`${user.tenant_id}:${user.user_id}`}
                        className={isSelected ? styles.tableRowSelected : styles.tableRow}
                        role='button'
                        tabIndex={0}
                        onClick={() => setSelectedUserId(user.user_id)}
                        onKeyDown={(event) => {
                          if (event.key === 'Enter' || event.key === ' ') {
                            event.preventDefault();
                            setSelectedUserId(user.user_id);
                          }
                        }}
                      >
                        <td>
                          <div className={styles.userPrimary}>{user.display_name || user.email || 'Sin nombre'}</div>
                          <div className={styles.userSecondary}>{user.email || 'Sin email'}</div>
                        </td>
                        <td>{formatMembershipRole(user.membership.role)}</td>
                        <td>{user.membership.status || '-'}</td>
                        <td>
                          <div className={styles.userPrimary}>{Number(user.metrics.runs_total || 0)}</div>
                          <div className={styles.userSecondary}>
                            OK: {Number(user.metrics.runs_completed || 0)} | KO: {Number(user.metrics.runs_failed || 0)}
                          </div>
                        </td>
                        <td>{formatDate(user.metrics.last_run_at)}</td>
                        <td>
                          <button
                            type='button'
                            className={isSelected ? `${styles.selectUserBtn} ${styles.selectUserBtnActive}` : styles.selectUserBtn}
                            onClick={(event) => {
                              event.stopPropagation();
                              setSelectedUserId(user.user_id);
                            }}
                          >
                            Ver detalle
                          </button>
                        </td>
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>
        </section>

        {selectedUser && selectedActivity ? (
          <section className={styles.section}>
            <div className={styles.activityHeader}>
              <div>
                <h2 className={styles.activityTitle}>Actividad de usuario</h2>
                <p className={styles.activitySubtitle}>
                  {selectedUser.display_name || selectedUser.email || selectedUser.user_id}
                </p>
              </div>
              <span className={styles.activityUserBadge}>
                <UserRound size={14} />
                {formatMembershipRole(selectedUser.membership.role)}
              </span>
            </div>

            <div className={styles.activityStatsGrid}>
              <article className={styles.activityStatCard}>
                <span className={styles.activityStatLabel}>Ejecuciones totales</span>
                <div className={styles.activityStatValue}>{Number(selectedSummary.runs_total || 0)}</div>
                <Activity size={16} className={styles.activityStatIcon} />
              </article>
              <article className={styles.activityStatCard}>
                <span className={styles.activityStatLabel}>Completadas / Fallidas</span>
                <div className={styles.activityStatValue}>
                  {Number(selectedSummary.runs_completed || 0)} / {Number(selectedSummary.runs_failed || 0)}
                </div>
                <BadgeCheck size={16} className={styles.activityStatIcon} />
              </article>
              <article className={styles.activityStatCard}>
                <span className={styles.activityStatLabel}>Promedio duracion</span>
                <div className={styles.activityStatValue}>{formatDuration(selectedSummary.avg_duration_seconds)}</div>
                <History size={16} className={styles.activityStatIcon} />
              </article>
              <article className={styles.activityStatCard}>
                <span className={styles.activityStatLabel}>Coste acumulado</span>
                <div className={styles.activityStatValue}>
                  {formatCostByCurrency(selectedSummary.cost_by_currency || [])}
                </div>
                <DollarSign size={16} className={styles.activityStatIcon} />
              </article>
              <article className={styles.activityStatCard}>
                <span className={styles.activityStatLabel}>Creditos netos</span>
                <div className={styles.activityStatValue}>
                  {formatCredits(selectedSummary.credits_net)} / {formatCredits(selectedSummary.credits_total)}
                </div>
                <span className={styles.activityStatMeta}>
                  Reembolsados: {formatCredits(selectedSummary.credits_refunded)}
                </span>
              </article>
            </div>

            <div className={styles.membershipGrid}>
              {selectedMembershipBreakdown.map((membership) => (
                <article
                  key={`${selectedUser.user_id}:${membership.membership_id || membership.role || 'membership'}`}
                  className={styles.membershipCard}
                >
                  <div className={styles.membershipHead}>
                    <span className={styles.membershipRole}>
                      <ShieldUser size={14} />
                      {formatMembershipRole(membership.role)}
                    </span>
                    <span className={styles.membershipStatus}>{membership.status || '-'}</span>
                  </div>
                  <div className={styles.membershipMetrics}>
                    <span>Total: {Number(membership.runs_total || 0)}</span>
                    <span>OK: {Number(membership.runs_completed || 0)}</span>
                    <span>KO: {Number(membership.runs_failed || 0)}</span>
                    <span>En curso: {Number(membership.runs_running || 0)}</span>
                  </div>
                  <div className={styles.membershipCost}>
                    Coste por membresia: {formatCostByCurrency(membership.cost_by_currency || [])}
                  </div>
                  <div className={styles.membershipCost}>
                    Creditos por membresia: {formatCredits(membership.credits_net)} netos /{' '}
                    {formatCredits(membership.credits_total)} consumidos
                  </div>
                </article>
              ))}
            </div>

            <div className={styles.activitySplit}>
              <article className={styles.activityPanel}>
                <h3 className={styles.activityPanelTitle}>
                  <ListTree size={15} />
                  Estadisticas por proyecto
                </h3>
                {selectedProjects.length === 0 ? (
                  <div className={styles.emptyInline}>Sin proyectos con actividad para este usuario.</div>
                ) : (
                  <div className={styles.projectList}>
                    {selectedProjects.map((project) => (
                      <div
                        key={`${selectedUser.user_id}:${project.project_id || project.project_name}`}
                        className={styles.projectRow}
                      >
                        <div>
                          <div className={styles.projectTitle}>{project.project_name || project.project_id || '-'}</div>
                          <div className={styles.projectMeta}>
                            Total: {Number(project.runs_total || 0)} | OK: {Number(project.runs_completed || 0)} | KO:{' '}
                            {Number(project.runs_failed || 0)}
                          </div>
                        </div>
                        <div className={styles.projectRight}>
                          <span>{formatCostByCurrency(project.cost_by_currency || [])}</span>
                          <span>{formatCredits(project.credits_net)} creditos netos</span>
                          <span>{formatDate(project.last_run_at)}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </article>
            </div>

            <article className={styles.activityPanel}>
              <h3 className={styles.activityPanelTitle}>Ejecuciones recientes del usuario</h3>
              {selectedRuns.length === 0 ? (
                <div className={styles.emptyInline}>Sin ejecuciones recientes para este usuario.</div>
              ) : (
                <div className={styles.runList}>
                  {selectedRuns.map((run) => {
                    const runKey = getRunIdentityKey(run);
                    const isExpanded = expandedRunKey === runKey;
                    const runId = String(run.run_id || '').trim();
                    const artifactState = runId ? runArtifactsByRunId[runId] : undefined;
                    const artifactItems = artifactState?.artifacts || [];
                    const runLogs = selectedLogs.filter((log) => {
                      const logRunId = String(log.run_id || '').trim();
                      const targetRunId = String(run.run_id || '').trim();
                      if (targetRunId && logRunId) return targetRunId === logRunId;
                      const logProject = String(log.project_id || '').trim();
                      const targetProject = String(run.project_id || '').trim();
                      if (targetProject && logProject) return targetProject === logProject;
                      return false;
                    });
                    return (
                      <article key={`${selectedUser.user_id}:run:${runKey}`} className={styles.runCard}>
                        <div className={styles.runHeader}>
                          <div>
                            <div className={styles.runTitle}>{run.title || run.project_name || run.run_id || 'Ejecucion'}</div>
                            <div className={styles.runSubTitle}>
                              {run.project_name || run.project_id || '-'} · {formatRunStatus(run.status)}
                            </div>
                          </div>
                          <div className={styles.runHeaderRight}>
                            <span className={styles.runCost}>
                              {formatCurrencyAmount(run.cost?.amount, run.cost?.currency || 'USD')}
                            </span>
                            <span className={styles.runCredits}>
                              {formatCredits(run.credits?.net_amount ?? run.credits?.amount)} creditos
                              {run.credits?.refunded ? ' (reemb.)' : ''}
                            </span>
                            <button
                              type='button'
                              className={isExpanded ? `${styles.runDetailsBtn} ${styles.runDetailsBtnActive}` : styles.runDetailsBtn}
                              onClick={() => setExpandedRunKey((previous) => (previous === runKey ? '' : runKey))}
                            >
                              {isExpanded ? (
                                <>
                                  <ChevronUp size={14} />
                                  Ocultar detalles
                                </>
                              ) : (
                                <>
                                  <ChevronDown size={14} />
                                  Ver detalles
                                </>
                              )}
                            </button>
                          </div>
                        </div>
                        {isExpanded ? (
                          <div className={styles.runDetails}>
                            <div className={styles.runMetaGrid}>
                              <div className={styles.runMetaItem}>
                                <span className={styles.runMetaLabel}>Inicio</span>
                                <span className={styles.runMetaValue}>{formatDate(run.started_at)}</span>
                              </div>
                              <div className={styles.runMetaItem}>
                                <span className={styles.runMetaLabel}>Fin</span>
                                <span className={styles.runMetaValue}>{formatDate(run.finished_at)}</span>
                              </div>
                              <div className={styles.runMetaItem}>
                                <span className={styles.runMetaLabel}>Duracion</span>
                                <span className={styles.runMetaValue}>{formatDuration(run.duration_seconds)}</span>
                              </div>
                              <div className={styles.runMetaItem}>
                                <span className={styles.runMetaLabel}>Creditos</span>
                                <span className={styles.runMetaValue}>
                                  {formatCredits(run.credits?.net_amount ?? run.credits?.amount)}
                                  {run.credits?.refunded ? ' (reembolsado)' : ''}
                                </span>
                              </div>
                              <div className={styles.runMetaItem}>
                                <span className={styles.runMetaLabel}>PDFs</span>
                                <span className={styles.runMetaValue}>{Number(run.files?.pdf_count || 0)}</span>
                              </div>
                              <div className={styles.runMetaItem}>
                                <span className={styles.runMetaLabel}>Pauta</span>
                                <span className={styles.runMetaValueTruncate} title={run.files?.pauta_filename || '-'}>
                                  {run.files?.pauta_filename || '-'}
                                </span>
                              </div>
                              <div className={styles.runMetaItem}>
                                <span className={styles.runMetaLabel}>Resultado</span>
                                <span className={styles.runMetaValueTruncate} title={run.files?.output_filename || '-'}>
                                  {run.files?.output_filename || '-'}
                                </span>
                              </div>
                            </div>
                            {run.error_message ? <div className={styles.runError}>{run.error_message}</div> : null}
                            <div className={styles.runArtifactsBlock}>
                              <div className={styles.runArtifactsHead}>
                                <div className={styles.runArtifactsTitle}>
                                  <FileJson size={14} />
                                  JSON por etapa del proceso
                                </div>
                                {runId ? (
                                  <button
                                    type='button'
                                    className={styles.runArtifactsReload}
                                    onClick={() => void loadRunArtifacts(run)}
                                    disabled={artifactState?.loading}
                                  >
                                    {artifactState?.loading ? 'Cargando...' : 'Actualizar'}
                                  </button>
                                ) : null}
                              </div>
                              {!runId ? (
                                <div className={styles.emptyInline}>
                                  Esta ejecucion no tiene `run_id`; no se pueden listar artefactos.
                                </div>
                              ) : artifactState?.loading ? (
                                <div className={styles.runArtifactsLoading}>
                                  <Loader2 size={14} className={styles.runArtifactsSpinner} />
                                  Cargando artefactos JSON...
                                </div>
                              ) : artifactState?.error ? (
                                <div className={styles.runError}>{artifactState.error}</div>
                              ) : artifactState?.loaded && artifactItems.length === 0 ? (
                                <div className={styles.emptyInline}>
                                  No se encontraron artefactos JSON para esta ejecucion.
                                </div>
                              ) : (
                                <div className={styles.runArtifactsList}>
                                  {artifactItems.map((artifact) => (
                                    <div key={artifact.artifact_id} className={styles.runArtifactRow}>
                                      <div className={styles.runArtifactMain}>
                                        <div className={styles.runArtifactTop}>
                                          <span className={styles.runArtifactLabel} title={artifact.label || artifact.artifact_key}>
                                            {artifact.label || artifact.artifact_key}
                                          </span>
                                          <span className={styles.runArtifactClass}>
                                            {(artifact.artifact_class_label || artifact.artifact_class || 'json').toUpperCase()}
                                          </span>
                                        </div>
                                        <div className={styles.runArtifactMeta}>
                                          <span className={styles.runArtifactMetaItem} title={artifact.source_pdf}>
                                            Fuente: {artifact.source_pdf || '-'}
                                          </span>
                                          <span className={styles.runArtifactMetaItem} title={artifact.filename || artifact.artifact_key}>
                                            Archivo: {artifact.filename || artifact.artifact_key}
                                          </span>
                                        </div>
                                      </div>
                                      <button
                                        type='button'
                                        className={styles.runArtifactDownloadBtn}
                                        onClick={() => void downloadRunArtifact(run, artifact)}
                                      >
                                        <Download size={14} />
                                        Descargar JSON
                                      </button>
                                    </div>
                                  ))}
                                </div>
                              )}
                            </div>
                            <div className={styles.runLogsBlock}>
                              <div className={styles.runLogsTitle}>
                                <History size={14} />
                                Logs de esta ejecucion
                              </div>
                              {runLogs.length === 0 ? (
                                <div className={styles.emptyInline}>Sin logs para esta ejecucion.</div>
                              ) : (
                                <div className={styles.logList}>
                                  {runLogs.map((log, index) => (
                                    <div key={`${selectedUser.user_id}:run:${runKey}:log:${index}`} className={styles.logRow}>
                                      <div className={styles.logMeta}>
                                        <span className={styles.logLevel}>{String(log.level || 'info').toUpperCase()}</span>
                                        <span>{String(log.event || 'evento')}</span>
                                      </div>
                                      <div className={styles.logMessage}>{log.message || '-'}</div>
                                      <div className={styles.logTime}>{formatDate(log.at)}</div>
                                    </div>
                                  ))}
                                </div>
                              )}
                            </div>
                          </div>
                        ) : null}
                      </article>
                    );
                  })}
                </div>
              )}
            </article>
          </section>
        ) : null}
      </div>

      {idleWarningVisible ? (
        <div className={styles.sessionOverlay} role='dialog' aria-modal='true'>
          <div className={styles.sessionDialog}>
            <h2 className={styles.sessionTitle}>Sesion a punto de caducar</h2>
            <p className={styles.sessionText}>
              Se cerrara la sesion por inactividad en <strong>{idleCountdownSeconds}s</strong>.
            </p>
            <div className={styles.sessionActions}>
              <button type='button' className={styles.reload} onClick={continueSession}>
                Continuar sesion
              </button>
              <button
                type='button'
                className={styles.backLink}
                onClick={() => redirectToAdminLogin('session_expired')}
              >
                Cerrar ahora
              </button>
            </div>
          </div>
        </div>
      ) : null}

      <div className={toast ? `${styles.toast} ${styles.toastVisible}` : styles.toast}>{toast}</div>
    </main>
  );
}
