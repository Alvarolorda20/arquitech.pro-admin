'use client';

import Link from 'next/link';
import {useCallback, useEffect, useMemo, useRef, useState} from 'react';
import {usePathname, useRouter, useSearchParams} from 'next/navigation';

import styles from './admin-memberships.module.css';
import {ThemeToggleButton} from '@/components/theme/theme-toggle-button';
import {
  clearStoredAdminAccessToken,
  fetchWithAdminAuth,
  getAdminIdleTimeoutMs,
  getAdminIdleWarningMs,
  getValidAdminAccessToken,
  isExpiredAdminToken,
} from '@/lib/admin-session';
import {buildWorkspaceSwitchHref} from '@/modules/admin/runtime';

type StatusKind = 'idle' | 'ok' | 'error';

interface TenantPlan {
  plan_key: string;
  display_name?: string;
  route_path?: string;
  status?: string;
}

interface TenantOverview {
  tenant_id: string;
  name?: string;
  products?: string[];
  plans?: TenantPlan[];
  metadata?: Record<string, unknown>;
  credits_balance?: number | null;
  memberships_total: number;
  active_memberships: number;
  admin_memberships: number;
  runs_total: number;
  runs_completed: number;
  runs_failed: number;
  last_run_at?: string | null;
}

interface OverviewPayload {
  actor?: {id?: string; email?: string};
  tenants?: TenantOverview[];
  available_plans?: Array<{
    plan_key: string;
    display_name?: string;
    route_path?: string;
  }>;
  detail?: string;
}

const ACTIVE_PLAN_STATUSES = new Set(['active', 'trial']);
const E2E_HINT_PATTERNS = [/\be2e\b/i, /playwright/i, /\btest(ing)?\b/i, /example\.com$/i];
const ACTIVITY_EVENTS = ['mousedown', 'keydown', 'touchstart', 'scroll', 'mousemove'] as const;

function normalizePlanKey(raw: string | null | undefined): string {
  return String(raw || '').trim().toLowerCase();
}

function hasE2EHint(...values: Array<string | null | undefined>): boolean {
  return values.some((value) => {
    const normalized = String(value || '').trim().toLowerCase();
    if (!normalized) return false;
    return E2E_HINT_PATTERNS.some((pattern) => pattern.test(normalized));
  });
}

function isE2ETenant(tenant: TenantOverview): boolean {
  return hasE2EHint(tenant.name, tenant.tenant_id);
}

function formatDate(raw?: string | null): string {
  if (!raw) return '-';
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return raw;
  return parsed.toLocaleString();
}

function getDisplayPlanLabel(plan: {display_name?: string; plan_key?: string} | undefined): string {
  const display = String(plan?.display_name || '').trim();
  return display || 'Plan';
}

function getTenantPlanKeySet(tenant: TenantOverview): Set<string> {
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

function getRequestedPlanKeySet(tenant: TenantOverview): Set<string> {
  const metadata = tenant.metadata;
  if (!metadata || typeof metadata !== 'object') return new Set<string>();
  const raw = metadata['requested_plan_keys'];
  if (!Array.isArray(raw)) return new Set<string>();
  return new Set(
    raw
      .map((value) => normalizePlanKey(String(value || '')))
      .filter(Boolean),
  );
}

export function AdminMembershipsView() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const initialTenantId = (searchParams.get('tenant_id') || '').trim();

  const [sessionReady, setSessionReady] = useState(false);
  const [tenants, setTenants] = useState<TenantOverview[]>([]);
  const [availablePlans, setAvailablePlans] = useState<
    Array<{plan_key: string; display_name?: string; route_path?: string}>
  >([]);
  const [actor, setActor] = useState<{id?: string; email?: string}>({});
  const [search, setSearch] = useState('');
  const [tenantFilter, setTenantFilter] = useState(initialTenantId);
  const [planFilter, setPlanFilter] = useState(normalizePlanKey(searchParams.get('plan_key')));
  const [loading, setLoading] = useState(false);
  const [statusText, setStatusText] = useState('Cargando clientes...');
  const [statusKind, setStatusKind] = useState<StatusKind>('idle');
  const [idleWarningVisible, setIdleWarningVisible] = useState(false);
  const [idleCountdownSeconds, setIdleCountdownSeconds] = useState(0);
  const lastActivityRef = useRef(Date.now());
  const lastRefreshAttemptRef = useRef(0);

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

  const loadOverview = useCallback(async () => {
    if (!sessionReady) {
      return;
    }

    setLoading(true);
    setStatus('Cargando resumen global...', 'idle');

    try {
      const scope = tenantFilter ? `?tenant_id=${encodeURIComponent(tenantFilter)}` : '';
      const response = await fetchWithAdminAuth(`/api/admin/tenant-overview${scope}`);

      const payload = (await response.json().catch(() => ({}))) as OverviewPayload;
      if (!response.ok) {
        if (isExpiredAdminToken(payload.detail, response.status)) {
          setStatus('Sesion admin expirada. Redirigiendo al login...', 'error');
          redirectToAdminLogin('session_expired');
          return;
        }
        throw new Error(payload.detail || `HTTP ${response.status}`);
      }

      setTenants(Array.isArray(payload.tenants) ? payload.tenants : []);
      setAvailablePlans(Array.isArray(payload.available_plans) ? payload.available_plans : []);
      setActor(payload.actor || {});
      setStatus(`Cargado: ${Number((payload.tenants || []).length)} clientes.`, 'ok');
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Error cargando resumen';
      setStatus(message, 'error');
    } finally {
      setLoading(false);
    }
  }, [redirectToAdminLogin, sessionReady, setStatus, tenantFilter]);

  useEffect(() => {
    void loadOverview();
  }, [loadOverview]);

  const visibleTenants = useMemo(() => {
    const withoutE2E = tenants.filter((tenant) => !isE2ETenant(tenant));
    const baseTenants = withoutE2E.length > 0 ? withoutE2E : tenants;

    const byPlan = !planFilter
      ? baseTenants
      : baseTenants.filter((tenant) => getTenantPlanKeySet(tenant).has(planFilter));

    const query = search.trim().toLowerCase();
    if (!query) return byPlan;

    return byPlan.filter((tenant) => {
      const plansText = (tenant.plans || [])
        .map((plan) => `${plan.display_name || ''} ${plan.plan_key || ''}`)
        .join(' ');
      const haystack = `${tenant.name || ''} ${plansText}`.toLowerCase();
      return haystack.includes(query);
    });
  }, [planFilter, search, tenants]);

  const visibleSummary = useMemo(() => {
    return visibleTenants.reduce(
      (acc, tenant) => {
        const tenantMemberships = Math.max(
          0,
          Number(tenant.memberships_total || 0) - Number(tenant.admin_memberships || 0),
        );
        const tenantActiveMemberships = Math.max(
          0,
          Number(tenant.active_memberships || 0) - Number(tenant.admin_memberships || 0),
        );
        acc.clientMemberships += tenantMemberships;
        acc.activeClientMemberships += tenantActiveMemberships;
        acc.totalRuns += Number(tenant.runs_total || 0);
        acc.activePlans += getTenantPlanKeySet(tenant).size;
        acc.totalCredits += Number(tenant.credits_balance || 0);
        return acc;
      },
      {
        clients: visibleTenants.length,
        clientMemberships: 0,
        activeClientMemberships: 0,
        activePlans: 0,
        totalRuns: 0,
        totalCredits: 0,
      },
    );
  }, [visibleTenants]);

  const scopeLabel = useMemo(() => {
    if (!tenantFilter) return 'Todos los clientes';
    const found = visibleTenants.find((tenant) => tenant.tenant_id === tenantFilter);
    return found?.name || 'Cliente seleccionado';
  }, [tenantFilter, visibleTenants]);

  const planScopeLabel = useMemo(() => {
    if (!planFilter) return 'Todos los planes';
    const found = availablePlans.find((plan) => normalizePlanKey(plan.plan_key) === planFilter);
    return getDisplayPlanLabel(found);
  }, [availablePlans, planFilter]);

  const planLabelMap = useMemo(() => {
    const map = new Map<string, string>();
    for (const plan of availablePlans) {
      map.set(normalizePlanKey(plan.plan_key), getDisplayPlanLabel(plan));
    }
    return map;
  }, [availablePlans]);

  return (
    <main className={styles.shell}>
      <div className={styles.wrap}>
        <section className={styles.hero}>
          <div style={{display: 'flex', justifyContent: 'flex-end', marginBottom: 8}}>
            <ThemeToggleButton showLabel className="theme-toggle-btn--inline" />
          </div>
          <h1 className={styles.title}>Consola Global de Clientes</h1>
          <p className={styles.muted}>
            Resumen de clientes. Haz clic en cada cliente para ver su panel detallado.
          </p>
          <div className={styles.heroMeta}>
            <div className={styles.pill}>
              Alcance: <strong>{scopeLabel}</strong>
            </div>
            <div className={styles.pill}>
              Plan: <strong>{planScopeLabel}</strong>
            </div>
            <div className={styles.pill}>
              Administrador: <strong>{actor.email || actor.id || '-'}</strong>
            </div>
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

        <section className={styles.panel}>
          <div className={styles.controls}>
            <input
              className={styles.input}
              placeholder='Buscar por cliente o plan'
              value={search}
              onChange={(event) => setSearch(event.target.value)}
            />
            <select
              className={styles.select}
              value={planFilter}
              onChange={(event) => setPlanFilter(normalizePlanKey(event.target.value))}
              disabled={loading}
            >
              <option value=''>Filtrar por plan: Todos</option>
              {availablePlans.map((plan) => {
                const planKey = normalizePlanKey(plan.plan_key);
                return (
                  <option key={planKey} value={planKey}>
                    {getDisplayPlanLabel(plan)}
                  </option>
                );
              })}
            </select>
            <select
              className={styles.select}
              value={tenantFilter}
              onChange={(event) => setTenantFilter(event.target.value)}
              disabled={loading}
            >
              <option value=''>Todos los clientes</option>
              {visibleTenants.map((tenant) => (
                <option key={tenant.tenant_id} value={tenant.tenant_id}>
                  {tenant.name || 'Cliente sin nombre'}
                </option>
              ))}
            </select>
            <button
              type='button'
              className={styles.button}
              onClick={() => void loadOverview()}
              disabled={loading}
            >
              {loading ? 'Cargando...' : 'Recargar'}
            </button>
          </div>
        </section>

        <section className={styles.summaryGrid}>
          {[
            ['Clientes', visibleSummary.clients],
            ['Membresias cliente', visibleSummary.clientMemberships],
            ['Activas cliente', visibleSummary.activeClientMemberships],
            ['Planes activos', visibleSummary.activePlans],
            ['Ejecuciones', visibleSummary.totalRuns],
            ['Creditos totales', visibleSummary.totalCredits],
          ].map(([key, value]) => (
            <article key={String(key)} className={styles.summaryCard}>
              <span className={styles.summaryKey}>{String(key)}</span>
              <div className={styles.summaryValue}>{Number(value || 0)}</div>
            </article>
          ))}
        </section>

        <section className={styles.tenantWrap}>
          <h2 className={styles.tableTitle}>Clientes</h2>
          <table className={styles.table}>
            <thead>
              <tr>
                <th>Cliente</th>
                <th>Planes activos</th>
                <th>Membresias cliente</th>
                <th>Ejecuciones</th>
                <th>Creditos</th>
                <th>Ultima actividad</th>
                <th>Panel</th>
                <th>Workspace</th>
              </tr>
            </thead>
            <tbody>
              {visibleTenants.length === 0 ? (
                <tr>
                  <td colSpan={8} className={styles.empty}>
                    No hay clientes para mostrar.
                  </td>
                </tr>
              ) : (
                visibleTenants.map((tenant) => {
                  const activePlanKeys = Array.from(getTenantPlanKeySet(tenant));
                  const requestedPlanKeys = Array.from(getRequestedPlanKeySet(tenant)).filter(
                    (planKey) => !activePlanKeys.includes(planKey),
                  );
                  const tenantMemberships = Math.max(
                    0,
                    Number(tenant.memberships_total || 0) - Number(tenant.admin_memberships || 0),
                  );
                  const tenantActiveMemberships = Math.max(
                    0,
                    Number(tenant.active_memberships || 0) - Number(tenant.admin_memberships || 0),
                  );
                  const detailHref = `/tenants/${tenant.tenant_id}`;
                  const workspaceHref = buildWorkspaceSwitchHref({
                    tenantId: tenant.tenant_id,
                    nextPath: '/products/comparacion-presupuestos',
                    adminReturnPath: `${pathname || '/memberships'}${searchParams?.toString() ? `?${searchParams.toString()}` : ''}`,
                  });

                  return (
                    <tr key={tenant.tenant_id}>
                      <td>
                        <div className={styles.userName}>{tenant.name || 'Cliente sin nombre'}</div>
                      </td>
                      <td>
                        {activePlanKeys.length === 0 ? (
                          <span className={styles.tiny}>Sin planes activos</span>
                        ) : (
                          <div className={styles.planList}>
                            {activePlanKeys.map((planKey) => (
                              <span key={`${tenant.tenant_id}:${planKey}`} className={styles.planPill}>
                                {planLabelMap.get(planKey) || 'Plan'}
                              </span>
                            ))}
                          </div>
                        )}
                        {requestedPlanKeys.length > 0 ? (
                          <div className={styles.tiny}>
                            Solicitados: {requestedPlanKeys.map((key) => planLabelMap.get(key) || 'Plan').join(', ')}
                          </div>
                        ) : null}
                      </td>
                      <td>
                        <div className={styles.userName}>{tenantMemberships}</div>
                        <div className={styles.tiny}>Activas: {tenantActiveMemberships}</div>
                      </td>
                      <td>
                        <div className={styles.userName}>{Number(tenant.runs_total || 0)}</div>
                        <div className={styles.tiny}>
                          OK: {Number(tenant.runs_completed || 0)} | KO: {Number(tenant.runs_failed || 0)}
                        </div>
                      </td>
                      <td>
                        <div className={styles.userName}>{Number(tenant.credits_balance || 0)}</div>
                        <div className={styles.tiny}>creditos</div>
                      </td>
                      <td>{formatDate(tenant.last_run_at)}</td>
                      <td>
                        <Link className={styles.detailLink} href={detailHref}>
                          Ver detalle
                        </Link>
                      </td>
                      <td>
                        <Link
                          className={styles.detailLink}
                          href={workspaceHref}
                        >
                          Abrir workspace
                        </Link>
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </section>
      </div>
      {idleWarningVisible ? (
        <div className={styles.sessionOverlay} role='dialog' aria-modal='true'>
          <div className={styles.sessionDialog}>
            <h2 className={styles.sessionTitle}>Sesion a punto de caducar</h2>
            <p className={styles.sessionText}>
              Se cerrara la sesion por inactividad en <strong>{idleCountdownSeconds}s</strong>.
            </p>
            <div className={styles.sessionActions}>
              <button type='button' className={styles.button} onClick={continueSession}>
                Continuar sesion
              </button>
              <button
                type='button'
                className={`${styles.button} ${styles.buttonAlt}`}
                onClick={() => redirectToAdminLogin('session_expired')}
              >
                Cerrar ahora
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </main>
  );
}
