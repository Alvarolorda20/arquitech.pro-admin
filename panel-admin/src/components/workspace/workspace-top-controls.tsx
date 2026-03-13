'use client';

import {useEffect, useMemo, useState} from 'react';
import {useTranslations} from 'next-intl';
import {BadgeCheck, Bell, Coins, PlayCircle, RotateCcw} from 'lucide-react';

import {LocaleSwitcher} from '@/components/LocaleSwitcher';
import {ThemeToggleButton} from '@/components/theme/theme-toggle-button';
import {getPublicBackendApiBaseUrl} from '@/lib/backend-api';
import {createSupabaseBrowserClient} from '@/lib/supabase/client';

type BillingKind = 'none' | 'credits' | 'quota';

interface WorkspaceTopControlsProps {
  tenantId?: string | null;
  appKey?: string | null;
}

interface QuotaAppState {
  app_key: string;
  executions_remaining: number;
  reruns_remaining: number;
}

interface CreditState {
  balance: number | null;
  loading: boolean;
  visible: boolean;
  billingKind: BillingKind;
  quotaApps: QuotaAppState[];
}

function appLabelForKey(appKey: string, t: (key: string) => string) {
  const normalized = String(appKey || '').trim().toLowerCase();
  if (normalized === 'comparacion_presupuestos') {
    return t('credits.apps.comparacion_presupuestos');
  }
  if (normalized === 'memoria_basica') {
    return t('credits.apps.memoria_basica');
  }
  return normalized || t('credits.unknownApp');
}

export function WorkspaceTopControls({tenantId, appKey}: WorkspaceTopControlsProps) {
  const t = useTranslations('workspace');
  const [credits, setCredits] = useState<CreditState>({
    balance: null,
    loading: false,
    visible: false,
    billingKind: 'none',
    quotaApps: [],
  });
  const backendBaseUrl = useMemo(() => getPublicBackendApiBaseUrl(), []);

  useEffect(() => {
    let cancelled = false;

    async function loadCredits() {
      const normalizedTenantId = String(tenantId || '').trim();
      if (!normalizedTenantId || !backendBaseUrl) {
        if (!cancelled) {
          setCredits({balance: null, loading: false, visible: false, billingKind: 'none', quotaApps: []});
        }
        return;
      }

      if (!cancelled) {
        setCredits((previous) => ({...previous, loading: true}));
      }
      try {
        const supabase = createSupabaseBrowserClient();
        const {
          data: {session},
        } = await supabase.auth.getSession();
        const accessToken = String(session?.access_token || '').trim();
        if (!accessToken) {
          if (!cancelled) {
            setCredits({balance: null, loading: false, visible: false, billingKind: 'none', quotaApps: []});
          }
          return;
        }

        const response = await fetch(
          `${backendBaseUrl}/api/credits/balance?tenant_id=${encodeURIComponent(normalizedTenantId)}`,
          {
            method: 'GET',
            headers: {
              authorization: `Bearer ${accessToken}`,
            },
            cache: 'no-store',
          },
        );
        if (!response.ok) {
          if (!cancelled) {
            setCredits({balance: null, loading: false, visible: false, billingKind: 'none', quotaApps: []});
          }
          return;
        }
        const payload = (await response.json().catch(() => ({}))) as {
          balance?: number | null;
          visible?: boolean;
          billing_kind?: string | null;
          quota?: {
            apps?: Array<{
              app_key?: string;
              executions_remaining?: number;
              reruns_remaining?: number;
            }>;
          };
        };
        const rawKind = String(payload.billing_kind || '').trim().toLowerCase();
        const billingKind: BillingKind =
          rawKind === 'credits' ? 'credits' : rawKind === 'quota' ? 'quota' : 'none';
        const balance = typeof payload.balance === 'number' ? payload.balance : null;
        const quotaApps = Array.isArray(payload.quota?.apps)
          ? payload.quota.apps
              .filter((item) => item && typeof item === 'object')
              .map((item) => ({
                app_key: String(item.app_key || '').trim().toLowerCase(),
                executions_remaining: Number.isFinite(Number(item.executions_remaining))
                  ? Number(item.executions_remaining)
                  : 0,
                reruns_remaining: Number.isFinite(Number(item.reruns_remaining))
                  ? Number(item.reruns_remaining)
                  : 0,
              }))
          : [];
        const visible = Boolean(payload.visible) && billingKind !== 'none';
        if (!cancelled) setCredits({balance, loading: false, visible, billingKind, quotaApps});
      } catch {
        if (!cancelled) {
          setCredits({balance: null, loading: false, visible: false, billingKind: 'none', quotaApps: []});
        }
      }
    }

    void loadCredits();
    return () => {
      cancelled = true;
    };
  }, [backendBaseUrl, tenantId]);

  const normalizedAppKey = useMemo(() => String(appKey || '').trim().toLowerCase(), [appKey]);
  const currentQuotaApp = useMemo(() => {
    if (credits.billingKind !== 'quota' || !normalizedAppKey) return null;
    return (
      credits.quotaApps.find((item) => String(item.app_key || '').trim().toLowerCase() === normalizedAppKey) || {
        app_key: normalizedAppKey,
        executions_remaining: 0,
        reruns_remaining: 0,
      }
    );
  }, [credits.billingKind, credits.quotaApps, normalizedAppKey]);

  const isQuotaPlan = credits.billingKind === 'quota';
  const isCreditsPlan = credits.billingKind === 'credits';
  const executionsRemaining = Math.max(0, Number(currentQuotaApp?.executions_remaining || 0));
  const rerunsRemaining = Math.max(0, Number(currentQuotaApp?.reruns_remaining || 0));
  const currentAppLabel = currentQuotaApp ? appLabelForKey(currentQuotaApp.app_key, t) : null;

  return (
    <div className="flex w-full flex-wrap items-center gap-3 py-1">
      <div className="flex min-w-0 flex-1 flex-wrap items-center gap-2">
        {credits.visible ? (
          <>
            {isQuotaPlan ? (
              <span className="workspace-top-chip workspace-top-chip--plan">
                <BadgeCheck className="h-4 w-4" strokeWidth={2.1} aria-hidden="true" />
                <span className="workspace-top-chip-label">{t('credits.customPlanLabel')}</span>
                <span className="workspace-top-chip-value">{t('credits.customPlanValue')}</span>
              </span>
            ) : null}

            {isCreditsPlan ? (
              <span className="workspace-top-chip workspace-top-chip--credits">
                <Coins className="h-4 w-4" strokeWidth={2.1} aria-hidden="true" />
                {t('credits.label')}: {credits.loading ? t('credits.loading') : credits.balance ?? '-'} {t('credits.unit')}
              </span>
            ) : null}

            {isQuotaPlan && !credits.loading && currentQuotaApp ? (
              <div className="workspace-top-chip-group" aria-live="polite">
                <span className="workspace-top-app-label">{currentAppLabel}</span>
                <span className="workspace-top-chip workspace-top-chip--quota">
                  <PlayCircle className="h-4 w-4" strokeWidth={2} aria-hidden="true" />
                  <span className="workspace-top-chip-label">{t('credits.executionsPill')}:</span>
                  <strong className="workspace-top-chip-value">{executionsRemaining}</strong>
                </span>
                <span className="workspace-top-chip workspace-top-chip--quota">
                  <RotateCcw className="h-4 w-4" strokeWidth={2} aria-hidden="true" />
                  <span className="workspace-top-chip-label">{t('credits.rerunsPill')}:</span>
                  <strong className="workspace-top-chip-value">{rerunsRemaining}</strong>
                </span>
              </div>
            ) : null}
          </>
        ) : null}
      </div>

      <div className="ml-auto flex shrink-0 items-center gap-2">
        <LocaleSwitcher />
        <button
          type="button"
          aria-label={t('notifications')}
          className="workspace-top-icon-btn"
        >
          <Bell className="h-4 w-4" strokeWidth={2} />
        </button>
        <ThemeToggleButton className="workspace-top-icon-btn" />
      </div>
    </div>
  );
}
