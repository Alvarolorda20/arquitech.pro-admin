'use client';

import Link from 'next/link';
import {useMemo, useState} from 'react';
import {Button} from '@gravity-ui/uikit';

import type {DashboardMembershipChip} from '@/lib/dashboard-context';

interface MembershipChipsProps {
  chips: DashboardMembershipChip[];
  activeLabel: string;
  blockedLabel: string;
  emptyLabel: string;
  blockedNoticeTitle: string;
  blockedNoticeText: string;
  requestProductLabel: string;
  requestProductAction?: (formData: FormData) => void | Promise<void>;
  requestOriginPath: string;
  requestStatus?: 'ok' | 'error' | null;
  requestStatusText?: string | null;
}

const PLAN_NAME_TOKEN = '__plan_name__';

export function MembershipChips({
  chips,
  activeLabel,
  blockedLabel,
  emptyLabel,
  blockedNoticeTitle,
  blockedNoticeText,
  requestProductLabel,
  requestProductAction,
  requestOriginPath,
  requestStatus,
  requestStatusText,
}: MembershipChipsProps) {
  const [selectedBlockedPlan, setSelectedBlockedPlan] = useState<DashboardMembershipChip | null>(null);
  const blockedPlansCount = useMemo(
    () => chips.filter((chip) => chip.status === 'blocked').length,
    [chips],
  );

  if (chips.length === 0) {
    return <p className="workspace-panel-text">{emptyLabel}</p>;
  }

  return (
    <div className="workspace-membership-chips-wrap">
      <ul className="workspace-membership-chip-list" aria-label="workspace-membership-chips">
        {chips.map((chip) => {
          const toneClass = chip.status === 'active' ? 'workspace-plan-chip--active' : 'workspace-plan-chip--blocked';
          const statusLabel = chip.status === 'active' ? activeLabel : blockedLabel;
          const content = (
            <>
              <span className="workspace-plan-chip-name">{chip.label}</span>
              <span className="workspace-plan-chip-state">{statusLabel}</span>
            </>
          );

          return (
            <li key={chip.planKey}>
              {chip.status === 'active' && chip.routePath ? (
                <Link href={chip.routePath} className={`workspace-plan-chip ${toneClass}`}>
                  {content}
                </Link>
              ) : chip.status === 'blocked' ? (
                <button
                  type="button"
                  className={`workspace-plan-chip workspace-plan-chip-btn ${toneClass}`}
                  onClick={() => setSelectedBlockedPlan(chip)}
                >
                  {content}
                </button>
              ) : (
                <span className={`workspace-plan-chip ${toneClass}`}>{content}</span>
              )}
            </li>
          );
        })}
      </ul>

      {selectedBlockedPlan ? (
        <div className="workspace-plan-blocked-notice">
          <p className="workspace-plan-blocked-title">{blockedNoticeTitle}</p>
          <p className="workspace-panel-text">
            {blockedNoticeText.replace(PLAN_NAME_TOKEN, selectedBlockedPlan.label)}
          </p>
          {requestProductAction ? (
            <form action={requestProductAction} className="workspace-plan-request-form">
              <input type="hidden" name="plan_key" value={selectedBlockedPlan.planKey} />
              <input type="hidden" name="origin_path" value={requestOriginPath} />
              <Button type="submit" size="m" view="outlined-action">
                {requestProductLabel}
              </Button>
            </form>
          ) : null}
        </div>
      ) : blockedPlansCount > 0 ? (
        <p className="workspace-panel-text">{blockedNoticeTitle}</p>
      ) : null}

      {requestStatus && requestStatusText ? (
        <p
          className={`workspace-plan-request-status${
            requestStatus === 'ok' ? ' workspace-plan-request-status--ok' : ' workspace-plan-request-status--error'
          }`}
        >
          {requestStatusText}
        </p>
      ) : null}
    </div>
  );
}
