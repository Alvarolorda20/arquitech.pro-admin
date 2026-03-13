'use client';

import {useMemo, useState} from 'react';
import {Button, Modal} from '@gravity-ui/uikit';

export interface TeamMemberWithMetrics {
  userId: string;
  fullName: string;
  roleLabel: string;
  statusLabel: string;
  runsTotal: number;
  runsCompleted: number;
  runsFailed: number;
  lastRunAtLabel: string;
}

interface TeamMembersManagerProps {
  members: TeamMemberWithMetrics[];
  currentUserId: string;
  canManageMembers: boolean;
  removeAction: (formData: FormData) => void | Promise<void>;
  removeLabel: string;
  modalTitle: string;
  modalDescription: string;
  modalCancel: string;
  modalConfirm: string;
  metricsRuns: string;
  metricsCompleted: string;
  metricsFailed: string;
  metricsLastRun: string;
}

const MEMBER_NAME_TOKEN = '__member_name__';

export function TeamMembersManager({
  members,
  currentUserId,
  canManageMembers,
  removeAction,
  removeLabel,
  modalTitle,
  modalDescription,
  modalCancel,
  modalConfirm,
  metricsRuns,
  metricsCompleted,
  metricsFailed,
  metricsLastRun,
}: TeamMembersManagerProps) {
  const [selectedUserId, setSelectedUserId] = useState<string | null>(null);
  const selectedMember = useMemo(
    () => members.find((member) => member.userId === selectedUserId) || null,
    [members, selectedUserId],
  );

  return (
    <>
      <ul className="team-page-member-list">
        {members.map((member) => (
          <li key={member.userId} className="team-page-member-card">
            <div className="team-page-member-top">
              <div>
                <p className="team-page-member-name">{member.fullName}</p>
                <p className="workspace-panel-text">
                  {member.roleLabel} - {member.statusLabel}
                </p>
              </div>
              {canManageMembers && member.userId !== currentUserId ? (
                <Button
                  view="flat-danger"
                  size="m"
                  onClick={() => setSelectedUserId(member.userId)}
                >
                  {removeLabel}
                </Button>
              ) : null}
            </div>

            <div className="team-page-metrics-grid">
              <article className="team-page-metric">
                <span className="team-page-metric-label">{metricsRuns}</span>
                <strong className="team-page-metric-value">{member.runsTotal}</strong>
              </article>
              <article className="team-page-metric">
                <span className="team-page-metric-label">{metricsCompleted}</span>
                <strong className="team-page-metric-value">{member.runsCompleted}</strong>
              </article>
              <article className="team-page-metric">
                <span className="team-page-metric-label">{metricsFailed}</span>
                <strong className="team-page-metric-value">{member.runsFailed}</strong>
              </article>
              <article className="team-page-metric">
                <span className="team-page-metric-label">{metricsLastRun}</span>
                <strong className="team-page-metric-value team-page-metric-value--date">
                  {member.lastRunAtLabel}
                </strong>
              </article>
            </div>
          </li>
        ))}
      </ul>

      <Modal
        open={Boolean(selectedMember)}
        onOpenChange={(open) => {
          if (!open) {
            setSelectedUserId(null);
          }
        }}
      >
        <div className="team-page-modal">
          <h3 className="team-page-modal-title">{modalTitle}</h3>
          <p className="workspace-panel-text">
            {modalDescription.replace(MEMBER_NAME_TOKEN, selectedMember?.fullName || '')}
          </p>
          <form action={removeAction} className="team-page-modal-actions">
            <input type="hidden" name="memberUserId" value={selectedMember?.userId || ''} />
            <Button type="button" view="outlined" size="m" onClick={() => setSelectedUserId(null)}>
              {modalCancel}
            </Button>
            <Button type="submit" view="flat-danger" size="m">
              {modalConfirm}
            </Button>
          </form>
        </div>
      </Modal>
    </>
  );
}
