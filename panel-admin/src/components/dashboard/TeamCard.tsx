import Link from 'next/link';
import {Avatar, Button} from '@gravity-ui/uikit';

import type {DashboardTeamMember} from '@/lib/dashboard-context';

interface TeamCardProps {
  title: string;
  subtitle: string;
  membersLabel: string;
  members: DashboardTeamMember[];
  teamDataAvailable: boolean;
  manageTeamHref: string;
  manageTeamLabel: string;
  inviteLabel: string;
  emptyTitle: string;
  emptyDescription: string;
  dataUnavailableDescription: string;
  roleOwnerLabel: string;
  roleEditorLabel: string;
  roleViewerLabel: string;
  activeStatusLabel: string;
  unnamedMemberLabel: string;
}

function roleLabel(
  role: DashboardTeamMember['role'],
  {
    roleOwnerLabel,
    roleEditorLabel,
    roleViewerLabel,
  }: {
    roleOwnerLabel: string;
    roleEditorLabel: string;
    roleViewerLabel: string;
  },
): string {
  if (role === 'owner') return roleOwnerLabel;
  if (role === 'editor') return roleEditorLabel;
  return roleViewerLabel;
}

function initialFromMember(member: DashboardTeamMember): string {
  const source = member.fullName?.trim() || member.userId;
  return source.slice(0, 1).toUpperCase();
}

export function TeamCard({
  title,
  subtitle,
  membersLabel,
  members,
  teamDataAvailable,
  manageTeamHref,
  manageTeamLabel,
  inviteLabel,
  emptyTitle,
  emptyDescription,
  dataUnavailableDescription,
  roleOwnerLabel,
  roleEditorLabel,
  roleViewerLabel,
  activeStatusLabel,
  unnamedMemberLabel,
}: TeamCardProps) {
  const visibleMembers = members.slice(0, 5);

  return (
    <section className="workspace-panel workspace-team-card">
      <div className="workspace-team-head">
        <div>
          <h2 className="section-title">{title}</h2>
          <p className="workspace-panel-text">{subtitle}</p>
        </div>
        <div className="section-actions align-left">
          <Link href={manageTeamHref}>
            <Button view="outlined-action" size="m">
              {manageTeamLabel}
            </Button>
          </Link>
        </div>
      </div>

      {!teamDataAvailable ? (
        <div className="workspace-empty-state">
          <p className="workspace-panel-text">{dataUnavailableDescription}</p>
        </div>
      ) : members.length === 0 ? (
        <div className="workspace-empty-state">
          <p className="workspace-empty-title">{emptyTitle}</p>
          <p className="workspace-panel-text">{emptyDescription}</p>
          <div className="section-actions align-left">
            <Link href={manageTeamHref}>
              <Button view="action" size="m">
                {inviteLabel}
              </Button>
            </Link>
          </div>
        </div>
      ) : (
        <>
          <p className="workspace-team-count">{membersLabel}</p>
          <ul className="workspace-team-list">
            {visibleMembers.map((member) => (
              <li key={member.userId} className="workspace-team-item">
                <div className="workspace-team-main">
                  <Avatar
                    size="s"
                    text={initialFromMember(member)}
                    imgUrl={member.avatarUrl || undefined}
                    view="filled"
                    theme="brand"
                  />
                  <div>
                    <p className="workspace-team-name">
                      {member.fullName || unnamedMemberLabel}
                    </p>
                    <p className="workspace-panel-text">
                      {member.status === 'active' ? activeStatusLabel : member.status}
                    </p>
                  </div>
                </div>
                <span className="workspace-team-role">
                  {roleLabel(member.role, {roleOwnerLabel, roleEditorLabel, roleViewerLabel})}
                </span>
              </li>
            ))}
          </ul>
        </>
      )}
    </section>
  );
}
