import Link from 'next/link';
import {Avatar} from '@gravity-ui/uikit';

import type {DashboardTenantSwitcherItem} from '@/lib/dashboard-context';

interface WorkspaceContextCardProps {
  workspaceName: string;
  workspaceAvatarUrl: string | null;
  roleLabel: string;
  roleTitle: string;
  workspaceTitle: string;
  productsTitle: string;
  tenantSwitcherTitle: string;
  tenantSwitcherItems: DashboardTenantSwitcherItem[];
  children: React.ReactNode;
}

function initialFromName(name: string): string {
  const trimmed = name.trim();
  if (!trimmed) return 'W';
  return trimmed.slice(0, 1).toUpperCase();
}

export function WorkspaceContextCard({
  workspaceName,
  workspaceAvatarUrl,
  roleLabel,
  roleTitle,
  workspaceTitle,
  productsTitle,
  tenantSwitcherTitle,
  tenantSwitcherItems,
  children,
}: WorkspaceContextCardProps) {
  return (
    <section className="workspace-panel workspace-context-card">
      <div className="workspace-context-header">
        <div className="workspace-context-main">
          <Avatar
            size="l"
            text={initialFromName(workspaceName)}
            imgUrl={workspaceAvatarUrl || undefined}
            view="filled"
            theme="brand"
          />
          <div>
            <p className="workspace-context-kicker">{workspaceTitle}</p>
            <h2 className="workspace-context-title">{workspaceName}</h2>
          </div>
        </div>
        <div className="workspace-context-role">
          <p className="workspace-context-kicker">{roleTitle}</p>
          <span className="workspace-role-pill">{roleLabel}</span>
        </div>
      </div>

      <div className="workspace-context-products">
        <p className="workspace-context-kicker">{productsTitle}</p>
        {children}
      </div>

      {tenantSwitcherItems.length > 1 ? (
        <div className="workspace-context-switcher">
          <p className="workspace-context-kicker">{tenantSwitcherTitle}</p>
          <div className="workspace-tenant-switcher-list">
            {tenantSwitcherItems.map((tenant) => (
              <Link
                key={tenant.tenantId}
                href={tenant.href}
                className={`workspace-tenant-switcher-item${tenant.isActive ? ' is-active' : ''}`}
              >
                {tenant.name}
              </Link>
            ))}
          </div>
        </div>
      ) : null}
    </section>
  );
}
