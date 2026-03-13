'use client';

import {useTranslations} from 'next-intl';
import {Avatar, DropdownMenu} from '@gravity-ui/uikit';

import type {TenantMembership} from '@/types/tenant';

interface WorkspaceAccountMenuProps {
  memberships: TenantMembership[];
  activeMembership: TenantMembership | null;
  nextPath: string;
}

function normalizeNextPath(value: string): string {
  if (!value.startsWith('/') || value.startsWith('//')) {
    return '/';
  }

  return value;
}

export function WorkspaceAccountMenu({
  memberships,
  activeMembership,
  nextPath,
}: WorkspaceAccountMenuProps) {
  const t = useTranslations('workspace');
  const safeNextPath = normalizeNextPath(nextPath);

  const tenantItems = memberships.map((membership) => ({
    text: membership.tenants?.name ?? membership.tenant_id,
    href: `/tenants/switch?tenantId=${encodeURIComponent(membership.tenant_id)}&next=${encodeURIComponent(safeNextPath)}`,
  }));

  const items = [tenantItems, [{text: t('menu.settings'), href: '#', disabled: true}], [{text: t('menu.logout'), href: '/logout'}]];

  const initial = activeMembership?.tenants?.name?.slice(0, 1).toUpperCase() ?? 'C';

  return (
    <DropdownMenu
      items={items}
      size="m"
      switcherWrapperClassName="workspace-account-switcher-wrap"
      menuProps={{className: 'workspace-account-gravity-menu'}}
      renderSwitcher={(switcherProps) => (
        <button type="button" className="workspace-account-switcher" aria-label={t('menu.clientMenu')} {...switcherProps}>
          <Avatar text={initial} size="s" theme="brand" view="filled" />
        </button>
      )}
    />
  );
}
