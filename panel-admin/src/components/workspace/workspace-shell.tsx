import Link from 'next/link';
import {getTranslations} from 'next-intl/server';

import {getAllProducts, type ProductDefinition} from '@/lib/products';
import type {TenantMembership} from '@/types/tenant';
import {WorkspaceAccountMenu} from '@/components/workspace/workspace-account-menu';
import {WorkspaceTopControls} from '@/components/workspace/workspace-top-controls';

interface WorkspaceShellProps {
  title: string;
  subtitle?: string;
  activePath: string;
  memberships: TenantMembership[];
  activeMembership: TenantMembership | null;
  enabledProducts: ProductDefinition[];
  children: React.ReactNode;
}

function isActive(activePath: string, href: string): boolean {
  return activePath === href || activePath.startsWith(`${href}/`);
}

function renderIcon(icon: 'dashboard' | 'memoria' | 'presupuestos') {
  if (icon === 'dashboard') {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <path d="M3 3h8v8H3zM13 3h8v5h-8zM13 10h8v11h-8zM3 13h8v8H3z" />
      </svg>
    );
  }

  if (icon === 'memoria') {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <path d="M6 2h9l5 5v15H6zM14 2v6h6M9 13h6M9 17h6M9 9h3" />
      </svg>
    );
  }

  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M4 5h16v4H4zM4 10h10v4H4zM4 15h16v4H4z" />
    </svg>
  );
}

function productIcon(productId: ProductDefinition['id']): 'memoria' | 'presupuestos' {
  if (productId === 'memoria_basica') {
    return 'memoria';
  }

  return 'presupuestos';
}

function resolveCurrentAppKey(activePath: string): ProductDefinition['id'] | null {
  if (activePath.startsWith('/products/comparacion-presupuestos')) {
    return 'comparacion_presupuestos';
  }
  if (activePath.startsWith('/products/memoria-basica')) {
    return 'memoria_basica';
  }
  return null;
}

export async function WorkspaceShell({
  title,
  subtitle,
  activePath,
  memberships,
  activeMembership,
  enabledProducts,
  children,
}: WorkspaceShellProps) {
  const t = await getTranslations('workspace');
  const productLabels: Record<ProductDefinition['id'], string> = {
    memoria_basica: t('products.memoria_basica'),
    comparacion_presupuestos: t('products.comparacion_presupuestos'),
  };
  const currentAppKey = resolveCurrentAppKey(activePath);
  const allProducts = getAllProducts();
  const enabledProductIds = new Set(enabledProducts.map((product) => product.id));
  return (
    <div className="workspace-shell">
      <aside className="workspace-sidebar">
        <div className="workspace-sidebar-main">
          <nav className="workspace-nav" aria-label={t('nav')}>
            <Link
              href="/"
              aria-label={t('dashboardLabel')}
              data-tooltip={t('dashboardLabel')}
              className={`workspace-nav-item${isActive(activePath, '/') ? ' active' : ''}`}
            >
              <span className="workspace-nav-item-icon">{renderIcon('dashboard')}</span>
              <span className="workspace-nav-item-sr">{t('dashboardLabel')}</span>
            </Link>

            {allProducts.length > 0 ? <div className="workspace-nav-divider" aria-hidden="true" /> : null}

            {allProducts.map((product) => {
              const productLabel = productLabels[product.id] || product.title;
              const enabled = enabledProductIds.has(product.id);
              const label = enabled ? productLabel : `${productLabel} (${t('disabledLabel')})`;
              if (!enabled) {
                return (
                  <div
                    key={product.id}
                    aria-label={label}
                    aria-disabled="true"
                    data-tooltip={label}
                    className="workspace-nav-item workspace-nav-item-disabled"
                  >
                    <span className="workspace-nav-item-icon">{renderIcon(productIcon(product.id))}</span>
                    <span className="workspace-nav-item-sr">{label}</span>
                  </div>
                );
              }

              return (
                <Link
                  key={product.id}
                  href={product.href}
                  aria-label={productLabel}
                  data-tooltip={productLabel}
                  className={`workspace-nav-item${isActive(activePath, product.href) ? ' active' : ''}`}
                >
                  <span className="workspace-nav-item-icon">{renderIcon(productIcon(product.id))}</span>
                  <span className="workspace-nav-item-sr">{productLabel}</span>
                </Link>
              );
            })}
          </nav>
        </div>
        <div className="workspace-sidebar-footer">
          <WorkspaceAccountMenu
            memberships={memberships}
            activeMembership={activeMembership}
            nextPath={activePath}
          />
        </div>
      </aside>

      <section className="workspace-main">
        <header className="workspace-topbar">
          <WorkspaceTopControls tenantId={activeMembership?.tenant_id || null} appKey={currentAppKey} />
        </header>

        <header className="workspace-page-head">
          <div>
            <h1 className="workspace-title">{title}</h1>
            {subtitle ? <p className="workspace-subtitle">{subtitle}</p> : null}
          </div>
        </header>

        <div className="workspace-content">{children}</div>
      </section>
    </div>
  );
}
