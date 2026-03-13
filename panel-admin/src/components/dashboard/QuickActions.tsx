import Link from 'next/link';

export interface QuickActionItem {
  id: string;
  label: string;
  href: string;
}

interface QuickActionsProps {
  title: string;
  actions: QuickActionItem[];
  emptyText: string;
}

function actionCategory(actionId: string): string {
  if (actionId.startsWith('comparison-')) return 'Comparacion';
  if (actionId.startsWith('memoria-')) return 'Memoria';
  return 'Workspace';
}

function actionIcon(actionId: string): React.ReactNode {
  if (actionId.startsWith('comparison-current-project')) {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <path d="M4 12h8M8 8l4 4-4 4M14 7h6v10h-6z" />
      </svg>
    );
  }

  if (actionId.startsWith('comparison-progress')) {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <path d="M4 13a8 8 0 1 1 4 6.9M4 19v-6h6M12 8v5l3 2" />
      </svg>
    );
  }

  if (actionId === 'comparison-create') {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <path d="M12 5v14M5 12h14M6 4h12v16H6z" />
      </svg>
    );
  }

  if (actionId === 'memoria-product-home') {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <path d="M6 2h9l5 5v15H6zM14 2v6h6M9 13h6M9 17h6M9 9h3" />
      </svg>
    );
  }

  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M4 5h16v14H4zM8 9h8M8 13h5" />
    </svg>
  );
}

export function QuickActions({title, actions, emptyText}: QuickActionsProps) {
  return (
    <section className="workspace-panel workspace-quick-actions">
      <h2 className="section-title">{title}</h2>
      {actions.length === 0 ? (
        <p className="workspace-panel-text">{emptyText}</p>
      ) : (
        <div className="workspace-quick-actions-grid">
          {actions.map((action) => (
            <Link key={action.id} href={action.href} className="workspace-quick-action-card">
              <span className="workspace-quick-action-icon">{actionIcon(action.id)}</span>
              <span className="workspace-quick-action-content">
                <span className="workspace-quick-action-kicker">{actionCategory(action.id)}</span>
                <span className="workspace-quick-action-title">{action.label}</span>
              </span>
              <span className="workspace-quick-action-arrow" aria-hidden="true">
                <svg viewBox="0 0 24 24">
                  <path d="M7 12h10M13 8l4 4-4 4" />
                </svg>
              </span>
            </Link>
          ))}
        </div>
      )}
    </section>
  );
}
