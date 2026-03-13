'use client';

import {Moon, Sun} from 'lucide-react';
import {useTranslations} from 'next-intl';

import {useAppTheme} from '@/components/providers/app-theme-provider';

interface ThemeToggleButtonProps {
  className?: string;
  showLabel?: boolean;
}

export function ThemeToggleButton({className = '', showLabel = false}: ThemeToggleButtonProps) {
  const t = useTranslations('workspace');
  const {theme, toggleTheme} = useAppTheme();
  const nextThemeLabel = theme === 'light' ? t('theme.dark') : t('theme.light');

  return (
    <button
      type="button"
      onClick={toggleTheme}
      aria-label={nextThemeLabel}
      title={nextThemeLabel}
      className={`theme-toggle-btn ${className}`.trim()}
    >
      {theme === 'light' ? (
        <Moon className="theme-toggle-btn-icon" strokeWidth={2} aria-hidden="true" />
      ) : (
        <Sun className="theme-toggle-btn-icon" strokeWidth={2} aria-hidden="true" />
      )}
      {showLabel ? <span className="theme-toggle-btn-label">{nextThemeLabel}</span> : null}
    </button>
  );
}
