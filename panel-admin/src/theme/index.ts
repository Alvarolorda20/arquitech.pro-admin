export const APP_THEME_CLASS = 'mb-theme';
export const APP_THEME_COOKIE_KEY = 'app-theme';
export const APP_THEME_STORAGE_KEY = 'app-theme';
export const LEGACY_WORKSPACE_THEME_STORAGE_KEY = 'workspace-theme';

export type AppThemeMode = 'light' | 'dark';

export const DEFAULT_APP_THEME_MODE: AppThemeMode = 'light';

export function normalizeAppTheme(value: string | null | undefined): AppThemeMode {
  return value === 'dark' ? 'dark' : 'light';
}
