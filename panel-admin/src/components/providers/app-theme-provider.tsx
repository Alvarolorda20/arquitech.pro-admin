'use client';

import {createContext, useContext, useEffect, useMemo, useState, type PropsWithChildren} from 'react';
import {ThemeProvider} from '@gravity-ui/uikit';

import {
  APP_THEME_COOKIE_KEY,
  APP_THEME_STORAGE_KEY,
  DEFAULT_APP_THEME_MODE,
  LEGACY_WORKSPACE_THEME_STORAGE_KEY,
  normalizeAppTheme,
  type AppThemeMode,
} from '@/theme';

interface AppThemeContextValue {
  theme: AppThemeMode;
  setTheme: (theme: AppThemeMode) => void;
  toggleTheme: () => void;
}

const AppThemeContext = createContext<AppThemeContextValue | null>(null);
const FALLBACK_CONTEXT: AppThemeContextValue = {
  theme: DEFAULT_APP_THEME_MODE,
  setTheme: () => {},
  toggleTheme: () => {},
};

interface AppThemeProviderProps extends PropsWithChildren {
  initialTheme?: AppThemeMode;
}

function readStoredTheme(): AppThemeMode | null {
  if (typeof window === 'undefined') return null;
  const direct = window.localStorage.getItem(APP_THEME_STORAGE_KEY);
  if (direct) return normalizeAppTheme(direct);

  const legacy = window.localStorage.getItem(LEGACY_WORKSPACE_THEME_STORAGE_KEY);
  if (legacy) return normalizeAppTheme(legacy);

  return null;
}

function applyThemeAttributes(theme: AppThemeMode) {
  const root = document.documentElement;
  root.setAttribute('data-theme', theme);
  root.setAttribute('data-app-theme', theme);
  // Keep legacy attribute while old selectors are migrated.
  root.setAttribute('data-workspace-theme', theme);
}

function persistTheme(theme: AppThemeMode) {
  window.localStorage.setItem(APP_THEME_STORAGE_KEY, theme);
  window.localStorage.setItem(LEGACY_WORKSPACE_THEME_STORAGE_KEY, theme);
  document.cookie = `${APP_THEME_COOKIE_KEY}=${theme}; path=/; max-age=31536000; samesite=lax`;
}

export function AppThemeProvider({children, initialTheme = DEFAULT_APP_THEME_MODE}: AppThemeProviderProps) {
  const [theme, setTheme] = useState<AppThemeMode>(() =>
    typeof window === 'undefined' ? initialTheme : readStoredTheme() || initialTheme,
  );

  useEffect(() => {
    applyThemeAttributes(theme);
    persistTheme(theme);
  }, [theme]);

  const value = useMemo<AppThemeContextValue>(
    () => ({
      theme,
      setTheme,
      toggleTheme: () => setTheme((previous) => (previous === 'light' ? 'dark' : 'light')),
    }),
    [theme],
  );

  return (
    <AppThemeContext.Provider value={value}>
      <ThemeProvider theme={theme}>{children}</ThemeProvider>
    </AppThemeContext.Provider>
  );
}

export function useAppTheme(): AppThemeContextValue {
  const context = useContext(AppThemeContext);
  return context || FALLBACK_CONTEXT;
}
