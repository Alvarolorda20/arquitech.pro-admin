import type {Metadata} from 'next';
import {Geist, Geist_Mono} from 'next/font/google';
import {cookies} from 'next/headers';
import {getLocale, getMessages} from 'next-intl/server';
import {NextIntlClientProvider} from 'next-intl';

import {AppThemeProvider} from '@/components/providers/app-theme-provider';
import {SessionActivityGuard} from '@/components/auth/session-activity-guard';
import {
  APP_THEME_CLASS,
  APP_THEME_COOKIE_KEY,
  normalizeAppTheme,
  type AppThemeMode,
} from '@/theme';

import '@gravity-ui/uikit/styles/fonts.css';
import '@gravity-ui/uikit/styles/styles.css';
import '@/theme/gravity-theme.generated.css';
import '@/theme/memoria-theme.css';
import './globals.css';

const geistSans = Geist({
  variable: '--font-geist-sans',
  subsets: ['latin'],
});

const geistMono = Geist_Mono({
  variable: '--font-geist-mono',
  subsets: ['latin'],
});

export const metadata: Metadata = {
  title: 'Architect.pro',
  description: 'Plataforma de herramientas Architect.pro.',
};

export default async function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  const locale = await getLocale();
  const messages = await getMessages();
  const cookieStore = await cookies();
  const initialTheme: AppThemeMode = normalizeAppTheme(cookieStore.get(APP_THEME_COOKIE_KEY)?.value);

  return (
    <html
      lang={locale}
      data-theme={initialTheme}
      data-app-theme={initialTheme}
      data-workspace-theme={initialTheme}
    >
      <body className={`${APP_THEME_CLASS} ${geistSans.variable} ${geistMono.variable}`}>
        <NextIntlClientProvider locale={locale} messages={messages}>
          <AppThemeProvider initialTheme={initialTheme}>
            {children}
            <SessionActivityGuard />
          </AppThemeProvider>
        </NextIntlClientProvider>
      </body>
    </html>
  );
}
