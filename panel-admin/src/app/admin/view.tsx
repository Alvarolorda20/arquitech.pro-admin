'use client';

import {FormEvent, useEffect, useState} from 'react';
import {useRouter, useSearchParams} from 'next/navigation';

import styles from './admin-login.module.css';
import {adminApiRequest, setStoredAdminSession} from '@/lib/admin-session';
import {createSupabaseBrowserClient} from '@/lib/supabase/client';
import {ThemeToggleButton} from '@/components/theme/theme-toggle-button';
import type {AdminAuthTokensResponse} from '@/modules/admin/contracts';

export function AdminLoginView() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const supabase = createSupabaseBrowserClient();
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [loading, setLoading] = useState(false);
  const [statusText, setStatusText] = useState('');
  const [statusKind, setStatusKind] = useState<'idle' | 'error' | 'ok'>('idle');

  useEffect(() => {
    const reason = String(searchParams.get('reason') || '').trim().toLowerCase();
    if (reason === 'session_expired') {
      setStatusKind('error');
      setStatusText('Sesion cerrada por inactividad. Vuelve a iniciar sesion.');
    }
  }, [searchParams]);

  const statusClassName =
    statusKind === 'error'
      ? `${styles.status} ${styles.statusError}`
      : statusKind === 'ok'
        ? `${styles.status} ${styles.statusOk}`
        : styles.status;

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const normalizedEmail = email.trim();
    if (!normalizedEmail || !password) {
      setStatusKind('error');
      setStatusText('Introduce email y password.');
      return;
    }

    setLoading(true);
    setStatusKind('idle');
    setStatusText('Verificando credenciales...');

    try {
      const response = await adminApiRequest('/api/admin/login', {
        method: 'POST',
        headers: {'content-type': 'application/json'},
        body: JSON.stringify({
          email: normalizedEmail,
          password,
        }),
      });

      const payload = (await response.json().catch(() => ({}))) as AdminAuthTokensResponse;
      if (!response.ok) {
        throw new Error(payload.detail || `HTTP ${response.status}`);
      }

      const accessToken = String(payload.access_token || '').trim();
      if (!accessToken) {
        throw new Error('No se recibio access_token.');
      }
      const refreshToken = String(payload.refresh_token || '').trim();
      if (!refreshToken) {
        throw new Error('No se recibio refresh_token.');
      }

      const {error: sessionError} = await supabase.auth.setSession({
        access_token: accessToken,
        refresh_token: refreshToken,
      });
      if (sessionError) {
        throw new Error(`No se pudo sincronizar sesion Supabase: ${sessionError.message}`);
      }

      setStoredAdminSession({
        access_token: accessToken,
        refresh_token: refreshToken,
        token_type: payload.token_type,
        expires_in: payload.expires_in,
        expires_at: payload.expires_at,
      });
      setStatusKind('ok');
      setStatusText('Login correcto. Redirigiendo al panel...');
      const nextPath = String(searchParams.get('next') || '').trim();
      router.replace(nextPath || '/memberships');
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Error de login';
      setStatusKind('error');
      setStatusText(message);
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className={styles.shell}>
      <section className={styles.card}>
        <div style={{display: 'flex', justifyContent: 'flex-end', marginBottom: 10}}>
          <ThemeToggleButton showLabel className="theme-toggle-btn--inline" />
        </div>
        <h1 className={styles.title}>Acceso Admin Global</h1>
        <p className={styles.subtitle}>
          Inicia sesion con usuario admin. El sistema recupera el token y abre el panel de
          membresias.
        </p>

        <form onSubmit={onSubmit}>
          <div className={styles.field}>
            <label htmlFor='admin-email' className={styles.label}>
              Email
            </label>
            <input
              id='admin-email'
              className={styles.input}
              value={email}
              onChange={(event) => setEmail(event.target.value)}
              type='email'
              autoComplete='username'
              required
            />
          </div>
          <div className={styles.field}>
            <label htmlFor='admin-password' className={styles.label}>
              Password
            </label>
            <input
              id='admin-password'
              className={styles.input}
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              type='password'
              autoComplete='current-password'
              required
            />
          </div>
          <button type='submit' className={styles.submit} disabled={loading}>
            {loading ? 'Entrando...' : 'Entrar al panel'}
          </button>
          <div className={statusClassName}>{statusText}</div>
        </form>
      </section>
    </main>
  );
}
