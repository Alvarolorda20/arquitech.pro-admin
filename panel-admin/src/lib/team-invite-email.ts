import type {TenantRole} from '@/types/tenant';
import {createClient} from '@supabase/supabase-js';

type InviteEmailFailureReason = 'provider_not_configured' | 'request_failed' | 'rate_limited';

interface InviteEmailInput {
  toEmail: string;
  tenantName: string;
  role: TenantRole;
  invitedByName?: string;
  invitedByEmail?: string;
}

type InviteEmailResult = {ok: true} | {ok: false; reason: InviteEmailFailureReason};

const RESEND_API_URL = 'https://api.resend.com/emails';

function isProductionRuntime(): boolean {
  const nodeEnv = String(process.env.NODE_ENV || '').trim().toLowerCase();
  const appEnv = String(process.env.ENVIRONMENT || '').trim().toLowerCase();
  return nodeEnv === 'production' || appEnv === 'production' || appEnv === 'prod';
}

function resolveAppBaseUrl(): string {
  const fromEnv =
    process.env.APP_BASE_URL ||
    process.env.NEXT_PUBLIC_APP_URL ||
    process.env.NEXT_PUBLIC_SITE_URL ||
    process.env.SITE_URL ||
    '';
  const normalized = fromEnv.trim().replace(/\/+$/, '');
  if (normalized) return normalized;

  const vercel = (process.env.VERCEL_URL || '').trim();
  if (vercel) return `https://${vercel.replace(/\/+$/, '')}`;

  if (isProductionRuntime()) {
    return '';
  }

  return 'http://localhost:3000';
}

function roleLabel(role: TenantRole): string {
  if (role === 'owner') return 'Owner';
  if (role === 'editor') return 'Editor';
  return 'Member';
}

function isRateLimitedError(error: {message?: string; status?: number} | null | undefined): boolean {
  if (!error) return false;
  if (typeof error.status === 'number' && error.status === 429) return true;
  const message = String(error.message || '').toLowerCase();
  return message.includes('rate limit') || message.includes('too many');
}

async function sendInviteViaSupabaseAuth(toEmail: string): Promise<InviteEmailResult> {
  const supabaseUrl = (process.env.NEXT_PUBLIC_SUPABASE_URL || '').trim();
  const supabaseAnonKey = (process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || '').trim();
  if (!supabaseUrl || !supabaseAnonKey) {
    return {ok: false, reason: 'provider_not_configured'};
  }

  const baseUrl = resolveAppBaseUrl();
  if (!baseUrl) {
    console.error('[team-invite-email] missing APP_BASE_URL/SITE_URL in production');
    return {ok: false, reason: 'provider_not_configured'};
  }
  const redirectTo = `${baseUrl}/register?invited=1&next=%2F`;

  try {
    const supabase = createClient(supabaseUrl, supabaseAnonKey, {
      auth: {
        autoRefreshToken: false,
        persistSession: false,
        detectSessionInUrl: false,
      },
    });

    const {error} = await supabase.auth.signInWithOtp({
      email: toEmail,
      options: {
        // Invitation flow should also work for users who do not exist yet.
        shouldCreateUser: true,
        emailRedirectTo: redirectTo,
      },
    });
    if (error) {
      if (isRateLimitedError(error)) {
        return {ok: false, reason: 'rate_limited'};
      }
      console.error('[team-invite-email] supabase auth otp failed', error.message);
      return {ok: false, reason: 'request_failed'};
    }

    return {ok: true};
  } catch (error) {
    console.error('[team-invite-email] supabase auth otp request error', error);
    return {ok: false, reason: 'request_failed'};
  }
}

export async function sendTenantInviteEmail({
  toEmail,
  tenantName,
  role,
  invitedByName,
  invitedByEmail,
}: InviteEmailInput): Promise<InviteEmailResult> {
  const apiKey = (process.env.RESEND_API_KEY || '').trim();
  const fromAddress = (process.env.INVITE_EMAIL_FROM || '').trim();

  if (!apiKey || !fromAddress) {
    return sendInviteViaSupabaseAuth(toEmail);
  }

  const baseUrl = resolveAppBaseUrl();
  if (!baseUrl) {
    console.error('[team-invite-email] missing APP_BASE_URL/SITE_URL in production');
    return {ok: false, reason: 'provider_not_configured'};
  }
  const registerUrl = `${baseUrl}/register?invited=1&next=%2F`;
  const inviter = invitedByName || invitedByEmail || 'El equipo';
  const tenant = tenantName.trim() || 'workspace';
  const roleText = roleLabel(role);

  const subject = `Invitacion al workspace ${tenant}`;
  const text = [
    `Hola,`,
    ``,
    `${inviter} te ha invitado al workspace "${tenant}" con rol ${roleText}.`,
    ``,
    `Completa tu acceso creando tu contrasena desde aqui: ${registerUrl}`,
    ``,
    `Importante: usa este mismo email para que la invitacion se active automaticamente.`,
  ].join('\n');

  const html = `
    <div style="font-family:Arial,Helvetica,sans-serif;line-height:1.5;color:#111827">
      <h2 style="margin:0 0 12px">Invitacion de equipo</h2>
      <p style="margin:0 0 10px">${inviter} te ha invitado al workspace <strong>${tenant}</strong> con rol <strong>${roleText}</strong>.</p>
      <p style="margin:0 0 14px">Para aceptar la invitacion y crear tu contrasena, usa este enlace:</p>
      <p style="margin:0 0 12px"><a href="${registerUrl}" style="color:#0f62fe">Completar acceso</a></p>
      <p style="margin:0;color:#6b7280;font-size:13px">La vinculacion al workspace se hara automaticamente al iniciar sesion.</p>
    </div>
  `;

  try {
    const response = await fetch(RESEND_API_URL, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        from: fromAddress,
        to: [toEmail],
        subject,
        text,
        html,
      }),
      cache: 'no-store',
    });

    if (!response.ok) {
      if (response.status === 429) {
        return {ok: false, reason: 'rate_limited'};
      }
      const details = await response.text();
      console.error('[team-invite-email] resend request failed', response.status, details);
      return sendInviteViaSupabaseAuth(toEmail);
    }

    return {ok: true};
  } catch (error) {
    console.error('[team-invite-email] resend request error', error);
    return sendInviteViaSupabaseAuth(toEmail);
  }
}
