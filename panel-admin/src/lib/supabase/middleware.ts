import { createServerClient } from '@supabase/ssr';
import { type NextRequest, NextResponse } from 'next/server';

/**
 * Refreshes the Supabase session via cookies and resolves the current user.
 * Returns the (possibly cookie-updated) response and the user object (or null).
 */
export async function syncSessionAndTenant(request: NextRequest) {
  // eslint-disable-next-line prefer-const -- reassigned inside supabase cookie setter
  let supabaseResponse = NextResponse.next({ request });

  const supabase = createServerClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!,
    {
      cookies: {
        getAll() {
          return request.cookies.getAll();
        },
        setAll(tokensToSet) {
          tokensToSet.forEach(({ name, value, options }) => {
            request.cookies.set(name, value);
            supabaseResponse.cookies.set(name, value, options);
          });
        },
      },
    },
  );

  // IMPORTANT: Do NOT use supabase.auth.getSession() here.
  // getUser() hits the Supabase Auth server and guarantees the token is valid.
  const {
    data: { user },
  } = await supabase.auth.getUser();

  let isGlobalAdmin = false;
  if (user) {
    try {
      const {data} = await supabase.rpc('is_global_admin');
      isGlobalAdmin = Boolean(data);
    } catch {
      isGlobalAdmin = false;
    }
  }

  return {response: supabaseResponse, user, isGlobalAdmin};
}
