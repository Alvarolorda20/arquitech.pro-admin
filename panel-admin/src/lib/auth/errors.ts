/**
 * Map raw Supabase / server error strings to i18n keys.
 * The returned key is looked up in messages/{locale}.json under "errors.*".
 * Exported for unit-testing without importing server-only modules.
 */
export type AuthErrorKey =
  | 'invalidCredentials'
  | 'emailNotConfirmed'
  | 'rateLimited'
  | 'alreadyRegistered'
  | 'missingLoginFields'
  | 'missingSignupFields'
  | 'missingEmail'
  | 'passwordMinLength'
  | 'passwordsDoNotMatch'
  | 'unknown';

export function friendlyAuthError(raw: string): AuthErrorKey {
  const lower = raw.toLowerCase();
  if (
    lower.includes('email and password are required') ||
    lower.includes('email/password required')
  ) {
    return 'missingLoginFields';
  }
  if (
    lower.includes('email, password and confirmation are required') ||
    lower.includes('password confirmation required')
  ) {
    return 'missingSignupFields';
  }
  if (lower.includes('email is required')) {
    return 'missingEmail';
  }
  if (
    lower.includes('password must have at least 8 characters') ||
    lower.includes('password too short') ||
    lower.includes('at least 8 characters')
  ) {
    return 'passwordMinLength';
  }
  if (lower.includes('passwords do not match')) {
    return 'passwordsDoNotMatch';
  }
  if (
    lower.includes('invalid login credentials') ||
    lower.includes('invalid email or password') ||
    lower.includes('wrong password') ||
    lower.includes('invalid password') ||
    lower.includes('invalid login')
  ) {
    return 'invalidCredentials';
  }
  if (lower.includes('email not confirmed')) {
    return 'emailNotConfirmed';
  }
  if (lower.includes('too many requests') || lower.includes('rate limit')) {
    return 'rateLimited';
  }
  if (lower.includes('user already registered') || lower.includes('already been registered')) {
    return 'alreadyRegistered';
  }
  return 'unknown';
}

