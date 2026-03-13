-- 108_e2e_seed_auth_routing.sql
-- Idempotent seed for auth/routing Playwright tests in staging.
-- Prerequisite: the auth user already exists in Supabase Auth with this email.
-- Update this email if your staging test user is different.

do $$
declare
  v_user_email text := 'e2e.staging.architech@example.com';
  v_user_id uuid;
  v_primary_tenant_id uuid := '11111111-1111-4111-8111-111111111111';
  v_secondary_tenant_id uuid := '22222222-2222-4222-8222-222222222222';
begin
  select u.id into v_user_id
  from auth.users u
  where lower(u.email) = lower(v_user_email)
  limit 1;

  if v_user_id is null then
    raise exception 'E2E seed aborted: auth user % not found. Create it first in Supabase Auth.', v_user_email;
  end if;

  update auth.users
  set instance_id = '00000000-0000-0000-0000-000000000000'::uuid,
      confirmation_token = coalesce(confirmation_token, ''),
      recovery_token = coalesce(recovery_token, ''),
      email_change_token_new = coalesce(email_change_token_new, ''),
      email_change_token_current = coalesce(email_change_token_current, ''),
      email_change = coalesce(email_change, ''),
      phone_change = coalesce(phone_change, ''),
      phone_change_token = coalesce(phone_change_token, ''),
      reauthentication_token = coalesce(reauthentication_token, ''),
      updated_at = timezone('utc', now())
  where id = v_user_id;

  insert into public.tenants (id, name, slug, products, metadata, created_by)
  values
    (
      v_primary_tenant_id,
      'E2E Tenant Primary',
      'e2e-tenant-primary',
      array['memoria_basica', 'comparacion_presupuestos']::text[],
      jsonb_build_object('seed', '108_e2e_seed_auth_routing'),
      v_user_id
    ),
    (
      v_secondary_tenant_id,
      'E2E Tenant Secondary',
      'e2e-tenant-secondary',
      array['memoria_basica']::text[],
      jsonb_build_object('seed', '108_e2e_seed_auth_routing'),
      v_user_id
    )
  on conflict (id) do update
    set
      name = excluded.name,
      slug = excluded.slug,
      products = excluded.products,
      metadata = excluded.metadata,
      updated_at = timezone('utc', now());

  insert into public.memberships (tenant_id, user_id, role, status, created_by)
  values
    (v_primary_tenant_id, v_user_id, 'owner', 'active', v_user_id),
    (v_secondary_tenant_id, v_user_id, 'owner', 'active', v_user_id)
  on conflict (tenant_id, user_id) do update
    set
      role = excluded.role,
      status = excluded.status,
      updated_at = timezone('utc', now());

  insert into public.profiles (tenant_id, user_id, full_name, settings, created_by)
  values
    (
      v_primary_tenant_id,
      v_user_id,
      'E2E User',
      jsonb_build_object('seed', true),
      v_user_id
    ),
    (
      v_secondary_tenant_id,
      v_user_id,
      'E2E User',
      jsonb_build_object('seed', true),
      v_user_id
    )
  on conflict (tenant_id, user_id) do update
    set
      full_name = excluded.full_name,
      settings = excluded.settings,
      updated_at = timezone('utc', now());
end;
$$;
