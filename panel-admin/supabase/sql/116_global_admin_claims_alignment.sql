-- 116_global_admin_claims_alignment.sql
-- Align Supabase global-admin detection with metadata claims and preserve read-only cross-tenant access.

begin;

create table if not exists public.global_admin_email_allowlist (
  email text primary key,
  is_active boolean not null default true,
  note text,
  created_at timestamptz not null default timezone('utc', now())
);

alter table if exists public.global_admin_email_allowlist enable row level security;

create or replace function public.is_global_admin()
returns boolean
language sql
stable
security definer
set search_path = public, auth
as $$
  with jwt_claims as (
    select coalesce(
      nullif(current_setting('request.jwt.claims', true), '')::jsonb,
      '{}'::jsonb
    ) as claims
  ),
  metadata as (
    select
      lower(trim(coalesce(claims->>'email', ''))) as email,
      case
        when jsonb_typeof(claims->'app_metadata') = 'object' then claims->'app_metadata'
        else '{}'::jsonb
      end as app_metadata,
      case
        when jsonb_typeof(claims->'user_metadata') = 'object' then claims->'user_metadata'
        else '{}'::jsonb
      end as user_metadata
    from jwt_claims
  )
  select
    exists (
      select 1
      from public.global_admin_users gau
      where gau.user_id = auth.uid()
    )
    or exists (
      select 1
      from metadata m
      join public.global_admin_email_allowlist gea
        on lower(trim(gea.email)) = m.email
      where gea.is_active = true
    )
    or exists (
      select 1
      from metadata m,
        lateral (
          values
            (lower(trim(coalesce(m.app_metadata->>'global_admin', '')))),
            (lower(trim(coalesce(m.app_metadata->>'is_global_admin', '')))),
            (lower(trim(coalesce(m.app_metadata->>'admin', '')))),
            (lower(trim(coalesce(m.user_metadata->>'global_admin', '')))),
            (lower(trim(coalesce(m.user_metadata->>'is_global_admin', '')))),
            (lower(trim(coalesce(m.user_metadata->>'admin', ''))))
        ) as flags(value)
      where flags.value in ('1', 'true', 'yes', 'on')
    )
    or exists (
      select 1
      from metadata m
      where lower(trim(coalesce(m.app_metadata->>'role', ''))) in ('global_admin', 'super_admin', 'admin')
         or lower(trim(coalesce(m.user_metadata->>'role', ''))) in ('global_admin', 'super_admin', 'admin')
    )
    or exists (
      select 1
      from metadata m
      cross join lateral (
        select lower(trim(value)) as role_value
        from jsonb_array_elements_text(
          case
            when jsonb_typeof(m.app_metadata->'roles') = 'array' then m.app_metadata->'roles'
            else '[]'::jsonb
          end
        )
        union all
        select lower(trim(value)) as role_value
        from jsonb_array_elements_text(
          case
            when jsonb_typeof(m.user_metadata->'roles') = 'array' then m.user_metadata->'roles'
            else '[]'::jsonb
          end
        )
      ) as role_list
      where role_list.role_value in ('global_admin', 'super_admin', 'admin')
    )
    or exists (
      select 1
      from metadata m
      cross join lateral unnest(
        string_to_array(lower(coalesce(m.app_metadata->>'roles', '')), ',')
      ) as app_roles(role_value)
      where trim(app_roles.role_value) in ('global_admin', 'super_admin', 'admin')
    )
    or exists (
      select 1
      from metadata m
      cross join lateral unnest(
        string_to_array(lower(coalesce(m.user_metadata->>'roles', '')), ',')
      ) as user_roles(role_value)
      where trim(user_roles.role_value) in ('global_admin', 'super_admin', 'admin')
    );
$$;

create or replace function public.can_read_tenant(p_tenant_id uuid)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select
    public.has_tenant_role(
      p_tenant_id,
      array['owner', 'editor', 'viewer']::public.app_role[]
    )
    or public.is_global_admin();
$$;

drop policy if exists tenant_subscriptions_select_policy on public.tenant_subscriptions;
create policy tenant_subscriptions_select_policy
  on public.tenant_subscriptions
  for select
  using (public.can_read_tenant(tenant_id));

revoke all on function public.is_global_admin() from public;
grant execute on function public.is_global_admin() to authenticated, anon;
grant execute on function public.can_read_tenant(uuid) to authenticated, anon;
grant select on public.global_admin_email_allowlist to service_role;

commit;
