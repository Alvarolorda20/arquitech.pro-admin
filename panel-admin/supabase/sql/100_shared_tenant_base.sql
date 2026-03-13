-- 100_shared_tenant_base.sql
-- Shared-table multi-tenant foundation for Supabase/PostgreSQL.
-- Tenancy model: every row is tenant-scoped through tenant_id + RLS.

create extension if not exists pgcrypto;

do $$
begin
  if not exists (
    select 1
    from pg_type
    where typname = 'app_role'
      and typnamespace = 'public'::regnamespace
  ) then
    create type public.app_role as enum ('owner', 'editor', 'viewer');
  elsif not exists (
    select 1
    from pg_enum e
    where e.enumtypid = 'public.app_role'::regtype
      and e.enumlabel = 'editor'
  ) then
    alter type public.app_role add value 'editor';
  end if;
end;
$$;

-- Generic updated_at trigger helper.
create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = timezone('utc', now());
  return new;
end;
$$;

-- Protect immutable row ownership fields from client-side tampering.
create or replace function public.protect_row_ownership_columns()
returns trigger
language plpgsql
as $$
begin
  if new.tenant_id is distinct from old.tenant_id then
    raise exception 'tenant_id is immutable';
  end if;

  if new.created_by is distinct from old.created_by then
    raise exception 'created_by is immutable';
  end if;

  if new.created_at is distinct from old.created_at then
    raise exception 'created_at is immutable';
  end if;

  return new;
end;
$$;

-- Protect immutable ownership fields in tenants table.
create or replace function public.protect_tenant_ownership_columns()
returns trigger
language plpgsql
as $$
begin
  if new.created_by is distinct from old.created_by then
    raise exception 'created_by is immutable';
  end if;

  if new.created_at is distinct from old.created_at then
    raise exception 'created_at is immutable';
  end if;

  return new;
end;
$$;

-- Ensure each tenant always has at least one owner membership.
create or replace function public.bootstrap_tenant_owner_membership()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  insert into public.memberships (tenant_id, user_id, role, status, created_by)
  values (new.id, new.created_by, 'owner', 'active', new.created_by)
  on conflict (tenant_id, user_id) do nothing;

  insert into public.profiles (tenant_id, user_id, full_name, created_by)
  values (new.id, new.created_by, null, new.created_by)
  on conflict (tenant_id, user_id) do nothing;

  return new;
end;
$$;

-- Workspace/account table.
create table if not exists public.tenants (
  id uuid primary key default gen_random_uuid(),
  name text not null check (length(trim(name)) > 0),
  slug text not null unique check (slug = lower(slug) and length(trim(slug)) > 0),
  products text[] not null default array[]::text[],
  metadata jsonb not null default '{}'::jsonb,
  created_by uuid not null references auth.users (id) on delete restrict,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

alter table public.tenants
  add column if not exists products text[] not null default array[]::text[];

-- User profile scoped to tenant.
create table if not exists public.profiles (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete cascade,
  user_id uuid not null references auth.users (id) on delete cascade,
  full_name text,
  avatar_url text,
  settings jsonb not null default '{}'::jsonb,
  created_by uuid not null references auth.users (id) on delete restrict,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  unique (tenant_id, user_id),
  unique (id, tenant_id)
);

-- Membership + role per user per tenant.
create table if not exists public.memberships (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete cascade,
  user_id uuid not null references auth.users (id) on delete cascade,
  role public.app_role not null,
  status text not null default 'active' check (status in ('active', 'invited', 'disabled')),
  created_by uuid not null references auth.users (id) on delete restrict,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  unique (tenant_id, user_id),
  unique (id, tenant_id)
);

-- Indexes optimized for tenant isolation and policy checks.
create index if not exists tenants_created_by_idx
  on public.tenants (created_by);

create index if not exists profiles_tenant_idx
  on public.profiles (tenant_id);

create index if not exists profiles_tenant_user_idx
  on public.profiles (tenant_id, user_id);

create index if not exists profiles_tenant_created_by_idx
  on public.profiles (tenant_id, created_by);

create index if not exists memberships_tenant_idx
  on public.memberships (tenant_id);

create index if not exists memberships_user_tenant_idx
  on public.memberships (user_id, tenant_id);

create index if not exists memberships_tenant_user_role_active_idx
  on public.memberships (tenant_id, user_id, role)
  where status = 'active';

-- Tenant role lookup helper used by policies.
create or replace function public.has_tenant_role(
  p_tenant_id uuid,
  p_roles public.app_role[]
)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select exists (
    select 1
    from public.memberships m
    where m.tenant_id = p_tenant_id
      and m.user_id = auth.uid()
      and m.status = 'active'
      and m.role = any (p_roles)
  );
$$;

create or replace function public.can_read_tenant(p_tenant_id uuid)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select public.has_tenant_role(
    p_tenant_id,
    array['owner', 'editor', 'viewer']::public.app_role[]
  );
$$;

create or replace function public.can_write_tenant(p_tenant_id uuid)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select public.has_tenant_role(
    p_tenant_id,
    array['owner', 'editor']::public.app_role[]
  );
$$;

create or replace function public.can_write_row(
  p_tenant_id uuid,
  p_created_by uuid
)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select public.has_tenant_role(
    p_tenant_id,
    array['owner', 'editor']::public.app_role[]
  );
$$;

-- Safety guard: prevent deleting/demoting the last active owner of a tenant.
create or replace function public.prevent_last_owner_removal()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_active_owner_count bigint;
begin
  if old.role = 'owner'
     and old.status = 'active'
     and (
       tg_op = 'DELETE'
       or new.role is distinct from 'owner'
       or new.status is distinct from 'active'
     ) then
    select count(*)
    into v_active_owner_count
    from public.memberships m
    where m.tenant_id = old.tenant_id
      and m.id <> old.id
      and m.role = 'owner'
      and m.status = 'active';

    if v_active_owner_count = 0 then
      raise exception 'Cannot remove the last active owner of tenant %', old.tenant_id;
    end if;
  end if;

  return case when tg_op = 'DELETE' then old else new end;
end;
$$;

-- RPC helper to create tenant + owner membership + owner profile atomically.
create or replace function public.create_tenant(
  p_name text,
  p_slug text,
  p_metadata jsonb default '{}'::jsonb,
  p_products text[] default array['memoria_basica']::text[]
)
returns uuid
language plpgsql
security definer
set search_path = public
as $$
declare
  v_user_id uuid := auth.uid();
  v_tenant_id uuid;
  v_products text[];
begin
  if v_user_id is null then
    raise exception 'Not authenticated';
  end if;

  select coalesce(
    array_agg(distinct lower(trim(product_value))),
    array['memoria_basica']::text[]
  )
  into v_products
  from unnest(coalesce(p_products, array[]::text[])) as product_rows(product_value)
  where length(trim(product_value)) > 0;

  insert into public.tenants (name, slug, products, metadata, created_by)
  values (trim(p_name), lower(trim(p_slug)), v_products, coalesce(p_metadata, '{}'::jsonb), v_user_id)
  returning id into v_tenant_id;

  return v_tenant_id;
end;
$$;

create or replace function public.create_tenant(
  p_name text,
  p_slug text,
  p_metadata jsonb default '{}'::jsonb
)
returns uuid
language sql
security definer
set search_path = public
as $$
  select public.create_tenant(
    p_name,
    p_slug,
    p_metadata,
    array['memoria_basica']::text[]
  );
$$;

-- Trigger wiring.
drop trigger if exists trg_tenants_set_updated_at on public.tenants;
create trigger trg_tenants_set_updated_at
before update on public.tenants
for each row
execute function public.set_updated_at();

drop trigger if exists trg_tenants_protect_columns on public.tenants;
create trigger trg_tenants_protect_columns
before update on public.tenants
for each row
execute function public.protect_tenant_ownership_columns();

drop trigger if exists trg_tenants_bootstrap_owner on public.tenants;
create trigger trg_tenants_bootstrap_owner
after insert on public.tenants
for each row
execute function public.bootstrap_tenant_owner_membership();

drop trigger if exists trg_profiles_set_updated_at on public.profiles;
create trigger trg_profiles_set_updated_at
before update on public.profiles
for each row
execute function public.set_updated_at();

drop trigger if exists trg_memberships_set_updated_at on public.memberships;
create trigger trg_memberships_set_updated_at
before update on public.memberships
for each row
execute function public.set_updated_at();

drop trigger if exists trg_profiles_protect_columns on public.profiles;
create trigger trg_profiles_protect_columns
before update on public.profiles
for each row
execute function public.protect_row_ownership_columns();

drop trigger if exists trg_memberships_protect_columns on public.memberships;
create trigger trg_memberships_protect_columns
before update on public.memberships
for each row
execute function public.protect_row_ownership_columns();

drop trigger if exists trg_memberships_prevent_last_owner_delete on public.memberships;
create trigger trg_memberships_prevent_last_owner_delete
before delete on public.memberships
for each row
execute function public.prevent_last_owner_removal();

drop trigger if exists trg_memberships_prevent_last_owner_update on public.memberships;
create trigger trg_memberships_prevent_last_owner_update
before update on public.memberships
for each row
execute function public.prevent_last_owner_removal();

-- RLS enabled and forced for strict tenant isolation.
alter table public.tenants enable row level security;
alter table public.profiles enable row level security;
alter table public.memberships enable row level security;

alter table public.tenants force row level security;
alter table public.profiles force row level security;
alter table public.memberships force row level security;

-- TENANTS policies.
drop policy if exists tenants_select_policy on public.tenants;
create policy tenants_select_policy
  on public.tenants
  for select
  using (public.can_read_tenant(id));

drop policy if exists tenants_insert_policy on public.tenants;
create policy tenants_insert_policy
  on public.tenants
  for insert
  to authenticated
  with check (auth.uid() is not null and created_by = auth.uid());

drop policy if exists tenants_update_policy on public.tenants;
create policy tenants_update_policy
  on public.tenants
  for update
  using (public.has_tenant_role(id, array['owner', 'editor']::public.app_role[]))
  with check (public.has_tenant_role(id, array['owner', 'editor']::public.app_role[]));

drop policy if exists tenants_delete_policy on public.tenants;
create policy tenants_delete_policy
  on public.tenants
  for delete
  using (public.has_tenant_role(id, array['owner']::public.app_role[]));

-- PROFILES policies.
drop policy if exists profiles_select_policy on public.profiles;
create policy profiles_select_policy
  on public.profiles
  for select
  using (public.can_read_tenant(tenant_id));

drop policy if exists profiles_insert_policy on public.profiles;
create policy profiles_insert_policy
  on public.profiles
  for insert
  to authenticated
  with check (
    auth.uid() is not null
    and created_by = auth.uid()
    and public.can_write_tenant(tenant_id)
  );

drop policy if exists profiles_update_policy on public.profiles;
create policy profiles_update_policy
  on public.profiles
  for update
  using (public.can_write_tenant(tenant_id))
  with check (public.can_write_tenant(tenant_id));

drop policy if exists profiles_delete_policy on public.profiles;
create policy profiles_delete_policy
  on public.profiles
  for delete
  using (public.can_write_tenant(tenant_id));

-- MEMBERSHIPS policies.
drop policy if exists memberships_select_policy on public.memberships;
create policy memberships_select_policy
  on public.memberships
  for select
  using (public.can_read_tenant(tenant_id));

drop policy if exists memberships_insert_owner_policy on public.memberships;
create policy memberships_insert_owner_policy
  on public.memberships
  for insert
  to authenticated
  with check (
    created_by = auth.uid()
    and public.has_tenant_role(tenant_id, array['owner']::public.app_role[])
  );

drop policy if exists memberships_insert_admin_policy on public.memberships;
drop policy if exists memberships_insert_editor_policy on public.memberships;
create policy memberships_insert_editor_policy
  on public.memberships
  for insert
  to authenticated
  with check (
    created_by = auth.uid()
    and public.has_tenant_role(tenant_id, array['editor']::public.app_role[])
    and role in ('viewer'::public.app_role)
  );

drop policy if exists memberships_update_owner_policy on public.memberships;
create policy memberships_update_owner_policy
  on public.memberships
  for update
  using (public.has_tenant_role(tenant_id, array['owner']::public.app_role[]))
  with check (public.has_tenant_role(tenant_id, array['owner']::public.app_role[]));

drop policy if exists memberships_update_admin_policy on public.memberships;
drop policy if exists memberships_update_editor_policy on public.memberships;
create policy memberships_update_editor_policy
  on public.memberships
  for update
  using (
    public.has_tenant_role(tenant_id, array['editor']::public.app_role[])
    and role in ('viewer'::public.app_role)
  )
  with check (
    public.has_tenant_role(tenant_id, array['editor']::public.app_role[])
    and role in ('viewer'::public.app_role)
  );

drop policy if exists memberships_delete_owner_policy on public.memberships;
create policy memberships_delete_owner_policy
  on public.memberships
  for delete
  using (public.has_tenant_role(tenant_id, array['owner']::public.app_role[]));

drop policy if exists memberships_delete_admin_policy on public.memberships;
drop policy if exists memberships_delete_editor_policy on public.memberships;
create policy memberships_delete_editor_policy
  on public.memberships
  for delete
  using (
    public.has_tenant_role(tenant_id, array['editor']::public.app_role[])
    and role in ('viewer'::public.app_role)
  );

-- Grants for direct Supabase client access. RLS remains the enforcement boundary.
grant usage on schema public to authenticated, anon;

grant select, insert, update, delete
  on table public.tenants, public.profiles, public.memberships
  to authenticated;

revoke all on function public.create_tenant(text, text, jsonb) from public;
revoke all on function public.create_tenant(text, text, jsonb, text[]) from public;
revoke all on function public.has_tenant_role(uuid, public.app_role[]) from public;
revoke all on function public.can_read_tenant(uuid) from public;
revoke all on function public.can_write_tenant(uuid) from public;
revoke all on function public.can_write_row(uuid, uuid) from public;

grant execute on function public.create_tenant(text, text, jsonb) to authenticated;
grant execute on function public.create_tenant(text, text, jsonb, text[]) to authenticated;
grant execute on function public.has_tenant_role(uuid, public.app_role[]) to authenticated;
grant execute on function public.can_read_tenant(uuid) to authenticated;
grant execute on function public.can_write_tenant(uuid) to authenticated;
grant execute on function public.can_write_row(uuid, uuid) to authenticated;
