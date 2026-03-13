-- 111_catalog_role_plan_subscriptions.sql
-- Catalog-driven roles, permissions and membership plans/subscriptions.
-- Additive migration: keeps backward compatibility with tenants.products + memberships.role.

begin;

-- -----------------------------------------------------------------------------
-- Global roles catalog + assignments
-- -----------------------------------------------------------------------------
create table if not exists public.global_roles (
  role_key text primary key check (role_key = lower(role_key) and length(trim(role_key)) > 0),
  display_name text not null check (length(trim(display_name)) > 0),
  description text,
  is_admin boolean not null default false,
  is_active boolean not null default true,
  sort_order int not null default 100,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

create table if not exists public.global_user_roles (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users (id) on delete cascade,
  role_key text not null references public.global_roles (role_key) on delete restrict,
  created_by uuid references auth.users (id) on delete set null,
  created_at timestamptz not null default timezone('utc', now()),
  unique (user_id, role_key)
);

create index if not exists global_user_roles_user_idx
  on public.global_user_roles (user_id);

create index if not exists global_user_roles_role_idx
  on public.global_user_roles (role_key);

insert into public.global_roles (role_key, display_name, description, is_admin, is_active, sort_order)
values
  ('global_admin', 'Global Admin', 'Full global administration access.', true, true, 10),
  ('support', 'Support', 'Support visibility without admin write privileges.', false, true, 20)
on conflict (role_key) do update
set
  display_name = excluded.display_name,
  description = excluded.description,
  is_admin = excluded.is_admin,
  is_active = excluded.is_active,
  sort_order = excluded.sort_order,
  updated_at = timezone('utc', now());

create or replace view public.global_admin_users as
select
  gur.user_id,
  gur.role_key,
  gr.display_name as role_name,
  gur.created_at
from public.global_user_roles gur
join public.global_roles gr on gr.role_key = gur.role_key
where gr.is_active = true
  and gr.is_admin = true;

-- -----------------------------------------------------------------------------
-- Tenant role + permission catalogs
-- -----------------------------------------------------------------------------
create table if not exists public.tenant_roles_catalog (
  role_key text primary key check (role_key = lower(role_key) and length(trim(role_key)) > 0),
  display_name text not null check (length(trim(display_name)) > 0),
  description text,
  is_admin boolean not null default false,
  can_manage_memberships boolean not null default false,
  is_active boolean not null default true,
  sort_order int not null default 100,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

create table if not exists public.tenant_permissions_catalog (
  permission_key text primary key check (permission_key = lower(permission_key) and length(trim(permission_key)) > 0),
  description text not null check (length(trim(description)) > 0),
  is_active boolean not null default true,
  created_at timestamptz not null default timezone('utc', now())
);

create table if not exists public.tenant_role_permissions (
  role_key text not null references public.tenant_roles_catalog (role_key) on delete cascade,
  permission_key text not null references public.tenant_permissions_catalog (permission_key) on delete cascade,
  created_at timestamptz not null default timezone('utc', now()),
  primary key (role_key, permission_key)
);

insert into public.tenant_roles_catalog (
  role_key, display_name, description, is_admin, can_manage_memberships, is_active, sort_order
)
values
  ('owner', 'Owner', 'Full ownership of the tenant.', true, true, true, 10),
  ('editor', 'Editor', 'Can edit content and operate workflows.', false, false, true, 20),
  ('viewer', 'Viewer', 'Read-only access.', false, false, true, 30)
on conflict (role_key) do update
set
  display_name = excluded.display_name,
  description = excluded.description,
  is_admin = excluded.is_admin,
  can_manage_memberships = excluded.can_manage_memberships,
  is_active = excluded.is_active,
  sort_order = excluded.sort_order,
  updated_at = timezone('utc', now());

insert into public.tenant_permissions_catalog (permission_key, description)
values
  ('tenant.read', 'View tenant data.'),
  ('tenant.write', 'Create/update tenant resources.'),
  ('tenant.memberships.manage', 'Manage tenant memberships.'),
  ('runs.execute', 'Execute processing runs.')
on conflict (permission_key) do update
set
  description = excluded.description,
  is_active = true;

insert into public.tenant_role_permissions (role_key, permission_key)
values
  ('owner', 'tenant.read'),
  ('owner', 'tenant.write'),
  ('owner', 'tenant.memberships.manage'),
  ('owner', 'runs.execute'),
  ('editor', 'tenant.read'),
  ('editor', 'tenant.write'),
  ('editor', 'runs.execute'),
  ('viewer', 'tenant.read')
on conflict (role_key, permission_key) do nothing;

-- -----------------------------------------------------------------------------
-- Global permissions catalog (optional but useful for admin tooling)
-- -----------------------------------------------------------------------------
create table if not exists public.global_permissions_catalog (
  permission_key text primary key check (permission_key = lower(permission_key) and length(trim(permission_key)) > 0),
  description text not null check (length(trim(description)) > 0),
  is_active boolean not null default true,
  created_at timestamptz not null default timezone('utc', now())
);

create table if not exists public.global_role_permissions (
  role_key text not null references public.global_roles (role_key) on delete cascade,
  permission_key text not null references public.global_permissions_catalog (permission_key) on delete cascade,
  created_at timestamptz not null default timezone('utc', now()),
  primary key (role_key, permission_key)
);

insert into public.global_permissions_catalog (permission_key, description)
values
  ('global.admin.read', 'Read global admin dashboards and metrics.'),
  ('global.admin.write', 'Modify memberships and global catalog settings.')
on conflict (permission_key) do update
set
  description = excluded.description,
  is_active = true;

insert into public.global_role_permissions (role_key, permission_key)
values
  ('global_admin', 'global.admin.read'),
  ('global_admin', 'global.admin.write'),
  ('support', 'global.admin.read')
on conflict (role_key, permission_key) do nothing;

-- -----------------------------------------------------------------------------
-- Membership plans catalog + tenant subscriptions
-- -----------------------------------------------------------------------------
create table if not exists public.membership_plans (
  plan_key text primary key check (plan_key = lower(plan_key) and length(trim(plan_key)) > 0),
  display_name text not null check (length(trim(display_name)) > 0),
  description text,
  route_path text,
  is_active boolean not null default true,
  is_default boolean not null default false,
  sort_order int not null default 100,
  features jsonb not null default '{}'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

create table if not exists public.tenant_subscriptions (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete cascade,
  plan_key text not null references public.membership_plans (plan_key) on delete restrict,
  status text not null default 'active'
    check (status in ('active', 'trial', 'paused', 'cancelled', 'expired')),
  starts_at timestamptz not null default timezone('utc', now()),
  ends_at timestamptz,
  created_by uuid references auth.users (id) on delete set null,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  unique (tenant_id, plan_key)
);

create index if not exists tenant_subscriptions_tenant_idx
  on public.tenant_subscriptions (tenant_id);

create index if not exists tenant_subscriptions_active_idx
  on public.tenant_subscriptions (tenant_id, status)
  where status in ('active', 'trial');

insert into public.membership_plans (
  plan_key, display_name, description, route_path, is_active, is_default, sort_order, features
)
values
  (
    'memoria_basica',
    'Memoria Basica',
    'Generacion de memoria tecnica basica.',
    '/products/memoria-basica',
    true,
    true,
    10,
    '{"module":"memoria_basica"}'::jsonb
  ),
  (
    'comparacion_presupuestos',
    'Comparacion de Presupuestos',
    'Comparador y analisis de presupuestos.',
    '/products/comparacion-presupuestos',
    true,
    true,
    20,
    '{"module":"comparacion_presupuestos"}'::jsonb
  )
on conflict (plan_key) do update
set
  display_name = excluded.display_name,
  description = excluded.description,
  route_path = excluded.route_path,
  is_active = excluded.is_active,
  is_default = excluded.is_default,
  sort_order = excluded.sort_order,
  features = excluded.features,
  updated_at = timezone('utc', now());

-- Backfill subscriptions from legacy tenants.products
insert into public.tenant_subscriptions (tenant_id, plan_key, status, starts_at, created_by)
select
  t.id as tenant_id,
  lower(trim(p.plan_key)) as plan_key,
  'active' as status,
  coalesce(t.created_at, timezone('utc', now())) as starts_at,
  t.created_by
from public.tenants t
cross join lateral unnest(coalesce(t.products, array[]::text[])) as p(plan_key)
join public.membership_plans mp
  on mp.plan_key = lower(trim(p.plan_key))
on conflict (tenant_id, plan_key) do update
set
  status = excluded.status,
  ends_at = null,
  updated_at = timezone('utc', now());

-- Ensure tenants with no products get default plans into subscriptions.
insert into public.tenant_subscriptions (tenant_id, plan_key, status, starts_at, created_by)
select
  t.id as tenant_id,
  mp.plan_key,
  'active',
  coalesce(t.created_at, timezone('utc', now())),
  t.created_by
from public.tenants t
join public.membership_plans mp on mp.is_default = true and mp.is_active = true
where not exists (
  select 1
  from public.tenant_subscriptions ts
  where ts.tenant_id = t.id
    and ts.status in ('active', 'trial')
)
on conflict (tenant_id, plan_key) do nothing;

create or replace function public.sync_tenant_products_from_subscriptions(
  p_tenant_id uuid
)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_products text[];
begin
  if p_tenant_id is null then
    return;
  end if;

  select coalesce(
    array_agg(ts.plan_key order by mp.sort_order, ts.plan_key),
    array[]::text[]
  )
  into v_products
  from public.tenant_subscriptions ts
  left join public.membership_plans mp on mp.plan_key = ts.plan_key
  where ts.tenant_id = p_tenant_id
    and ts.status in ('active', 'trial');

  update public.tenants
  set
    products = coalesce(v_products, array[]::text[]),
    updated_at = timezone('utc', now())
  where id = p_tenant_id;
end;
$$;

create or replace function public.trg_sync_tenant_products_from_subscriptions()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  perform public.sync_tenant_products_from_subscriptions(
    case
      when tg_op = 'DELETE' then old.tenant_id
      else new.tenant_id
    end
  );
  return case when tg_op = 'DELETE' then old else new end;
end;
$$;

drop trigger if exists trg_tenant_subscriptions_sync_tenant_products on public.tenant_subscriptions;
create trigger trg_tenant_subscriptions_sync_tenant_products
after insert or update or delete on public.tenant_subscriptions
for each row
execute function public.trg_sync_tenant_products_from_subscriptions();

-- Normalize all legacy tenants.products from subscription truth source.
do $$
declare
  v_tenant_id uuid;
begin
  for v_tenant_id in
    select id from public.tenants
  loop
    perform public.sync_tenant_products_from_subscriptions(v_tenant_id);
  end loop;
end;
$$;

-- -----------------------------------------------------------------------------
-- create_tenant RPC updates: validate plans against membership_plans catalog and
-- materialize tenant_subscriptions atomically.
-- -----------------------------------------------------------------------------
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
    array[]::text[]
  )
  into v_products
  from unnest(coalesce(p_products, array[]::text[])) as product_rows(product_value)
  join public.membership_plans mp
    on mp.plan_key = lower(trim(product_rows.product_value))
   and mp.is_active = true
  where length(trim(product_value)) > 0;

  if coalesce(array_length(v_products, 1), 0) = 0 then
    select coalesce(
      array_agg(plan_key order by sort_order, plan_key),
      array['memoria_basica']::text[]
    )
    into v_products
    from public.membership_plans
    where is_active = true
      and is_default = true;
  end if;

  insert into public.tenants (name, slug, products, metadata, created_by)
  values (
    trim(p_name),
    lower(trim(p_slug)),
    coalesce(v_products, array['memoria_basica']::text[]),
    coalesce(p_metadata, '{}'::jsonb),
    v_user_id
  )
  returning id into v_tenant_id;

  insert into public.tenant_subscriptions (tenant_id, plan_key, status, starts_at, created_by)
  select
    v_tenant_id,
    product_key,
    'active',
    timezone('utc', now()),
    v_user_id
  from unnest(coalesce(v_products, array['memoria_basica']::text[])) as product_keys(product_key)
  on conflict (tenant_id, plan_key) do update
  set
    status = 'active',
    ends_at = null,
    updated_at = timezone('utc', now());

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

-- -----------------------------------------------------------------------------
-- RLS policies for catalog tables
-- -----------------------------------------------------------------------------
alter table if exists public.membership_plans enable row level security;
alter table if exists public.tenant_subscriptions enable row level security;
alter table if exists public.global_roles enable row level security;
alter table if exists public.global_user_roles enable row level security;
alter table if exists public.tenant_roles_catalog enable row level security;
alter table if exists public.tenant_permissions_catalog enable row level security;
alter table if exists public.tenant_role_permissions enable row level security;
alter table if exists public.global_permissions_catalog enable row level security;
alter table if exists public.global_role_permissions enable row level security;

drop policy if exists membership_plans_select_authenticated on public.membership_plans;
create policy membership_plans_select_authenticated
  on public.membership_plans
  for select
  to authenticated
  using (is_active = true);

drop policy if exists tenant_subscriptions_select_policy on public.tenant_subscriptions;
create policy tenant_subscriptions_select_policy
  on public.tenant_subscriptions
  for select
  using (
    public.has_tenant_role(
      tenant_id,
      array['owner', 'editor', 'viewer']::public.app_role[]
    )
  );

drop policy if exists tenant_subscriptions_manage_owner_policy on public.tenant_subscriptions;
create policy tenant_subscriptions_manage_owner_policy
  on public.tenant_subscriptions
  for all
  using (
    public.has_tenant_role(
      tenant_id,
      array['owner']::public.app_role[]
    )
  )
  with check (
    public.has_tenant_role(
      tenant_id,
      array['owner']::public.app_role[]
    )
  );

-- Catalog reads for authenticated users.
drop policy if exists tenant_roles_catalog_select_authenticated on public.tenant_roles_catalog;
create policy tenant_roles_catalog_select_authenticated
  on public.tenant_roles_catalog
  for select
  to authenticated
  using (is_active = true);

drop policy if exists tenant_permissions_catalog_select_authenticated on public.tenant_permissions_catalog;
create policy tenant_permissions_catalog_select_authenticated
  on public.tenant_permissions_catalog
  for select
  to authenticated
  using (is_active = true);

drop policy if exists tenant_role_permissions_select_authenticated on public.tenant_role_permissions;
create policy tenant_role_permissions_select_authenticated
  on public.tenant_role_permissions
  for select
  to authenticated
  using (true);

drop policy if exists global_roles_select_authenticated on public.global_roles;
create policy global_roles_select_authenticated
  on public.global_roles
  for select
  to authenticated
  using (is_active = true);

drop policy if exists global_permissions_catalog_select_authenticated on public.global_permissions_catalog;
create policy global_permissions_catalog_select_authenticated
  on public.global_permissions_catalog
  for select
  to authenticated
  using (is_active = true);

drop policy if exists global_role_permissions_select_authenticated on public.global_role_permissions;
create policy global_role_permissions_select_authenticated
  on public.global_role_permissions
  for select
  to authenticated
  using (true);

-- Keep global_user_roles private by default (no select policy for authenticated users).

grant select on public.membership_plans to authenticated;
grant select on public.tenant_roles_catalog to authenticated;
grant select on public.tenant_permissions_catalog to authenticated;
grant select on public.tenant_role_permissions to authenticated;
grant select on public.global_roles to authenticated;
grant select on public.global_permissions_catalog to authenticated;
grant select on public.global_role_permissions to authenticated;
grant execute on function public.sync_tenant_products_from_subscriptions(uuid) to authenticated;
grant select on public.global_admin_users to authenticated;

-- Keep updated_at in sync for new tables.
drop trigger if exists trg_global_roles_set_updated_at on public.global_roles;
create trigger trg_global_roles_set_updated_at
before update on public.global_roles
for each row
execute function public.set_updated_at();

drop trigger if exists trg_tenant_roles_catalog_set_updated_at on public.tenant_roles_catalog;
create trigger trg_tenant_roles_catalog_set_updated_at
before update on public.tenant_roles_catalog
for each row
execute function public.set_updated_at();

drop trigger if exists trg_membership_plans_set_updated_at on public.membership_plans;
create trigger trg_membership_plans_set_updated_at
before update on public.membership_plans
for each row
execute function public.set_updated_at();

drop trigger if exists trg_tenant_subscriptions_set_updated_at on public.tenant_subscriptions;
create trigger trg_tenant_subscriptions_set_updated_at
before update on public.tenant_subscriptions
for each row
execute function public.set_updated_at();

commit;
