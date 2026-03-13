-- 106_rebind_core_policies_to_canonical_app_role.sql
-- Phase B: rebind helper functions and core policies to canonical app_role.

create or replace function public.normalize_legacy_membership_role()
returns trigger
language plpgsql
as $$
begin
  if new.role::text in ('admin', 'member') then
    new.role = 'editor'::public.app_role;
  end if;
  return new;
end;
$$;

drop trigger if exists trg_memberships_normalize_legacy_role on public.memberships;
create trigger trg_memberships_normalize_legacy_role
before insert or update on public.memberships
for each row
execute function public.normalize_legacy_membership_role();

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

drop policy if exists projects_select_policy on public.projects;
create policy projects_select_policy
  on public.projects
  for select
  using (public.can_read_tenant(tenant_id));

drop policy if exists projects_insert_policy on public.projects;
create policy projects_insert_policy
  on public.projects
  for insert
  to authenticated
  with check (
    created_by = auth.uid()
    and public.has_tenant_role(
      tenant_id,
      array['owner', 'editor']::public.app_role[]
    )
  );

drop policy if exists projects_update_policy on public.projects;
create policy projects_update_policy
  on public.projects
  for update
  using (public.can_write_row(tenant_id, created_by))
  with check (public.can_write_row(tenant_id, created_by));

drop policy if exists projects_delete_policy on public.projects;
create policy projects_delete_policy
  on public.projects
  for delete
  using (public.can_write_row(tenant_id, created_by));

drop policy if exists documents_select_policy on public.documents;
create policy documents_select_policy
  on public.documents
  for select
  using (public.can_read_tenant(tenant_id));

drop policy if exists documents_insert_policy on public.documents;
create policy documents_insert_policy
  on public.documents
  for insert
  to authenticated
  with check (
    created_by = auth.uid()
    and public.has_tenant_role(
      tenant_id,
      array['owner', 'editor']::public.app_role[]
    )
  );

drop policy if exists documents_update_policy on public.documents;
create policy documents_update_policy
  on public.documents
  for update
  using (public.can_write_row(tenant_id, created_by))
  with check (public.can_write_row(tenant_id, created_by));

drop policy if exists documents_delete_policy on public.documents;
create policy documents_delete_policy
  on public.documents
  for delete
  using (public.can_write_row(tenant_id, created_by));

revoke all on function public.has_tenant_role(uuid, public.app_role[]) from public;
revoke all on function public.can_read_tenant(uuid) from public;
revoke all on function public.can_write_tenant(uuid) from public;
revoke all on function public.can_write_row(uuid, uuid) from public;
grant execute on function public.has_tenant_role(uuid, public.app_role[]) to authenticated, anon;
grant execute on function public.can_read_tenant(uuid) to authenticated, anon;
grant execute on function public.can_write_tenant(uuid) to authenticated, anon;
grant execute on function public.can_write_row(uuid, uuid) to authenticated, anon;
