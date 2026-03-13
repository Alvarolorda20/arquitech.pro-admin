-- 107_rebind_tool_policies_and_drop_legacy_app_role.sql
-- Phase C: rebind tool-table policies and drop legacy enum type.

drop policy if exists tasks_select_policy on public.tasks;
create policy tasks_select_policy
  on public.tasks
  for select
  using (public.can_read_tenant(tenant_id));

drop policy if exists tasks_insert_policy on public.tasks;
create policy tasks_insert_policy
  on public.tasks
  for insert
  to authenticated
  with check (
    created_by = auth.uid()
    and public.has_tenant_role(
      tenant_id,
      array['owner', 'editor']::public.app_role[]
    )
  );

drop policy if exists tasks_update_policy on public.tasks;
create policy tasks_update_policy
  on public.tasks
  for update
  using (public.can_write_row(tenant_id, created_by))
  with check (public.can_write_row(tenant_id, created_by));

drop policy if exists tasks_delete_policy on public.tasks;
create policy tasks_delete_policy
  on public.tasks
  for delete
  using (public.can_write_row(tenant_id, created_by));

drop policy if exists variables_select_policy on public.variables;
create policy variables_select_policy
  on public.variables
  for select
  using (public.can_read_tenant(tenant_id));

drop policy if exists variables_insert_policy on public.variables;
create policy variables_insert_policy
  on public.variables
  for insert
  to authenticated
  with check (
    created_by = auth.uid()
    and public.has_tenant_role(
      tenant_id,
      array['owner', 'editor']::public.app_role[]
    )
  );

drop policy if exists variables_update_policy on public.variables;
create policy variables_update_policy
  on public.variables
  for update
  using (public.can_write_row(tenant_id, created_by))
  with check (public.can_write_row(tenant_id, created_by));

drop policy if exists variables_delete_policy on public.variables;
create policy variables_delete_policy
  on public.variables
  for delete
  using (public.can_write_row(tenant_id, created_by));

drop policy if exists extractions_select_policy on public.extractions;
create policy extractions_select_policy
  on public.extractions
  for select
  using (public.can_read_tenant(tenant_id));

drop policy if exists extractions_insert_policy on public.extractions;
create policy extractions_insert_policy
  on public.extractions
  for insert
  to authenticated
  with check (
    created_by = auth.uid()
    and public.has_tenant_role(
      tenant_id,
      array['owner', 'editor']::public.app_role[]
    )
  );

drop policy if exists extractions_update_policy on public.extractions;
create policy extractions_update_policy
  on public.extractions
  for update
  using (public.can_write_row(tenant_id, created_by))
  with check (public.can_write_row(tenant_id, created_by));

drop policy if exists extractions_delete_policy on public.extractions;
create policy extractions_delete_policy
  on public.extractions
  for delete
  using (public.can_write_row(tenant_id, created_by));

do $$
begin
  if exists (
    select 1
    from pg_type
    where typnamespace = 'public'::regnamespace
      and typname = 'app_role_legacy'
  ) then
    execute 'drop function if exists public.has_tenant_role(uuid, public.app_role_legacy[])';
    drop type public.app_role_legacy;
  end if;
end;
$$;
