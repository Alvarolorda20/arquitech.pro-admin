-- 104_align_tool_insert_policies_editor.sql
-- Align tool-table INSERT policies with owner/editor write model.

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
