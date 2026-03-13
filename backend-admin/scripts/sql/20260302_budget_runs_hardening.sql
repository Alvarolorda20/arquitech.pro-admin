-- 2026-03-02
-- Hardening for budget run persistence:
-- - Add RLS policies to public.budget_runs
-- - Add covering FK indexes for core tenant-scoped tables
-- - Seed one initial project per tenant when missing

alter table public.budget_runs enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'budget_runs'
      and policyname = 'budget_runs_select_policy'
  ) then
    create policy budget_runs_select_policy
      on public.budget_runs
      for select
      using (can_read_tenant(tenant_id));
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'budget_runs'
      and policyname = 'budget_runs_insert_policy'
  ) then
    create policy budget_runs_insert_policy
      on public.budget_runs
      for insert
      to authenticated
      with check (
        created_by = auth.uid()
        and has_tenant_role(tenant_id, array['owner'::app_role, 'editor'::app_role])
      );
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'budget_runs'
      and policyname = 'budget_runs_update_policy'
  ) then
    create policy budget_runs_update_policy
      on public.budget_runs
      for update
      using (can_write_row(tenant_id, created_by))
      with check (can_write_row(tenant_id, created_by));
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'budget_runs'
      and policyname = 'budget_runs_delete_policy'
  ) then
    create policy budget_runs_delete_policy
      on public.budget_runs
      for delete
      using (can_write_row(tenant_id, created_by));
  end if;
end
$$;

create index if not exists budget_runs_created_by_fk_idx on public.budget_runs (created_by);
create index if not exists budget_runs_project_fk_idx on public.budget_runs (project_id, tenant_id);
create index if not exists budget_runs_task_fk_idx on public.budget_runs (task_id, tenant_id) where task_id is not null;

create index if not exists projects_created_by_fk_idx on public.projects (created_by);

create index if not exists documents_created_by_fk_idx on public.documents (created_by);
create index if not exists documents_project_fk_cover_idx on public.documents (project_id, tenant_id);

create index if not exists tasks_created_by_fk_idx on public.tasks (created_by);
create index if not exists tasks_project_fk_cover_idx on public.tasks (project_id, tenant_id);
create index if not exists tasks_document_fk_cover_idx on public.tasks (document_id, tenant_id) where document_id is not null;

create index if not exists variables_created_by_fk_idx on public.variables (created_by);
create index if not exists variables_project_fk_cover_idx on public.variables (project_id, tenant_id);

create index if not exists extractions_created_by_fk_idx on public.extractions (created_by);
create index if not exists extractions_project_fk_cover_idx on public.extractions (project_id, tenant_id);
create index if not exists extractions_document_fk_cover_idx on public.extractions (document_id, tenant_id) where document_id is not null;
create index if not exists extractions_run_fk_idx on public.extractions (run_id) where run_id is not null;

insert into public.projects (
  id,
  tenant_id,
  name,
  status,
  data,
  created_by,
  created_at,
  updated_at
)
select
  gen_random_uuid(),
  t.id,
  'Proyecto inicial - ' || t.name,
  'active',
  '{}'::jsonb,
  t.created_by,
  timezone('utc', now()),
  timezone('utc', now())
from public.tenants t
where not exists (
  select 1
  from public.projects p
  where p.tenant_id = t.id
);
