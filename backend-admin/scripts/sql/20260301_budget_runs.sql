-- 2026-03-01
-- Budget execution persistence schema (non-breaking / idempotent)

create extension if not exists pgcrypto;

create table if not exists public.budget_runs (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null,
  project_id uuid not null,
  task_id uuid null,
  pipeline_job_id text not null,
  status text not null,
  force_rerun boolean not null default false,
  request_payload jsonb not null default '{}'::jsonb,
  result_payload jsonb not null default '{}'::jsonb,
  error_message text null,
  started_at timestamptz not null default now(),
  finished_at timestamptz null,
  created_by uuid not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint budget_runs_status_check check (status = any (array['queued','running','completed','failed','cancelled']::text[])),
  constraint budget_runs_project_fk foreign key (project_id, tenant_id) references public.projects (id, tenant_id),
  constraint budget_runs_created_by_fk foreign key (created_by) references auth.users (id),
  constraint budget_runs_task_fk foreign key (task_id, tenant_id) references public.tasks (id, tenant_id),
  constraint budget_runs_project_job_unique unique (project_id, pipeline_job_id)
);

create index if not exists budget_runs_project_started_idx on public.budget_runs (project_id, started_at desc);
create index if not exists budget_runs_tenant_status_idx on public.budget_runs (tenant_id, status);

alter table public.documents add column if not exists source_hash text;
alter table public.documents add column if not exists source_size_bytes bigint;
alter table public.documents add column if not exists source_mime text;

create index if not exists documents_project_type_hash_idx
  on public.documents (project_id, document_type, source_hash)
  where source_hash is not null;

alter table public.extractions add column if not exists run_id uuid;
alter table public.extractions add column if not exists extraction_signature text;

create index if not exists extractions_document_provider_sig_idx
  on public.extractions (document_id, provider, extraction_signature)
  where extraction_signature is not null;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'extractions_run_fk'
      and conrelid = 'public.extractions'::regclass
  ) then
    alter table public.extractions
      add constraint extractions_run_fk
      foreign key (run_id) references public.budget_runs (id);
  end if;
end$$;
