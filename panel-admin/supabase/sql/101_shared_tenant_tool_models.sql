-- 101_shared_tenant_tool_models.sql
-- Tool/domain tables in a shared database. Every row carries tenant_id.
-- Depends on: 100_shared_tenant_base.sql

create extension if not exists pgcrypto;

-- Projects.
create table if not exists public.projects (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete cascade,
  name text not null check (length(trim(name)) > 0),
  status text not null default 'draft' check (status in ('draft', 'active', 'archived')),
  data jsonb not null default '{}'::jsonb,
  created_by uuid not null references auth.users (id) on delete restrict,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  unique (id, tenant_id)
);

-- Documents.
create table if not exists public.documents (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null,
  project_id uuid not null,
  title text not null check (length(trim(title)) > 0),
  document_type text not null default 'generic',
  status text not null default 'draft' check (status in ('draft', 'review', 'published', 'archived')),
  content jsonb not null default '{}'::jsonb,
  created_by uuid not null references auth.users (id) on delete restrict,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  unique (id, tenant_id),
  constraint documents_project_fk
    foreign key (project_id, tenant_id)
    references public.projects (id, tenant_id)
    on delete cascade
);

-- Tasks.
create table if not exists public.tasks (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null,
  project_id uuid not null,
  document_id uuid,
  title text not null check (length(trim(title)) > 0),
  description text,
  status text not null default 'todo' check (status in ('todo', 'in_progress', 'done', 'cancelled')),
  payload jsonb not null default '{}'::jsonb,
  due_at timestamptz,
  created_by uuid not null references auth.users (id) on delete restrict,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  unique (id, tenant_id),
  constraint tasks_project_fk
    foreign key (project_id, tenant_id)
    references public.projects (id, tenant_id)
    on delete cascade,
  constraint tasks_document_fk
    foreign key (document_id, tenant_id)
    references public.documents (id, tenant_id)
    on delete set null
);

-- Variables.
create table if not exists public.variables (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null,
  project_id uuid not null,
  variable_key text not null check (length(trim(variable_key)) > 0),
  value jsonb not null default 'null'::jsonb,
  source text,
  confidence numeric(5,4) check (confidence is null or (confidence >= 0 and confidence <= 1)),
  metadata jsonb not null default '{}'::jsonb,
  created_by uuid not null references auth.users (id) on delete restrict,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  unique (id, tenant_id),
  unique (tenant_id, project_id, variable_key),
  constraint variables_project_fk
    foreign key (project_id, tenant_id)
    references public.projects (id, tenant_id)
    on delete cascade
);

-- Extractions.
create table if not exists public.extractions (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null,
  project_id uuid not null,
  document_id uuid,
  provider text not null check (length(trim(provider)) > 0),
  status text not null default 'pending' check (status in ('pending', 'running', 'completed', 'failed')),
  raw_payload jsonb not null default '{}'::jsonb,
  normalized_payload jsonb not null default '{}'::jsonb,
  field_confidence jsonb not null default '{}'::jsonb,
  warnings jsonb not null default '[]'::jsonb,
  error_message text,
  created_by uuid not null references auth.users (id) on delete restrict,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  unique (id, tenant_id),
  constraint extractions_project_fk
    foreign key (project_id, tenant_id)
    references public.projects (id, tenant_id)
    on delete cascade,
  constraint extractions_document_fk
    foreign key (document_id, tenant_id)
    references public.documents (id, tenant_id)
    on delete set null
);

-- Tenant and access-path indexes for scale.
create index if not exists projects_tenant_idx
  on public.projects (tenant_id);

create index if not exists projects_tenant_created_by_idx
  on public.projects (tenant_id, created_by);

create index if not exists projects_tenant_created_at_idx
  on public.projects (tenant_id, created_at desc);

create index if not exists documents_tenant_idx
  on public.documents (tenant_id);

create index if not exists documents_tenant_project_idx
  on public.documents (tenant_id, project_id);

create index if not exists documents_tenant_created_by_idx
  on public.documents (tenant_id, created_by);

create index if not exists documents_tenant_created_at_idx
  on public.documents (tenant_id, created_at desc);

create index if not exists tasks_tenant_idx
  on public.tasks (tenant_id);

create index if not exists tasks_tenant_project_idx
  on public.tasks (tenant_id, project_id);

create index if not exists tasks_tenant_document_idx
  on public.tasks (tenant_id, document_id);

create index if not exists tasks_tenant_created_by_idx
  on public.tasks (tenant_id, created_by);

create index if not exists tasks_tenant_status_due_idx
  on public.tasks (tenant_id, status, due_at);

create index if not exists variables_tenant_idx
  on public.variables (tenant_id);

create index if not exists variables_tenant_project_idx
  on public.variables (tenant_id, project_id);

create index if not exists variables_tenant_created_by_idx
  on public.variables (tenant_id, created_by);

create index if not exists extractions_tenant_idx
  on public.extractions (tenant_id);

create index if not exists extractions_tenant_project_idx
  on public.extractions (tenant_id, project_id);

create index if not exists extractions_tenant_document_idx
  on public.extractions (tenant_id, document_id);

create index if not exists extractions_tenant_status_idx
  on public.extractions (tenant_id, status, created_at desc);

create index if not exists extractions_tenant_created_by_idx
  on public.extractions (tenant_id, created_by);

-- Trigger wiring for immutable ownership columns and updated_at.
drop trigger if exists trg_projects_set_updated_at on public.projects;
create trigger trg_projects_set_updated_at
before update on public.projects
for each row
execute function public.set_updated_at();

drop trigger if exists trg_documents_set_updated_at on public.documents;
create trigger trg_documents_set_updated_at
before update on public.documents
for each row
execute function public.set_updated_at();

drop trigger if exists trg_tasks_set_updated_at on public.tasks;
create trigger trg_tasks_set_updated_at
before update on public.tasks
for each row
execute function public.set_updated_at();

drop trigger if exists trg_variables_set_updated_at on public.variables;
create trigger trg_variables_set_updated_at
before update on public.variables
for each row
execute function public.set_updated_at();

drop trigger if exists trg_extractions_set_updated_at on public.extractions;
create trigger trg_extractions_set_updated_at
before update on public.extractions
for each row
execute function public.set_updated_at();

drop trigger if exists trg_projects_protect_columns on public.projects;
create trigger trg_projects_protect_columns
before update on public.projects
for each row
execute function public.protect_row_ownership_columns();

drop trigger if exists trg_documents_protect_columns on public.documents;
create trigger trg_documents_protect_columns
before update on public.documents
for each row
execute function public.protect_row_ownership_columns();

drop trigger if exists trg_tasks_protect_columns on public.tasks;
create trigger trg_tasks_protect_columns
before update on public.tasks
for each row
execute function public.protect_row_ownership_columns();

drop trigger if exists trg_variables_protect_columns on public.variables;
create trigger trg_variables_protect_columns
before update on public.variables
for each row
execute function public.protect_row_ownership_columns();

drop trigger if exists trg_extractions_protect_columns on public.extractions;
create trigger trg_extractions_protect_columns
before update on public.extractions
for each row
execute function public.protect_row_ownership_columns();

-- RLS enabled + forced on all tool tables.
alter table public.projects enable row level security;
alter table public.documents enable row level security;
alter table public.tasks enable row level security;
alter table public.variables enable row level security;
alter table public.extractions enable row level security;

alter table public.projects force row level security;
alter table public.documents force row level security;
alter table public.tasks force row level security;
alter table public.variables force row level security;
alter table public.extractions force row level security;

-- Standard tenant policies:
-- viewer: select only
-- editor/owner: insert/update/delete tenant rows

-- PROJECTS policies.
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

-- DOCUMENTS policies.
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

-- TASKS policies.
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

-- VARIABLES policies.
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

-- EXTRACTIONS policies.
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

-- Grants for direct client operations; RLS enforces isolation.
grant select, insert, update, delete
  on table public.projects, public.documents, public.tasks, public.variables, public.extractions
  to authenticated;
