-- 112_tenant_invites_onboarding.sql
-- Owner onboarding invites by email + auto-claim on login.

create table if not exists public.tenant_invites (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants (id) on delete cascade,
  email text not null check (email = lower(trim(email)) and length(trim(email)) > 3),
  role public.app_role not null default 'viewer',
  status text not null default 'pending' check (status in ('pending', 'accepted', 'revoked')),
  invited_by uuid not null references auth.users (id) on delete restrict,
  accepted_by uuid references auth.users (id) on delete set null,
  accepted_at timestamptz,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  unique (tenant_id, email)
);

create index if not exists tenant_invites_email_status_idx
  on public.tenant_invites (email, status);

create index if not exists tenant_invites_tenant_status_idx
  on public.tenant_invites (tenant_id, status);

drop trigger if exists trg_tenant_invites_set_updated_at on public.tenant_invites;
create trigger trg_tenant_invites_set_updated_at
before update on public.tenant_invites
for each row
execute function public.set_updated_at();

create or replace function public.accept_pending_tenant_invites()
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  v_user_id uuid := auth.uid();
  v_email text := lower(trim(coalesce(auth.jwt() ->> 'email', '')));
  v_count integer := 0;
  v_invite record;
begin
  if v_user_id is null then
    return 0;
  end if;

  if v_email = '' then
    return 0;
  end if;

  for v_invite in
    select id, tenant_id, role, invited_by
    from public.tenant_invites
    where email = v_email
      and status = 'pending'
  loop
    insert into public.memberships (tenant_id, user_id, role, status, created_by)
    values (
      v_invite.tenant_id,
      v_user_id,
      v_invite.role,
      'active',
      coalesce(v_invite.invited_by, v_user_id)
    )
    on conflict (tenant_id, user_id) do update
    set
      role = excluded.role,
      status = 'active',
      updated_at = timezone('utc', now());

    insert into public.profiles (tenant_id, user_id, full_name, created_by)
    values (
      v_invite.tenant_id,
      v_user_id,
      null,
      coalesce(v_invite.invited_by, v_user_id)
    )
    on conflict (tenant_id, user_id) do nothing;

    update public.tenant_invites
    set
      status = 'accepted',
      accepted_by = v_user_id,
      accepted_at = timezone('utc', now()),
      updated_at = timezone('utc', now())
    where id = v_invite.id;

    v_count := v_count + 1;
  end loop;

  return v_count;
end;
$$;

alter table public.tenant_invites enable row level security;
alter table public.tenant_invites force row level security;

drop policy if exists tenant_invites_select_policy on public.tenant_invites;
create policy tenant_invites_select_policy
  on public.tenant_invites
  for select
  to authenticated
  using (
    public.has_tenant_role(
      tenant_id,
      array['owner', 'editor', 'viewer']::public.app_role[]
    )
  );

drop policy if exists tenant_invites_insert_policy on public.tenant_invites;
create policy tenant_invites_insert_policy
  on public.tenant_invites
  for insert
  to authenticated
  with check (
    invited_by = auth.uid()
    and (
      public.has_tenant_role(
        tenant_id,
        array['owner']::public.app_role[]
      )
      or (
        public.has_tenant_role(
          tenant_id,
          array['editor']::public.app_role[]
        )
        and role = 'viewer'::public.app_role
      )
    )
  );

drop policy if exists tenant_invites_update_policy on public.tenant_invites;
create policy tenant_invites_update_policy
  on public.tenant_invites
  for update
  to authenticated
  using (
    public.has_tenant_role(
      tenant_id,
      array['owner', 'editor']::public.app_role[]
    )
  )
  with check (
    (
      public.has_tenant_role(
        tenant_id,
        array['owner']::public.app_role[]
      )
      or (
        public.has_tenant_role(
          tenant_id,
          array['editor']::public.app_role[]
        )
        and role = 'viewer'::public.app_role
      )
    )
  );

drop policy if exists tenant_invites_delete_policy on public.tenant_invites;
create policy tenant_invites_delete_policy
  on public.tenant_invites
  for delete
  to authenticated
  using (
    public.has_tenant_role(
      tenant_id,
      array['owner', 'editor']::public.app_role[]
    )
  );

grant select, insert, update, delete on public.tenant_invites to authenticated;
revoke all on function public.accept_pending_tenant_invites() from public;
grant execute on function public.accept_pending_tenant_invites() to authenticated;
