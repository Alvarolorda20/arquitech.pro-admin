-- 114_profile_names_and_team_soft_delete.sql
-- Ensure full_name is propagated to tenant profiles and keep soft-delete semantics for memberships.

create or replace function public.resolve_auth_full_name(p_user_id uuid)
returns text
language sql
stable
security definer
set search_path = public, auth
as $$
  select nullif(
    trim(
      coalesce(
        u.raw_user_meta_data ->> 'full_name',
        nullif(
          trim(
            concat_ws(
              ' ',
              u.raw_user_meta_data ->> 'first_name',
              u.raw_user_meta_data ->> 'last_name'
            )
          ),
          ''
        ),
        u.raw_user_meta_data ->> 'name'
      )
    ),
    ''
  )
  from auth.users u
  where u.id = p_user_id
$$;

create or replace function public.bootstrap_tenant_owner_membership()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_full_name text := public.resolve_auth_full_name(new.created_by);
begin
  insert into public.memberships (tenant_id, user_id, role, status, created_by)
  values (new.id, new.created_by, 'owner', 'active', new.created_by)
  on conflict (tenant_id, user_id) do nothing;

  insert into public.profiles (tenant_id, user_id, full_name, created_by)
  values (new.id, new.created_by, v_full_name, new.created_by)
  on conflict (tenant_id, user_id) do update
  set
    full_name = coalesce(excluded.full_name, public.profiles.full_name),
    updated_at = timezone('utc', now());

  return new;
end;
$$;

create or replace function public.accept_pending_tenant_invites()
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  v_user_id uuid := auth.uid();
  v_email text := lower(trim(coalesce(auth.jwt() ->> 'email', '')));
  v_full_name text;
  v_count integer := 0;
  v_invite record;
begin
  if v_user_id is null then
    return 0;
  end if;

  if v_email = '' then
    return 0;
  end if;

  v_full_name := public.resolve_auth_full_name(v_user_id);

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
      v_full_name,
      coalesce(v_invite.invited_by, v_user_id)
    )
    on conflict (tenant_id, user_id) do update
    set
      full_name = coalesce(excluded.full_name, public.profiles.full_name),
      updated_at = timezone('utc', now());

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

create or replace function public.sync_authenticated_profile_full_name(p_full_name text)
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  v_user_id uuid := auth.uid();
  v_full_name text := nullif(trim(coalesce(p_full_name, '')), '');
  v_count integer := 0;
  v_membership record;
begin
  if v_user_id is null or v_full_name is null then
    return 0;
  end if;

  for v_membership in
    select distinct tenant_id
    from public.memberships
    where user_id = v_user_id
  loop
    insert into public.profiles (tenant_id, user_id, full_name, created_by)
    values (v_membership.tenant_id, v_user_id, v_full_name, v_user_id)
    on conflict (tenant_id, user_id) do update
    set
      full_name = excluded.full_name,
      updated_at = timezone('utc', now());

    v_count := v_count + 1;
  end loop;

  return v_count;
end;
$$;

revoke all on function public.resolve_auth_full_name(uuid) from public;
grant execute on function public.resolve_auth_full_name(uuid) to authenticated;

revoke all on function public.accept_pending_tenant_invites() from public;
grant execute on function public.accept_pending_tenant_invites() to authenticated;

revoke all on function public.sync_authenticated_profile_full_name(text) from public;
grant execute on function public.sync_authenticated_profile_full_name(text) to authenticated;
