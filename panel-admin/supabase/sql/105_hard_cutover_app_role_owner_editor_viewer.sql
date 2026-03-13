-- 105_hard_cutover_app_role_owner_editor_viewer.sql
-- Phase A: create canonical app_role and move memberships.role to it.
-- Safe data mapping: admin/member -> editor.

do $$
begin
  if exists (
    select 1
    from pg_type
    where typnamespace = 'public'::regnamespace
      and typname = 'app_role'
  ) and not exists (
    select 1
    from pg_type
    where typnamespace = 'public'::regnamespace
      and typname = 'app_role_legacy'
  ) then
    alter type public.app_role rename to app_role_legacy;
  end if;

  if not exists (
    select 1
    from pg_type
    where typnamespace = 'public'::regnamespace
      and typname = 'app_role'
  ) then
    create type public.app_role as enum ('owner', 'editor', 'viewer');
  end if;
end;
$$;

drop policy if exists memberships_select_policy on public.memberships;
drop policy if exists memberships_insert_owner_policy on public.memberships;
drop policy if exists memberships_insert_admin_policy on public.memberships;
drop policy if exists memberships_insert_editor_policy on public.memberships;
drop policy if exists memberships_update_owner_policy on public.memberships;
drop policy if exists memberships_update_admin_policy on public.memberships;
drop policy if exists memberships_update_editor_policy on public.memberships;
drop policy if exists memberships_delete_owner_policy on public.memberships;
drop policy if exists memberships_delete_admin_policy on public.memberships;
drop policy if exists memberships_delete_editor_policy on public.memberships;

do $$
begin
  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'memberships'
      and column_name = 'role'
      and udt_name <> 'app_role'
  ) then
    execute $sql$
      alter table public.memberships
      alter column role
      type public.app_role
      using (
        case role::text
          when 'admin' then 'editor'::public.app_role
          when 'member' then 'editor'::public.app_role
          else role::text::public.app_role
        end
      )
    $sql$;
  end if;
end;
$$;
