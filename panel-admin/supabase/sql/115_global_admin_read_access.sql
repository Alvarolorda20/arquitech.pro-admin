-- 115_global_admin_read_access.sql
-- Allow global admins to read any tenant while keeping write policies tenant-scoped.

begin;

create or replace function public.is_global_admin()
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select exists (
    select 1
    from public.global_admin_users gau
    where gau.user_id = auth.uid()
  );
$$;

create or replace function public.can_read_tenant(p_tenant_id uuid)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select
    public.has_tenant_role(
      p_tenant_id,
      array['owner', 'editor', 'viewer']::public.app_role[]
    )
    or public.is_global_admin();
$$;

commit;

