-- 102_auth_multi_tenant_roles_products_alignment.sql
-- Phase 1: enum/column foundation.
-- NOTE: PostgreSQL requires enum value additions to commit before reuse.

create extension if not exists pgcrypto;

do $$
begin
  if not exists (
    select 1
    from pg_type
    where typname = 'app_role'
      and typnamespace = 'public'::regnamespace
  ) then
    create type public.app_role as enum ('owner', 'editor', 'viewer');
  end if;

  if not exists (
    select 1
    from pg_enum e
    where e.enumtypid = 'public.app_role'::regtype
      and e.enumlabel = 'editor'
  ) then
    alter type public.app_role add value 'editor';
  end if;

  if not exists (
    select 1
    from pg_enum e
    where e.enumtypid = 'public.app_role'::regtype
      and e.enumlabel = 'viewer'
  ) then
    alter type public.app_role add value 'viewer';
  end if;
end;
$$;

alter table if exists public.tenants
  add column if not exists products text[] not null default array[]::text[];

update public.tenants
set products = array['memoria_basica']::text[]
where coalesce(array_length(products, 1), 0) = 0;
