-- 113_owner_onboarding_interest_only.sql
-- Owner onboarding only captures plan interest. Plan activation remains admin-only.

begin;

-- Authenticated tenant owners can no longer mutate subscriptions directly.
drop policy if exists tenant_subscriptions_manage_owner_policy on public.tenant_subscriptions;

create or replace function public.create_tenant(
  p_name text,
  p_slug text,
  p_metadata jsonb default '{}'::jsonb,
  p_products text[] default array[]::text[],
  p_auto_activate_default boolean default false
)
returns uuid
language plpgsql
security definer
set search_path = public
as $$
declare
  v_user_id uuid := auth.uid();
  v_tenant_id uuid;
  v_products text[];
begin
  if v_user_id is null then
    raise exception 'Not authenticated';
  end if;

  select coalesce(
    array_agg(distinct lower(trim(product_value))),
    array[]::text[]
  )
  into v_products
  from unnest(coalesce(p_products, array[]::text[])) as product_rows(product_value)
  join public.membership_plans mp
    on mp.plan_key = lower(trim(product_rows.product_value))
   and mp.is_active = true
  where length(trim(product_value)) > 0;

  if p_auto_activate_default and coalesce(array_length(v_products, 1), 0) = 0 then
    select coalesce(
      array_agg(plan_key order by sort_order, plan_key),
      array[]::text[]
    )
    into v_products
    from public.membership_plans
    where is_active = true
      and is_default = true;
  end if;

  insert into public.tenants (name, slug, products, metadata, created_by)
  values (
    trim(p_name),
    lower(trim(p_slug)),
    coalesce(v_products, array[]::text[]),
    coalesce(p_metadata, '{}'::jsonb),
    v_user_id
  )
  returning id into v_tenant_id;

  if coalesce(array_length(v_products, 1), 0) > 0 then
    insert into public.tenant_subscriptions (tenant_id, plan_key, status, starts_at, created_by)
    select
      v_tenant_id,
      product_key,
      'active',
      timezone('utc', now()),
      v_user_id
    from unnest(v_products) as product_keys(product_key)
    on conflict (tenant_id, plan_key) do update
    set
      status = 'active',
      ends_at = null,
      updated_at = timezone('utc', now());
  end if;

  return v_tenant_id;
end;
$$;

create or replace function public.create_tenant(
  p_name text,
  p_slug text,
  p_metadata jsonb default '{}'::jsonb,
  p_products text[] default array[]::text[]
)
returns uuid
language sql
security definer
set search_path = public
as $$
  select public.create_tenant(
    p_name,
    p_slug,
    p_metadata,
    p_products,
    false
  );
$$;

create or replace function public.create_tenant(
  p_name text,
  p_slug text,
  p_metadata jsonb default '{}'::jsonb
)
returns uuid
language sql
security definer
set search_path = public
as $$
  select public.create_tenant(
    p_name,
    p_slug,
    p_metadata,
    array[]::text[],
    false
  );
$$;

revoke all on function public.create_tenant(text, text, jsonb, text[], boolean) from public;
grant execute on function public.create_tenant(text, text, jsonb, text[], boolean) to authenticated;

commit;
