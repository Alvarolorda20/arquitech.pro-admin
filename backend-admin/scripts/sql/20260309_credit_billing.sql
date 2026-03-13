-- 2026-03-09
-- Credit billing model for monthly subscriptions:
-- - Ledger table (auditable movements)
-- - Atomic SQL functions for grant / consume / refund
-- - Plan catalog extension with monthly_credits

create extension if not exists pgcrypto;

alter table if exists public.membership_plans
  add column if not exists monthly_credits integer not null default 0;

update public.membership_plans
set monthly_credits = case
  when lower(plan_key) in ('comparacion_presupuestos', 'comparacion-presupuestos') then greatest(monthly_credits, 200)
  when lower(plan_key) in ('memoria_basica', 'memoria-basica') then greatest(monthly_credits, 50)
  else monthly_credits
end
where monthly_credits is not null;

create table if not exists public.tenant_credit_ledger (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null,
  event_type text not null,
  credits_delta integer not null check (credits_delta <> 0),
  reason text null,
  reference_type text null,
  reference_id text null,
  cycle_month date null,
  expires_at timestamptz null,
  metadata jsonb not null default '{}'::jsonb,
  idempotency_key text not null,
  created_by uuid null,
  created_at timestamptz not null default timezone('utc', now()),
  constraint tenant_credit_ledger_event_type_check
    check (event_type = any (array['grant', 'consume', 'refund', 'expire', 'adjustment']::text[])),
  constraint tenant_credit_ledger_idempotency_unique unique (idempotency_key),
  constraint tenant_credit_ledger_tenant_fk foreign key (tenant_id) references public.tenants (id),
  constraint tenant_credit_ledger_created_by_fk foreign key (created_by) references auth.users (id)
);

create index if not exists tenant_credit_ledger_tenant_created_idx
  on public.tenant_credit_ledger (tenant_id, created_at desc);

create index if not exists tenant_credit_ledger_tenant_expiry_idx
  on public.tenant_credit_ledger (tenant_id, expires_at);

create or replace function public.get_tenant_credit_balance(
  p_tenant_id uuid
)
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  v_balance integer;
begin
  select coalesce(sum(l.credits_delta), 0)::integer
  into v_balance
  from public.tenant_credit_ledger l
  where l.tenant_id = p_tenant_id
    and (l.expires_at is null or l.expires_at > timezone('utc', now()));

  return coalesce(v_balance, 0);
end;
$$;

create or replace function public.ensure_monthly_credit_grant(
  p_tenant_id uuid,
  p_created_by uuid default null
)
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  v_cycle_start date;
  v_cycle_end timestamptz;
  v_grant_credits integer;
  v_idempotency_key text;
begin
  v_cycle_start := date_trunc('month', timezone('utc', now()))::date;
  v_cycle_end := (date_trunc('month', timezone('utc', now())) + interval '1 month');

  select coalesce(sum(greatest(mp.monthly_credits, 0)), 0)::integer
  into v_grant_credits
  from public.tenant_subscriptions ts
  join public.membership_plans mp on mp.plan_key = ts.plan_key
  where ts.tenant_id = p_tenant_id
    and lower(coalesce(ts.status, '')) in ('active', 'trial')
    and coalesce(mp.monthly_credits, 0) > 0
    and (ts.starts_at is null or ts.starts_at <= timezone('utc', now()))
    and (ts.ends_at is null or ts.ends_at > timezone('utc', now()));

  if coalesce(v_grant_credits, 0) <= 0 then
    return 0;
  end if;

  v_idempotency_key := format(
    'grant:monthly:%s:%s',
    p_tenant_id::text,
    to_char(v_cycle_start, 'YYYY-MM-DD')
  );

  insert into public.tenant_credit_ledger (
    tenant_id,
    event_type,
    credits_delta,
    reason,
    reference_type,
    reference_id,
    cycle_month,
    expires_at,
    metadata,
    idempotency_key,
    created_by
  )
  values (
    p_tenant_id,
    'grant',
    v_grant_credits,
    'monthly_subscription_grant',
    'monthly_cycle',
    to_char(v_cycle_start, 'YYYY-MM'),
    v_cycle_start,
    v_cycle_end,
    jsonb_build_object(
      'cycle_start', to_char(v_cycle_start, 'YYYY-MM-DD'),
      'cycle_end', to_char(v_cycle_end, 'YYYY-MM-DD"T"HH24:MI:SSOF')
    ),
    v_idempotency_key,
    p_created_by
  )
  on conflict (idempotency_key) do nothing;

  if found then
    return v_grant_credits;
  end if;
  return 0;
end;
$$;

create or replace function public.consume_tenant_credits(
  p_tenant_id uuid,
  p_amount integer,
  p_idempotency_key text,
  p_reason text default null,
  p_reference_type text default null,
  p_reference_id text default null,
  p_metadata jsonb default '{}'::jsonb,
  p_created_by uuid default null
)
returns table(
  success boolean,
  balance integer,
  consumed integer,
  message text
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_amount integer;
  v_balance integer;
begin
  v_amount := greatest(coalesce(p_amount, 0), 0);
  if v_amount <= 0 then
    return query select false, public.get_tenant_credit_balance(p_tenant_id), 0, 'invalid_amount';
    return;
  end if;
  if coalesce(trim(p_idempotency_key), '') = '' then
    return query select false, public.get_tenant_credit_balance(p_tenant_id), 0, 'missing_idempotency_key';
    return;
  end if;

  perform pg_advisory_xact_lock(hashtext(p_tenant_id::text));

  perform public.ensure_monthly_credit_grant(p_tenant_id, p_created_by);

  if exists (
    select 1
    from public.tenant_credit_ledger l
    where l.idempotency_key = p_idempotency_key
  ) then
    return query select true, public.get_tenant_credit_balance(p_tenant_id), v_amount, 'already_consumed';
    return;
  end if;

  v_balance := public.get_tenant_credit_balance(p_tenant_id);
  if v_balance < v_amount then
    return query select false, v_balance, 0, 'insufficient_credits';
    return;
  end if;

  insert into public.tenant_credit_ledger (
    tenant_id,
    event_type,
    credits_delta,
    reason,
    reference_type,
    reference_id,
    metadata,
    idempotency_key,
    created_by
  )
  values (
    p_tenant_id,
    'consume',
    -v_amount,
    coalesce(p_reason, 'budget_execution'),
    coalesce(p_reference_type, 'budget_run'),
    p_reference_id,
    coalesce(p_metadata, '{}'::jsonb),
    p_idempotency_key,
    p_created_by
  );

  return query select true, public.get_tenant_credit_balance(p_tenant_id), v_amount, 'consumed';
end;
$$;

create or replace function public.refund_tenant_credits(
  p_tenant_id uuid,
  p_amount integer,
  p_idempotency_key text,
  p_reason text default null,
  p_reference_type text default null,
  p_reference_id text default null,
  p_metadata jsonb default '{}'::jsonb,
  p_created_by uuid default null
)
returns table(
  success boolean,
  balance integer,
  refunded integer,
  message text
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_amount integer;
begin
  v_amount := greatest(coalesce(p_amount, 0), 0);
  if v_amount <= 0 then
    return query select false, public.get_tenant_credit_balance(p_tenant_id), 0, 'invalid_amount';
    return;
  end if;
  if coalesce(trim(p_idempotency_key), '') = '' then
    return query select false, public.get_tenant_credit_balance(p_tenant_id), 0, 'missing_idempotency_key';
    return;
  end if;

  perform pg_advisory_xact_lock(hashtext(p_tenant_id::text));

  insert into public.tenant_credit_ledger (
    tenant_id,
    event_type,
    credits_delta,
    reason,
    reference_type,
    reference_id,
    metadata,
    idempotency_key,
    created_by
  )
  values (
    p_tenant_id,
    'refund',
    v_amount,
    coalesce(p_reason, 'budget_execution_refund'),
    coalesce(p_reference_type, 'budget_run'),
    p_reference_id,
    coalesce(p_metadata, '{}'::jsonb),
    p_idempotency_key,
    p_created_by
  )
  on conflict (idempotency_key) do nothing;

  if found then
    return query select true, public.get_tenant_credit_balance(p_tenant_id), v_amount, 'refunded';
  else
    return query select true, public.get_tenant_credit_balance(p_tenant_id), v_amount, 'already_refunded';
  end if;
end;
$$;
