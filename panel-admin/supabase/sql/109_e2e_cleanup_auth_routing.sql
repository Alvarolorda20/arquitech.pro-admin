-- 109_e2e_cleanup_auth_routing.sql
-- Optional cleanup for auth/routing E2E seed data.

do $$
declare
  v_primary_tenant_id uuid := '11111111-1111-4111-8111-111111111111';
  v_secondary_tenant_id uuid := '22222222-2222-4222-8222-222222222222';
begin
  delete from public.tenants
  where id in (v_primary_tenant_id, v_secondary_tenant_id);
end;
$$;
