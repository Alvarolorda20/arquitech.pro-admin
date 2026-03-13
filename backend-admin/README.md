# Backend Admin (api-admin)

Backend FastAPI aislado para operaciones global-admin.

Host objetivo: `api-admin.arquitech.pro`.

## Endpoints expuestos

- `GET /admin`
- `POST /api/admin/login`
- `POST /api/admin/refresh`
- `GET /admin/memberships`
- `GET /api/admin/tenant-overview`
- `GET /api/admin/run-artifacts`
- `GET /api/admin/run-artifact/download`
- `PATCH /api/admin/memberships/status`
- `PATCH /api/admin/memberships/role`
- `DELETE /api/admin/memberships`
- `PATCH /api/admin/tenant-subscriptions/status`
- `PATCH /api/admin/tenant-credits/adjust`
- `PATCH /api/admin/tenant-billing-config`
- `GET /health`

## Variables minimas

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `SUPABASE_PUBLISHABLE_KEY` (o `SUPABASE_ANON_KEY`)
- `BACKEND_CORS_ORIGINS` (ejemplo: `https://admin.arquitech.pro`)
- `GLOBAL_ADMIN_USER_IDS` y/o `GLOBAL_ADMIN_EMAILS` (lista blanca real de global admins)

## Hardening recomendado (produccion)

- `GLOBAL_ADMIN_ALLOWED_ROLES=global_admin,super_admin`
- `GLOBAL_ADMIN_METADATA_FLAGS=global_admin,is_global_admin`

## Ejecutar

```bash
uvicorn server:app --host 0.0.0.0 --port 8000 --reload
```
