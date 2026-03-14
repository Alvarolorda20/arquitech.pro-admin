# Panel Admin (frontend)

Frontend Next.js aislado para operaciones global-admin.

Host objetivo: `admin.arquitech.pro`.

Este proyecto consume `api-admin.arquitech.pro` y enlaza al workspace cliente en `app.arquitech.pro`.

## Rutas principales

- `/admin`
- `/admin/memberships`
- `/admin/tenants/[tenantId]`
- `/admin/logout`
- `/api/admin/*` (proxy interno hacia backend admin)

## Variables minimas

```env
NEXT_PUBLIC_SUPABASE_URL=...
NEXT_PUBLIC_SUPABASE_ANON_KEY=...
NEXT_PUBLIC_API_URL=https://api-admin.arquitech.pro
NEXT_PUBLIC_BACKEND_URL=https://api-admin.arquitech.pro
API_URL=https://api-admin.arquitech.pro
NEXT_PUBLIC_ADMIN_APP_URL=https://admin.arquitech.pro
NEXT_PUBLIC_WORKSPACE_APP_URL=https://app.arquitech.pro
ADMIN_APP_URL=https://admin.arquitech.pro
WORKSPACE_APP_URL=https://app.arquitech.pro
ADMIN_PANEL_HOST=admin.arquitech.pro
WORKSPACE_APP_HOST=app.arquitech.pro
```

## Hardening produccion

- En `NODE_ENV=production`, el panel rechaza configuraciones con URLs locales (`localhost`, `127.0.0.1`, `0.0.0.0`).
- Si `API_URL` / `NEXT_PUBLIC_API_URL` apunta a local en prod, `/api/admin/*` responde `503` con error explicito.
- `panel-admin/.env.local` es solo para desarrollo local.

## Ejecutar

```bash
npm install
npm run dev
```
