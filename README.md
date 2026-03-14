# Arquitech Admin Monorepo

Repositorio para el stack de administracion en produccion:
- `panel-admin` -> `https://admin.arquitech.pro`
- `backend-admin` -> `https://api-admin.arquitech.pro`

## Estructura

- `panel-admin/`: Next.js 16 (UI admin + proxy `/api/admin/*`).
- `backend-admin/`: FastAPI (endpoints admin global).
- `docker-compose.yml`: stack de despliegue admin con Traefik.
- `.env.example`: plantilla de variables para produccion.

## Requisitos

1. Docker + Docker Compose.
2. Traefik activo en la red docker externa `n8n_default`.
3. DNS apuntando a la VPS:
- `admin.arquitech.pro`
- `api-admin.arquitech.pro`

## Configuracion de entorno

1. Copia la plantilla:

```bash
cp .env.example .env
```

2. Rellena claves obligatorias en `.env`:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `SUPABASE_PUBLISHABLE_KEY`
- `NEXT_PUBLIC_SUPABASE_URL`
- `NEXT_PUBLIC_SUPABASE_ANON_KEY`
- `ADMIN_PANEL_HOST`
- `ADMIN_API_HOST`

3. Hardening obligatorio de admins globales:
- `GLOBAL_ADMIN_USER_IDS` y/o `GLOBAL_ADMIN_EMAILS`
- Mantener:
  - `GLOBAL_ADMIN_ALLOWED_ROLES=global_admin,super_admin`
  - `GLOBAL_ADMIN_METADATA_FLAGS=global_admin,is_global_admin`

## Despliegue rapido

```bash
docker compose pull
docker compose up -d --build
docker compose ps
```

## Verificacion post-deploy

1. `https://api-admin.arquitech.pro/health` devuelve 200.
2. `https://admin.arquitech.pro` carga el login admin.
3. Usuario no global-admin no entra a vistas admin.
4. Desde admin, "Abrir workspace" redirige a `https://app.arquitech.pro`.
5. Ninguna variable de produccion usa `localhost`, `127.0.0.1` o `0.0.0.0`.

## Seguridad

- El backend admin autoriza acciones sensibles solo con global admins.
- El panel admin aplica aislamiento por host (`admin.*`) y scope de rutas admin.
- El repo cliente no debe desplegar rutas/admin API admin en `app.*` / `api.*`.
- Hardening en produccion:
  - frontend y proxy admin rechazan URLs locales,
  - backend falla en arranque si `BACKEND_CORS_ORIGINS` incluye origenes locales.

## Desarrollo local

Hay referencias a `localhost` en:
- tests e2e/playwright,
- scripts locales,
- fallbacks no-productivos.

En produccion, con `NODE_ENV=production` y variables de `.env` configuradas, no se usan endpoints locales.
