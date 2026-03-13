# Admin Extraction Kit

Guía para mover el panel admin a un repositorio independiente sin refactor funcional adicional.

## 1) Frontera del módulo admin

Bloques que deben considerarse parte del dominio admin:

- `src/app/admin/*`
- `src/app/api/admin/[...path]/route.ts`
- `src/lib/admin-session.ts`
- `src/modules/admin/*` (contratos + runtime host/orígenes)

Dependencias que deben mantenerse compartidas o copiarse al nuevo repo:

- Supabase client helpers (`src/lib/supabase/*`)
- Backend API base resolver (`src/lib/backend-api.ts`)
- Theme toggle/estilos usados por admin (`src/components/theme/*`, estilos admin)

## 2) Matriz de variables de entorno

Variables mínimas para repo admin separado:

- `NEXT_PUBLIC_SUPABASE_URL`
- `NEXT_PUBLIC_SUPABASE_ANON_KEY`
- `NEXT_PUBLIC_API_URL` o `NEXT_PUBLIC_BACKEND_URL`
- `API_URL` (server runtime)
- `NEXT_PUBLIC_WORKSPACE_APP_URL` (destino de “Abrir workspace”)
- `NEXT_PUBLIC_ADMIN_APP_URL` (origen público admin)
- `ADMIN_APP_URL` (server runtime admin origin)
- `ADMIN_PANEL_HOST` (enforce host en middleware)

Variables opcionales recomendadas:

- `WORKSPACE_APP_HOST`
- `WORKSPACE_APP_URL`
- `NEXT_PUBLIC_APP_URL`

## 3) Checklist de extracción

1. Crear nuevo repo admin con Next.js App Router.
2. Copiar frontera admin listada en sección 1.
3. Copiar dependencias compartidas mínimas (Supabase + backend resolver + theme toggle).
4. Ajustar alias `@/*` y `tsconfig.json` al nuevo árbol.
5. Mantener `/api/admin/*` proxy con la misma allowlist.
6. Configurar host admin (`admin.*`) y variables de entorno del bloque admin.
7. Verificar que “Abrir workspace” apunta a `app.*`.
8. Ejecutar typecheck/build.

## 4) Bootstrap rápido en nuevo repo

1. `npm install`
2. Definir `.env` con matriz de sección 2.
3. Levantar `npm run dev`
4. Probar:
   - `/admin` login
   - `/admin/memberships`
   - `/admin/tenants/:tenantId`
   - llamadas `/api/admin/*`

## 5) Validaciones post-migración

- No hay imports residuales a rutas de workspace del repo original.
- `/admin*` funciona sin depender de páginas cliente.
- `admin_session_v1` expira y limpia contexto tenant en logout admin.
- “Abrir workspace” redirige a `https://app.../tenants/switch?...`.
- No hay cruce de tenant entre usuarios tras alternar sesiones.

