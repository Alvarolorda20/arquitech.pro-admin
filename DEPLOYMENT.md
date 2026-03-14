# Deployment Guide (Admin)

Guia operativa para desplegar `arquitech.pro-admin` en VPS.

## 1) Acceso por SSH

Desde tu maquina local:

```bash
ssh root@<IP_O_HOST_VPS>
```

Si usas clave privada:

```bash
ssh -i ~/.ssh/<tu_clave_privada> root@<IP_O_HOST_VPS>
```

## 2) Actualizar codigo

```bash
cd /ruta/repos/arquitech.pro-admin
git fetch origin
git checkout main
git pull origin main
git log -1 --oneline
```

## 3) Preparar variables de entorno

```bash
cp -n .env.example .env
nano .env
```

Asegura especialmente:
- dominios admin correctos (`admin.arquitech.pro` / `api-admin.arquitech.pro`),
- claves Supabase,
- lista blanca global admin (`GLOBAL_ADMIN_USER_IDS` y/o `GLOBAL_ADMIN_EMAILS`),
- `ADMIN_API_HOST=api-admin.arquitech.pro`,
- sin valores locales en prod (`localhost`, `127.0.0.1`, `0.0.0.0`).

Checklist rapido DNS antes de levantar:
- `admin.arquitech.pro` -> VPS
- `api-admin.arquitech.pro` -> VPS

## 4) Levantar stack

```bash
docker compose pull
docker compose up -d --build
docker compose ps
```

## 5) Checks de salud

```bash
curl -i https://api-admin.arquitech.pro/health
```

Comprobar en navegador:
- `https://admin.arquitech.pro`
- login con cuenta global admin
- acceso a memberships y tenant detail

## 6) Logs utiles

```bash
docker compose logs -f backend-admin
docker compose logs -f panel-admin
```

## 7) Rollout seguro

Si quieres minimizar riesgo:
1. `git pull origin main`
2. `docker compose up -d --build backend-admin`
3. validar `/health`
4. `docker compose up -d --build panel-admin`

## 8) Errores frecuentes

1. `404` en admin: revisar reglas Traefik host y red `n8n_default`.
2. `403` inesperado en admin: revisar whitelist global admin en `.env`.
3. CORS bloqueado o backend no arranca: revisar `BACKEND_CORS_ORIGINS=https://admin.arquitech.pro` (sin localhost en prod).
4. `503` en `/api/admin/*`: revisar `API_URL` / `NEXT_PUBLIC_API_URL` y quitar cualquier URL local.
5. Admin no puede abrir workspace: revisar variables `WORKSPACE_APP_URL` y `WORKSPACE_APP_HOST`.
