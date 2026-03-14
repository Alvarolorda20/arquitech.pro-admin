# Deployment (Backend Admin)

Servicio objetivo: `api-admin.arquitech.pro`.

## Build y run

```bash
docker compose build --no-cache backend-admin
docker compose up -d backend-admin
docker compose logs -f backend-admin
```

## Verificacion

```bash
curl -i https://api-admin.arquitech.pro/health
```

Debe responder `200`.

## Nota de hardening

- En produccion, `BACKEND_CORS_ORIGINS` no puede contener URLs locales (`localhost`, `127.0.0.1`, `0.0.0.0`).
- Si ocurre, el backend aborta el arranque para evitar una configuracion insegura.
