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
