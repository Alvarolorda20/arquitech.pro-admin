# Vertical Slice Roadmap

## Estado aplicado

- API separada por feature:
  - `src/features/comparison/api/router.py`
  - `src/features/admin/api/router.py`
- `server.py` es entrypoint delgado (solo arranque ASGI).
- Bootstrap de app movido a `src/app/main.py`:
  - creacion FastAPI
  - CORS
  - registro de routers
  - startup hook
- Runtime legacy movido a `src/app/runtime.py` (servicios/orquestacion).
- `TEST_MODE` eliminado del runtime.
- Handlers HTTP extraidos de runtime a use cases por dominio:
  - `src/features/comparison/application/use_cases/http_handlers.py`
  - `src/features/admin/application/use_cases/http_handlers.py`
- Servicios extraidos desde runtime:
  - Auth/memberships: `src/shared/security/runtime_auth_service.py`
  - Persistencia: `src/features/runs/infrastructure/runtime_persistence_service.py`
  - Pipeline: `src/features/comparison/application/services/runtime_pipeline_service.py`
- `runtime.py` ahora actua como composition host de helpers y wiring compartido.

## Imports actuales (compatibles)

- Routers llaman a runtime con import lazy:
  - `import_module("src.app.runtime")`
- Esto evita ciclos de import en el arranque y mantiene endpoints existentes:
  - `POST /api/process-budget`
  - `POST /api/process-budget/rerun`
  - `GET /api/status/{job_id}`
  - `GET /api/download/{job_id}`
  - `GET /health`
  - endpoints `/admin/*`

## Siguiente corte recomendado (para reducir `src/app/runtime.py`)

1. Mover casos de uso de comparacion:
   - `process_budget`, `rerun_budget_from_last_inputs`, `get_job_status`, `download_result`
   - destino: `src/features/comparison/application/use_cases/`
2. Mover casos de uso de admin:
   - overview, membership status/role/delete, subscriptions
   - destino: `src/features/admin/application/use_cases/`
3. Dejar `server.py` como composition root:
   - ya aplicado; mantenerlo sin logica de negocio
4. Introducir adaptadores de infraestructura por feature:
   - wrappers de `_RUN_REPOSITORY` por bounded context

## Checklist Pro (backend)

1. Variables de entorno en produccion:
   - `SUPABASE_URL`
   - `SUPABASE_SERVICE_ROLE_KEY`
   - `SUPABASE_REQUIRE_PERSISTENCE=true`
   - `SUPABASE_ENFORCE_USER_AUTH=true`
2. CORS:
   - reemplazar `http://localhost:3000` por dominios reales.
3. Ejecutar validaciones antes de deploy:
   - `python -m compileall server.py src`
   - `pytest -q`
4. Healthcheck:
   - `GET /health` debe responder `200`.
