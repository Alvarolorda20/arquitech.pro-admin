# Instalacion (Backend Admin)

## 1. Entorno

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# .venv\\Scripts\\activate   # Windows
pip install -r requirements.txt
```

## 2. Variables

Crear `.env` desde `.env.example` y completar:

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `SUPABASE_PUBLISHABLE_KEY`
- `BACKEND_CORS_ORIGINS=https://admin.arquitech.pro`

## 3. Arranque

```bash
uvicorn server:app --host 0.0.0.0 --port 8000 --reload
```
