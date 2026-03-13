# ─────────────────────────────────────────────────────────────────────────────
# Budget Comparator — Backend Dockerfile
# ─────────────────────────────────────────────────────────────────────────────
# Multi-stage build: deps cached separately from application code.
#
#   docker build -t budget-backend .
#   docker run -p 8000:8000 --env-file .env budget-backend
# ─────────────────────────────────────────────────────────────────────────────

FROM python:3.12-slim AS base

# Prevent Python from writing .pyc files and enable unbuffered stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# ─── Dependencies layer (cached unless requirements.txt changes) ─────────────
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# ─── Application code ───────────────────────────────────────────────────────
COPY . .

# Default to structured JSON logs in containers
ENV LOG_FORMAT=json \
    LOG_LEVEL=INFO

EXPOSE 8000

# Uvicorn with 2 workers (adjust via UVICORN_WORKERS env var at runtime)
CMD ["sh", "-c", "uvicorn server:app --host 0.0.0.0 --port 8000 --workers ${UVICORN_WORKERS:-2}"]
