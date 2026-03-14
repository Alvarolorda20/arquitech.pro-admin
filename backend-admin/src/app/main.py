"""Application bootstrap for FastAPI."""

import os
import time
import uuid
from urllib.parse import urlsplit

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from src.app import runtime
from src.features.admin.api import router as admin_router
from src.shared.observability.logger import get_logger

_logger = get_logger(__name__)


def _is_production_runtime() -> bool:
    node_env = os.environ.get("NODE_ENV", "").strip().lower()
    app_env = os.environ.get("ENVIRONMENT", "").strip().lower()
    return node_env == "production" or app_env in {"production", "prod"}


def _default_cors_origins() -> list[str]:
    candidates = [
        os.environ.get("ADMIN_APP_URL", ""),
        os.environ.get("APP_BASE_URL", ""),
        os.environ.get("NEXT_PUBLIC_APP_URL", ""),
        os.environ.get("NEXT_PUBLIC_SITE_URL", ""),
        os.environ.get("SITE_URL", ""),
        os.environ.get("FRONTEND_URL", ""),
    ]
    normalized = [item.strip().rstrip("/") for item in candidates if item and item.strip()]
    if normalized:
        return normalized

    vercel = os.environ.get("VERCEL_URL", "").strip().rstrip("/")
    if vercel:
        return [f"https://{vercel}"]

    if _is_production_runtime():
        return []
    return ["http://localhost:3001"]


def _extract_hostname(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    probe = raw if "://" in raw else f"https://{raw}"
    try:
        return (urlsplit(probe).hostname or "").strip().lower()
    except Exception:
        return ""


def _is_local_hostname(hostname: str) -> bool:
    host = (hostname or "").strip().lower()
    if not host:
        return False
    if host in {"localhost", "0.0.0.0", "::1"}:
        return True
    return host.startswith("127.")


def _is_local_origin(value: str) -> bool:
    return _is_local_hostname(_extract_hostname(value))


def _load_cors_origins() -> list[str]:
    raw = os.environ.get("BACKEND_CORS_ORIGINS", "")
    if raw.strip():
        origins = [item.strip().rstrip("/") for item in raw.split(",") if item and item.strip()]
    else:
        origins = _default_cors_origins()

    if _is_production_runtime():
        if not origins:
            raise RuntimeError(
                "No CORS origins configured for production. Set BACKEND_CORS_ORIGINS (e.g. https://admin.arquitech.pro)."
            )
        local_origins = [origin for origin in origins if _is_local_origin(origin)]
        if local_origins:
            joined = ", ".join(local_origins)
            raise RuntimeError(
                f"Invalid BACKEND_CORS_ORIGINS for production. Local origins are not allowed: {joined}"
            )
    return origins


def create_app() -> FastAPI:
    cors_origins = _load_cors_origins()
    app = FastAPI(title="Budget Comparator API", version="1.0.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def _request_logging_middleware(request: Request, call_next):
        started = time.perf_counter()
        request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
        request.state.request_id = request_id
        client_ip = request.client.host if request.client else None
        method = request.method
        path = request.url.path
        query = request.url.query or None

        _logger.info(
            "HTTP request started",
            extra={
                "request_id": request_id,
                "http_method": method,
                "path": path,
                "query": query,
                "client_ip": client_ip,
            },
        )

        try:
            response = await call_next(request)
        except Exception:
            duration_ms = int((time.perf_counter() - started) * 1000)
            _logger.exception(
                "HTTP request failed with unhandled exception",
                extra={
                    "request_id": request_id,
                    "http_method": method,
                    "path": path,
                    "query": query,
                    "client_ip": client_ip,
                    "status_code": 500,
                    "duration_ms": duration_ms,
                },
            )
            raise

        duration_ms = int((time.perf_counter() - started) * 1000)
        response.headers["X-Request-Id"] = request_id
        log_extra = {
            "request_id": request_id,
            "http_method": method,
            "path": path,
            "query": query,
            "client_ip": client_ip,
            "status_code": response.status_code,
            "duration_ms": duration_ms,
        }
        if response.status_code >= 500:
            _logger.error("HTTP request completed with server error", extra=log_extra)
        elif response.status_code >= 400:
            _logger.warning("HTTP request completed with client error", extra=log_extra)
        else:
            _logger.info("HTTP request completed", extra=log_extra)

        return response

    _logger.info(
        "FastAPI admin app configured",
        extra={
            "cors_origins": cors_origins,
            "comparison_routes_enabled": False,
            "admin_routes_enabled": True,
        },
    )
    app.add_event_handler("startup", runtime.startup_temp_housekeeping)
    app.include_router(admin_router)
    return app


app = create_app()

__all__ = ["app", "create_app"]
