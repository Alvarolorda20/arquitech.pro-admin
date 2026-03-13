"""Runtime auth and membership helpers extracted from runtime monolith."""

import os
import time
import uuid
from collections import defaultdict
from typing import Any

import requests
from fastapi import HTTPException, Request

from src.app.runtime import (
    ACTIVE_MEMBERSHIP_STATUSES,
    CATALOG_CACHE_TTL_SECONDS,
    DEFAULT_NEW_MEMBERSHIP_ROLE,
    GLOBAL_ADMIN_ALLOWED_ROLES,
    GLOBAL_ADMIN_EMAILS,
    GLOBAL_ADMIN_METADATA_FLAGS,
    GLOBAL_ADMIN_USER_IDS,
    MANAGED_MEMBERSHIP_ROLES,
    TENANT_ADMIN_MEMBERSHIP_ROLES,
    _CATALOG_CACHE,
    _RUN_REPOSITORY,
)


def _extract_bearer_token(request: Request) -> str:
    auth_header = request.headers.get("authorization", "").strip()
    if not auth_header:
        raise HTTPException(
            status_code=401,
            detail="Missing Authorization header. Expected Bearer token.",
        )
    parts = auth_header.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1].strip():
        raise HTTPException(
            status_code=401,
            detail="Invalid Authorization header. Expected Bearer token.",
        )
    return parts[1].strip()


def _resolve_authenticated_user(token: str) -> dict[str, Any]:
    supabase_url = os.environ.get("SUPABASE_URL", "").strip()
    supabase_service_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not supabase_url or not supabase_service_key:
        raise HTTPException(
            status_code=503,
            detail=(
                "Supabase auth validation is unavailable "
                "(SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY missing)."
            ),
        )

    try:
        resp = requests.get(
            f"{supabase_url.rstrip('/')}/auth/v1/user",
            headers={
                "apikey": supabase_service_key,
                "Authorization": f"Bearer {token}",
            },
            timeout=10,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Could not validate user session against Supabase Auth: {exc}",
        ) from exc

    if resp.status_code == 401:
        raise HTTPException(status_code=401, detail="Invalid or expired session token.")
    if not (200 <= resp.status_code < 300):
        body = resp.text.strip()
        if len(body) > 500:
            body = body[:500] + "..."
        raise HTTPException(
            status_code=503,
            detail=f"Supabase Auth validation failed ({resp.status_code}): {body}",
        )

    payload = resp.json() if resp.text else {}
    user_id = str(payload.get("id") or "").strip()
    if not user_id:
        raise HTTPException(status_code=401, detail="Session token has no user id.")
    app_metadata = payload.get("app_metadata")
    user_metadata = payload.get("user_metadata")
    if not isinstance(app_metadata, dict):
        app_metadata = {}
    if not isinstance(user_metadata, dict):
        user_metadata = {}

    return {
        "id": user_id,
        "email": str(payload.get("email") or "").strip(),
        "app_metadata": app_metadata,
        "user_metadata": user_metadata,
    }


def _extract_request_token(request: Request, **_kwargs: object) -> str:
    """Extract the Bearer token from the Authorization header.

    The ``allow_query_token`` keyword is accepted for backwards compatibility
    but is intentionally ignored — tokens must never travel in URLs.
    """
    return _extract_bearer_token(request)


def _resolve_supabase_public_auth_key() -> str:
    for env_name in (
        "SUPABASE_PUBLISHABLE_KEY",
        "SUPABASE_ANON_KEY",
        "NEXT_PUBLIC_SUPABASE_ANON_KEY",
        "VITE_SUPABASE_ANON_KEY",
        "SUPABASE_SERVICE_ROLE_KEY",  # last-resort fallback
    ):
        candidate = os.environ.get(env_name, "").strip()
        if candidate:
            return candidate
    raise HTTPException(
        status_code=503,
        detail=(
            "Supabase login is unavailable. Define SUPABASE_PUBLISHABLE_KEY "
            "or SUPABASE_ANON_KEY in environment."
        ),
    )


def _exchange_email_password_for_access_token(*, email: str, password: str) -> dict[str, Any]:
    supabase_url = os.environ.get("SUPABASE_URL", "").strip()
    if not supabase_url:
        raise HTTPException(
            status_code=503,
            detail="Supabase login is unavailable (SUPABASE_URL missing).",
        )
    auth_apikey = _resolve_supabase_public_auth_key()

    try:
        resp = requests.post(
            f"{supabase_url.rstrip('/')}/auth/v1/token",
            params={"grant_type": "password"},
            headers={
                "apikey": auth_apikey,
                "Content-Type": "application/json",
            },
            json={
                "email": email,
                "password": password,
            },
            timeout=15,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Could not reach Supabase Auth: {exc}",
        ) from exc

    if resp.status_code in {400, 401}:
        raise HTTPException(
            status_code=401,
            detail="Invalid admin credentials.",
        )
    if not (200 <= resp.status_code < 300):
        body = resp.text.strip()
        if len(body) > 500:
            body = body[:500] + "..."
        raise HTTPException(
            status_code=503,
            detail=f"Supabase login failed ({resp.status_code}): {body}",
        )

    session_payload = resp.json() if resp.text else {}
    access_token = str(session_payload.get("access_token") or "").strip()
    if not access_token:
        raise HTTPException(
            status_code=401,
            detail="Supabase login response did not include access_token.",
        )
    return session_payload


def _refresh_access_token(*, refresh_token: str) -> dict[str, Any]:
    token = str(refresh_token or "").strip()
    if not token:
        raise HTTPException(status_code=422, detail="refresh_token is required.")

    supabase_url = os.environ.get("SUPABASE_URL", "").strip()
    if not supabase_url:
        raise HTTPException(
            status_code=503,
            detail="Supabase login is unavailable (SUPABASE_URL missing).",
        )
    auth_apikey = _resolve_supabase_public_auth_key()

    try:
        resp = requests.post(
            f"{supabase_url.rstrip('/')}/auth/v1/token",
            params={"grant_type": "refresh_token"},
            headers={
                "apikey": auth_apikey,
                "Content-Type": "application/json",
            },
            json={"refresh_token": token},
            timeout=15,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Could not reach Supabase Auth: {exc}",
        ) from exc

    if resp.status_code in {400, 401}:
        raise HTTPException(
            status_code=401,
            detail="Invalid or expired refresh token.",
        )
    if not (200 <= resp.status_code < 300):
        body = resp.text.strip()
        if len(body) > 500:
            body = body[:500] + "..."
        raise HTTPException(
            status_code=503,
            detail=f"Supabase refresh failed ({resp.status_code}): {body}",
        )

    session_payload = resp.json() if resp.text else {}
    access_token = str(session_payload.get("access_token") or "").strip()
    if not access_token:
        raise HTTPException(
            status_code=401,
            detail="Supabase refresh response did not include access_token.",
        )
    return session_payload


def _normalize_required_uuid(raw: str, field_name: str) -> str:
    candidate = (raw or "").strip()
    try:
        return str(uuid.UUID(candidate))
    except Exception as exc:
        raise HTTPException(
            status_code=422,
            detail=f"{field_name} must be a valid UUID.",
        ) from exc


def _get_membership_row(
    *,
    tenant_id: str,
    user_id: str,
    active_only: bool = False,
) -> dict[str, Any] | None:
    if _RUN_REPOSITORY is None:
        raise HTTPException(
            status_code=503,
            detail="Supabase repository is required for membership checks.",
        )

    params = {
        "select": "id,tenant_id,user_id,role,status,created_at,updated_at",
        "tenant_id": f"eq.{tenant_id}",
        "user_id": f"eq.{user_id}",
        "limit": "1",
    }
    if active_only:
        params["status"] = "eq.active"

    try:
        status_code, rows = _RUN_REPOSITORY._request(  # noqa: SLF001
            "GET",
            "/rest/v1/memberships",
            params=params,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Could not query memberships in Supabase: {exc}",
        ) from exc

    if status_code < 200 or status_code >= 300 or not rows:
        return None
    return rows[0]


def _is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return False


def _get_cached_catalog_value(key: str) -> Any | None:
    if CATALOG_CACHE_TTL_SECONDS <= 0:
        return None
    cached = _CATALOG_CACHE.get(key)
    if not cached:
        return None
    cached_at, value = cached
    if time.time() - cached_at > CATALOG_CACHE_TTL_SECONDS:
        _CATALOG_CACHE.pop(key, None)
        return None
    return value


def _set_cached_catalog_value(key: str, value: Any) -> Any:
    if CATALOG_CACHE_TTL_SECONDS > 0:
        _CATALOG_CACHE[key] = (time.time(), value)
    return value


def _load_tenant_roles_catalog() -> list[dict[str, Any]]:
    cache_key = "tenant_roles_catalog"
    cached = _get_cached_catalog_value(cache_key)
    if isinstance(cached, list):
        return cached

    fallback_roles = sorted(
        {role for role in MANAGED_MEMBERSHIP_ROLES if role} | {"owner", "editor", "viewer"}
    )
    fallback_payload = [
        {
            "role_key": role,
            "is_admin": role in TENANT_ADMIN_MEMBERSHIP_ROLES,
            "is_active": True,
            "sort_order": index,
        }
        for index, role in enumerate(fallback_roles, start=1)
    ]

    if _RUN_REPOSITORY is None:
        return _set_cached_catalog_value(cache_key, fallback_payload)

    try:
        _, rows = _RUN_REPOSITORY._request(  # noqa: SLF001
            "GET",
            "/rest/v1/tenant_roles_catalog",
            params={
                "select": "role_key,is_admin,is_active,sort_order",
                "is_active": "eq.true",
                "order": "sort_order.asc,role_key.asc",
                "limit": "200",
            },
            allow_not_found=True,
        )
    except Exception:
        return _set_cached_catalog_value(cache_key, fallback_payload)

    normalized: list[dict[str, Any]] = []
    for row in rows or []:
        role_key = str(row.get("role_key") or "").strip().lower()
        if not role_key:
            continue
        normalized.append(
            {
                "role_key": role_key,
                "is_admin": bool(row.get("is_admin")),
                "is_active": bool(row.get("is_active", True)),
                "sort_order": int(row.get("sort_order") or 100),
            }
        )

    if not normalized:
        return _set_cached_catalog_value(cache_key, fallback_payload)
    return _set_cached_catalog_value(cache_key, normalized)


def _get_managed_membership_roles() -> list[str]:
    roles = [str(row.get("role_key") or "").strip().lower() for row in _load_tenant_roles_catalog()]
    normalized = [role for role in roles if role]
    return sorted(set(normalized))


def _get_tenant_admin_roles() -> set[str]:
    admin_roles = {
        str(row.get("role_key") or "").strip().lower()
        for row in _load_tenant_roles_catalog()
        if bool(row.get("is_admin"))
    }
    admin_roles = {role for role in admin_roles if role}
    if admin_roles:
        return admin_roles
    return set(TENANT_ADMIN_MEMBERSHIP_ROLES)


def _resolve_default_membership_role(allowed_roles: list[str]) -> str:
    normalized_allowed = [role for role in allowed_roles if role]
    if not normalized_allowed:
        return "viewer"
    if DEFAULT_NEW_MEMBERSHIP_ROLE in normalized_allowed:
        return DEFAULT_NEW_MEMBERSHIP_ROLE
    if "viewer" in normalized_allowed:
        return "viewer"
    return normalized_allowed[0]


def _load_membership_plans_catalog() -> list[dict[str, Any]]:
    cache_key = "membership_plans_catalog"
    cached = _get_cached_catalog_value(cache_key)
    if isinstance(cached, list):
        return cached

    fallback = [
        {
            "plan_key": "memoria_basica",
            "display_name": "Memoria Basica",
            "route_path": "/products/memoria-basica",
            "is_default": True,
            "sort_order": 10,
        },
        {
            "plan_key": "comparacion_presupuestos",
            "display_name": "Comparacion de Presupuestos",
            "route_path": "/products/comparacion-presupuestos",
            "is_default": True,
            "sort_order": 20,
        },
    ]

    if _RUN_REPOSITORY is None:
        return _set_cached_catalog_value(cache_key, fallback)

    try:
        _, rows = _RUN_REPOSITORY._request(  # noqa: SLF001
            "GET",
            "/rest/v1/membership_plans",
            params={
                "select": "plan_key,display_name,route_path,is_default,sort_order",
                "is_active": "eq.true",
                "order": "sort_order.asc,plan_key.asc",
                "limit": "200",
            },
            allow_not_found=True,
        )
    except Exception:
        return _set_cached_catalog_value(cache_key, fallback)

    normalized: list[dict[str, Any]] = []
    for row in rows or []:
        plan_key = str(row.get("plan_key") or "").strip().lower()
        if not plan_key:
            continue
        normalized.append(
            {
                "plan_key": plan_key,
                "display_name": str(row.get("display_name") or "").strip() or plan_key,
                "route_path": str(row.get("route_path") or "").strip() or "",
                "is_default": bool(row.get("is_default")),
                "sort_order": int(row.get("sort_order") or 100),
            }
        )

    if not normalized:
        return _set_cached_catalog_value(cache_key, fallback)
    return _set_cached_catalog_value(cache_key, normalized)


def _load_tenant_subscriptions_map(
    *,
    tenant_id: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    if _RUN_REPOSITORY is None:
        return {}

    params: dict[str, str] = {
        "select": "tenant_id,plan_key,status,starts_at,ends_at,membership_plans(plan_key,display_name,route_path,sort_order)",
        "order": "created_at.desc",
        "limit": "10000",
    }
    if tenant_id:
        params["tenant_id"] = f"eq.{tenant_id}"

    try:
        _, rows = _RUN_REPOSITORY._request(  # noqa: SLF001
            "GET",
            "/rest/v1/tenant_subscriptions",
            params=params,
            allow_not_found=True,
        )
    except Exception:
        return {}

    by_tenant: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows or []:
        status = str(row.get("status") or "").strip().lower()
        if status not in ACTIVE_MEMBERSHIP_STATUSES:
            continue
        tenant_key = str(row.get("tenant_id") or "").strip()
        plan_key = str(row.get("plan_key") or "").strip().lower()
        if not tenant_key or not plan_key:
            continue
        plan_node = row.get("membership_plans")
        if not isinstance(plan_node, dict):
            plan_node = {}
        by_tenant[tenant_key].append(
            {
                "plan_key": plan_key,
                "display_name": str(plan_node.get("display_name") or "").strip() or plan_key,
                "route_path": str(plan_node.get("route_path") or "").strip() or "",
                "status": status,
                "starts_at": row.get("starts_at"),
                "ends_at": row.get("ends_at"),
                "sort_order": int(plan_node.get("sort_order") or 100),
            }
        )

    for tenant_key, plans in by_tenant.items():
        unique_by_key: dict[str, dict[str, Any]] = {}
        for item in plans:
            unique_by_key[item["plan_key"]] = item
        by_tenant[tenant_key] = sorted(
            unique_by_key.values(),
            key=lambda item: (int(item.get("sort_order") or 100), item.get("plan_key") or ""),
        )

    return dict(by_tenant)


def _is_global_admin_from_catalog(user_id: str) -> bool:
    normalized_user_id = str(user_id or "").strip()
    if not normalized_user_id or _RUN_REPOSITORY is None:
        return False

    cache_key = f"global_admin_user:{normalized_user_id}"
    cached = _get_cached_catalog_value(cache_key)
    if isinstance(cached, bool):
        return cached

    try:
        _, rows = _RUN_REPOSITORY._request(  # noqa: SLF001
            "GET",
            "/rest/v1/global_admin_users",
            params={
                "select": "user_id,role_key",
                "user_id": f"eq.{normalized_user_id}",
                "limit": "1",
            },
            allow_not_found=True,
        )
    except Exception:
        return _set_cached_catalog_value(cache_key, False)

    return _set_cached_catalog_value(cache_key, bool(rows))


def _is_global_admin_user(user: dict[str, Any]) -> tuple[bool, str]:
    user_id = str(user.get("id") or "").strip().lower()
    email = str(user.get("email") or "").strip().lower()

    if user_id and user_id in GLOBAL_ADMIN_USER_IDS:
        return True, "global_admin_user_ids"
    if email and email in GLOBAL_ADMIN_EMAILS:
        return True, "global_admin_emails"
    if user_id and _is_global_admin_from_catalog(user_id):
        return True, "global_admin_users_table"

    app_metadata = user.get("app_metadata")
    if not isinstance(app_metadata, dict):
        app_metadata = {}

    for flag in GLOBAL_ADMIN_METADATA_FLAGS:
        # Security hardening: only trust app_metadata (set by server/admin),
        # never user_metadata (potentially user-controlled).
        if _is_truthy(app_metadata.get(flag)):
            return True, f"metadata_flag:{flag}"

    scalar_roles = {
        str(app_metadata.get("role") or "").strip().lower(),
    }
    if scalar_roles & GLOBAL_ADMIN_ALLOWED_ROLES:
        return True, "metadata_role"

    for role_field in ("roles",):
        role_values: set[str] = set()
        app_roles = app_metadata.get(role_field)
        if isinstance(app_roles, list):
            role_values.update(str(item).strip().lower() for item in app_roles if str(item).strip())
        elif isinstance(app_roles, str):
            role_values.update(part.strip().lower() for part in app_roles.split(",") if part.strip())
        if role_values & GLOBAL_ADMIN_ALLOWED_ROLES:
            return True, "metadata_roles"

    return False, ""


def _authorize_global_admin(
    *,
    request: Request,
    allow_query_token: bool = False,
) -> dict[str, str]:
    token = _extract_request_token(request, allow_query_token=allow_query_token)
    user = _resolve_authenticated_user(token)
    is_admin, granted_by = _is_global_admin_user(user)
    if not is_admin:
        raise HTTPException(
            status_code=403,
            detail=(
                "User is not a global admin. Configure GLOBAL_ADMIN_USER_IDS / "
                "GLOBAL_ADMIN_EMAILS, or assign role in public.global_user_roles "
                "(global_roles.is_admin=true), or use global admin flags in Supabase app_metadata."
            ),
        )

    return {
        "id": str(user.get("id") or "").strip(),
        "email": user.get("email") or "",
        "granted_by": granted_by,
    }


def _authorize_tenant_read_access(
    *,
    request: Request,
    tenant_id: str,
) -> str:
    """Allow tenant read access for active members or global admins."""
    token = _extract_bearer_token(request)
    user = _resolve_authenticated_user(token)
    actor_user_id = str(user.get("id") or "").strip()
    if not actor_user_id:
        raise HTTPException(status_code=401, detail="Session token has no user id.")

    membership = _get_membership_row(
        tenant_id=tenant_id,
        user_id=actor_user_id,
        active_only=True,
    )
    if membership is not None:
        return actor_user_id

    is_admin, _ = _is_global_admin_user(user)
    if is_admin:
        return actor_user_id

    raise HTTPException(
        status_code=403,
        detail="User has no active membership in the selected tenant.",
    )


def _fetch_auth_users_map(user_ids: set[str]) -> dict[str, dict[str, str]]:
    if not user_ids:
        return {}

    supabase_url = os.environ.get("SUPABASE_URL", "").strip()
    supabase_service_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not supabase_url or not supabase_service_key:
        return {}

    users_by_id: dict[str, dict[str, str]] = {}
    page = 1
    per_page = min(500, max(100, len(user_ids)))

    while page <= 20 and len(users_by_id) < len(user_ids):
        try:
            resp = requests.get(
                f"{supabase_url.rstrip('/')}/auth/v1/admin/users",
                headers={
                    "apikey": supabase_service_key,
                    "Authorization": f"Bearer {supabase_service_key}",
                },
                params={"page": page, "per_page": per_page},
                timeout=15,
            )
        except Exception:
            break

        if not (200 <= resp.status_code < 300):
            break

        payload = resp.json() if resp.text else {}
        users = payload.get("users") if isinstance(payload, dict) else []
        if not isinstance(users, list) or not users:
            break

        for item in users:
            uid = str(item.get("id") or "").strip()
            if not uid or uid not in user_ids:
                continue
            metadata = item.get("user_metadata") if isinstance(item, dict) else {}
            if not isinstance(metadata, dict):
                metadata = {}
            display_name = (
                str(metadata.get("full_name") or "").strip()
                or str(metadata.get("name") or "").strip()
            )
            users_by_id[uid] = {
                "email": str(item.get("email") or "").strip(),
                "display_name": display_name,
            }

        if len(users) < per_page:
            break
        page += 1

    return users_by_id


def _build_run_metrics_maps(
    tenant_id: str | None = None,
) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[str, dict[str, Any]], int]:
    if _RUN_REPOSITORY is None:
        return {}, {}, 0

    has_budget_runs = False
    try:
        has_budget_runs = _RUN_REPOSITORY._has_budget_runs_table()  # noqa: SLF001
    except Exception:
        has_budget_runs = False
    if not has_budget_runs:
        return {}, {}, 0

    params = {
        "select": "tenant_id,created_by,status,started_at",
        "order": "started_at.desc",
        "limit": "10000",
    }
    if tenant_id:
        params["tenant_id"] = f"eq.{tenant_id}"

    try:
        status_code, rows = _RUN_REPOSITORY._request(  # noqa: SLF001
            "GET",
            "/rest/v1/budget_runs",
            params=params,
            allow_not_found=True,
        )
    except Exception as exc:
        print(f"[warn] Could not load budget run metrics: {exc}")
        return {}, {}, 0

    if status_code == 404 or not rows:
        return {}, {}, 0

    user_tenant_metrics: dict[tuple[str, str], dict[str, Any]] = defaultdict(
        lambda: {
            "runs_total": 0,
            "runs_completed": 0,
            "runs_failed": 0,
            "last_run_at": None,
        }
    )
    tenant_metrics: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "runs_total": 0,
            "runs_completed": 0,
            "runs_failed": 0,
            "last_run_at": None,
        }
    )
    total_runs = 0

    for row in rows:
        run_tenant_id = str(row.get("tenant_id") or "").strip()
        user_id = str(row.get("created_by") or "").strip()
        if not run_tenant_id or not user_id:
            continue
        total_runs += 1
        user_entry = user_tenant_metrics[(run_tenant_id, user_id)]
        tenant_entry = tenant_metrics[run_tenant_id]
        user_entry["runs_total"] += 1
        tenant_entry["runs_total"] += 1

        status = str(row.get("status") or "").strip().lower()
        if status == "completed":
            user_entry["runs_completed"] += 1
            tenant_entry["runs_completed"] += 1
        elif status == "failed":
            user_entry["runs_failed"] += 1
            tenant_entry["runs_failed"] += 1

        started_at = str(row.get("started_at") or "").strip() or None
        if started_at and (
            not user_entry["last_run_at"] or started_at > str(user_entry["last_run_at"])
        ):
            user_entry["last_run_at"] = started_at
        if started_at and (
            not tenant_entry["last_run_at"] or started_at > str(tenant_entry["last_run_at"])
        ):
            tenant_entry["last_run_at"] = started_at

    return dict(user_tenant_metrics), dict(tenant_metrics), total_runs


def _authorize_budget_execution(
    *,
    request: Request,
    tenant_id: str,
) -> str:
    token = _extract_bearer_token(request)
    user = _resolve_authenticated_user(token)
    user_id = user["id"]

    if _RUN_REPOSITORY is None:
        raise HTTPException(
            status_code=503,
            detail="Supabase repository is required for authorization checks.",
        )

    try:
        status_code, rows = _RUN_REPOSITORY._request(  # noqa: SLF001
            "GET",
            "/rest/v1/memberships",
            params={
                "select": "role,status,tenants(products)",
                "tenant_id": f"eq.{tenant_id}",
                "user_id": f"eq.{user_id}",
                "status": "eq.active",
                "limit": "1",
            },
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Could not validate tenant membership against Supabase: {exc}",
        ) from exc
    if status_code < 200 or status_code >= 300 or not rows:
        raise HTTPException(
            status_code=403,
            detail="User has no active membership in the selected tenant.",
        )

    membership = rows[0]
    role = str(membership.get("role") or "").strip().lower()
    if role not in {"owner", "editor"}:
        raise HTTPException(
            status_code=403,
            detail="User role is not allowed to run budget comparison executions.",
        )

    tenant_node = membership.get("tenants") or {}
    products = tenant_node.get("products") if isinstance(tenant_node, dict) else []
    normalized_products = {str(product).strip().lower() for product in (products or [])}
    if (
        "comparacion_presupuestos" not in normalized_products
        and "comparacion-presupuestos" not in normalized_products
    ):
        raise HTTPException(
            status_code=403,
            detail="Comparador de presupuestos is not enabled for the selected tenant.",
        )

    return user_id


