"""Tenant credit billing helpers backed by Supabase RPC functions."""

from __future__ import annotations

import math
import os
from datetime import datetime, timezone
from typing import Any


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return max(minimum, int(raw))
    except Exception:
        return default


class CreditBalanceError(RuntimeError):
    def __init__(self, *, available: int, required: int, message: str):
        super().__init__(message)
        self.available = available
        self.required = required


class CreditBillingNotInitializedError(RuntimeError):
    """Raised when the credit ledger schema/functions are not available."""


def _normalize_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _extract_rpc_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, (int, float, str)):
        return [{"value": payload}]
    return []


def _rpc(repo: Any, fn_name: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    _, data = repo._request(  # noqa: SLF001
        "POST",
        f"/rest/v1/rpc/{fn_name}",
        payload=payload,
    )
    return _extract_rpc_rows(data)


def _is_missing_rpc(exc: Exception, fn_name: str) -> bool:
    text = str(exc or "")
    return "PGRST202" in text and f"/rpc/{fn_name}" in text


def _ledger_exists(repo: Any) -> bool:
    try:
        status, _ = repo._request(  # noqa: SLF001
            "GET",
            "/rest/v1/tenant_credit_ledger",
            params={"select": "id", "limit": "1"},
            allow_not_found=True,
        )
        return status != 404
    except Exception:
        return False


def _to_iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _month_start_utc_date() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.year:04d}-{now.month:02d}-01"


def _month_end_utc_iso() -> str:
    now = datetime.now(timezone.utc)
    next_month_year = now.year + (1 if now.month == 12 else 0)
    next_month = 1 if now.month == 12 else now.month + 1
    return datetime(next_month_year, next_month, 1, tzinfo=timezone.utc).isoformat()


def _manual_current_balance(repo: Any, tenant_id: str) -> int:
    if not _ledger_exists(repo):
        raise CreditBillingNotInitializedError(
            "Credit ledger table is missing. Run scripts/sql/20260309_credit_billing.sql."
        )
    now_iso = _to_iso_utc_now()
    _, rows = repo._request(  # noqa: SLF001
        "GET",
        "/rest/v1/tenant_credit_ledger",
        params={
            "select": "credits_delta,expires_at",
            "tenant_id": f"eq.{tenant_id}",
            "or": f"(expires_at.is.null,expires_at.gt.{now_iso})",
            "limit": "10000",
        },
        allow_not_found=True,
    )
    balance = 0
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        balance += _normalize_int(row.get("credits_delta"), default=0)
    return int(balance)


def _manual_ensure_monthly_grant(repo: Any, tenant_id: str, actor_user_id: str | None) -> int:
    if not _ledger_exists(repo):
        raise CreditBillingNotInitializedError(
            "Credit ledger table is missing. Run scripts/sql/20260309_credit_billing.sql."
        )
    cycle_start = _month_start_utc_date()
    cycle_end = _month_end_utc_iso()
    idempotency_key = f"grant:monthly:{tenant_id}:{cycle_start}"

    # Check idempotency first.
    _, existing = repo._request(  # noqa: SLF001
        "GET",
        "/rest/v1/tenant_credit_ledger",
        params={"select": "id", "idempotency_key": f"eq.{idempotency_key}", "limit": "1"},
        allow_not_found=True,
    )
    if existing:
        return 0

    # Sum monthly credits from active subscriptions.
    try:
        _, subscriptions = repo._request(  # noqa: SLF001
            "GET",
            "/rest/v1/tenant_subscriptions",
            params={
                "select": "status,starts_at,ends_at,membership_plans(monthly_credits)",
                "tenant_id": f"eq.{tenant_id}",
                "limit": "200",
            },
            allow_not_found=True,
        )
    except Exception:
        subscriptions = []

    now_iso = _to_iso_utc_now()
    grant_credits = 0
    for row in subscriptions or []:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status") or "").strip().lower()
        if status not in {"active", "trial"}:
            continue
        starts_at = str(row.get("starts_at") or "").strip()
        ends_at = str(row.get("ends_at") or "").strip()
        if starts_at and starts_at > now_iso:
            continue
        if ends_at and ends_at <= now_iso:
            continue

        plan_node = row.get("membership_plans")
        if isinstance(plan_node, list):
            plan_node = plan_node[0] if plan_node else {}
        if not isinstance(plan_node, dict):
            plan_node = {}
        grant_credits += max(0, _normalize_int(plan_node.get("monthly_credits"), default=0))

    if grant_credits <= 0:
        return 0

    row = {
        "tenant_id": tenant_id,
        "event_type": "grant",
        "credits_delta": grant_credits,
        "reason": "monthly_subscription_grant",
        "reference_type": "monthly_cycle",
        "reference_id": cycle_start[:7],
        "cycle_month": cycle_start,
        "expires_at": cycle_end,
        "metadata": {"cycle_start": cycle_start, "cycle_end": cycle_end},
        "idempotency_key": idempotency_key,
        "created_by": actor_user_id,
    }
    try:
        repo._request(  # noqa: SLF001
            "POST",
            "/rest/v1/tenant_credit_ledger",
            payload=row,
            prefer="return=minimal",
        )
        return grant_credits
    except Exception as exc:
        text = str(exc or "")
        if "duplicate key value" in text or "23505" in text:
            return 0
        raise


def _manual_consume(
    *,
    repo: Any,
    tenant_id: str,
    amount: int,
    idempotency_key: str,
    reason: str,
    reference_type: str,
    reference_id: str | None,
    metadata: dict[str, Any],
    actor_user_id: str | None,
) -> dict[str, Any]:
    _manual_ensure_monthly_grant(repo, tenant_id, actor_user_id)
    _, existing = repo._request(  # noqa: SLF001
        "GET",
        "/rest/v1/tenant_credit_ledger",
        params={"select": "id", "idempotency_key": f"eq.{idempotency_key}", "limit": "1"},
        allow_not_found=True,
    )
    if existing:
        return {"success": True, "balance": _manual_current_balance(repo, tenant_id), "consumed": amount, "message": "already_consumed"}

    balance = _manual_current_balance(repo, tenant_id)
    if balance < amount:
        return {"success": False, "balance": balance, "consumed": 0, "message": "insufficient_credits"}

    row = {
        "tenant_id": tenant_id,
        "event_type": "consume",
        "credits_delta": -abs(amount),
        "reason": reason,
        "reference_type": reference_type,
        "reference_id": reference_id,
        "metadata": metadata or {},
        "idempotency_key": idempotency_key,
        "created_by": actor_user_id,
    }
    repo._request(  # noqa: SLF001
        "POST",
        "/rest/v1/tenant_credit_ledger",
        payload=row,
        prefer="return=minimal",
    )
    return {"success": True, "balance": _manual_current_balance(repo, tenant_id), "consumed": amount, "message": "consumed"}


def _manual_refund(
    *,
    repo: Any,
    tenant_id: str,
    amount: int,
    idempotency_key: str,
    reason: str,
    reference_type: str,
    reference_id: str | None,
    metadata: dict[str, Any],
    actor_user_id: str | None,
) -> dict[str, Any]:
    _, existing = repo._request(  # noqa: SLF001
        "GET",
        "/rest/v1/tenant_credit_ledger",
        params={"select": "id", "idempotency_key": f"eq.{idempotency_key}", "limit": "1"},
        allow_not_found=True,
    )
    if existing:
        return {"success": True, "balance": _manual_current_balance(repo, tenant_id), "refunded": amount, "message": "already_refunded"}

    row = {
        "tenant_id": tenant_id,
        "event_type": "refund",
        "credits_delta": abs(amount),
        "reason": reason,
        "reference_type": reference_type,
        "reference_id": reference_id,
        "metadata": metadata or {},
        "idempotency_key": idempotency_key,
        "created_by": actor_user_id,
    }
    repo._request(  # noqa: SLF001
        "POST",
        "/rest/v1/tenant_credit_ledger",
        payload=row,
        prefer="return=minimal",
    )
    return {"success": True, "balance": _manual_current_balance(repo, tenant_id), "refunded": amount, "message": "refunded"}


def _execution_cost_breakdown(
    pdf_count: int,
    total_bytes: int,
    file_sizes_bytes: list[int] | None = None,
    *,
    is_rerun: bool,
) -> dict[str, Any]:
    base = _env_int("BILLING_BUDGET_RUN_BASE_CREDITS", 9, minimum=0)
    per_pdf = _env_int("BILLING_BUDGET_RUN_PER_PDF_CREDITS", 3, minimum=0)
    per_mb = _env_int("BILLING_BUDGET_RUN_PER_MB_CREDITS", 2, minimum=0)
    margin_pct = _env_int("BILLING_EXECUTION_ESTIMATE_MARGIN_PERCENT", 12, minimum=0)
    rerun_discount_pct = _env_int("BILLING_RERUN_DISCOUNT_PERCENT", 25, minimum=0)

    normalized_pdf_count = max(1, pdf_count)
    normalized_sizes = [
        max(0, int(size))
        for size in (file_sizes_bytes or [])
        if isinstance(size, (int, float))
    ]
    normalized_total_bytes = (
        int(sum(normalized_sizes))
        if normalized_sizes
        else max(0, int(total_bytes))
    )
    total_mb = float(normalized_total_bytes) / (1024.0 * 1024.0)
    if per_mb > 0:
        if normalized_sizes:
            size_credits = int(
                sum(math.ceil((float(size) / (1024.0 * 1024.0)) * per_mb) for size in normalized_sizes)
            )
        else:
            size_credits = int(math.ceil(total_mb * per_mb))
    else:
        size_credits = 0

    base_amount = base + (normalized_pdf_count * per_pdf) + max(0, size_credits)
    amount_with_margin = int(math.ceil(base_amount * (1 + (margin_pct / 100.0))))
    amount = max(1, amount_with_margin)

    if is_rerun and rerun_discount_pct > 0:
        discount = int(round(amount * min(100, rerun_discount_pct) / 100))
        amount = max(1, amount - discount)

    return {
        "base_credits": int(base),
        "pdf_credits": int(normalized_pdf_count * per_pdf),
        "size_credits": int(max(0, size_credits)),
        "total_megabytes": round(total_mb, 4),
        "size_mode": "per_file" if normalized_sizes else "total",
        "margin_percent": int(margin_pct),
        "estimated_before_discount": int(max(1, amount_with_margin)),
        "rerun_discount_percent": int(min(100, rerun_discount_pct) if is_rerun else 0),
        "final_credits": int(max(1, amount)),
    }


def estimate_execution_credits(
    *,
    pdf_count: int,
    total_bytes: int,
    file_sizes_bytes: list[int] | None = None,
    is_rerun: bool,
) -> dict[str, Any]:
    normalized_pdf_count = max(1, int(pdf_count))
    normalized_total_bytes = max(0, int(total_bytes))
    breakdown = _execution_cost_breakdown(
        normalized_pdf_count,
        normalized_total_bytes,
        file_sizes_bytes=file_sizes_bytes,
        is_rerun=is_rerun,
    )
    return {
        "pdf_count": normalized_pdf_count,
        "total_bytes": normalized_total_bytes,
        "mode": "rerun" if is_rerun else "run",
        **breakdown,
    }


def _credit_unit_price_usd() -> float:
    raw = os.environ.get("BILLING_CREDIT_UNIT_PRICE_USD")
    if raw is None:
        return 0.08
    try:
        return max(0.005, float(raw))
    except Exception:
        return 0.06


def _credit_cost_safety_margin() -> float:
    raw = os.environ.get("BILLING_CREDIT_COST_SAFETY_MARGIN")
    if raw is None:
        return 1.30
    try:
        return max(1.0, float(raw))
    except Exception:
        return 1.35


def _default_run_cost_usd() -> float:
    raw = os.environ.get("BILLING_DEFAULT_RUN_COST_USD")
    if raw is None:
        return 1.15
    try:
        return max(0.05, float(raw))
    except Exception:
        return 1.2


_APP_KEY_ALIASES = {
    "comparacion_presupuestos": "comparacion_presupuestos",
    "comparacion-presupuestos": "comparacion_presupuestos",
    "comparador_presupuestos": "comparacion_presupuestos",
    "budget_comparison": "comparacion_presupuestos",
    "memoria_basica": "memoria_basica",
    "memoria-basica": "memoria_basica",
    "memoria_basic": "memoria_basica",
}


def normalize_app_key(raw: Any) -> str:
    candidate = str(raw or "").strip().lower()
    if not candidate:
        return ""
    normalized = candidate.replace(" ", "_").replace("-", "_")
    return _APP_KEY_ALIASES.get(candidate) or _APP_KEY_ALIASES.get(normalized) or normalized


def _normalize_non_negative_int(value: Any, default: int = 0) -> int:
    try:
        return max(0, int(value))
    except Exception:
        return max(0, int(default))


def normalize_tenant_billing_config(raw: Any) -> dict[str, Any]:
    source = raw if isinstance(raw, dict) else {}
    show_client_badge = bool(source.get("show_client_badge", source.get("show_client_credits", True)))
    use_credit_plan = bool(source.get("use_credit_plan", True))
    use_custom_plan = bool(source.get("use_custom_plan", False))
    if use_credit_plan and use_custom_plan:
        # Exclusive by design: custom plan wins if both were set manually.
        use_credit_plan = False

    custom_plan_raw = source.get("custom_plan") if isinstance(source.get("custom_plan"), dict) else {}
    apps_raw: Any = custom_plan_raw.get("apps", source.get("custom_apps", {}))
    normalized_apps: dict[str, dict[str, int]] = {}
    if isinstance(apps_raw, dict):
        iterator = apps_raw.items()
    elif isinstance(apps_raw, list):
        iterator = [
            (str(item.get("app_key") or "").strip(), item)
            for item in apps_raw
            if isinstance(item, dict)
        ]
    else:
        iterator = []

    for raw_key, raw_payload in iterator:
        app_key = normalize_app_key(raw_key or (raw_payload.get("app_key") if isinstance(raw_payload, dict) else ""))
        if not app_key or not isinstance(raw_payload, dict):
            continue
        normalized_apps[app_key] = {
            "executions_limit": _normalize_non_negative_int(
                raw_payload.get("executions_limit", raw_payload.get("executions_total", raw_payload.get("executions")))
            ),
            "reruns_limit": _normalize_non_negative_int(
                raw_payload.get("reruns_limit", raw_payload.get("reruns_total", raw_payload.get("reruns")))
            ),
            "executions_used": _normalize_non_negative_int(raw_payload.get("executions_used")),
            "reruns_used": _normalize_non_negative_int(raw_payload.get("reruns_used")),
        }

    idempotency_raw = custom_plan_raw.get("idempotency")
    normalized_idempotency: dict[str, dict[str, Any]] = {}
    if isinstance(idempotency_raw, dict):
        for key, payload in idempotency_raw.items():
            idem_key = str(key or "").strip()
            if not idem_key or not isinstance(payload, dict):
                continue
            normalized_idempotency[idem_key] = {
                "action": str(payload.get("action") or "").strip(),
                "app_key": normalize_app_key(payload.get("app_key")),
                "kind": str(payload.get("kind") or "").strip(),
                "quantity": _normalize_non_negative_int(payload.get("quantity"), default=1),
                "at": str(payload.get("at") or "").strip() or _to_iso_utc_now(),
            }

    return {
        "show_client_badge": show_client_badge,
        "use_credit_plan": use_credit_plan,
        "use_custom_plan": use_custom_plan,
        "custom_plan": {
            "apps": normalized_apps,
            "idempotency": normalized_idempotency,
        },
    }


def _load_tenant_metadata(
    *,
    repo: Any,
    tenant_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    _, rows = repo._request(  # noqa: SLF001
        "GET",
        "/rest/v1/tenants",
        params={
            "select": "id,metadata",
            "id": f"eq.{tenant_id}",
            "limit": "1",
        },
        allow_not_found=True,
    )
    row = rows[0] if rows else None
    if not isinstance(row, dict):
        raise RuntimeError(f"Tenant not found: {tenant_id}")
    metadata = row.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
    return row, metadata


def _persist_tenant_metadata(
    *,
    repo: Any,
    tenant_id: str,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    _, rows = repo._request(  # noqa: SLF001
        "PATCH",
        "/rest/v1/tenants",
        params={"id": f"eq.{tenant_id}"},
        payload={"metadata": metadata},
        prefer="return=representation",
    )
    row = rows[0] if rows else None
    if not isinstance(row, dict):
        raise RuntimeError(f"Could not persist tenant metadata for {tenant_id}.")
    updated_metadata = row.get("metadata")
    if not isinstance(updated_metadata, dict):
        updated_metadata = metadata
    return updated_metadata


def get_tenant_billing_config(
    *,
    repo: Any,
    tenant_id: str,
) -> dict[str, Any]:
    _, metadata = _load_tenant_metadata(repo=repo, tenant_id=tenant_id)
    return normalize_tenant_billing_config(metadata.get("billing_config"))


def set_tenant_billing_config(
    *,
    repo: Any,
    tenant_id: str,
    billing_config: dict[str, Any],
) -> dict[str, Any]:
    _, metadata = _load_tenant_metadata(repo=repo, tenant_id=tenant_id)
    normalized = normalize_tenant_billing_config(billing_config)
    next_metadata = dict(metadata)
    next_metadata["billing_config"] = normalized
    persisted = _persist_tenant_metadata(repo=repo, tenant_id=tenant_id, metadata=next_metadata)
    return normalize_tenant_billing_config(persisted.get("billing_config"))


def _prune_idempotency_entries(idempotency: dict[str, dict[str, Any]], max_entries: int = 250) -> dict[str, dict[str, Any]]:
    if len(idempotency) <= max_entries:
        return idempotency
    ordered_keys = list(idempotency.keys())
    trimmed: dict[str, dict[str, Any]] = {}
    for key in ordered_keys[-max_entries:]:
        trimmed[key] = idempotency[key]
    return trimmed


def _build_quota_snapshot_for_app(*, config: dict[str, Any], app_key: str) -> dict[str, Any]:
    custom_plan = config.get("custom_plan") if isinstance(config.get("custom_plan"), dict) else {}
    apps = custom_plan.get("apps") if isinstance(custom_plan.get("apps"), dict) else {}
    app_quota = apps.get(app_key) if isinstance(apps.get(app_key), dict) else {}
    executions_limit = _normalize_non_negative_int(app_quota.get("executions_limit"))
    reruns_limit = _normalize_non_negative_int(app_quota.get("reruns_limit"))
    executions_used = _normalize_non_negative_int(app_quota.get("executions_used"))
    reruns_used = _normalize_non_negative_int(app_quota.get("reruns_used"))
    return {
        "app_key": app_key,
        "executions_limit": executions_limit,
        "executions_used": executions_used,
        "executions_remaining": max(0, executions_limit - executions_used),
        "reruns_limit": reruns_limit,
        "reruns_used": reruns_used,
        "reruns_remaining": max(0, reruns_limit - reruns_used),
    }


def _consume_custom_plan_quota(
    *,
    repo: Any,
    tenant_id: str,
    app_key: str,
    is_rerun: bool,
    idempotency_key: str,
) -> dict[str, Any]:
    _, metadata = _load_tenant_metadata(repo=repo, tenant_id=tenant_id)
    current_config = normalize_tenant_billing_config(metadata.get("billing_config"))
    custom_plan = current_config.setdefault("custom_plan", {"apps": {}, "idempotency": {}})
    apps = custom_plan.setdefault("apps", {})
    idempotency_map = custom_plan.setdefault("idempotency", {})
    if not isinstance(idempotency_map, dict):
        idempotency_map = {}
        custom_plan["idempotency"] = idempotency_map

    if idempotency_key and idempotency_key in idempotency_map:
        snapshot = _build_quota_snapshot_for_app(config=current_config, app_key=app_key)
        return {
            "success": True,
            "message": "already_consumed",
            "snapshot": snapshot,
        }

    app_quota = apps.get(app_key) if isinstance(apps.get(app_key), dict) else {}
    if not isinstance(app_quota, dict):
        app_quota = {}
    executions_limit = _normalize_non_negative_int(app_quota.get("executions_limit"))
    reruns_limit = _normalize_non_negative_int(app_quota.get("reruns_limit"))
    executions_used = _normalize_non_negative_int(app_quota.get("executions_used"))
    reruns_used = _normalize_non_negative_int(app_quota.get("reruns_used"))

    if is_rerun:
        remaining = max(0, reruns_limit - reruns_used)
        if remaining <= 0:
            return {
                "success": False,
                "message": "insufficient_custom_reruns",
                "snapshot": {
                    "app_key": app_key,
                    "executions_limit": executions_limit,
                    "executions_used": executions_used,
                    "executions_remaining": max(0, executions_limit - executions_used),
                    "reruns_limit": reruns_limit,
                    "reruns_used": reruns_used,
                    "reruns_remaining": remaining,
                },
            }
        reruns_used += 1
    else:
        remaining = max(0, executions_limit - executions_used)
        if remaining <= 0:
            return {
                "success": False,
                "message": "insufficient_custom_executions",
                "snapshot": {
                    "app_key": app_key,
                    "executions_limit": executions_limit,
                    "executions_used": executions_used,
                    "executions_remaining": remaining,
                    "reruns_limit": reruns_limit,
                    "reruns_used": reruns_used,
                    "reruns_remaining": max(0, reruns_limit - reruns_used),
                },
            }
        executions_used += 1

    apps[app_key] = {
        "executions_limit": executions_limit,
        "reruns_limit": reruns_limit,
        "executions_used": executions_used,
        "reruns_used": reruns_used,
    }
    if idempotency_key:
        idempotency_map[idempotency_key] = {
            "action": "consume",
            "app_key": app_key,
            "kind": "rerun" if is_rerun else "execution",
            "quantity": 1,
            "at": _to_iso_utc_now(),
        }
    custom_plan["idempotency"] = _prune_idempotency_entries(idempotency_map)

    next_metadata = dict(metadata)
    next_metadata["billing_config"] = current_config
    persisted_metadata = _persist_tenant_metadata(repo=repo, tenant_id=tenant_id, metadata=next_metadata)
    persisted_config = normalize_tenant_billing_config(persisted_metadata.get("billing_config"))
    return {
        "success": True,
        "message": "consumed",
        "snapshot": _build_quota_snapshot_for_app(config=persisted_config, app_key=app_key),
    }


def _refund_custom_plan_quota(
    *,
    repo: Any,
    tenant_id: str,
    app_key: str,
    is_rerun: bool,
    idempotency_key: str,
) -> dict[str, Any]:
    _, metadata = _load_tenant_metadata(repo=repo, tenant_id=tenant_id)
    current_config = normalize_tenant_billing_config(metadata.get("billing_config"))
    custom_plan = current_config.setdefault("custom_plan", {"apps": {}, "idempotency": {}})
    apps = custom_plan.setdefault("apps", {})
    idempotency_map = custom_plan.setdefault("idempotency", {})
    if not isinstance(idempotency_map, dict):
        idempotency_map = {}
        custom_plan["idempotency"] = idempotency_map

    if idempotency_key and idempotency_key in idempotency_map:
        snapshot = _build_quota_snapshot_for_app(config=current_config, app_key=app_key)
        return {
            "success": True,
            "message": "already_refunded",
            "snapshot": snapshot,
        }

    app_quota = apps.get(app_key) if isinstance(apps.get(app_key), dict) else {}
    if not isinstance(app_quota, dict):
        app_quota = {}
    executions_limit = _normalize_non_negative_int(app_quota.get("executions_limit"))
    reruns_limit = _normalize_non_negative_int(app_quota.get("reruns_limit"))
    executions_used = _normalize_non_negative_int(app_quota.get("executions_used"))
    reruns_used = _normalize_non_negative_int(app_quota.get("reruns_used"))

    if is_rerun:
        reruns_used = max(0, reruns_used - 1)
    else:
        executions_used = max(0, executions_used - 1)

    apps[app_key] = {
        "executions_limit": executions_limit,
        "reruns_limit": reruns_limit,
        "executions_used": executions_used,
        "reruns_used": reruns_used,
    }
    if idempotency_key:
        idempotency_map[idempotency_key] = {
            "action": "refund",
            "app_key": app_key,
            "kind": "rerun" if is_rerun else "execution",
            "quantity": 1,
            "at": _to_iso_utc_now(),
        }
    custom_plan["idempotency"] = _prune_idempotency_entries(idempotency_map)

    next_metadata = dict(metadata)
    next_metadata["billing_config"] = current_config
    persisted_metadata = _persist_tenant_metadata(repo=repo, tenant_id=tenant_id, metadata=next_metadata)
    persisted_config = normalize_tenant_billing_config(persisted_metadata.get("billing_config"))
    return {
        "success": True,
        "message": "refunded",
        "snapshot": _build_quota_snapshot_for_app(config=persisted_config, app_key=app_key),
    }


def _load_active_plan_keys(*, repo: Any, tenant_id: str) -> list[str]:
    now_iso = _to_iso_utc_now()
    active: set[str] = set()
    try:
        _, rows = repo._request(  # noqa: SLF001
            "GET",
            "/rest/v1/tenant_subscriptions",
            params={
                "select": "plan_key,status,starts_at,ends_at",
                "tenant_id": f"eq.{tenant_id}",
                "limit": "200",
            },
            allow_not_found=True,
        )
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            status = str(row.get("status") or "").strip().lower()
            if status not in {"active", "trial"}:
                continue
            starts_at = str(row.get("starts_at") or "").strip()
            ends_at = str(row.get("ends_at") or "").strip()
            if starts_at and starts_at > now_iso:
                continue
            if ends_at and ends_at <= now_iso:
                continue
            key = normalize_app_key(row.get("plan_key"))
            if key:
                active.add(key)
    except Exception:
        active = set()

    if active:
        return sorted(active)

    try:
        _, rows = repo._request(  # noqa: SLF001
            "GET",
            "/rest/v1/tenants",
            params={"select": "products", "id": f"eq.{tenant_id}", "limit": "1"},
            allow_not_found=True,
        )
        row = rows[0] if rows else {}
        products = row.get("products") if isinstance(row, dict) else []
        if isinstance(products, list):
            for entry in products:
                key = normalize_app_key(entry)
                if key:
                    active.add(key)
    except Exception:
        pass
    return sorted(active)


def _build_custom_quota_summary(
    *,
    config: dict[str, Any],
    active_app_keys: list[str] | None = None,
) -> dict[str, Any]:
    custom_plan = config.get("custom_plan") if isinstance(config.get("custom_plan"), dict) else {}
    apps = custom_plan.get("apps") if isinstance(custom_plan.get("apps"), dict) else {}
    configured_keys = [normalize_app_key(key) for key in apps.keys()]
    keys_set = {key for key in configured_keys if key}
    for key in active_app_keys or []:
        normalized = normalize_app_key(key)
        if normalized:
            keys_set.add(normalized)
    app_items: list[dict[str, Any]] = []
    for app_key in sorted(keys_set):
        snapshot = _build_quota_snapshot_for_app(config=config, app_key=app_key)
        app_items.append(snapshot)

    total_exec_remaining = sum(int(item.get("executions_remaining") or 0) for item in app_items)
    total_rerun_remaining = sum(int(item.get("reruns_remaining") or 0) for item in app_items)
    return {
        "apps": app_items,
        "total_executions_remaining": total_exec_remaining,
        "total_reruns_remaining": total_rerun_remaining,
    }


def _billing_enabled() -> bool:
    return _env_bool("BILLING_CREDITS_ENABLED", True)


def consume_execution_credits(
    *,
    repo: Any,
    tenant_id: str,
    project_id: str,
    job_id: str,
    pdf_count: int,
    total_bytes: int = 0,
    file_sizes_bytes: list[int] | None = None,
    actor_user_id: str | None,
    is_rerun: bool,
    source_run_id: str | None = None,
    app_key: str | None = None,
) -> dict[str, Any]:
    normalized_app_key = normalize_app_key(app_key) or "comparacion_presupuestos"
    mode = "rerun" if is_rerun else "run"
    if repo is None or not _billing_enabled():
        return {
            "enforced": False,
            "consumed": False,
            "amount": 0,
            "balance_after": None,
            "mode": mode,
            "job_id": job_id,
            "project_id": project_id,
            "tenant_id": tenant_id,
            "app_key": normalized_app_key,
            "billing_kind": "none",
        }

    billing_config = get_tenant_billing_config(repo=repo, tenant_id=tenant_id)
    use_custom_plan = bool(billing_config.get("use_custom_plan"))
    use_credit_plan = bool(billing_config.get("use_credit_plan"))
    if use_custom_plan:
        consume_key = f"consume:custom:{mode}:{tenant_id}:{normalized_app_key}:{job_id}"
        custom_result = _consume_custom_plan_quota(
            repo=repo,
            tenant_id=tenant_id,
            app_key=normalized_app_key,
            is_rerun=is_rerun,
            idempotency_key=consume_key,
        )
        if not bool(custom_result.get("success")):
            snapshot = custom_result.get("snapshot") if isinstance(custom_result.get("snapshot"), dict) else {}
            available = _normalize_non_negative_int(
                snapshot.get("reruns_remaining" if is_rerun else "executions_remaining"),
                default=0,
            )
            required = 1
            action_label = "re-ejecuciones" if is_rerun else "ejecuciones"
            raise CreditBalanceError(
                available=available,
                required=required,
                message=(
                    f"No quedan {action_label} disponibles para la aplicacion '{normalized_app_key}'. "
                    f"Disponibles: {available}. Requeridos: {required}."
                ),
            )

        snapshot = custom_result.get("snapshot") if isinstance(custom_result.get("snapshot"), dict) else {}
        return {
            "enforced": True,
            "consumed": True,
            "amount": 0,
            "balance_after": None,
            "mode": mode,
            "job_id": job_id,
            "project_id": project_id,
            "tenant_id": tenant_id,
            "pdf_count": max(1, pdf_count),
            "total_bytes": max(0, int(total_bytes)),
            "file_sizes_bytes": [max(0, int(size)) for size in (file_sizes_bytes or [])],
            "estimate": None,
            "source_run_id": source_run_id,
            "app_key": normalized_app_key,
            "billing_kind": "quota",
            "quota": snapshot,
            "consume_idempotency_key": consume_key,
            "refund_idempotency_key": f"refund:{consume_key}",
            "refunded": False,
        }

    if not use_credit_plan:
        return {
            "enforced": False,
            "consumed": False,
            "amount": 0,
            "balance_after": None,
            "mode": mode,
            "job_id": job_id,
            "project_id": project_id,
            "tenant_id": tenant_id,
            "app_key": normalized_app_key,
            "billing_kind": "none",
        }

    estimate = estimate_execution_credits(
        pdf_count=pdf_count,
        total_bytes=total_bytes,
        file_sizes_bytes=file_sizes_bytes,
        is_rerun=is_rerun,
    )
    amount = int(estimate.get("final_credits") or 0)
    consume_key = f"consume:{mode}:{tenant_id}:{job_id}"

    consume_payload = {
        "p_tenant_id": tenant_id,
        "p_amount": amount,
        "p_idempotency_key": consume_key,
        "p_reason": "budget_execution_rerun" if is_rerun else "budget_execution",
        "p_reference_type": "budget_rerun" if is_rerun else "budget_run",
        "p_reference_id": source_run_id or job_id,
        "p_metadata": {
            "job_id": job_id,
            "project_id": project_id,
            "mode": mode,
            "pdf_count": max(1, pdf_count),
            "total_bytes": max(0, int(total_bytes)),
            "file_sizes_bytes": [max(0, int(size)) for size in (file_sizes_bytes or [])],
            "total_megabytes": estimate.get("total_megabytes"),
            "margin_percent": estimate.get("margin_percent"),
            "size_mode": estimate.get("size_mode"),
            "source_run_id": source_run_id,
        },
        "p_created_by": actor_user_id,
    }
    try:
        rows = _rpc(repo, "consume_tenant_credits", consume_payload)
        row = rows[0] if rows else {}
    except Exception as exc:
        if _is_missing_rpc(exc, "consume_tenant_credits"):
            row = _manual_consume(
                repo=repo,
                tenant_id=tenant_id,
                amount=amount,
                idempotency_key=consume_key,
                reason=str(consume_payload["p_reason"]),
                reference_type=str(consume_payload["p_reference_type"]),
                reference_id=consume_payload["p_reference_id"],
                metadata=consume_payload["p_metadata"],
                actor_user_id=actor_user_id,
            )
        else:
            raise
    success = bool(row.get("success"))
    balance_after = _normalize_int(row.get("balance"), default=0)
    consumed = _normalize_int(row.get("consumed"), default=0)
    message = str(row.get("message") or "unknown").strip().lower()
    if not success:
        if message == "insufficient_credits":
            raise CreditBalanceError(
                available=balance_after,
                required=amount,
                message=(
                    "No hay creditos suficientes para ejecutar la comparativa. "
                    f"Disponibles: {balance_after}. Requeridos: {amount}."
                ),
            )
        raise RuntimeError(f"Credit consumption failed with message '{message}'.")

    return {
        "enforced": True,
        "consumed": True,
        "amount": consumed or amount,
        "balance_after": balance_after,
        "mode": mode,
        "job_id": job_id,
        "project_id": project_id,
        "tenant_id": tenant_id,
        "pdf_count": max(1, pdf_count),
        "total_bytes": max(0, int(total_bytes)),
        "file_sizes_bytes": [max(0, int(size)) for size in (file_sizes_bytes or [])],
        "estimate": estimate,
        "source_run_id": source_run_id,
        "app_key": normalized_app_key,
        "billing_kind": "credits",
        "consume_idempotency_key": consume_key,
        "refund_idempotency_key": f"refund:{consume_key}",
        "refunded": False,
    }


def maybe_refund_execution_credits(
    *,
    repo: Any,
    billing_context: dict[str, Any] | None,
    actor_user_id: str | None,
    reason: str,
) -> dict[str, Any] | None:
    if repo is None:
        return billing_context
    if not billing_context:
        return None
    if not billing_context.get("enforced"):
        return billing_context
    if not billing_context.get("consumed"):
        return billing_context
    if billing_context.get("refunded"):
        return billing_context

    tenant_id = str(billing_context.get("tenant_id") or "").strip()
    if not tenant_id:
        return billing_context

    billing_kind = str(billing_context.get("billing_kind") or "credits").strip().lower()
    refund_key = str(billing_context.get("refund_idempotency_key") or "").strip()
    if not refund_key:
        consume_key = str(billing_context.get("consume_idempotency_key") or "").strip()
        if not consume_key:
            return billing_context
        refund_key = f"refund:{consume_key}"

    if billing_kind == "quota":
        app_key = normalize_app_key(billing_context.get("app_key")) or "comparacion_presupuestos"
        mode = str(billing_context.get("mode") or "").strip().lower()
        refund_result = _refund_custom_plan_quota(
            repo=repo,
            tenant_id=tenant_id,
            app_key=app_key,
            is_rerun=mode == "rerun",
            idempotency_key=refund_key,
        )
        if bool(refund_result.get("success")):
            billing_context["refunded"] = True
            billing_context["refund_reason"] = reason
            billing_context["refund_message"] = str(refund_result.get("message") or "refunded")
            snapshot = refund_result.get("snapshot") if isinstance(refund_result.get("snapshot"), dict) else {}
            billing_context["quota"] = snapshot
        return billing_context

    amount = _normalize_int(billing_context.get("amount"), default=0)
    if amount <= 0:
        return billing_context

    reference_id = str(
        billing_context.get("source_run_id")
        or billing_context.get("job_id")
        or ""
    ).strip()
    refund_payload = {
        "p_tenant_id": tenant_id,
        "p_amount": amount,
        "p_idempotency_key": refund_key,
        "p_reason": reason,
        "p_reference_type": "budget_run",
        "p_reference_id": reference_id,
        "p_metadata": {
            "job_id": billing_context.get("job_id"),
            "project_id": billing_context.get("project_id"),
            "mode": billing_context.get("mode"),
            "reason": reason,
        },
        "p_created_by": actor_user_id,
    }
    try:
        rows = _rpc(repo, "refund_tenant_credits", refund_payload)
        row = rows[0] if rows else {}
    except Exception as exc:
        if _is_missing_rpc(exc, "refund_tenant_credits"):
            row = _manual_refund(
                repo=repo,
                tenant_id=tenant_id,
                amount=amount,
                idempotency_key=refund_key,
                reason=reason,
                reference_type="budget_run",
                reference_id=reference_id,
                metadata=refund_payload["p_metadata"],
                actor_user_id=actor_user_id,
            )
        else:
            raise
    if bool(row.get("success")):
        billing_context["refunded"] = True
        billing_context["refund_reason"] = reason
        billing_context["refund_message"] = str(row.get("message") or "refunded")
        billing_context["balance_after_refund"] = _normalize_int(row.get("balance"), default=0)
    return billing_context


def adjust_tenant_credits(
    *,
    repo: Any,
    tenant_id: str,
    delta_credits: int,
    actor_user_id: str | None,
    reason: str,
    reference_id: str | None,
) -> dict[str, Any]:
    if repo is None:
        raise RuntimeError("Supabase repository is required for tenant credit adjustments.")

    delta = int(delta_credits)
    if delta == 0:
        raise ValueError("delta_credits must be non-zero.")

    normalized_reason = str(reason or "").strip() or "admin_credit_adjustment"
    normalized_reference_id = str(reference_id or "").strip() or tenant_id
    idempotency_key = f"admin-adjust:{tenant_id}:{normalized_reference_id}:{delta}"

    if delta > 0:
        refund_payload = {
            "p_tenant_id": tenant_id,
            "p_amount": delta,
            "p_idempotency_key": idempotency_key,
            "p_reason": normalized_reason,
            "p_reference_type": "admin_adjustment",
            "p_reference_id": normalized_reference_id,
            "p_metadata": {"delta_credits": delta, "kind": "manual_admin_credit"},
            "p_created_by": actor_user_id,
        }
        try:
            rows = _rpc(repo, "refund_tenant_credits", refund_payload)
            row = rows[0] if rows else {}
        except Exception as exc:
            if _is_missing_rpc(exc, "refund_tenant_credits"):
                row = _manual_refund(
                    repo=repo,
                    tenant_id=tenant_id,
                    amount=delta,
                    idempotency_key=idempotency_key,
                    reason=normalized_reason,
                    reference_type="admin_adjustment",
                    reference_id=normalized_reference_id,
                    metadata=refund_payload["p_metadata"],
                    actor_user_id=actor_user_id,
                )
            else:
                raise
        if not bool(row.get("success")):
            raise RuntimeError("Could not apply positive tenant credit adjustment.")
        return {
            "ok": True,
            "delta_credits": delta,
            "balance_after": _normalize_int(row.get("balance"), default=0),
            "message": str(row.get("message") or "adjusted"),
        }

    amount = abs(delta)
    consume_payload = {
        "p_tenant_id": tenant_id,
        "p_amount": amount,
        "p_idempotency_key": idempotency_key,
        "p_reason": normalized_reason,
        "p_reference_type": "admin_adjustment",
        "p_reference_id": normalized_reference_id,
        "p_metadata": {"delta_credits": delta, "kind": "manual_admin_debit"},
        "p_created_by": actor_user_id,
    }
    try:
        rows = _rpc(repo, "consume_tenant_credits", consume_payload)
        row = rows[0] if rows else {}
    except Exception as exc:
        if _is_missing_rpc(exc, "consume_tenant_credits"):
            row = _manual_consume(
                repo=repo,
                tenant_id=tenant_id,
                amount=amount,
                idempotency_key=idempotency_key,
                reason=normalized_reason,
                reference_type="admin_adjustment",
                reference_id=normalized_reference_id,
                metadata=consume_payload["p_metadata"],
                actor_user_id=actor_user_id,
            )
        else:
            raise
    if bool(row.get("success")):
        return {
            "ok": True,
            "delta_credits": delta,
            "balance_after": _normalize_int(row.get("balance"), default=0),
            "message": str(row.get("message") or "adjusted"),
        }
    message = str(row.get("message") or "unknown").strip().lower()
    if message == "insufficient_credits":
        raise CreditBalanceError(
            available=_normalize_int(row.get("balance"), default=0),
            required=amount,
            message=(
                "No hay creditos suficientes para descontar esta cantidad. "
                f"Disponibles: {_normalize_int(row.get('balance'), default=0)}. Requeridos: {amount}."
            ),
        )
    raise RuntimeError(f"Could not apply negative tenant credit adjustment ({message}).")


def build_credit_policy_recommendation(
    *,
    avg_run_cost_usd: float | None,
    runs_sampled: int,
) -> dict[str, Any]:
    effective_avg = float(avg_run_cost_usd) if isinstance(avg_run_cost_usd, (int, float)) else _default_run_cost_usd()
    effective_avg = max(0.05, effective_avg)
    safety_margin = _credit_cost_safety_margin()
    credit_unit_price = _credit_unit_price_usd()
    target_runs_per_month = _env_int("BILLING_TARGET_RUNS_PER_MONTH", 25, minimum=1)
    min_credits_per_execution = _env_int("BILLING_MIN_CREDITS_PER_EXECUTION", 12, minimum=1)

    raw_credits_per_execution = (effective_avg * safety_margin) / max(credit_unit_price, 0.001)
    credits_per_execution = max(min_credits_per_execution, int(math.ceil(raw_credits_per_execution)))
    recommended_starting_credits = credits_per_execution * target_runs_per_month
    tier_starter = recommended_starting_credits
    tier_growth = int(math.ceil(recommended_starting_credits * 1.8))
    tier_scale = int(math.ceil(recommended_starting_credits * 3.2))

    return {
        "avg_run_cost_usd": round(effective_avg, 6),
        "runs_sampled": int(max(0, runs_sampled)),
        "credit_unit_price_usd": round(credit_unit_price, 6),
        "safety_margin": round(safety_margin, 4),
        "recommended_credits_per_execution": int(credits_per_execution),
        "recommended_starting_credits": int(recommended_starting_credits),
        "recommended_monthly_credits": int(recommended_starting_credits),
        "target_runs_per_month": int(target_runs_per_month),
        "recommended_tiers": [
            {
                "key": "starter",
                "label": "Starter",
                "monthly_credits": int(tier_starter),
                "estimated_runs": int(max(1, math.floor(tier_starter / credits_per_execution))),
            },
            {
                "key": "growth",
                "label": "Growth",
                "monthly_credits": int(tier_growth),
                "estimated_runs": int(max(1, math.floor(tier_growth / credits_per_execution))),
            },
            {
                "key": "scale",
                "label": "Scale",
                "monthly_credits": int(tier_scale),
                "estimated_runs": int(max(1, math.floor(tier_scale / credits_per_execution))),
            },
        ],
    }


def get_tenant_credit_balance(
    *,
    repo: Any,
    tenant_id: str,
    actor_user_id: str | None,
) -> dict[str, Any]:
    if repo is None:
        raise RuntimeError("Supabase repository is required for credit balance checks.")
    billing_config = get_tenant_billing_config(repo=repo, tenant_id=tenant_id)
    show_client_badge = bool(billing_config.get("show_client_badge"))
    use_custom_plan = bool(billing_config.get("use_custom_plan"))
    use_credit_plan = bool(billing_config.get("use_credit_plan"))

    if not _billing_enabled():
        active_apps = _load_active_plan_keys(repo=repo, tenant_id=tenant_id)
        quota_summary = _build_custom_quota_summary(config=billing_config, active_app_keys=active_apps)
        return {
            "enabled": False,
            "tenant_id": tenant_id,
            "balance": None,
            "monthly_granted_now": 0,
            "visible": False,
            "billing_kind": "none",
            "show_client_badge": show_client_badge,
            "use_credit_plan": use_credit_plan,
            "use_custom_plan": use_custom_plan,
            "quota": quota_summary,
            "billing_config": billing_config,
        }

    active_apps = _load_active_plan_keys(repo=repo, tenant_id=tenant_id)
    quota_summary = _build_custom_quota_summary(config=billing_config, active_app_keys=active_apps)

    if use_custom_plan:
        return {
            "enabled": True,
            "tenant_id": tenant_id,
            "balance": None,
            "monthly_granted_now": 0,
            "visible": bool(show_client_badge),
            "billing_kind": "quota",
            "show_client_badge": show_client_badge,
            "use_credit_plan": use_credit_plan,
            "use_custom_plan": use_custom_plan,
            "quota": quota_summary,
            "billing_config": billing_config,
        }

    if not use_credit_plan:
        return {
            "enabled": False,
            "tenant_id": tenant_id,
            "balance": None,
            "monthly_granted_now": 0,
            "visible": False,
            "billing_kind": "none",
            "show_client_badge": show_client_badge,
            "use_credit_plan": use_credit_plan,
            "use_custom_plan": use_custom_plan,
            "quota": quota_summary,
            "billing_config": billing_config,
        }

    granted = 0
    try:
        granted_rows = _rpc(
            repo,
            "ensure_monthly_credit_grant",
            {
                "p_tenant_id": tenant_id,
                "p_created_by": actor_user_id,
            },
        )
        if granted_rows:
            first = granted_rows[0]
            if isinstance(first, dict):
                granted = _normalize_int(
                    first.get("ensure_monthly_credit_grant", next(iter(first.values()), 0)),
                    default=0,
                )
    except Exception as exc:
        if _is_missing_rpc(exc, "ensure_monthly_credit_grant"):
            granted = _manual_ensure_monthly_grant(repo, tenant_id, actor_user_id)
        else:
            raise

    try:
        balance_rows = _rpc(
            repo,
            "get_tenant_credit_balance",
            {
                "p_tenant_id": tenant_id,
            },
        )
        balance = 0
        if balance_rows:
            first = balance_rows[0]
            if isinstance(first, dict):
                balance = _normalize_int(
                    first.get("get_tenant_credit_balance", next(iter(first.values()), 0)),
                    default=0,
                )
    except Exception as exc:
        if _is_missing_rpc(exc, "get_tenant_credit_balance"):
            balance = _manual_current_balance(repo, tenant_id)
        else:
            raise

    return {
        "enabled": True,
        "tenant_id": tenant_id,
        "balance": balance,
        "monthly_granted_now": granted,
        "visible": bool(show_client_badge),
        "billing_kind": "credits",
        "show_client_badge": show_client_badge,
        "use_credit_plan": use_credit_plan,
        "use_custom_plan": use_custom_plan,
        "quota": quota_summary,
        "billing_config": billing_config,
    }
