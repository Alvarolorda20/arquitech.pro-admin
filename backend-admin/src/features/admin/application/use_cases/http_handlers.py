"""Admin HTTP handlers extracted from runtime monolith."""

import os
import re
from collections import defaultdict
from datetime import datetime
from typing import Any
from urllib.parse import quote

from fastapi import HTTPException, Query, Request
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel

from src.app.runtime import (
    DISABLED_MEMBERSHIP_STATUS,
    STATIC_BASE_DIR,
    _RUN_REPOSITORY,
    _safe_filename,
)
from src.shared.billing.credit_service import (
    CreditBalanceError,
    CreditBillingNotInitializedError,
    adjust_tenant_credits,
    build_credit_policy_recommendation,
    get_tenant_billing_config,
    get_tenant_credit_balance,
    normalize_app_key,
    normalize_tenant_billing_config,
    set_tenant_billing_config,
)
from src.shared.security.runtime_auth_service import (
    _authorize_global_admin,
    _build_run_metrics_maps,
    _exchange_email_password_for_access_token,
    _fetch_auth_users_map,
    _get_managed_membership_roles,
    _get_membership_row,
    _get_tenant_admin_roles,
    _is_global_admin_user,
    _load_membership_plans_catalog,
    _load_tenant_subscriptions_map,
    _normalize_required_uuid,
    _refresh_access_token,
    _resolve_authenticated_user,
    _resolve_default_membership_role,
)


class MembershipStatusPatch(BaseModel):
    tenant_id: str
    user_id: str
    enabled: bool


class MembershipRolePatch(BaseModel):
    tenant_id: str
    user_id: str
    role: str


class MembershipDeletePayload(BaseModel):
    tenant_id: str
    user_id: str


class TenantSubscriptionStatusPatch(BaseModel):
    tenant_id: str
    plan_key: str
    enabled: bool


class TenantCreditsAdjustPatch(BaseModel):
    tenant_id: str
    delta_credits: int
    reason: str | None = None


class TenantBillingAppQuotaPatch(BaseModel):
    app_key: str
    executions_limit: int
    reruns_limit: int


class TenantBillingConfigPatch(BaseModel):
    tenant_id: str
    show_client_badge: bool | None = None
    use_credit_plan: bool | None = None
    use_custom_plan: bool | None = None
    custom_plan_apps: list[TenantBillingAppQuotaPatch] | None = None


class AdminLoginPayload(BaseModel):
    email: str
    password: str


class AdminRefreshPayload(BaseModel):
    refresh_token: str


_ARTIFACT_CLASS_PRIORITY = {
    "canonical": 0,
    "trace": 1,
    "debug": 2,
}

_ARTIFACT_CLASS_LABELS = {
    "canonical": "final",
    "trace": "traza",
    "debug": "debug",
}

_ARTIFACT_LABELS = {
    "final_json": {"label": "JSON final", "stage": "resultado", "priority": 0},
    "mapping_links_final": {"label": "Enlaces finales", "stage": "mapeo", "priority": 10},
    "audit_qualitative_input": {"label": "Entrada auditoria", "stage": "auditoria", "priority": 20},
    "auditoria_validada": {"label": "Auditoria validada", "stage": "auditoria", "priority": 21},
    "project_details": {"label": "Detalles del proyecto", "stage": "contexto", "priority": 22},
    "plan_log": {"label": "Plan de ejecucion", "stage": "traza", "priority": 40},
    "cap_mapping": {"label": "Mapeo de capitulos", "stage": "traza", "priority": 41},
    "mapping_links": {"label": "Enlaces intermedios", "stage": "traza", "priority": 42},
    "extra_review": {"label": "Revision extra", "stage": "traza", "priority": 43},
    "auditoria": {"label": "Auditoria base", "stage": "traza", "priority": 44},
    "auditoria_enriquecida": {"label": "Auditoria enriquecida", "stage": "traza", "priority": 45},
}


def _describe_admin_artifact(artifact_key: str, artifact_class: str) -> dict[str, Any]:
    normalized_key = str(artifact_key or "").strip().lower()
    normalized_class = str(artifact_class or "").strip().lower()
    if normalized_key in _ARTIFACT_LABELS:
        return _ARTIFACT_LABELS[normalized_key]
    if normalized_key.startswith("chunk_"):
        return {"label": "Chunk", "stage": "debug", "priority": 80}
    if normalized_key.startswith("debug_"):
        return {"label": "Batch debug", "stage": "debug", "priority": 81}
    return {
        "label": normalized_key.replace("_", " ").strip() or "Artefacto",
        "stage": normalized_class or "desconocido",
        "priority": 95,
    }


def _coerce_extraction_ids(result_payload: Any) -> list[str]:
    if not isinstance(result_payload, dict):
        return []
    raw = result_payload.get("extraction_ids")
    if not isinstance(raw, list):
        return []
    deduplicated: list[str] = []
    seen: set[str] = set()
    for value in raw:
        extraction_id = str(value or "").strip()
        if not extraction_id or extraction_id in seen:
            continue
        seen.add(extraction_id)
        deduplicated.append(extraction_id)
    return deduplicated


def _load_extractions_by_ids(extraction_ids: list[str]) -> list[dict[str, Any]]:
    if _RUN_REPOSITORY is None or not extraction_ids:
        return []

    rows: list[dict[str, Any]] = []
    for offset in range(0, len(extraction_ids), 75):
        chunk = extraction_ids[offset : offset + 75]
        if not chunk:
            continue
        try:
            _, extraction_rows = _RUN_REPOSITORY._request(  # noqa: SLF001
                "GET",
                "/rest/v1/extractions",
                params={
                    "select": (
                        "id,run_id,status,error_message,created_at,"
                        "raw_payload"
                    ),
                    "id": f"in.({','.join(chunk)})",
                    "limit": str(len(chunk)),
                },
                allow_not_found=True,
            )
            if isinstance(extraction_rows, list):
                rows.extend([row for row in extraction_rows if isinstance(row, dict)])
        except Exception as exc:
            print(f"[warn] Could not load extraction rows for admin artifacts: {exc}")
    return rows


def _build_admin_run_artifact_items(
    *,
    run_id: str,
    run_tenant_id: str,
    extraction_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for extraction in extraction_rows:
        extraction_id = str(extraction.get("id") or "").strip()
        if not extraction_id:
            continue
        raw_payload = extraction.get("raw_payload") if isinstance(extraction.get("raw_payload"), dict) else {}
        source_pdf = str(raw_payload.get("source_pdf") or "").strip() or str(raw_payload.get("safe_name") or "").strip() or "sin_pdf"
        artifacts = raw_payload.get("artifacts") if isinstance(raw_payload.get("artifacts"), dict) else {}

        for artifact_class, class_payload in artifacts.items():
            class_name = str(artifact_class or "").strip().lower()
            if not isinstance(class_payload, dict):
                continue
            for artifact_key, artifact_ref in class_payload.items():
                key = str(artifact_key or "").strip()
                ref = artifact_ref if isinstance(artifact_ref, dict) else {}
                bucket = str(ref.get("bucket") or "").strip()
                object_path = str(ref.get("path") or "").strip().strip("/")
                if not bucket or not object_path:
                    continue
                descriptor = _describe_admin_artifact(key, class_name)
                filename = os.path.basename(object_path) or f"{key}.json"
                items.append(
                    {
                        "artifact_id": f"{extraction_id}:{class_name}:{key}",
                        "run_id": run_id,
                        "tenant_id": run_tenant_id,
                        "extraction_id": extraction_id,
                        "source_pdf": source_pdf,
                        "artifact_key": key,
                        "artifact_class": class_name,
                        "artifact_class_label": _ARTIFACT_CLASS_LABELS.get(class_name, class_name or "archivo"),
                        "stage": descriptor["stage"],
                        "label": descriptor["label"],
                        "filename": filename,
                        "retention_days": ref.get("retention_days"),
                        "download_path": (
                            "/api/admin/run-artifact/download"
                            f"?run_id={quote(run_id)}"
                            f"&tenant_id={quote(run_tenant_id)}"
                            f"&extraction_id={quote(extraction_id)}"
                            f"&artifact_class={quote(class_name)}"
                            f"&artifact_key={quote(key)}"
                        ),
                        "_sort": (
                            source_pdf.lower(),
                            _ARTIFACT_CLASS_PRIORITY.get(class_name, 9),
                            int(descriptor.get("priority") or 99),
                            filename.lower(),
                        ),
                    }
                )

    items.sort(key=lambda item: item.pop("_sort", ("", 9, 99, "")))
    return items


def _parse_iso_datetime(raw: Any) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00") if text.endswith("Z") else text
    try:
        return datetime.fromisoformat(normalized)
    except Exception:
        return None


def _coerce_float(raw: Any) -> float | None:
    if isinstance(raw, (int, float)):
        return float(raw)
    if not isinstance(raw, str):
        return None
    value = raw.strip()
    if not value:
        return None
    cleaned = value.lower()
    cleaned = cleaned.replace("usd", "").replace("eur", "")
    cleaned = cleaned.replace("$", "").replace("€", "")
    cleaned = cleaned.replace(" ", "")
    cleaned = re.sub(r"[^0-9,.-]", "", cleaned)
    if cleaned.count(",") == 1 and "." not in cleaned:
        cleaned = cleaned.replace(",", ".")
    elif "," in cleaned:
        cleaned = cleaned.replace(",", "")
    try:
        return float(cleaned)
    except Exception:
        return None


def _normalize_currency(raw: Any) -> str | None:
    candidate = str(raw or "").strip().upper()
    if not candidate:
        return None
    if candidate in {"USD", "EUR", "GBP"}:
        return candidate
    return None


def _extract_cost_entry_from_payloads(*payloads: Any) -> dict[str, Any] | None:
    key_priority = [
        "cost_usd",
        "total_cost_usd",
        "estimated_cost_usd",
        "execution_cost_usd",
        "cost_eur",
        "total_cost_eur",
        "estimated_cost_eur",
        "execution_cost_eur",
        "total_cost",
        "estimated_cost",
        "execution_cost",
        "cost",
    ]
    priority_map = {key: idx for idx, key in enumerate(key_priority)}
    matches: list[dict[str, Any]] = []

    for source_index, payload in enumerate(payloads):
        if not isinstance(payload, (dict, list)):
            continue

        queue: list[tuple[str, Any, int, dict[str, Any] | None]] = [
            (f"source[{source_index}]", payload, 0, payload if isinstance(payload, dict) else None)
        ]
        visited = 0
        while queue and visited < 180:
            path, node, depth, parent_dict = queue.pop(0)
            visited += 1
            if depth > 4:
                continue

            if isinstance(node, dict):
                for key, value in node.items():
                    normalized_key = str(key or "").strip().lower()
                    child_path = f"{path}.{normalized_key}" if normalized_key else path
                    if normalized_key in priority_map:
                        amount = _coerce_float(value)
                        if amount is not None:
                            currency = _normalize_currency(node.get("currency"))
                            if not currency:
                                if normalized_key.endswith("_usd") or "usd" in normalized_key:
                                    currency = "USD"
                                elif normalized_key.endswith("_eur") or "eur" in normalized_key:
                                    currency = "EUR"
                            matches.append(
                                {
                                    "amount": amount,
                                    "currency": currency or "USD",
                                    "source": child_path,
                                    "priority": priority_map[normalized_key],
                                    "depth": depth,
                                }
                            )
                    if isinstance(value, (dict, list)):
                        queue.append((child_path, value, depth + 1, node))
            elif isinstance(node, list):
                for index, item in enumerate(node):
                    if isinstance(item, (dict, list)):
                        queue.append((f"{path}[{index}]", item, depth + 1, parent_dict))

    if not matches:
        return None

    matches.sort(key=lambda item: (item["priority"], item["depth"], item["source"]))
    selected = matches[0]
    return {
        "amount": round(float(selected["amount"]), 6),
        "currency": selected["currency"],
        "source": selected["source"],
    }


def _extract_credit_entry_from_result_payload(result_payload: Any) -> dict[str, Any] | None:
    if not isinstance(result_payload, dict):
        return None
    billing = result_payload.get("billing")
    if not isinstance(billing, dict):
        return None

    amount = int(billing.get("amount") or 0)
    if amount <= 0:
        return None
    refunded = bool(billing.get("refunded"))
    net_amount = 0 if refunded else amount
    return {
        "amount": amount,
        "refunded": refunded,
        "net_amount": net_amount,
        "mode": str(billing.get("mode") or "").strip() or "run",
        "balance_after": billing.get("balance_after"),
        "balance_after_refund": billing.get("balance_after_refund"),
    }


def _collect_credit_balances_by_tenant(
    *,
    tenant_ids: list[str],
    actor_user_id: str | None,
) -> dict[str, dict[str, Any]]:
    balances: dict[str, dict[str, Any]] = {}
    if _RUN_REPOSITORY is None:
        return balances

    for tenant_id in tenant_ids:
        normalized_tenant_id = str(tenant_id or "").strip()
        if not normalized_tenant_id:
            continue
        try:
            balances[normalized_tenant_id] = get_tenant_credit_balance(
                repo=_RUN_REPOSITORY,
                tenant_id=normalized_tenant_id,
                actor_user_id=actor_user_id,
            )
        except Exception:
            balances[normalized_tenant_id] = {
                "enabled": False,
                "tenant_id": normalized_tenant_id,
                "balance": None,
                "monthly_granted_now": 0,
                "visible": False,
                "billing_kind": "none",
                "quota": {"apps": [], "total_executions_remaining": 0, "total_reruns_remaining": 0},
                "billing_config": normalize_tenant_billing_config({}),
            }
    return balances


def _build_credit_policy_for_tenant_activity(
    user_activity_by_user_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    total_runs_sampled = 0
    total_priced_runs = 0
    total_usd = 0.0

    for payload in user_activity_by_user_id.values():
        summary = payload.get("summary") if isinstance(payload, dict) else {}
        if not isinstance(summary, dict):
            continue
        total_runs_sampled += int(summary.get("runs_total") or 0)
        total_priced_runs += int(summary.get("priced_runs") or 0)
        cost_by_currency = summary.get("cost_by_currency")
        if not isinstance(cost_by_currency, list):
            continue
        for entry in cost_by_currency:
            if not isinstance(entry, dict):
                continue
            currency = str(entry.get("currency") or "").strip().upper()
            if currency != "USD":
                continue
            amount = _coerce_float(entry.get("amount"))
            if amount is None:
                continue
            total_usd += float(amount)

    avg_usd_per_run = None
    if total_priced_runs > 0 and total_usd > 0:
        avg_usd_per_run = total_usd / total_priced_runs

    recommendation = build_credit_policy_recommendation(
        avg_run_cost_usd=avg_usd_per_run,
        runs_sampled=total_priced_runs,
    )
    recommendation["runs_total_sampled"] = total_runs_sampled
    recommendation["total_cost_usd_sampled"] = round(total_usd, 6)
    return recommendation


def _build_user_activity_by_user_for_tenant(tenant_id: str) -> dict[str, dict[str, Any]]:
    if _RUN_REPOSITORY is None:
        return {}

    try:
        has_budget_runs = _RUN_REPOSITORY._has_budget_runs_table()  # noqa: SLF001
    except Exception:
        has_budget_runs = False
    if not has_budget_runs:
        return {}

    try:
        status_code, run_rows = _RUN_REPOSITORY._request(  # noqa: SLF001
            "GET",
            "/rest/v1/budget_runs",
            params={
                "select": (
                    "id,tenant_id,project_id,task_id,pipeline_job_id,created_by,status,"
                    "started_at,finished_at,created_at,updated_at,request_payload,result_payload,error_message"
                ),
                "tenant_id": f"eq.{tenant_id}",
                "order": "started_at.desc",
                "limit": "3000",
            },
            allow_not_found=True,
        )
    except Exception as exc:
        print(f"[warn] Could not load detailed budget runs for admin tenant detail: {exc}")
        return {}

    if status_code == 404 or not run_rows:
        return {}

    task_ids = sorted(
        {
            str(row.get("task_id") or "").strip()
            for row in run_rows
            if str(row.get("task_id") or "").strip()
        }
    )
    tasks_by_id: dict[str, dict[str, Any]] = {}
    for offset in range(0, len(task_ids), 75):
        chunk = task_ids[offset : offset + 75]
        if not chunk:
            continue
        try:
            _, task_rows = _RUN_REPOSITORY._request(  # noqa: SLF001
                "GET",
                "/rest/v1/tasks",
                params={
                    "select": "id,title,status,payload,created_at,updated_at",
                    "id": f"in.({','.join(chunk)})",
                    "limit": str(len(chunk)),
                },
                allow_not_found=True,
            )
            for task_row in task_rows or []:
                task_id = str(task_row.get("id") or "").strip()
                if task_id:
                    tasks_by_id[task_id] = task_row
        except Exception as exc:
            print(f"[warn] Could not load admin task details chunk: {exc}")

    user_payload_map: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "summary": {
                "runs_total": 0,
                "runs_completed": 0,
                "runs_failed": 0,
                "runs_running": 0,
                "first_run_at": None,
                "last_run_at": None,
                "total_duration_seconds": 0,
                "duration_samples": 0,
                "avg_duration_seconds": None,
                "priced_runs": 0,
                "cost_by_currency": {},
                "credits_total": 0,
                "credits_refunded": 0,
                "credits_net": 0,
                "credited_runs": 0,
            },
            "project_breakdown": {},
            "recent_runs": [],
            "logs": [],
        }
    )

    for row in run_rows:
        user_id = str(row.get("created_by") or "").strip()
        run_tenant_id = str(row.get("tenant_id") or "").strip()
        if not user_id or run_tenant_id != tenant_id:
            continue

        payload = user_payload_map[user_id]
        summary = payload["summary"]

        status = str(row.get("status") or "").strip().lower() or "unknown"
        started_at = str(row.get("started_at") or "").strip() or str(row.get("created_at") or "").strip() or None
        finished_at = str(row.get("finished_at") or "").strip() or None
        run_id = str(row.get("id") or "").strip()
        project_id = str(row.get("project_id") or "").strip() or "sin-proyecto"
        task_id = str(row.get("task_id") or "").strip()
        task_row = tasks_by_id.get(task_id, {})
        task_payload = task_row.get("payload") if isinstance(task_row.get("payload"), dict) else {}
        request_payload = row.get("request_payload") if isinstance(row.get("request_payload"), dict) else {}
        result_payload = row.get("result_payload") if isinstance(row.get("result_payload"), dict) else {}
        output_excel = result_payload.get("output_excel") if isinstance(result_payload.get("output_excel"), dict) else {}
        output_path = str(output_excel.get("path") or "").strip()

        started_dt = _parse_iso_datetime(started_at)
        finished_dt = _parse_iso_datetime(finished_at)
        duration_seconds: int | None = None
        if started_dt and finished_dt:
            try:
                elapsed_seconds = int((finished_dt - started_dt).total_seconds())
            except Exception:
                elapsed_seconds = -1
            if elapsed_seconds >= 0:
                duration_seconds = elapsed_seconds

        summary["runs_total"] += 1
        if status == "completed":
            summary["runs_completed"] += 1
        elif status == "failed":
            summary["runs_failed"] += 1
        elif status in {"running", "queued", "in_progress"}:
            summary["runs_running"] += 1

        if started_at and (
            not summary["last_run_at"] or started_at > str(summary["last_run_at"])
        ):
            summary["last_run_at"] = started_at
        if started_at and (
            not summary["first_run_at"] or started_at < str(summary["first_run_at"])
        ):
            summary["first_run_at"] = started_at

        if isinstance(duration_seconds, int):
            summary["total_duration_seconds"] += duration_seconds
            summary["duration_samples"] += 1

        cost_entry = _extract_cost_entry_from_payloads(result_payload, task_payload, request_payload)
        credit_entry = _extract_credit_entry_from_result_payload(result_payload)
        if cost_entry:
            currency = str(cost_entry.get("currency") or "USD").upper()
            amount = float(cost_entry.get("amount") or 0.0)
            cost_by_currency = summary["cost_by_currency"]
            cost_by_currency[currency] = round(float(cost_by_currency.get(currency) or 0.0) + amount, 6)
            summary["priced_runs"] += 1
        if credit_entry:
            summary["credits_total"] += int(credit_entry.get("amount") or 0)
            if bool(credit_entry.get("refunded")):
                summary["credits_refunded"] += int(credit_entry.get("amount") or 0)
            summary["credits_net"] += int(credit_entry.get("net_amount") or 0)
            summary["credited_runs"] += 1

        project_node = payload["project_breakdown"].setdefault(
            project_id,
            {
                "project_id": project_id,
                "project_name": str(request_payload.get("project_name") or "").strip() or project_id,
                "runs_total": 0,
                "runs_completed": 0,
                "runs_failed": 0,
                "runs_running": 0,
                "last_run_at": None,
                "cost_by_currency": {},
                "credits_total": 0,
                "credits_refunded": 0,
                "credits_net": 0,
                "credited_runs": 0,
            },
        )
        project_node["runs_total"] += 1
        if status == "completed":
            project_node["runs_completed"] += 1
        elif status == "failed":
            project_node["runs_failed"] += 1
        elif status in {"running", "queued", "in_progress"}:
            project_node["runs_running"] += 1
        if started_at and (
            not project_node["last_run_at"] or started_at > str(project_node["last_run_at"])
        ):
            project_node["last_run_at"] = started_at
        if cost_entry:
            currency = str(cost_entry.get("currency") or "USD").upper()
            amount = float(cost_entry.get("amount") or 0.0)
            project_cost_by_currency = project_node["cost_by_currency"]
            project_cost_by_currency[currency] = round(
                float(project_cost_by_currency.get(currency) or 0.0) + amount,
                6,
            )
        if credit_entry:
            project_node["credits_total"] += int(credit_entry.get("amount") or 0)
            if bool(credit_entry.get("refunded")):
                project_node["credits_refunded"] += int(credit_entry.get("amount") or 0)
            project_node["credits_net"] += int(credit_entry.get("net_amount") or 0)
            project_node["credited_runs"] += 1

        run_item = {
            "run_id": run_id,
            "pipeline_job_id": str(row.get("pipeline_job_id") or "").strip(),
            "task_id": task_id,
            "project_id": project_id,
            "project_name": str(request_payload.get("project_name") or "").strip() or project_node["project_name"],
            "title": str(task_row.get("title") or "").strip() or f"Ejecucion {run_id[:8] if run_id else 'sin-id'}",
            "status": status,
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_seconds": duration_seconds,
            "error_message": str(row.get("error_message") or "").strip() or None,
            "cost": cost_entry,
            "credits": credit_entry,
            "files": {
                "pauta_filename": str(request_payload.get("pauta_filename") or "").strip() or None,
                "pdf_count": len(request_payload.get("pdf_filenames") or [])
                if isinstance(request_payload.get("pdf_filenames"), list)
                else 0,
                "pdf_filenames": request_payload.get("pdf_filenames")
                if isinstance(request_payload.get("pdf_filenames"), list)
                else [],
                "output_filename": os.path.basename(output_path) if output_path else None,
            },
        }
        payload["recent_runs"].append(run_item)

        if started_at:
            payload["logs"].append(
                {
                    "at": started_at,
                    "level": "info",
                    "event": "run_started",
                    "message": f"Inicio de ejecucion ({status}).",
                    "run_id": run_id,
                    "project_id": project_id,
                }
            )
        if finished_at:
            finished_level = "error" if status == "failed" else "info"
            finished_message = "Ejecucion finalizada correctamente."
            if status == "failed":
                finished_message = str(row.get("error_message") or "").strip() or "Ejecucion finalizada con error."
            elif status == "cancelled":
                finished_level = "warning"
                finished_message = "Ejecucion cancelada."

            payload["logs"].append(
                {
                    "at": finished_at,
                    "level": finished_level,
                    "event": "run_finished",
                    "message": finished_message,
                    "run_id": run_id,
                    "project_id": project_id,
                }
            )

        status_detail = str(task_payload.get("status_detail") or "").strip().lower()
        if status_detail in {"failed", "cancelled"}:
            detail_error = str(task_payload.get("error") or "").strip()
            if detail_error:
                payload["logs"].append(
                    {
                        "at": finished_at or started_at,
                        "level": "error" if status_detail == "failed" else "warning",
                        "event": "task_status_detail",
                        "message": detail_error,
                        "run_id": run_id,
                        "project_id": project_id,
                    }
                )

    normalized: dict[str, dict[str, Any]] = {}
    for user_id, payload in user_payload_map.items():
        summary = payload["summary"]
        duration_samples = int(summary.get("duration_samples") or 0)
        if duration_samples > 0:
            summary["avg_duration_seconds"] = round(
                float(summary.get("total_duration_seconds") or 0) / duration_samples,
                1,
            )
        summary.pop("duration_samples", None)

        cost_entries = []
        for currency, amount in (summary.get("cost_by_currency") or {}).items():
            amount_value = _coerce_float(amount)
            if amount_value is None:
                continue
            cost_entries.append(
                {
                    "currency": str(currency).upper(),
                    "amount": round(float(amount_value), 6),
                }
            )
        summary["cost_by_currency"] = sorted(
            cost_entries,
            key=lambda item: (item.get("currency") or ""),
        )

        project_breakdown = list((payload.get("project_breakdown") or {}).values())
        for project_item in project_breakdown:
            project_item["credits_total"] = int(project_item.get("credits_total") or 0)
            project_item["credits_refunded"] = int(project_item.get("credits_refunded") or 0)
            project_item["credits_net"] = int(project_item.get("credits_net") or 0)
            project_item["credited_runs"] = int(project_item.get("credited_runs") or 0)
            costs = []
            for currency, amount in (project_item.get("cost_by_currency") or {}).items():
                amount_value = _coerce_float(amount)
                if amount_value is None:
                    continue
                costs.append(
                    {
                        "currency": str(currency).upper(),
                        "amount": round(float(amount_value), 6),
                    }
                )
            project_item["cost_by_currency"] = sorted(
                costs,
                key=lambda item: (item.get("currency") or ""),
            )

        project_breakdown.sort(
            key=lambda item: (
                -int(item.get("runs_total") or 0),
                str(item.get("project_name") or "").lower(),
            )
        )

        recent_runs = sorted(
            payload.get("recent_runs") or [],
            key=lambda item: str(item.get("started_at") or ""),
            reverse=True,
        )[:25]
        logs = sorted(
            payload.get("logs") or [],
            key=lambda item: str(item.get("at") or ""),
            reverse=True,
        )[:40]

        normalized[user_id] = {
            "summary": summary,
            "project_breakdown": project_breakdown,
            "recent_runs": recent_runs,
            "logs": logs,
        }

    return normalized


async def admin_login_portal():
    login_path = os.path.join(STATIC_BASE_DIR, "admin_login.html")
    if not os.path.exists(login_path):
        raise HTTPException(
            status_code=500,
            detail="Admin login HTML file is missing in /static.",
        )
    return FileResponse(path=login_path, media_type="text/html; charset=utf-8")


async def admin_login(payload: AdminLoginPayload):
    email = (payload.email or "").strip().lower()
    password = payload.password or ""

    if not email:
        raise HTTPException(status_code=422, detail="Email is required.")
    if not password:
        raise HTTPException(status_code=422, detail="Password is required.")

    session = _exchange_email_password_for_access_token(
        email=email,
        password=password,
    )
    access_token = str(session.get("access_token") or "").strip()
    user = _resolve_authenticated_user(access_token)
    is_admin, granted_by = _is_global_admin_user(user)
    if not is_admin:
        raise HTTPException(
            status_code=403,
            detail="Authenticated user is not a global admin.",
        )

    return {
        "ok": True,
        "access_token": access_token,
        "refresh_token": str(session.get("refresh_token") or "").strip(),
        "token_type": str(session.get("token_type") or "bearer"),
        "expires_in": session.get("expires_in"),
        "expires_at": session.get("expires_at"),
        "user": {
            "id": str(user.get("id") or "").strip(),
            "email": str(user.get("email") or "").strip(),
            "granted_by": granted_by,
        },
        "redirect_to": "/admin/memberships",
    }


async def admin_refresh(payload: AdminRefreshPayload):
    refresh_token = str(payload.refresh_token or "").strip()
    if not refresh_token:
        raise HTTPException(status_code=422, detail="refresh_token is required.")

    session = _refresh_access_token(refresh_token=refresh_token)
    access_token = str(session.get("access_token") or "").strip()
    user = _resolve_authenticated_user(access_token)
    is_admin, granted_by = _is_global_admin_user(user)
    if not is_admin:
        raise HTTPException(
            status_code=403,
            detail="Authenticated user is not a global admin.",
        )

    return {
        "ok": True,
        "access_token": access_token,
        "refresh_token": str(session.get("refresh_token") or refresh_token).strip(),
        "token_type": str(session.get("token_type") or "bearer"),
        "expires_in": session.get("expires_in"),
        "expires_at": session.get("expires_at"),
        "user": {
            "id": str(user.get("id") or "").strip(),
            "email": str(user.get("email") or "").strip(),
            "granted_by": granted_by,
        },
    }


async def admin_memberships_panel(request: Request, tenant_id: str | None = None):
    """
    Minimal admin UI for tenant membership management.
    Access is restricted to global admins via Authorization: Bearer header.
    """
    if tenant_id:
        _normalize_required_uuid(tenant_id, "tenant_id")

    _authorize_global_admin(
        request=request,
    )
    panel_path = os.path.join(STATIC_BASE_DIR, "admin_memberships.html")
    if not os.path.exists(panel_path):
        raise HTTPException(
            status_code=500,
            detail="Admin panel HTML file is missing in /static.",
        )
    return FileResponse(path=panel_path, media_type="text/html; charset=utf-8")


async def get_admin_tenant_overview(request: Request, tenant_id: str | None = None):
    normalized_tenant_id: str | None = None
    if tenant_id and tenant_id.strip():
        normalized_tenant_id = _normalize_required_uuid(tenant_id, "tenant_id")
    actor = _authorize_global_admin(
        request=request,
    )
    if _RUN_REPOSITORY is None:
        raise HTTPException(
            status_code=503,
            detail="Supabase repository is not configured.",
        )

    managed_roles = _get_managed_membership_roles()
    tenant_admin_roles = _get_tenant_admin_roles()
    available_plans = _load_membership_plans_catalog()

    membership_rows: list[dict[str, Any]] = []
    membership_params = {
        "select": (
            "id,tenant_id,user_id,role,status,created_at,updated_at,"
            "tenants(id,name,products,created_at,metadata)"
        ),
        "order": "created_at.desc",
        "limit": "5000",
    }
    if normalized_tenant_id:
        membership_params["tenant_id"] = f"eq.{normalized_tenant_id}"
    try:
        _, membership_rows = _RUN_REPOSITORY._request(  # noqa: SLF001
            "GET",
            "/rest/v1/memberships",
            params=membership_params,
        )
    except Exception as primary_exc:
        fallback_params = {
            "select": "id,tenant_id,user_id,role,status,created_at,updated_at",
            "order": "created_at.desc",
            "limit": "5000",
        }
        if normalized_tenant_id:
            fallback_params["tenant_id"] = f"eq.{normalized_tenant_id}"
        try:
            _, membership_rows = _RUN_REPOSITORY._request(  # noqa: SLF001
                "GET",
                "/rest/v1/memberships",
                params=fallback_params,
            )
            print(
                "[warn] memberships overview loaded with fallback schema "
                f"(without tenants join): {primary_exc}"
            )
        except Exception as fallback_exc:
            raise HTTPException(
                status_code=503,
                detail=f"Could not load memberships overview: {fallback_exc}",
            ) from fallback_exc

    membership_rows = membership_rows or []
    tenants_map: dict[str, dict[str, Any]] = {}
    for row in membership_rows:
        tenant_id_from_row = str(row.get("tenant_id") or "").strip()
        if not tenant_id_from_row:
            continue
        tenant_node = row.get("tenants") if isinstance(row, dict) else None
        if not isinstance(tenant_node, dict):
            tenant_node = {}
        tenant_metadata = tenant_node.get("metadata")
        if not isinstance(tenant_metadata, dict):
            tenant_metadata = {}
        tenant_status = (
            str(tenant_metadata.get("status") or "").strip().lower()
            or str(tenant_metadata.get("state") or "").strip().lower()
            or "unknown"
        )
        tenants_map.setdefault(
            tenant_id_from_row,
            {
                "tenant_id": tenant_id_from_row,
                "name": str(tenant_node.get("name") or "").strip() or tenant_id_from_row,
                "status": tenant_status,
                "products": tenant_node.get("products") if isinstance(tenant_node.get("products"), list) else [],
                "created_at": tenant_node.get("created_at"),
                "metadata": tenant_metadata,
            },
        )

    try:
        tenant_params = {
            "select": "id,name,products,created_at,metadata",
            "order": "created_at.desc",
            "limit": "2000",
        }
        if normalized_tenant_id:
            tenant_params["id"] = f"eq.{normalized_tenant_id}"
        _, tenant_rows = _RUN_REPOSITORY._request(  # noqa: SLF001
            "GET",
            "/rest/v1/tenants",
            params=tenant_params,
            allow_not_found=True,
        )
        for tenant in (tenant_rows or []):
            tid = str(tenant.get("id") or "").strip()
            if not tid:
                continue
            tenant_metadata = tenant.get("metadata")
            if not isinstance(tenant_metadata, dict):
                tenant_metadata = {}
            tenant_status = (
                str(tenant_metadata.get("status") or "").strip().lower()
                or str(tenant_metadata.get("state") or "").strip().lower()
                or "unknown"
            )
            tenants_map[tid] = {
                "tenant_id": tid,
                "name": str(tenant.get("name") or "").strip() or tid,
                "status": tenant_status,
                "products": tenant.get("products") if isinstance(tenant.get("products"), list) else [],
                "created_at": tenant.get("created_at"),
                "metadata": tenant_metadata,
            }
    except Exception as exc:
        print(f"[warn] Could not load tenants catalog for admin overview: {exc}")

    subscriptions_by_tenant = _load_tenant_subscriptions_map(tenant_id=normalized_tenant_id)
    for tenant_key, plans in subscriptions_by_tenant.items():
        tenant_meta = tenants_map.setdefault(
            tenant_key,
            {
                "tenant_id": tenant_key,
                "name": tenant_key,
                "status": "unknown",
                "products": [],
                "created_at": None,
                "metadata": {},
            },
        )
        tenant_meta["plans"] = plans
        tenant_meta["products"] = [str(plan.get("plan_key") or "").strip() for plan in plans if str(plan.get("plan_key") or "").strip()]

    user_ids = {
        str(row.get("user_id") or "").strip()
        for row in membership_rows
        if str(row.get("user_id") or "").strip()
    }
    users_map = _fetch_auth_users_map(user_ids)
    run_metrics_by_tenant_user, run_metrics_by_tenant, total_runs = _build_run_metrics_maps(
        normalized_tenant_id
    )
    include_user_activity = bool(normalized_tenant_id)
    user_activity_by_user_id = (
        _build_user_activity_by_user_for_tenant(normalized_tenant_id)
        if include_user_activity and normalized_tenant_id
        else {}
    )

    users: list[dict[str, Any]] = []
    tenant_aggregates: dict[str, dict[str, Any]] = {}

    for tid, meta in tenants_map.items():
        tenant_aggregates[tid] = {
            "tenant_id": tid,
            "name": meta["name"],
            "status": meta["status"],
            "products": meta.get("products") or [],
            "plans": meta.get("plans") or [],
            "created_at": meta.get("created_at"),
            "metadata": meta.get("metadata") if isinstance(meta.get("metadata"), dict) else {},
            "billing_config": normalize_tenant_billing_config(
                (meta.get("metadata") or {}).get("billing_config")
                if isinstance(meta.get("metadata"), dict)
                else {}
            ),
            "memberships_total": 0,
            "active_memberships": 0,
            "admin_memberships": 0,
            "runs_total": 0,
            "runs_completed": 0,
            "runs_failed": 0,
            "last_run_at": None,
            "_users": set(),
        }

    for row in membership_rows:
        tenant_id_from_row = str(row.get("tenant_id") or "").strip()
        user_id = str(row.get("user_id") or "").strip()
        role = str(row.get("role") or "").strip().lower()
        status = str(row.get("status") or "").strip().lower()
        profile = users_map.get(user_id, {})
        metrics = run_metrics_by_tenant_user.get(
            (tenant_id_from_row, user_id),
            {
                "runs_total": 0,
                "runs_completed": 0,
                "runs_failed": 0,
                "last_run_at": None,
            },
        )
        tenant_meta = tenants_map.get(
            tenant_id_from_row,
            {
                "tenant_id": tenant_id_from_row,
                "name": tenant_id_from_row,
                "status": "unknown",
                "products": [],
                "plans": [],
                "created_at": None,
                "metadata": {},
            },
        )
        aggregate = tenant_aggregates.setdefault(
            tenant_id_from_row,
            {
                "tenant_id": tenant_id_from_row,
                "name": tenant_meta["name"],
                "status": tenant_meta["status"],
                "products": tenant_meta.get("products") or [],
                "plans": tenant_meta.get("plans") or [],
                "created_at": tenant_meta.get("created_at"),
                "metadata": tenant_meta.get("metadata") if isinstance(tenant_meta.get("metadata"), dict) else {},
                "billing_config": normalize_tenant_billing_config(
                    (tenant_meta.get("metadata") or {}).get("billing_config")
                    if isinstance(tenant_meta.get("metadata"), dict)
                    else {}
                ),
                "memberships_total": 0,
                "active_memberships": 0,
                "admin_memberships": 0,
                "runs_total": 0,
                "runs_completed": 0,
                "runs_failed": 0,
                "last_run_at": None,
                "_users": set(),
            },
        )
        aggregate["memberships_total"] += 1
        if status == "active":
            aggregate["active_memberships"] += 1
        if role in tenant_admin_roles:
            aggregate["admin_memberships"] += 1
        if user_id:
            aggregate["_users"].add(user_id)

        user_item = {
            "membership_id": row.get("id"),
            "tenant_id": tenant_id_from_row,
            "tenant_name": tenant_meta["name"],
            "user_id": user_id,
            "email": profile.get("email", ""),
            "display_name": profile.get("display_name", ""),
            "membership": {
                "role": role,
                "status": status,
                "is_admin": role in tenant_admin_roles,
                "enabled": status == "active",
                "created_at": row.get("created_at"),
                "updated_at": row.get("updated_at"),
            },
            "metrics": metrics,
        }
        if include_user_activity:
            user_activity = user_activity_by_user_id.get(user_id) or {
                "summary": {
                    "runs_total": int(metrics.get("runs_total") or 0),
                    "runs_completed": int(metrics.get("runs_completed") or 0),
                    "runs_failed": int(metrics.get("runs_failed") or 0),
                    "runs_running": 0,
                    "first_run_at": None,
                    "last_run_at": metrics.get("last_run_at"),
                    "total_duration_seconds": 0,
                    "avg_duration_seconds": None,
                    "priced_runs": 0,
                    "cost_by_currency": [],
                    "credits_total": 0,
                    "credits_refunded": 0,
                    "credits_net": 0,
                    "credited_runs": 0,
                },
                "project_breakdown": [],
                "recent_runs": [],
                "logs": [],
            }
            summary = user_activity.get("summary") if isinstance(user_activity.get("summary"), dict) else {}
            user_item["membership_breakdown"] = [
                {
                    "membership_id": row.get("id"),
                    "role": role,
                    "status": status,
                    "runs_total": int(summary.get("runs_total") or 0),
                    "runs_completed": int(summary.get("runs_completed") or 0),
                    "runs_failed": int(summary.get("runs_failed") or 0),
                    "runs_running": int(summary.get("runs_running") or 0),
                    "priced_runs": int(summary.get("priced_runs") or 0),
                    "credits_total": int(summary.get("credits_total") or 0),
                    "credits_refunded": int(summary.get("credits_refunded") or 0),
                    "credits_net": int(summary.get("credits_net") or 0),
                    "credited_runs": int(summary.get("credited_runs") or 0),
                    "cost_by_currency": summary.get("cost_by_currency")
                    if isinstance(summary.get("cost_by_currency"), list)
                    else [],
                }
            ]
            user_item["activity"] = user_activity

        users.append(user_item)

    for tid, run_metrics in run_metrics_by_tenant.items():
        aggregate = tenant_aggregates.setdefault(
            tid,
            {
                "tenant_id": tid,
                "name": tenants_map.get(tid, {}).get("name", tid),
                "status": tenants_map.get(tid, {}).get("status", "unknown"),
                "products": tenants_map.get(tid, {}).get("products", []),
                "plans": tenants_map.get(tid, {}).get("plans", []),
                "created_at": tenants_map.get(tid, {}).get("created_at"),
                "metadata": tenants_map.get(tid, {}).get("metadata", {}),
                "billing_config": normalize_tenant_billing_config(
                    (tenants_map.get(tid, {}).get("metadata") or {}).get("billing_config")
                    if isinstance(tenants_map.get(tid, {}).get("metadata"), dict)
                    else {}
                ),
                "memberships_total": 0,
                "active_memberships": 0,
                "admin_memberships": 0,
                "runs_total": 0,
                "runs_completed": 0,
                "runs_failed": 0,
                "last_run_at": None,
                "_users": set(),
            },
        )
        aggregate["runs_total"] = run_metrics.get("runs_total", 0)
        aggregate["runs_completed"] = run_metrics.get("runs_completed", 0)
        aggregate["runs_failed"] = run_metrics.get("runs_failed", 0)
        aggregate["last_run_at"] = run_metrics.get("last_run_at")

    users.sort(
        key=lambda item: (
            (item.get("tenant_name") or "").lower(),
            item["membership"]["status"] != "active",
            (item.get("email") or "").lower(),
            item.get("user_id") or "",
        )
    )

    tenants: list[dict[str, Any]] = []
    for aggregate in tenant_aggregates.values():
        users_count = len(aggregate.pop("_users", set()))
        aggregate["users_total"] = users_count
        tenants.append(aggregate)
    tenants.sort(key=lambda t: ((t.get("name") or "").lower(), t.get("tenant_id") or ""))

    active_count = sum(1 for item in users if item["membership"]["enabled"])
    admin_count = sum(1 for item in users if item["membership"]["is_admin"])
    users_with_runs = sum(1 for item in users if item["metrics"]["runs_total"] > 0)
    active_plan_links = sum(len(tenant.get("plans") or []) for tenant in tenants)
    tenant_ids = [str(item.get("tenant_id") or "").strip() for item in tenants if str(item.get("tenant_id") or "").strip()]
    credit_balances_by_tenant = _collect_credit_balances_by_tenant(
        tenant_ids=tenant_ids,
        actor_user_id=str(actor.get("id") or "").strip() or None,
    )
    for tenant in tenants:
        tid = str(tenant.get("tenant_id") or "").strip()
        credit_info = credit_balances_by_tenant.get(tid) or {}
        tenant["credits_balance"] = credit_info.get("balance")
        tenant["credits_enabled"] = bool(credit_info.get("enabled"))
        tenant["credits_monthly_granted_now"] = int(credit_info.get("monthly_granted_now") or 0)
        tenant["client_badge_visible"] = bool(credit_info.get("visible"))
        tenant["billing_kind"] = str(credit_info.get("billing_kind") or "none")
        tenant["quota"] = credit_info.get("quota") if isinstance(credit_info.get("quota"), dict) else {
            "apps": [],
            "total_executions_remaining": 0,
            "total_reruns_remaining": 0,
        }
        tenant["billing_config"] = normalize_tenant_billing_config(
            credit_info.get("billing_config")
            if isinstance(credit_info.get("billing_config"), dict)
            else tenant.get("billing_config")
        )

    credit_balances = [
        int(value.get("balance") or 0)
        for value in credit_balances_by_tenant.values()
        if isinstance(value, dict) and isinstance(value.get("balance"), int)
    ]
    credit_policy = (
        _build_credit_policy_for_tenant_activity(user_activity_by_user_id)
        if include_user_activity
        else None
    )

    return {
        "tenant_id": normalized_tenant_id,
        "scope_tenant_id": normalized_tenant_id,
        "generated_at": datetime.now().isoformat(),
        "actor": actor,
        "summary": {
            "tenants_total": len(tenants),
            "memberships_total": len(users),
            "active_memberships": active_count,
            "admin_memberships": admin_count,
            "runs_total": total_runs,
            "users_with_runs": users_with_runs,
            "active_plan_links": active_plan_links,
            "credits_balance_total": sum(credit_balances),
        },
        "available_roles": managed_roles,
        "tenant_admin_roles": sorted(tenant_admin_roles),
        "admin_roles": sorted(tenant_admin_roles),
        "available_plans": available_plans,
        "credit_policy": credit_policy,
        "tenants": tenants,
        "users": users,
    }


async def get_admin_run_artifacts(
    request: Request,
    run_id: str,
    tenant_id: str | None = None,
):
    normalized_run_id = _normalize_required_uuid(run_id, "run_id")
    normalized_tenant_id = (
        _normalize_required_uuid(tenant_id, "tenant_id")
        if tenant_id and tenant_id.strip()
        else None
    )
    _authorize_global_admin(request=request)
    if _RUN_REPOSITORY is None:
        raise HTTPException(status_code=503, detail="Supabase repository is not configured.")

    run_row = _RUN_REPOSITORY.get_budget_run_by_id(normalized_run_id)
    if not run_row:
        raise HTTPException(status_code=404, detail="Run not found.")

    run_tenant_id = str(run_row.get("tenant_id") or "").strip()
    if normalized_tenant_id and run_tenant_id != normalized_tenant_id:
        raise HTTPException(status_code=403, detail="Run does not belong to the requested tenant.")

    extraction_ids = _coerce_extraction_ids(run_row.get("result_payload"))
    extraction_rows = _load_extractions_by_ids(extraction_ids)
    artifacts = _build_admin_run_artifact_items(
        run_id=normalized_run_id,
        run_tenant_id=run_tenant_id,
        extraction_rows=extraction_rows,
    )

    return {
        "run_id": normalized_run_id,
        "tenant_id": run_tenant_id,
        "project_id": str(run_row.get("project_id") or "").strip() or None,
        "artifacts_count": len(artifacts),
        "artifacts": artifacts,
    }


async def download_admin_run_artifact(
    request: Request,
    run_id: str,
    extraction_id: str,
    artifact_class: str = Query(..., pattern="^(canonical|trace|debug)$"),
    artifact_key: str = Query(..., min_length=1),
    tenant_id: str | None = None,
):
    normalized_run_id = _normalize_required_uuid(run_id, "run_id")
    normalized_extraction_id = _normalize_required_uuid(extraction_id, "extraction_id")
    normalized_tenant_id = (
        _normalize_required_uuid(tenant_id, "tenant_id")
        if tenant_id and tenant_id.strip()
        else None
    )
    normalized_class = str(artifact_class or "").strip().lower()
    normalized_key = str(artifact_key or "").strip()
    if not normalized_key:
        raise HTTPException(status_code=422, detail="artifact_key is required.")

    _authorize_global_admin(request=request)
    if _RUN_REPOSITORY is None:
        raise HTTPException(status_code=503, detail="Supabase repository is not configured.")

    run_row = _RUN_REPOSITORY.get_budget_run_by_id(normalized_run_id)
    if not run_row:
        raise HTTPException(status_code=404, detail="Run not found.")

    run_tenant_id = str(run_row.get("tenant_id") or "").strip()
    if normalized_tenant_id and run_tenant_id != normalized_tenant_id:
        raise HTTPException(status_code=403, detail="Run does not belong to the requested tenant.")

    extraction_rows = _load_extractions_by_ids([normalized_extraction_id])
    extraction_row = extraction_rows[0] if extraction_rows else None
    if not extraction_row:
        raise HTTPException(status_code=404, detail="Extraction not found.")
    extraction_run_id = str(extraction_row.get("run_id") or "").strip()
    if extraction_run_id and extraction_run_id != normalized_run_id:
        raise HTTPException(status_code=403, detail="Extraction does not belong to requested run.")

    raw_payload = extraction_row.get("raw_payload") if isinstance(extraction_row.get("raw_payload"), dict) else {}
    artifacts = raw_payload.get("artifacts") if isinstance(raw_payload.get("artifacts"), dict) else {}
    class_payload = artifacts.get(normalized_class)
    if not isinstance(class_payload, dict):
        raise HTTPException(status_code=404, detail="Artifact class not available in extraction.")
    artifact_ref = class_payload.get(normalized_key)
    if not isinstance(artifact_ref, dict):
        raise HTTPException(status_code=404, detail="Artifact key not available in extraction.")

    bucket = str(artifact_ref.get("bucket") or "").strip()
    object_path = str(artifact_ref.get("path") or "").strip().strip("/")
    if not bucket or not object_path:
        raise HTTPException(status_code=404, detail="Artifact storage reference is missing.")

    try:
        blob = _RUN_REPOSITORY.download_bytes(bucket=bucket, object_path=object_path)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=f"Could not download artifact from storage: {exc}") from exc

    source_pdf = str(raw_payload.get("source_pdf") or "").strip()
    source_prefix = os.path.splitext(_safe_filename(source_pdf, max_len=60))[0] if source_pdf else "artifact"
    artifact_file_name = os.path.basename(object_path) or f"{normalized_key}.json"
    download_name = _safe_filename(
        f"{source_prefix}_{normalized_key}_{artifact_file_name}",
        max_len=150,
    )
    return Response(
        content=blob,
        media_type="application/json",
        headers={
            "Content-Disposition": f'attachment; filename="{download_name}"',
        },
    )


async def patch_membership_status(payload: MembershipStatusPatch, request: Request):
    normalized_tenant_id = _normalize_required_uuid(payload.tenant_id, "tenant_id")
    normalized_user_id = _normalize_required_uuid(payload.user_id, "user_id")
    actor = _authorize_global_admin(
        request=request,
    )

    if _RUN_REPOSITORY is None:
        raise HTTPException(
            status_code=503,
            detail="Supabase repository is not configured.",
        )

    managed_roles = _get_managed_membership_roles()
    default_role = _resolve_default_membership_role(managed_roles)
    desired_status = "active" if payload.enabled else DISABLED_MEMBERSHIP_STATUS
    existing_membership = _get_membership_row(
        tenant_id=normalized_tenant_id,
        user_id=normalized_user_id,
    )

    updated_membership: dict[str, Any] | None = None

    try:
        if existing_membership is None:
            if not payload.enabled:
                raise HTTPException(
                    status_code=404,
                    detail="Membership not found for tenant_id/user_id.",
                )

            create_payload = {
                "tenant_id": normalized_tenant_id,
                "user_id": normalized_user_id,
                "role": default_role,
                "status": "active",
            }
            _, created_rows = _RUN_REPOSITORY._request(  # noqa: SLF001
                "POST",
                "/rest/v1/memberships",
                payload=create_payload,
                prefer="return=representation",
            )
            updated_membership = created_rows[0] if created_rows else None
        else:
            _, updated_rows = _RUN_REPOSITORY._request(  # noqa: SLF001
                "PATCH",
                "/rest/v1/memberships",
                params={
                    "tenant_id": f"eq.{normalized_tenant_id}",
                    "user_id": f"eq.{normalized_user_id}",
                },
                payload={"status": desired_status},
                prefer="return=representation",
            )
            updated_membership = updated_rows[0] if updated_rows else None
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Could not update membership status in Supabase: {exc}",
        ) from exc

    if updated_membership is None:
        updated_membership = _get_membership_row(
            tenant_id=normalized_tenant_id,
            user_id=normalized_user_id,
        )

    print(
        "[admin] membership status updated "
        f"actor={actor['id']} tenant={normalized_tenant_id} "
        f"user={normalized_user_id} status={desired_status}"
    )

    return {
        "ok": True,
        "tenant_id": normalized_tenant_id,
        "user_id": normalized_user_id,
        "status": desired_status,
        "membership": updated_membership,
    }


async def patch_membership_role(payload: MembershipRolePatch, request: Request):
    normalized_tenant_id = _normalize_required_uuid(payload.tenant_id, "tenant_id")
    normalized_user_id = _normalize_required_uuid(payload.user_id, "user_id")
    normalized_role = (payload.role or "").strip().lower()
    managed_roles = _get_managed_membership_roles()
    if normalized_role not in managed_roles:
        raise HTTPException(
            status_code=422,
            detail=(
                "Invalid role. Allowed roles: "
                f"{', '.join(managed_roles)}"
            ),
        )

    actor = _authorize_global_admin(
        request=request,
    )
    if _RUN_REPOSITORY is None:
        raise HTTPException(
            status_code=503,
            detail="Supabase repository is not configured.",
        )

    existing_membership = _get_membership_row(
        tenant_id=normalized_tenant_id,
        user_id=normalized_user_id,
    )
    if existing_membership is None:
        raise HTTPException(
            status_code=404,
            detail="Membership not found for tenant_id/user_id.",
        )

    try:
        _, updated_rows = _RUN_REPOSITORY._request(  # noqa: SLF001
            "PATCH",
            "/rest/v1/memberships",
            params={
                "tenant_id": f"eq.{normalized_tenant_id}",
                "user_id": f"eq.{normalized_user_id}",
            },
            payload={"role": normalized_role},
            prefer="return=representation",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Could not update membership role in Supabase: {exc}",
        ) from exc

    updated_membership = updated_rows[0] if updated_rows else _get_membership_row(
        tenant_id=normalized_tenant_id,
        user_id=normalized_user_id,
    )

    print(
        "[admin] membership role updated "
        f"actor={actor['id']} tenant={normalized_tenant_id} "
        f"user={normalized_user_id} role={normalized_role}"
    )

    return {
        "ok": True,
        "tenant_id": normalized_tenant_id,
        "user_id": normalized_user_id,
        "role": normalized_role,
        "membership": updated_membership,
    }


# ─── Health Check ─────────────────────────────────────────────────────────────
async def delete_membership(payload: MembershipDeletePayload, request: Request):
    normalized_tenant_id = _normalize_required_uuid(payload.tenant_id, "tenant_id")
    normalized_user_id = _normalize_required_uuid(payload.user_id, "user_id")
    actor = _authorize_global_admin(
        request=request,
    )
    if _RUN_REPOSITORY is None:
        raise HTTPException(
            status_code=503,
            detail="Supabase repository is not configured.",
        )

    existing_membership = _get_membership_row(
        tenant_id=normalized_tenant_id,
        user_id=normalized_user_id,
    )
    if existing_membership is None:
        raise HTTPException(
            status_code=404,
            detail="Membership not found for tenant_id/user_id.",
        )

    try:
        _RUN_REPOSITORY._request(  # noqa: SLF001
            "DELETE",
            "/rest/v1/memberships",
            params={
                "tenant_id": f"eq.{normalized_tenant_id}",
                "user_id": f"eq.{normalized_user_id}",
            },
            prefer="return=minimal",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Could not delete membership in Supabase: {exc}",
        ) from exc

    print(
        "[admin] membership deleted "
        f"actor={actor['id']} tenant={normalized_tenant_id} user={normalized_user_id}"
    )
    return {
        "ok": True,
        "deleted": True,
        "tenant_id": normalized_tenant_id,
        "user_id": normalized_user_id,
    }


async def patch_tenant_subscription_status(
    payload: TenantSubscriptionStatusPatch,
    request: Request,
):
    normalized_tenant_id = _normalize_required_uuid(payload.tenant_id, "tenant_id")
    normalized_plan_key = str(payload.plan_key or "").strip().lower()
    if not normalized_plan_key:
        raise HTTPException(
            status_code=422,
            detail="plan_key is required.",
        )

    actor = _authorize_global_admin(
        request=request,
    )
    if _RUN_REPOSITORY is None:
        raise HTTPException(
            status_code=503,
            detail="Supabase repository is not configured.",
        )

    available_plans = {item["plan_key"] for item in _load_membership_plans_catalog()}
    if normalized_plan_key not in available_plans:
        raise HTTPException(
            status_code=404,
            detail=f"Plan not found or inactive: {normalized_plan_key}",
        )

    desired_status = "active" if payload.enabled else "cancelled"
    timestamp_now = datetime.now().isoformat()

    try:
        _, existing_rows = _RUN_REPOSITORY._request(  # noqa: SLF001
            "GET",
            "/rest/v1/tenant_subscriptions",
            params={
                "select": "id,tenant_id,plan_key,status,starts_at,ends_at,updated_at",
                "tenant_id": f"eq.{normalized_tenant_id}",
                "plan_key": f"eq.{normalized_plan_key}",
                "limit": "1",
            },
            allow_not_found=True,
        )

        existing = existing_rows[0] if existing_rows else None

        if payload.enabled and existing is None:
            _RUN_REPOSITORY._request(  # noqa: SLF001
                "POST",
                "/rest/v1/tenant_subscriptions",
                payload={
                    "tenant_id": normalized_tenant_id,
                    "plan_key": normalized_plan_key,
                    "status": "active",
                    "starts_at": timestamp_now,
                    "created_by": actor.get("id") or None,
                },
                prefer="return=representation",
            )
        elif existing is not None:
            _RUN_REPOSITORY._request(  # noqa: SLF001
                "PATCH",
                "/rest/v1/tenant_subscriptions",
                params={
                    "tenant_id": f"eq.{normalized_tenant_id}",
                    "plan_key": f"eq.{normalized_plan_key}",
                },
                payload={
                    "status": desired_status,
                    "ends_at": None if payload.enabled else timestamp_now,
                },
                prefer="return=representation",
            )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Could not update tenant subscription in Supabase: {exc}",
        ) from exc

    updated_map = _load_tenant_subscriptions_map(tenant_id=normalized_tenant_id)
    active_plans = updated_map.get(normalized_tenant_id, [])

    print(
        "[admin] tenant subscription updated "
        f"actor={actor['id']} tenant={normalized_tenant_id} "
        f"plan={normalized_plan_key} status={desired_status}"
    )

    return {
        "ok": True,
        "tenant_id": normalized_tenant_id,
        "plan_key": normalized_plan_key,
        "enabled": payload.enabled,
        "status": desired_status,
        "active_plans": active_plans,
    }


async def patch_tenant_credits_adjust(
    payload: TenantCreditsAdjustPatch,
    request: Request,
):
    normalized_tenant_id = _normalize_required_uuid(payload.tenant_id, "tenant_id")
    actor = _authorize_global_admin(request=request)
    if _RUN_REPOSITORY is None:
        raise HTTPException(
            status_code=503,
            detail="Supabase repository is not configured.",
        )

    delta_credits = int(payload.delta_credits or 0)
    if delta_credits == 0:
        raise HTTPException(status_code=422, detail="delta_credits must be non-zero.")

    reason = str(payload.reason or "").strip() or "admin_manual_adjustment"
    reference_id = datetime.now().strftime("%Y%m%d%H%M%S")

    try:
        adjustment = adjust_tenant_credits(
            repo=_RUN_REPOSITORY,
            tenant_id=normalized_tenant_id,
            delta_credits=delta_credits,
            actor_user_id=str(actor.get("id") or "").strip() or None,
            reason=reason,
            reference_id=reference_id,
        )
        balance_info = get_tenant_credit_balance(
            repo=_RUN_REPOSITORY,
            tenant_id=normalized_tenant_id,
            actor_user_id=str(actor.get("id") or "").strip() or None,
        )
    except CreditBillingNotInitializedError as billing_exc:
        raise HTTPException(status_code=503, detail=str(billing_exc)) from billing_exc
    except CreditBalanceError as credit_exc:
        raise HTTPException(status_code=422, detail=str(credit_exc)) from credit_exc
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Could not adjust tenant credits in Supabase: {exc}",
        ) from exc

    print(
        "[admin] tenant credits adjusted "
        f"actor={actor['id']} tenant={normalized_tenant_id} "
        f"delta={delta_credits} reason={reason}"
    )

    return {
        "ok": True,
        "tenant_id": normalized_tenant_id,
        "delta_credits": delta_credits,
        "reason": reason,
        "adjustment": adjustment,
        "credit_balance": balance_info.get("balance"),
        "credit_state": balance_info,
    }


async def patch_tenant_billing_config(
    payload: TenantBillingConfigPatch,
    request: Request,
):
    normalized_tenant_id = _normalize_required_uuid(payload.tenant_id, "tenant_id")
    actor = _authorize_global_admin(request=request)
    if _RUN_REPOSITORY is None:
        raise HTTPException(
            status_code=503,
            detail="Supabase repository is not configured.",
        )

    try:
        current_config = get_tenant_billing_config(
            repo=_RUN_REPOSITORY,
            tenant_id=normalized_tenant_id,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Could not load tenant billing config: {exc}",
        ) from exc

    next_config = normalize_tenant_billing_config(current_config)
    if payload.show_client_badge is not None:
        next_config["show_client_badge"] = bool(payload.show_client_badge)
    if payload.use_credit_plan is not None:
        next_config["use_credit_plan"] = bool(payload.use_credit_plan)
    if payload.use_custom_plan is not None:
        next_config["use_custom_plan"] = bool(payload.use_custom_plan)

    if payload.custom_plan_apps is not None:
        existing_apps = (
            current_config.get("custom_plan", {}).get("apps", {})
            if isinstance(current_config.get("custom_plan"), dict)
            else {}
        )
        if not isinstance(existing_apps, dict):
            existing_apps = {}
        next_apps: dict[str, dict[str, int]] = {}
        for app_payload in payload.custom_plan_apps:
            app_key = normalize_app_key(app_payload.app_key)
            if not app_key:
                raise HTTPException(status_code=422, detail="Each custom_plan_apps item requires app_key.")
            existing_usage = existing_apps.get(app_key) if isinstance(existing_apps.get(app_key), dict) else {}
            next_apps[app_key] = {
                "executions_limit": max(0, int(app_payload.executions_limit)),
                "reruns_limit": max(0, int(app_payload.reruns_limit)),
                "executions_used": max(0, int(existing_usage.get("executions_used") or 0)),
                "reruns_used": max(0, int(existing_usage.get("reruns_used") or 0)),
            }
        custom_plan = next_config.get("custom_plan") if isinstance(next_config.get("custom_plan"), dict) else {}
        if not isinstance(custom_plan.get("idempotency"), dict):
            custom_plan["idempotency"] = {}
        custom_plan["apps"] = next_apps
        next_config["custom_plan"] = custom_plan

    if bool(next_config.get("use_credit_plan")) and bool(next_config.get("use_custom_plan")):
        raise HTTPException(
            status_code=422,
            detail="use_credit_plan and use_custom_plan cannot both be true.",
        )

    try:
        updated_config = set_tenant_billing_config(
            repo=_RUN_REPOSITORY,
            tenant_id=normalized_tenant_id,
            billing_config=next_config,
        )
        balance_info = get_tenant_credit_balance(
            repo=_RUN_REPOSITORY,
            tenant_id=normalized_tenant_id,
            actor_user_id=str(actor.get("id") or "").strip() or None,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Could not update tenant billing config: {exc}",
        ) from exc

    print(
        "[admin] tenant billing config updated "
        f"actor={actor['id']} tenant={normalized_tenant_id} "
        f"show_badge={updated_config.get('show_client_badge')} "
        f"credit_plan={updated_config.get('use_credit_plan')} "
        f"custom_plan={updated_config.get('use_custom_plan')}"
    )

    return {
        "ok": True,
        "tenant_id": normalized_tenant_id,
        "billing_config": updated_config,
        "credit_state": balance_info,
    }



