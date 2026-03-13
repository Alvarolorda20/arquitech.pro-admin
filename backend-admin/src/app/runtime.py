"""Legacy runtime services for Budget Comparator API."""

import os
import re
import sys
import json
import hashlib
import shutil
import time
import uuid
import asyncio
import tempfile
import traceback
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional
from concurrent.futures import ThreadPoolExecutor as _ThreadPoolExecutor

from src.shared.observability.logger import get_logger as _get_logger

_logger = _get_logger(__name__)

# ─── Force UTF-8 stdout/stderr so accented filenames in print() never crash ───
# Windows defaults to cp1252 which cannot encode characters like É, Ñ, etc.
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import requests
from fastapi import File, UploadFile, Form, HTTPException, Request
from fastapi.responses import FileResponse, Response
from dotenv import load_dotenv
from pydantic import BaseModel

# ─── Add src/ to path so all existing agents/utils imports resolve ────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SRC_PATH = os.path.join(PROJECT_ROOT, "src")
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

try:
    from src.features.runs.infrastructure.supabase_run_repository import SupabaseRunRepository
except Exception as _repo_import_err:
    # Fallback: try top-level import for backwards compatibility at runtime
    try:
        from persistence_run_repository import SupabaseRunRepository
    except Exception:
        SupabaseRunRepository = None  # type: ignore[assignment]
        _logger.warning("Supabase repository unavailable: %s", _repo_import_err)
    
load_dotenv()

# Base Directories
TEMP_BASE_DIR = os.path.abspath(
    os.environ.get(
        "WORKSPACE_TMP_DIR",
        os.path.join(PROJECT_ROOT, "temp_processing"),
    )
)
STATIC_BASE_DIR = os.path.join(PROJECT_ROOT, "static")
os.makedirs(TEMP_BASE_DIR, exist_ok=True)

# Job Store
# In-memory dict mapping job_id -> job state.  Persists for the lifetime of
# the process; sufficient for single-server / single-worker deployments.
JOBS: dict[str, dict] = {}


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _env_csv_set(name: str, default: str) -> set[str]:
    raw = os.environ.get(name, default)
    values = {
        item.strip().lower()
        for item in raw.split(",")
        if item and item.strip()
    }
    return values


SUPABASE_REQUIRE_PERSISTENCE: bool = _env_bool("SUPABASE_REQUIRE_PERSISTENCE", True)
SUPABASE_ENFORCE_USER_AUTH: bool = _env_bool("SUPABASE_ENFORCE_USER_AUTH", True)
SUPABASE_PERSIST_TRACE_ARTIFACTS: bool = _env_bool("SUPABASE_PERSIST_TRACE_ARTIFACTS", False)
SUPABASE_PERSIST_DEBUG_ARTIFACTS: bool = _env_bool("SUPABASE_PERSIST_DEBUG_ARTIFACTS", False)
SUPABASE_RETENTION_CANONICAL_DAYS: int = int(
    os.environ.get("SUPABASE_RETENTION_CANONICAL_DAYS", "3650")
)
SUPABASE_RETENTION_TRACE_DAYS: int = int(
    os.environ.get("SUPABASE_RETENTION_TRACE_DAYS", "60")
)
SUPABASE_RETENTION_DEBUG_DAYS: int = int(
    os.environ.get("SUPABASE_RETENTION_DEBUG_DAYS", "14")
)

USER_START_FAILURE_MESSAGE = (
    "No se pudo iniciar la comparativa. Intentalo de nuevo en unos minutos."
)
USER_RUN_FAILURE_MESSAGE = (
    "No se pudo completar la comparativa. Intentalo de nuevo."
)
USER_OUTPUT_FAILURE_MESSAGE = (
    "No se pudo generar el archivo final de la comparativa. Intentalo de nuevo."
)
ORPHAN_RUN_FAILURE_MESSAGE = (
    "Ejecucion interrumpida o huerfana; marcada como fallida automaticamente."
)
ORPHAN_RECONCILE_MINUTES: int = max(
    1,
    int(os.environ.get("ORPHAN_RECONCILE_MINUTES", "10")),
)
TEMP_WORKSPACE_TTL_HOURS: int = max(
    1,
    int(os.environ.get("TEMP_WORKSPACE_TTL_HOURS", "24")),
)
UPLOAD_IO_CHUNK_BYTES: int = max(
    64 * 1024,
    int(os.environ.get("UPLOAD_IO_CHUNK_BYTES", str(1024 * 1024))),
)
MAX_PAUTA_UPLOAD_BYTES: int = max(
    1_000_000,
    int(os.environ.get("MAX_PAUTA_UPLOAD_BYTES", str(50 * 1024 * 1024))),
)
MAX_PDF_UPLOAD_BYTES: int = max(
    1_000_000,
    int(os.environ.get("MAX_PDF_UPLOAD_BYTES", str(100 * 1024 * 1024))),
)
MAX_TOTAL_UPLOAD_BYTES: int = max(
    1_000_000,
    int(os.environ.get("MAX_TOTAL_UPLOAD_BYTES", str(400 * 1024 * 1024))),
)

_RUN_REPOSITORY = (
    SupabaseRunRepository.from_env()
    if SupabaseRunRepository is not None
    else None
)
TENANT_ADMIN_MEMBERSHIP_ROLES: set[str] = _env_csv_set(
    "TENANT_ADMIN_MEMBERSHIP_ROLES",
    os.environ.get("ADMIN_ALLOWED_MEMBERSHIP_ROLES", "owner,admin"),
)
if not TENANT_ADMIN_MEMBERSHIP_ROLES:
    TENANT_ADMIN_MEMBERSHIP_ROLES = {"owner", "admin"}

GLOBAL_ADMIN_USER_IDS: set[str] = _env_csv_set("GLOBAL_ADMIN_USER_IDS", "")
GLOBAL_ADMIN_EMAILS: set[str] = _env_csv_set("GLOBAL_ADMIN_EMAILS", "")
GLOBAL_ADMIN_ALLOWED_ROLES: set[str] = _env_csv_set(
    "GLOBAL_ADMIN_ALLOWED_ROLES",
    "global_admin,super_admin",
)
GLOBAL_ADMIN_METADATA_FLAGS: set[str] = _env_csv_set(
    "GLOBAL_ADMIN_METADATA_FLAGS",
    "global_admin,is_global_admin",
)
if not GLOBAL_ADMIN_ALLOWED_ROLES:
    GLOBAL_ADMIN_ALLOWED_ROLES = {"global_admin", "super_admin"}
if not GLOBAL_ADMIN_METADATA_FLAGS:
    GLOBAL_ADMIN_METADATA_FLAGS = {"global_admin", "is_global_admin"}

MANAGED_MEMBERSHIP_ROLES: set[str] = _env_csv_set(
    "MANAGED_MEMBERSHIP_ROLES",
    "owner,editor,viewer",
)
if not MANAGED_MEMBERSHIP_ROLES:
    MANAGED_MEMBERSHIP_ROLES = {"owner", "editor", "viewer"}

DEFAULT_NEW_MEMBERSHIP_ROLE: str = (
    os.environ.get("DEFAULT_NEW_MEMBERSHIP_ROLE", "viewer").strip().lower() or "viewer"
)
if DEFAULT_NEW_MEMBERSHIP_ROLE not in MANAGED_MEMBERSHIP_ROLES:
    DEFAULT_NEW_MEMBERSHIP_ROLE = "viewer"

ACTIVE_MEMBERSHIP_STATUSES: set[str] = {"active", "trial"}
DISABLED_MEMBERSHIP_STATUS = "disabled"

CATALOG_CACHE_TTL_SECONDS: int = max(
    0,
    int(os.environ.get("CATALOG_CACHE_TTL_SECONDS", "45")),
)
_CATALOG_CACHE: dict[str, tuple[float, Any]] = {}

# Concurrency / Rate-Limit config
# How many PDFs to process simultaneously.
PDF_SEMAPHORE_LIMIT: int = int(os.environ.get("PDF_SEMAPHORE_LIMIT", "3"))

# Batch sizes are token-budget-driven, not RPM-driven — fixed regardless of PDF count.
# 20 items × ~200 output tokens = ~4 000 tokens, well within the 8 192 Flash cap.
# 10 items × ~500 output tokens = ~5 000 tokens, comfortably inside the 65 536
# Flash output ceiling even when full pauta descriptions are included.
# Raise via AUDITOR_BATCH_SIZE env var if needed.
AUDITOR_BATCH_SIZE: int       = int(os.environ.get("AUDITOR_BATCH_SIZE",      "20"))
# 15 items per sequential Flash call (cuts rounds by 33% vs old default of 10).
EXTRA_REVIEW_BATCH_SIZE: int  = int(os.environ.get("EXTRA_REVIEW_BATCH_SIZE", "15"))

# Extraction backend:
# - gemini   (default): existing Planner + Extractor + Chunk Consolidator flow
# - landingai: LandingAI Parse+Extract, then direct normalization into chunks/final
EXTRACTION_BACKEND: str = os.environ.get("EXTRACTION_BACKEND", "gemini").strip().lower()
if EXTRACTION_BACKEND not in {"gemini", "landingai"}:
    _logger.warning("Unknown EXTRACTION_BACKEND=%r; falling back to 'gemini'.", EXTRACTION_BACKEND)
    EXTRACTION_BACKEND = "gemini"

LANDING_AI_MODEL: str = os.environ.get("LANDING_AI_MODEL", "dpt-2").strip() or "dpt-2"
LANDING_AI_SCHEMA_PATH: str | None = os.environ.get("LANDING_AI_SCHEMA_PATH", "").strip() or None


def compute_concurrency(n_pdfs: int) -> dict:
    """
    Computes per-PDF worker counts that keep total API usage within Gemini
    Tier-1 rate limits, automatically adapting to the number of PDFs in the
    current request.

    Math
    ----
    max_concurrent_calls = RPM_budget × (avg_req_seconds / 60)
    workers_per_pdf      = max(1, max_concurrent_calls ÷ effective_pdfs)

    effective_pdfs = min(n_pdfs, PDF_SEMAPHORE_LIMIT) so that sending 10 PDFs
    with a semaphore of 3 does not artificially constrain per-PDF workers.

    Pro budget  : 120 RPM (80% of 150) × 20 s avg = 40 max concurrent Pro calls
    Flash budget: 1600 RPM (80% of 2000) × 8 s avg = 213  max concurrent Flash calls

    Pro is split between extraction and mapping/reflection:
      extractor   up to half the Pro budget   (heaviest user)
      mapper      up to quarter Pro budget     (large input, slower calls)
      reflector   up to quarter Pro budget     (PDF-attached, slower calls)

    Flash (auditor) is always capped at 32 workers — Flash is so generous
    that the practical cap is a sanity/thread-overhead limit, not an RPM limit.

    Manual overrides
    ----------------
    Any value can be pinned via environment variable (see .env for the full
    list).  If the env var is set, compute_concurrency respects it.
    """
    # Effective simultaneous PDFs (semaphore bounds actual concurrency)
    n: int = max(1, min(n_pdfs, PDF_SEMAPHORE_LIMIT))

    # Tier-1 budgets with 20% safety margin
    PRO_RPM:   int = 120   # 80% of 150
    FLASH_RPM: int = 1600  # 80% of 2000

    # Conservative average request durations per model
    PRO_AVG_S:   int = 20  # extraction / mapping / reflection
    FLASH_AVG_S: int =  8  # auditor classification

    # Max simultaneous calls before hitting the rate ceiling
    max_pro_conc:   int = max(1, int(PRO_RPM   * PRO_AVG_S   / 60))  # = 40
    max_flash_conc: int = max(1, int(FLASH_RPM * FLASH_AVG_S / 60))  # = 213

    computed: dict = {
        # Extractor gets half the Pro budget per PDF (most calls, longest phase)
        "extractor_workers":     min(20, max(1, (max_pro_conc // 2) // n)),
        # Mapper gets a quarter: each call carries the full pauta context (~30 kB)
        "mapper_workers":        min(8,  max(1, (max_pro_conc // 4) // n)),
        # Reflector gets a quarter: PDF-attached calls are the slowest Pro requests
        "reflector_concurrency": min(8,  max(1, (max_pro_conc // 4) // n)),
        # Auditor always hits the sanity cap — Flash limit is never the bottleneck
        "auditor_concurrency":   min(32, max(1,  max_flash_conc      // n)),
    }

    # Apply optional manual env-var overrides (commented out in .env by default)
    _overrides: dict[str, str] = {
        "extractor_workers":     "EXTRACTOR_WORKERS",
        "mapper_workers":        "MAPPER_WORKERS",
        "reflector_concurrency": "REFLECTOR_CONCURRENCY",
        "auditor_concurrency":   "AUDITOR_CONCURRENCY",
    }
    for key, env_var in _overrides.items():
        raw = os.environ.get(env_var)
        if raw:
            computed[key] = max(1, int(raw))
            computed[key + "_overridden"] = True

    # Log the resolved plan so it is visible in server logs
    overridden = [k for k in computed if k.endswith("_overridden")]
    override_note = f" [{len(overridden)//1} override(s)]" if overridden else " [auto]"
    _logger.info(
        "Concurrency plan for %d PDF(s)%s: extractor=%d mapper=%d auditor=%d reflector=%d",
        n_pdfs, override_note,
        computed['extractor_workers'], computed['mapper_workers'],
        computed['auditor_concurrency'], computed['reflector_concurrency'],
    )
    return computed

# Token Usage Tracker (in-memory, per-process)
# Rough per-minute token counter used for soft throttling.
# Thread-safe via asyncio lock; reset every 60 seconds.
_token_lock = asyncio.Lock()   # created lazily inside event-loop context
_token_usage: dict = {"count": 0, "window_start": time.monotonic()}
_TPM_SOFT_LIMIT = 900_000          # stay 10% below the 1 M hard limit
_TOKEN_COOLDOWN_SLEEP = 5          # seconds to sleep when near the limit


# Filesystem helpers
def _safe_dirname(name: str, max_len: int = 24) -> str:
    """
    Converts an arbitrary string (e.g. a PDF filename stem) into a name that is
    safe to use as a directory on Windows and Linux, and short enough to stay
    well under the Windows MAX_PATH (260 chars) limit.

    The PDF filename appears TWICE in the deepest path
    (once as the output subdirectory, once inside filenames like
    MAPPING_LINKS_{name}.json), so we keep it short (24 chars) to guarantee
    the full path stays below MAX_PATH even on deeply nested workspaces.

    Rules applied:
      - Replace characters illegal or problematic on Windows  \\ / : * ? " < > |
        and the ampersand & (interpreted by cmd / some Win32 paths) with '_'.
      - Collapse consecutive underscores/spaces to a single underscore.
      - Strip leading/trailing underscores and spaces.
      - Truncate to max_len characters.
      - If truncation is needed, append a short hash suffix to reduce collisions.
    """
    safe = re.sub(r'[\\/:*?"<>|&]', '_', name)
    safe = re.sub(r'[\s_]+', '_', safe)
    safe = safe.strip('_') or 'pdf'
    if len(safe) <= max_len:
        return safe

    # Preserve uniqueness when long stems share the same prefix.
    digest = hashlib.sha1(safe.encode("utf-8")).hexdigest()[:6]
    prefix_len = max(1, max_len - len(digest) - 1)
    compact = f"{safe[:prefix_len]}_{digest}"
    return compact.rstrip('_')


def _safe_filename(name: str, max_len: int = 48) -> str:
    """
    Sanitizes an uploaded filename (stem + extension) so it is safe on Windows
    and passable as an HTTP header value (e.g. to genai.upload_file).

    The stem is processed by _safe_dirname; the extension is preserved as-is
    (lowercased). This ensures paths handed to the Gemini SDK are pure ASCII
    and contain no spaces or special characters.

    Examples:
      "2025-12-05_RCP Ricard - Pressupost Daufés Golfet 1.pdf"
        -> "2025-12-05_RCP_Ricard_-_Pressupost_Dauf.pdf"  (max_len=48 on stem)
      "06 08 2025 PRESSUPOST EXCEL DAUFES.xlsx"
        -> "06_08_2025_PRESSUPOST_EXCEL_DAUFES.xlsx"
    """
    stem, ext = os.path.splitext(name)
    # Encode any non-ASCII characters as their ASCII approximations where
    # possible, then strip whatever remains — this handles accented chars like é→e
    try:
        import unicodedata
        stem = unicodedata.normalize('NFKD', stem)
        stem = stem.encode('ascii', errors='ignore').decode('ascii')
    except Exception:
        pass
    safe_stem = _safe_dirname(stem, max_len=max_len)
    return safe_stem + ext.lower()


def _build_project_output_filename(project_name: str | None, timestamp: str) -> str:
    base_name = (project_name or "comparativo").strip() or "comparativo"
    safe_name = _safe_filename(f"{base_name}.xlsx", max_len=80)
    stem, _ = os.path.splitext(safe_name)
    if not stem:
        stem = "comparativo"
    return f"{stem}_comparativo_{timestamp}.xlsx"


def _norm_real_path(path: str) -> str:
    return os.path.normcase(os.path.realpath(path))


def _is_within_temp_base(path: str) -> bool:
    base = _norm_real_path(TEMP_BASE_DIR)
    target = _norm_real_path(path)
    return target == base or target.startswith(base + os.sep)


def _create_job_workspace(job_id: str, timestamp: str) -> tuple[str, str]:
    prefix = f"job_{job_id[:8]}_{timestamp}_"
    workspace_dir = tempfile.mkdtemp(prefix=prefix, dir=TEMP_BASE_DIR)
    inputs_dir = os.path.join(workspace_dir, "inputs")
    os.makedirs(inputs_dir, exist_ok=True)
    return workspace_dir, inputs_dir


async def _save_upload_to_disk(
    *,
    upload: UploadFile,
    dest_path: str,
    max_bytes: int,
    error_label: str,
) -> int:
    """
    Streams UploadFile to disk in chunks to avoid loading full files in RAM.
    """
    total = 0
    try:
        with open(dest_path, "wb") as fh:
            while True:
                chunk = await upload.read(UPLOAD_IO_CHUNK_BYTES)
                if not chunk:
                    break
                total += len(chunk)
                if total > max_bytes:
                    raise HTTPException(
                        status_code=413,
                        detail=f"{error_label} excede el tamano maximo permitido.",
                    )
                fh.write(chunk)
    finally:
        try:
            await upload.close()
        except Exception:
            pass
    return total


def _cleanup_stale_temp_workspaces(*, ttl_hours: int) -> int:
    """
    Deletes stale workspace folders in TEMP_BASE_DIR not associated with
    active in-memory jobs.
    """
    ttl_seconds = max(1, ttl_hours) * 3600
    cutoff = time.time() - ttl_seconds
    active_workspaces = {
        _norm_real_path(str(job.get("workspace_dir") or ""))
        for job in JOBS.values()
        if str(job.get("workspace_dir") or "").strip()
    }

    removed = 0
    try:
        with os.scandir(TEMP_BASE_DIR) as entries:
            for entry in entries:
                if not entry.is_dir():
                    continue
                if not entry.name.startswith("job_"):
                    continue
                try:
                    stat = entry.stat()
                except FileNotFoundError:
                    continue
                if stat.st_mtime > cutoff:
                    continue
                target = _norm_real_path(entry.path)
                if target in active_workspaces:
                    continue
                cleanup_temp_folder(entry.path)
                removed += 1
    except FileNotFoundError:
        os.makedirs(TEMP_BASE_DIR, exist_ok=True)
    except Exception as ex:
        _logger.warning("Could not run stale temp workspace cleanup: %s", ex)
    return removed


def cleanup_temp_folder(folder_path: str):
    """Deletes the temporary workspace folder after the response has been sent."""
    if not folder_path:
        return
    if not _is_within_temp_base(folder_path):
        _logger.warning("Refusing to delete non-temp path: %s", folder_path)
        return
    try:
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)
            _logger.info("Cleaned up temp folder: %s", folder_path)
    except Exception as e:
        _logger.warning("Could not delete temp folder %s: %s", folder_path, e)


async def startup_temp_housekeeping() -> None:
    removed = _cleanup_stale_temp_workspaces(ttl_hours=TEMP_WORKSPACE_TTL_HOURS)
    if removed:
        _logger.info("Startup cleanup removed %d stale temp workspace(s).", removed)


def _sha256_file(path: str, chunk_size: int = 1024 * 1024) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            block = fh.read(chunk_size)
            if not block:
                break
            hasher.update(block)
    return hasher.hexdigest()


def _parse_iso_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _reconcile_orphan_budget_runs(
    *,
    tenant_id: str | None = None,
    pipeline_job_id: str | None = None,
    force: bool = False,
) -> int:
    """
    Marks orphaned queued/running budget_runs as failed when the corresponding
    in-memory job is no longer active.
    """
    if _RUN_REPOSITORY is None:
        return 0
    try:
        if not _RUN_REPOSITORY._has_budget_runs_table():  # noqa: SLF001
            return 0
    except Exception:
        return 0

    params: dict[str, str] = {
        "select": "id,task_id,pipeline_job_id,status,started_at,updated_at,result_payload",
        "status": "in.(running,queued)",
        "order": "started_at.asc",
        "limit": "200",
    }
    if tenant_id:
        params["tenant_id"] = f"eq.{tenant_id}"
    if pipeline_job_id:
        params["pipeline_job_id"] = f"eq.{pipeline_job_id}"

    try:
        _, rows = _RUN_REPOSITORY._request(
            "GET",
            "/rest/v1/budget_runs",
            params=params,
            allow_not_found=True,
        )
    except Exception as ex:
        _logger.warning("Could not list running budget_runs for reconciliation: %s", ex)
        return 0

    if not rows:
        return 0

    now = datetime.now(timezone.utc)
    reconciled = 0

    for row in rows:
        run_id = str(row.get("id") or "").strip()
        if not run_id:
            continue
        run_job_id = str(row.get("pipeline_job_id") or "").strip()
        in_memory = JOBS.get(run_job_id)
        if in_memory and in_memory.get("status") == "processing":
            continue

        started_at = _parse_iso_datetime(row.get("started_at"))
        updated_at = _parse_iso_datetime(row.get("updated_at"))
        last_seen = updated_at or started_at
        if not force and last_seen is not None:
            if now - last_seen < timedelta(minutes=ORPHAN_RECONCILE_MINUTES):
                continue

        payload = row.get("result_payload")
        if not isinstance(payload, dict):
            payload = {}
        payload = dict(payload)
        payload.update(
            {
                "reconciled_orphan": True,
                "reconciled_at": now.isoformat(),
            }
        )

        try:
            _RUN_REPOSITORY._request(  # noqa: SLF001
                "PATCH",
                "/rest/v1/budget_runs",
                params={"id": f"eq.{run_id}"},
                payload={
                    "status": "failed",
                    "error_message": ORPHAN_RUN_FAILURE_MESSAGE,
                    "result_payload": payload,
                    "finished_at": now.isoformat(),
                    "updated_at": now.isoformat(),
                },
                prefer="return=minimal",
            )

            task_id = str(row.get("task_id") or "").strip()
            if task_id:
                task_payload: dict[str, Any] = {}
                try:
                    _, task_rows = _RUN_REPOSITORY._request(  # noqa: SLF001
                        "GET",
                        "/rest/v1/tasks",
                        params={"select": "payload", "id": f"eq.{task_id}", "limit": "1"},
                        allow_not_found=True,
                    )
                    if task_rows:
                        raw_task_payload = task_rows[0].get("payload")
                        if isinstance(raw_task_payload, dict):
                            task_payload = dict(raw_task_payload)
                except Exception:
                    task_payload = {}

                task_payload.update(
                    {
                        "status_detail": "failed",
                        "error": ORPHAN_RUN_FAILURE_MESSAGE,
                        "reconciled_orphan": True,
                        "reconciled_at": now.isoformat(),
                    }
                )
                _RUN_REPOSITORY.update_task_run(
                    task_id,
                    status="cancelled",
                    payload=task_payload,
                )

            reconciled += 1
        except Exception as ex:
            _logger.warning("Could not reconcile orphan run %s: %s", run_id, ex)

    return reconciled


from src.shared.security.runtime_auth_service import (
    _authorize_budget_execution,
    _authorize_global_admin,
    _build_run_metrics_maps,
    _exchange_email_password_for_access_token,
    _extract_bearer_token,
    _extract_request_token,
    _fetch_auth_users_map,
    _get_cached_catalog_value,
    _get_managed_membership_roles,
    _get_membership_row,
    _get_tenant_admin_roles,
    _is_global_admin_from_catalog,
    _is_global_admin_user,
    _is_truthy,
    _load_membership_plans_catalog,
    _load_tenant_roles_catalog,
    _load_tenant_subscriptions_map,
    _normalize_required_uuid,
    _resolve_authenticated_user,
    _resolve_default_membership_role,
    _resolve_supabase_public_auth_key,
    _set_cached_catalog_value,
)
from src.features.runs.infrastructure.runtime_persistence_service import (
    _build_artifact_prefix,
    _build_extraction_signature,
    _init_persistence_context,
    _normalize_project_id,
    _normalize_tenant_id,
    _persist_execution_result,
    _upload_artifact,
)
from src.features.comparison.application.services.runtime_pipeline_service import (
    PipelineError,
    _async_pipeline,
    _enrich_audit_data,
    _inject_texto_oferta,
    _load_json,
    _normalize_cap_cod,
    _process_single_pdf,
    _register_gemini_file,
    _throttle_tokens,
    _unregister_gemini_file,
    _upload_pdf_to_gemini,
    run_pipeline,
    run_pipeline_worker,
)

from src.features.comparison.application.use_cases.http_handlers import (
    ProcessBudgetRerunPayload,
    _download_rerun_inputs_from_storage,
    _extract_output_storage_ref,
    _load_budget_run_for_job,
    download_result,
    get_job_status,
    health_check,
    process_budget,
    rerun_budget_from_last_inputs,
)
from src.features.admin.application.use_cases.http_handlers import (
    AdminLoginPayload,
    MembershipDeletePayload,
    MembershipRolePatch,
    MembershipStatusPatch,
    TenantSubscriptionStatusPatch,
    admin_login,
    admin_login_portal,
    admin_memberships_panel,
    delete_membership,
    get_admin_tenant_overview,
    patch_membership_role,
    patch_membership_status,
    patch_tenant_subscription_status,
)

