"""Comparison HTTP handlers extracted from runtime monolith."""

import os
import uuid
import asyncio
from datetime import datetime
from typing import Any, List

from fastapi import File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel

from src.app.runtime import (
    JOBS,
    MAX_PAUTA_UPLOAD_BYTES,
    MAX_PDF_UPLOAD_BYTES,
    MAX_TOTAL_UPLOAD_BYTES,
    SUPABASE_ENFORCE_USER_AUTH,
    SUPABASE_REQUIRE_PERSISTENCE,
    USER_RUN_FAILURE_MESSAGE,
    USER_START_FAILURE_MESSAGE,
    _RUN_REPOSITORY,
    _build_project_output_filename,
    _create_job_workspace,
    _reconcile_orphan_budget_runs,
    _safe_filename,
    _save_upload_to_disk,
    cleanup_temp_folder,
)
from src.features.runs.infrastructure.runtime_persistence_service import (
    _init_persistence_context,
    _normalize_project_id,
    _normalize_tenant_id,
)
from src.shared.billing.credit_service import (
    CreditBalanceError,
    CreditBillingNotInitializedError,
    consume_execution_credits,
    estimate_execution_credits,
    get_tenant_credit_balance,
    maybe_refund_execution_credits,
)
from src.shared.security.runtime_auth_service import (
    _authorize_budget_execution,
    _authorize_tenant_read_access,
    _normalize_required_uuid,
)
from src.features.comparison.application.services.runtime_pipeline_service import (
    run_pipeline_worker,
)
from src.shared.observability.logger import get_logger

_logger = get_logger(__name__)


class ProcessBudgetRerunPayload(BaseModel):
    run_id: str
    project_id: str
    tenant_id: str
    force_rerun: bool = True


async def process_budget(
    request: Request,
    pauta: UploadFile = File(..., description="Master Excel file (.xlsx or .xls)"),
    files: List[UploadFile] = File(..., description="One or more PDF offer files"),
    project_id: str = Form(
        ...,
        description="Supabase project id used to persist execution artifacts.",
    ),
    tenant_id: str = Form(
        ...,
        description="Tenant id expected to own the selected project_id.",
    ),
    force_rerun: bool = Form(
        False,
        description="When true, marks this execution as forced rerun in persistence.",
    ),
):
    """
    Saves uploads, registers a job, fires run_pipeline_worker as a background
    asyncio task, and returns {job_id} IMMEDIATELY (< 1 s).
    """
    request_id = getattr(getattr(request, "state", None), "request_id", None)
    _logger.info(
        "Received budget processing request",
        extra={
            "request_id": request_id,
            "project_id": project_id,
            "tenant_id": tenant_id,
            "pdf_count": len(files),
            "force_rerun": force_rerun,
        },
    )
    # ── Validate extensions ───────────────────────────────────────────────────
    pauta_filename = str(pauta.filename or "").strip()
    if not pauta_filename.lower().endswith((".xlsx", ".xls")):
        _logger.warning(
            "Rejected budget request because pauta is not Excel",
            extra={"request_id": request_id, "file_name": pauta_filename},
        )
        raise HTTPException(
            status_code=422,
            detail="'pauta' must be an Excel file (.xlsx or .xls).",
        )
    for f in files:
        safe_pdf_name = str(f.filename or "").strip()
        if not safe_pdf_name.lower().endswith(".pdf"):
            _logger.warning(
                "Rejected budget request because one file is not PDF",
                extra={"request_id": request_id, "file_name": safe_pdf_name},
            )
            raise HTTPException(
                status_code=422,
                detail=f"'{safe_pdf_name or 'archivo'}' is not a PDF. All offer files must be PDFs.",
            )
    try:
        project_id = _normalize_project_id(project_id)
        tenant_id = _normalize_tenant_id(tenant_id)
    except ValueError as project_exc:
        _logger.warning(
            "Rejected budget request due to invalid project or tenant id",
            extra={"request_id": request_id, "error": str(project_exc)},
        )
        raise HTTPException(status_code=422, detail=str(project_exc)) from project_exc
    if SUPABASE_REQUIRE_PERSISTENCE:
        if _RUN_REPOSITORY is None:
            raise HTTPException(
                status_code=503,
                detail=(
                    "Supabase persistence is required but not configured "
                    "(SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY missing)."
                ),
            )
        if not project_id:
            raise HTTPException(
                status_code=422,
                detail="project_id is required and must be a valid UUID.",
            )
        if not tenant_id:
            raise HTTPException(
                status_code=422,
                detail="tenant_id is required and must be a valid UUID.",
            )

    actor_user_id: str | None = None
    if SUPABASE_ENFORCE_USER_AUTH:
        actor_user_id = _authorize_budget_execution(
            request=request,
            tenant_id=tenant_id or "",
        )

    # Reconcile stale queued/running runs from previous interrupted workers.
    try:
        reconciled = _reconcile_orphan_budget_runs(tenant_id=tenant_id)
        if reconciled:
            _logger.info(
                "Reconciled stale runs before starting job",
                extra={"tenant_id": tenant_id, "reconciled_runs": reconciled},
            )
    except Exception as reconcile_exc:
        _logger.warning(
            "Could not reconcile stale runs before starting job",
            extra={"tenant_id": tenant_id, "error": str(reconcile_exc)},
        )

    # ── Create workspace ──────────────────────────────────────────────────────
    timestamp     = datetime.now().strftime("%Y%m%d_%H%M%S")
    job_id        = str(uuid.uuid4())
    workspace_dir, inputs_dir = _create_job_workspace(job_id, timestamp)
    _logger.info(
        "Created workspace for new job",
        extra={"job_id": job_id, "tenant_id": tenant_id, "workspace_dir": workspace_dir},
    )

    # ── Save uploads ──────────────────────────────────────────────────────────
    total_uploaded_bytes = 0
    uploaded_file_sizes_bytes: list[int] = []
    try:
        pauta_path = os.path.join(inputs_dir, _safe_filename(pauta_filename))
        pauta_size = await _save_upload_to_disk(
            upload=pauta,
            dest_path=pauta_path,
            max_bytes=MAX_PAUTA_UPLOAD_BYTES,
            error_label="La pauta",
        )
        total_uploaded_bytes += pauta_size
        uploaded_file_sizes_bytes.append(int(max(0, pauta_size)))
        _logger.info(
            "Saved pauta upload",
            extra={
                "job_id": job_id,
                "tenant_id": tenant_id,
                "file_name": os.path.basename(pauta_path),
                "size_bytes": pauta_size,
            },
        )

        pdf_paths = []
        for upload in files:
            dest = os.path.join(inputs_dir, _safe_filename(upload.filename))
            pdf_size = await _save_upload_to_disk(
                upload=upload,
                dest_path=dest,
                max_bytes=MAX_PDF_UPLOAD_BYTES,
                error_label=f"El PDF {upload.filename or 'archivo'}",
            )
            total_uploaded_bytes += pdf_size
            uploaded_file_sizes_bytes.append(int(max(0, pdf_size)))
            if total_uploaded_bytes > MAX_TOTAL_UPLOAD_BYTES:
                raise HTTPException(
                    status_code=413,
                    detail="El total de archivos excede el tamano maximo permitido.",
                )
            pdf_paths.append(dest)
            _logger.info(
                "Saved PDF upload",
                extra={
                    "job_id": job_id,
                    "tenant_id": tenant_id,
                    "file_name": os.path.basename(dest),
                    "size_bytes": pdf_size,
                },
            )
    except HTTPException:
        cleanup_temp_folder(workspace_dir)
        _logger.warning(
            "Budget request failed during upload validation",
            extra={"job_id": job_id, "tenant_id": tenant_id},
        )
        raise
    except Exception as upload_exc:
        cleanup_temp_folder(workspace_dir)
        _logger.exception(
            "Budget request failed while saving uploads",
            extra={"job_id": job_id, "tenant_id": tenant_id, "error": str(upload_exc)},
        )
        raise HTTPException(
            status_code=500,
            detail=USER_START_FAILURE_MESSAGE,
        ) from upload_exc

    persistence_context: dict[str, Any] | None = None
    persistence_error: str | None = None
    billing_context: dict[str, Any] | None = None

    try:
        billing_context = consume_execution_credits(
            repo=_RUN_REPOSITORY,
            tenant_id=tenant_id,
            project_id=project_id,
            job_id=job_id,
            app_key="comparacion_presupuestos",
            pdf_count=len(pdf_paths),
            total_bytes=total_uploaded_bytes,
            file_sizes_bytes=uploaded_file_sizes_bytes,
            actor_user_id=actor_user_id,
            is_rerun=False,
            source_run_id=None,
        )
        _logger.info(
            "Credits consumed for budget execution",
            extra={
                "job_id": job_id,
                "tenant_id": tenant_id,
                "project_id": project_id,
                "credits_amount": billing_context.get("amount"),
                "credits_balance_after": billing_context.get("balance_after"),
            },
        )
    except CreditBalanceError as billing_exc:
        cleanup_temp_folder(workspace_dir)
        _logger.warning(
            "Budget request rejected due to insufficient credits",
            extra={
                "job_id": job_id,
                "tenant_id": tenant_id,
                "project_id": project_id,
                "required_credits": billing_exc.required,
                "available_credits": billing_exc.available,
            },
        )
        raise HTTPException(status_code=402, detail=str(billing_exc)) from billing_exc
    except Exception as billing_exc:
        cleanup_temp_folder(workspace_dir)
        _logger.exception(
            "Credit consumption failed while starting job",
            extra={"job_id": job_id, "tenant_id": tenant_id, "project_id": project_id},
        )
        if isinstance(billing_exc, CreditBillingNotInitializedError):
            raise HTTPException(status_code=503, detail=str(billing_exc)) from billing_exc
        raise HTTPException(status_code=503, detail=USER_START_FAILURE_MESSAGE) from billing_exc

    try:
        persistence_context = _init_persistence_context(
            job_id=job_id,
            timestamp=timestamp,
            project_id=project_id,
            expected_tenant_id=tenant_id,
            actor_user_id=actor_user_id,
            force_rerun=force_rerun,
            pauta_path=pauta_path,
            pdf_paths=pdf_paths,
            rerun_context=None,
        )
        if not persistence_context:
            raise RuntimeError("Persistence context initialization returned empty context.")
        _logger.info(
            "Persistence context initialized for job",
            extra={"job_id": job_id, "tenant_id": tenant_id, "project_id": project_id},
        )
    except ValueError as persist_exc:
        try:
            maybe_refund_execution_credits(
                repo=_RUN_REPOSITORY,
                billing_context=billing_context,
                actor_user_id=actor_user_id,
                reason="startup_persistence_validation_failure",
            )
        except Exception:
            pass
        persist_detail = str(persist_exc)
        status_code = 404 if "not found" in persist_detail.lower() else 422
        _logger.warning(
            "Persistence validation failed while starting job",
            extra={
                "job_id": job_id,
                "tenant_id": tenant_id,
                "project_id": project_id,
                "error": persist_detail,
            },
        )
        raise HTTPException(
            status_code=status_code,
            detail="No se pudo validar el proyecto seleccionado. Vuelve a intentarlo.",
        ) from persist_exc
    except Exception as persist_exc:
        try:
            maybe_refund_execution_credits(
                repo=_RUN_REPOSITORY,
                billing_context=billing_context,
                actor_user_id=actor_user_id,
                reason="startup_persistence_failure",
            )
        except Exception:
            pass
        persistence_error = str(persist_exc)
        _logger.exception(
            "Persistence initialization failed while starting job",
            extra={
                "job_id": job_id,
                "tenant_id": tenant_id,
                "project_id": project_id,
                "error": persistence_error,
            },
        )
        raise HTTPException(
            status_code=500,
            detail=USER_START_FAILURE_MESSAGE,
        ) from persist_exc

    # ── Register job ──────────────────────────────────────────────────────────
    JOBS[job_id] = {
        "status":    "processing",
        "progress":  0,
        "message":   "Iniciando pipeline...",
        "timestamp": timestamp,
        "file_path": None,
        "error":     None,
        "workspace_dir": workspace_dir,
        "project_id": project_id,
        "project_name": (persistence_context or {}).get("project_name"),
        "force_rerun": force_rerun,
        "persistence": persistence_context,
        "persistence_error": persistence_error,
        "billing": billing_context,
    }

    # ── Fire worker (non-blocking) ────────────────────────────────────────────
    asyncio.create_task(
        run_pipeline_worker(job_id, workspace_dir, pauta_path, pdf_paths, timestamp)
    )
    _logger.info(
        "Budget job accepted and worker started",
        extra={
            "job_id": job_id,
            "tenant_id": tenant_id,
            "project_id": project_id,
            "pdf_count": len(pdf_paths),
            "uploaded_total_bytes": total_uploaded_bytes,
        },
    )

    # ── Return immediately (<1 s) ─────────────────────────────────────────────
    return {"job_id": job_id}


def _extract_output_storage_ref(run_row: dict[str, Any] | None) -> dict[str, str] | None:
    if not isinstance(run_row, dict):
        return None
    result_payload = run_row.get("result_payload")
    if not isinstance(result_payload, dict):
        return None
    output_excel = result_payload.get("output_excel")
    if not isinstance(output_excel, dict):
        return None
    bucket = str(output_excel.get("bucket") or "").strip()
    path = str(output_excel.get("path") or "").strip()
    if not bucket or not path:
        return None
    return {"bucket": bucket, "path": path}


def _load_budget_run_for_job(job_id: str) -> dict[str, Any] | None:
    if _RUN_REPOSITORY is None:
        return None

    # Prefer exact run_id from in-memory context when present.
    in_memory_job = JOBS.get(job_id) or {}
    persistence_ctx = in_memory_job.get("persistence")
    if isinstance(persistence_ctx, dict):
        run_id = str(persistence_ctx.get("run_id") or "").strip()
        if run_id:
            try:
                row = _RUN_REPOSITORY.get_budget_run_by_id(run_id)
                if row:
                    return row
            except Exception:
                pass
    try:
        return _RUN_REPOSITORY.get_budget_run_by_pipeline_job_id(job_id)
    except Exception:
        return None


def _download_rerun_inputs_from_storage(
    *,
    run_row: dict[str, Any],
    inputs_dir: str,
) -> tuple[str, dict[str, str], list[str]]:
    if _RUN_REPOSITORY is None:
        raise RuntimeError("Supabase repository is not configured.")

    request_payload = run_row.get("request_payload")
    if not isinstance(request_payload, dict):
        raise RuntimeError("Run request payload is not available.")

    artifacts_prefix = str(request_payload.get("artifacts_prefix") or "").strip().strip("/")
    pauta_filename = str(request_payload.get("pauta_filename") or "").strip()
    pdf_filenames_raw = request_payload.get("pdf_filenames")

    if not artifacts_prefix:
        raise RuntimeError("Run artifacts prefix is missing.")
    if not pauta_filename:
        raise RuntimeError("Run pauta filename is missing.")
    if not isinstance(pdf_filenames_raw, list) or not pdf_filenames_raw:
        raise RuntimeError("Run PDF filenames are missing.")

    bucket = _RUN_REPOSITORY.storage_bucket

    pauta_object = f"{artifacts_prefix}/inputs/pauta/{_safe_filename(pauta_filename)}"
    pauta_dest = os.path.join(inputs_dir, _safe_filename(pauta_filename))
    pauta_bytes = _RUN_REPOSITORY.download_bytes(bucket=bucket, object_path=pauta_object)
    with open(pauta_dest, "wb") as fh:
        fh.write(pauta_bytes)

    pdf_paths_by_name: dict[str, str] = {}
    ordered_pdf_names: list[str] = []
    for name_raw in pdf_filenames_raw:
        pdf_filename = str(name_raw or "").strip()
        if not pdf_filename:
            continue
        object_path = f"{artifacts_prefix}/inputs/pdfs/{_safe_filename(pdf_filename)}"
        dest = os.path.join(inputs_dir, _safe_filename(pdf_filename))
        pdf_bytes = _RUN_REPOSITORY.download_bytes(bucket=bucket, object_path=object_path)
        with open(dest, "wb") as fh:
            fh.write(pdf_bytes)
        safe_pdf_name = _safe_filename(pdf_filename)
        pdf_paths_by_name[safe_pdf_name] = dest
        ordered_pdf_names.append(safe_pdf_name)

    if not pdf_paths_by_name:
        raise RuntimeError("No PDFs could be recovered from storage for rerun.")

    return pauta_dest, pdf_paths_by_name, ordered_pdf_names


def _as_storage_ref(raw_ref: Any) -> dict[str, str] | None:
    if not isinstance(raw_ref, dict):
        return None
    bucket = str(raw_ref.get("bucket") or "").strip()
    path = str(raw_ref.get("path") or "").strip()
    if not bucket or not path:
        return None
    return {"bucket": bucket, "path": path}


def _build_rerun_reuse_plan(
    *,
    run_id: str,
    unchanged_pdf_names: list[str],
) -> dict[str, dict[str, dict[str, str]]]:
    if _RUN_REPOSITORY is None:
        return {}
    targets = {str(name or "").strip() for name in unchanged_pdf_names if str(name or "").strip()}
    if not targets:
        return {}

    rows = _RUN_REPOSITORY.list_extractions_by_run_id(run_id)
    plan: dict[str, dict[str, dict[str, str]]] = {}
    for row in rows:
        raw_payload = row.get("raw_payload")
        if not isinstance(raw_payload, dict):
            continue
        source_pdf = str(raw_payload.get("source_pdf") or "").strip()
        if source_pdf not in targets:
            continue
        artifacts = raw_payload.get("artifacts")
        if not isinstance(artifacts, dict):
            continue
        canonical = artifacts.get("canonical") if isinstance(artifacts.get("canonical"), dict) else {}
        trace = artifacts.get("trace") if isinstance(artifacts.get("trace"), dict) else {}

        refs: dict[str, dict[str, str]] = {}
        for key in (
            "final_json",
            "mapping_links_final",
            "audit_qualitative_input",
            "auditoria_validada",
            "project_details",
        ):
            ref = _as_storage_ref(canonical.get(key))
            if ref:
                refs[key] = ref
        for key in (
            "mapping_links",
            "extra_review",
            "cap_mapping",
            "plan_log",
            "auditoria",
            "auditoria_enriquecida",
        ):
            ref = _as_storage_ref(trace.get(key))
            if ref:
                refs[key] = ref
        if "final_json" in refs and "mapping_links_final" in refs:
            plan[source_pdf] = refs
    return plan


def _materialize_rerun_reuse_artifacts(
    *,
    workspace_dir: str,
    pdf_paths: list[str],
    reuse_plan: dict[str, dict[str, dict[str, str]]],
    mode: str,
) -> list[str]:
    if _RUN_REPOSITORY is None:
        return []
    if not reuse_plan:
        return []

    output_base_dir = os.path.join(workspace_dir, "output")
    os.makedirs(output_base_dir, exist_ok=True)
    reused: list[str] = []

    for pdf_path in pdf_paths:
        pdf_name = os.path.basename(pdf_path)
        artifact_refs = reuse_plan.get(pdf_name)
        if not artifact_refs:
            continue

        safe_name = _safe_dirname(os.path.splitext(pdf_name)[0])
        pdf_output_dir = os.path.join(output_base_dir, safe_name)
        mapping_batches_dir = os.path.join(pdf_output_dir, "mapping_batches")
        os.makedirs(mapping_batches_dir, exist_ok=True)

        target_paths = {
            "final_json": os.path.join(pdf_output_dir, f"FINAL_{safe_name}.json"),
            "mapping_links_final": os.path.join(pdf_output_dir, f"MAPPING_LINKS_FINAL_{safe_name}.json"),
            "audit_qualitative_input": os.path.join(pdf_output_dir, "audit_qualitative_input.json"),
            "auditoria_validada": os.path.join(pdf_output_dir, f"AUDITORIA_VALIDADA_{safe_name}.json"),
            "project_details": os.path.join(pdf_output_dir, "project_details.json"),
            "mapping_links": os.path.join(mapping_batches_dir, f"MAPPING_LINKS_{safe_name}.json"),
            "extra_review": os.path.join(mapping_batches_dir, f"EXTRA_REVIEW_{safe_name}.json"),
            "cap_mapping": os.path.join(mapping_batches_dir, f"CAP_MAPPING_{safe_name}.json"),
            "plan_log": os.path.join(pdf_output_dir, "plan_log.json"),
            "auditoria": os.path.join(pdf_output_dir, f"AUDITORIA_{safe_name}.json"),
            "auditoria_enriquecida": os.path.join(pdf_output_dir, f"AUDITORIA_ENRIQUECIDA_{safe_name}.json"),
        }

        required_keys = (
            ("final_json", "mapping_links_final")
            if mode == "rerun_audit_judge"
            else ("final_json", "mapping_links_final", "auditoria_validada")
        )
        if any(key not in artifact_refs for key in required_keys):
            continue

        try:
            for key, ref in artifact_refs.items():
                target_path = target_paths.get(key)
                if not target_path:
                    continue
                payload = _RUN_REPOSITORY.download_bytes(
                    bucket=ref["bucket"],
                    object_path=ref["path"],
                )
                with open(target_path, "wb") as fh:
                    fh.write(payload)
            reused.append(pdf_name)
        except Exception as ex:
            _logger.warning(
                "Could not materialize cached artifacts for rerun PDF",
                extra={"pdf_name": pdf_name, "error": str(ex)},
            )

    return reused


def _build_persisted_response(
    job_id: str,
    status_value: str,
    run_row: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a status response from a persisted budget_run row."""
    normalized = str(status_value or "").strip().lower()

    # Extract progress/message from result_payload when available
    result_payload = {}
    if isinstance(run_row, dict):
        rp = run_row.get("result_payload")
        if isinstance(rp, dict):
            result_payload = rp

    if normalized in {"completed", "failed", "cancelled"}:
        return {
            "job_id": job_id,
            "status": normalized,
            "progress": 100,
            "message": (
                result_payload.get("message")
                or ("Proceso completado" if normalized == "completed" else USER_RUN_FAILURE_MESSAGE)
            ),
            "error": (
                None if normalized == "completed"
                else (result_payload.get("error") or USER_RUN_FAILURE_MESSAGE)
            ),
        }
    if normalized in {"running", "queued"}:
        return {
            "job_id": job_id,
            "status": "processing",
            "progress": int(result_payload.get("progress", 5 if normalized == "running" else 1)),
            "message": result_payload.get("message") or "Procesando...",
            "error": None,
        }
    return {
        "job_id": job_id,
        "status": "failed",
        "progress": 100,
        "message": USER_RUN_FAILURE_MESSAGE,
        "error": USER_RUN_FAILURE_MESSAGE,
    }


async def rerun_budget_from_last_inputs(
    payload: ProcessBudgetRerunPayload,
    request: Request,
):
    request_id = getattr(getattr(request, "state", None), "request_id", None)
    _logger.info(
        "Received budget rerun request",
        extra={
            "request_id": request_id,
            "run_id": payload.run_id,
            "project_id": payload.project_id,
            "tenant_id": payload.tenant_id,
            "force_rerun": payload.force_rerun,
        },
    )
    try:
        run_id = _normalize_required_uuid(payload.run_id, "run_id")
        project_id = _normalize_project_id(payload.project_id)
        tenant_id = _normalize_tenant_id(payload.tenant_id)
    except ValueError as exc:
        _logger.warning(
            "Rejected budget rerun request due to invalid identifiers",
            extra={"request_id": request_id, "error": str(exc)},
        )
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    if _RUN_REPOSITORY is None:
        raise HTTPException(status_code=503, detail=USER_START_FAILURE_MESSAGE)
    if not project_id or not tenant_id:
        raise HTTPException(status_code=422, detail=USER_START_FAILURE_MESSAGE)

    actor_user_id: str | None = None
    if SUPABASE_ENFORCE_USER_AUTH:
        actor_user_id = _authorize_budget_execution(
            request=request,
            tenant_id=tenant_id,
        )

    run_row = _RUN_REPOSITORY.get_budget_run_by_id(run_id)
    if not run_row:
        raise HTTPException(status_code=404, detail="No se encontro la ejecucion solicitada.")

    run_project_id = str(run_row.get("project_id") or "").strip()
    run_tenant_id = str(run_row.get("tenant_id") or "").strip()
    if run_project_id != project_id or run_tenant_id != tenant_id:
        raise HTTPException(
            status_code=403,
            detail="La ejecucion solicitada no pertenece al proyecto activo.",
        )

    # Reconcile stale queued/running runs from previous interrupted workers.
    try:
        reconciled = _reconcile_orphan_budget_runs(tenant_id=tenant_id)
        if reconciled:
            _logger.info(
                "Reconciled stale runs before rerun",
                extra={"tenant_id": tenant_id, "run_id": run_id, "reconciled_runs": reconciled},
            )
    except Exception as reconcile_exc:
        _logger.warning(
            "Could not reconcile stale runs before rerun",
            extra={"tenant_id": tenant_id, "run_id": run_id, "error": str(reconcile_exc)},
        )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    job_id = str(uuid.uuid4())
    workspace_dir, inputs_dir = _create_job_workspace(job_id, timestamp)
    _logger.info(
        "Created workspace for rerun job",
        extra={
            "job_id": job_id,
            "tenant_id": tenant_id,
            "run_id": run_id,
            "workspace_dir": workspace_dir,
        },
    )

    try:
        pauta_path, pdf_paths_by_name, ordered_pdf_names = _download_rerun_inputs_from_storage(
            run_row=run_row,
            inputs_dir=inputs_dir,
        )
        pdf_paths = [pdf_paths_by_name[name] for name in ordered_pdf_names if name in pdf_paths_by_name]
    except Exception as ex:
        cleanup_temp_folder(workspace_dir)
        _logger.exception(
            "Failed to recover rerun inputs from storage",
            extra={"job_id": job_id, "tenant_id": tenant_id, "run_id": run_id, "error": str(ex)},
        )
        raise HTTPException(
            status_code=500,
            detail=USER_START_FAILURE_MESSAGE,
        ) from ex
    uploaded_file_sizes_bytes = [int(max(0, os.path.getsize(pauta_path)))]
    total_uploaded_bytes = int(uploaded_file_sizes_bytes[0])
    for pdf_path in pdf_paths:
        file_size = int(max(0, os.path.getsize(pdf_path)))
        uploaded_file_sizes_bytes.append(file_size)
        total_uploaded_bytes += file_size
    if total_uploaded_bytes > MAX_TOTAL_UPLOAD_BYTES:
        cleanup_temp_folder(workspace_dir)
        raise HTTPException(
            status_code=413,
            detail="El total de archivos excede el tamano maximo permitido.",
        )

    persistence_context: dict[str, Any] | None = None
    persistence_error: str | None = None
    billing_context: dict[str, Any] | None = None

    try:
        billing_context = consume_execution_credits(
            repo=_RUN_REPOSITORY,
            tenant_id=tenant_id,
            project_id=project_id,
            job_id=job_id,
            app_key="comparacion_presupuestos",
            pdf_count=len(pdf_paths),
            total_bytes=total_uploaded_bytes,
            file_sizes_bytes=uploaded_file_sizes_bytes,
            actor_user_id=actor_user_id,
            is_rerun=True,
            source_run_id=run_id,
        )
        _logger.info(
            "Credits consumed for budget rerun",
            extra={
                "job_id": job_id,
                "tenant_id": tenant_id,
                "project_id": project_id,
                "source_run_id": run_id,
                "credits_amount": billing_context.get("amount"),
                "credits_balance_after": billing_context.get("balance_after"),
            },
        )
    except CreditBalanceError as billing_exc:
        cleanup_temp_folder(workspace_dir)
        _logger.warning(
            "Budget rerun request rejected due to insufficient credits",
            extra={
                "job_id": job_id,
                "tenant_id": tenant_id,
                "project_id": project_id,
                "run_id": run_id,
                "required_credits": billing_exc.required,
                "available_credits": billing_exc.available,
            },
        )
        raise HTTPException(status_code=402, detail=str(billing_exc)) from billing_exc
    except Exception as billing_exc:
        cleanup_temp_folder(workspace_dir)
        _logger.exception(
            "Credit consumption failed while starting rerun job",
            extra={"job_id": job_id, "tenant_id": tenant_id, "project_id": project_id, "run_id": run_id},
        )
        if isinstance(billing_exc, CreditBillingNotInitializedError):
            raise HTTPException(status_code=503, detail=str(billing_exc)) from billing_exc
        raise HTTPException(status_code=503, detail=USER_START_FAILURE_MESSAGE) from billing_exc

    try:
        persistence_context = _init_persistence_context(
            job_id=job_id,
            timestamp=timestamp,
            project_id=project_id,
            expected_tenant_id=tenant_id,
            actor_user_id=actor_user_id,
            force_rerun=payload.force_rerun,
            pauta_path=pauta_path,
            pdf_paths=pdf_paths,
            rerun_context={
                "source_run_id": run_id,
                "mode": "same_inputs",
                "changed_pdf_filenames": [],
            },
        )
        if not persistence_context:
            raise RuntimeError("Persistence context initialization returned empty context.")
        _logger.info(
            "Persistence context initialized for rerun job",
            extra={
                "job_id": job_id,
                "tenant_id": tenant_id,
                "project_id": project_id,
                "run_id": run_id,
            },
        )
    except ValueError as persist_exc:
        try:
            maybe_refund_execution_credits(
                repo=_RUN_REPOSITORY,
                billing_context=billing_context,
                actor_user_id=actor_user_id,
                reason="startup_persistence_validation_failure",
            )
        except Exception:
            pass
        cleanup_temp_folder(workspace_dir)
        persist_detail = str(persist_exc)
        status_code = 404 if "not found" in persist_detail.lower() else 422
        _logger.warning(
            "Persistence validation failed while starting rerun job",
            extra={
                "job_id": job_id,
                "tenant_id": tenant_id,
                "project_id": project_id,
                "run_id": run_id,
                "error": persist_detail,
            },
        )
        raise HTTPException(
            status_code=status_code,
            detail="No se pudo validar el proyecto seleccionado. Vuelve a intentarlo.",
        ) from persist_exc
    except Exception as persist_exc:
        try:
            maybe_refund_execution_credits(
                repo=_RUN_REPOSITORY,
                billing_context=billing_context,
                actor_user_id=actor_user_id,
                reason="startup_persistence_failure",
            )
        except Exception:
            pass
        cleanup_temp_folder(workspace_dir)
        persistence_error = str(persist_exc)
        _logger.exception(
            "Persistence initialization failed while starting rerun job",
            extra={
                "job_id": job_id,
                "tenant_id": tenant_id,
                "project_id": project_id,
                "run_id": run_id,
                "error": persistence_error,
            },
        )
        raise HTTPException(
            status_code=500,
            detail=USER_START_FAILURE_MESSAGE,
        ) from persist_exc

    JOBS[job_id] = {
        "status": "processing",
        "progress": 0,
        "message": "Iniciando pipeline...",
        "timestamp": timestamp,
        "file_path": None,
        "error": None,
        "workspace_dir": workspace_dir,
        "project_id": project_id,
        "project_name": (persistence_context or {}).get("project_name"),
        "force_rerun": payload.force_rerun,
        "persistence": persistence_context,
        "persistence_error": persistence_error,
        "billing": billing_context,
    }

    asyncio.create_task(
        run_pipeline_worker(job_id, workspace_dir, pauta_path, pdf_paths, timestamp)
    )
    _logger.info(
        "Budget rerun job accepted and worker started",
        extra={
            "job_id": job_id,
            "tenant_id": tenant_id,
            "project_id": project_id,
            "run_id": run_id,
            "pdf_count": len(pdf_paths),
        },
    )
    return {"job_id": job_id}


async def rerun_budget_with_pdf_overrides(
    request: Request,
    run_id: str = Form(...),
    project_id: str = Form(...),
    tenant_id: str = Form(...),
    force_rerun: bool = Form(True),
    rerun_pdf_filenames_json: str = Form("[]"),
    reuse_pdf_filenames_json: str = Form("[]"),
    files: List[UploadFile] = File(default=[]),
):
    request_id = getattr(getattr(request, "state", None), "request_id", None)
    _logger.info(
        "Received budget rerun override request",
        extra={
            "request_id": request_id,
            "run_id": run_id,
            "project_id": project_id,
            "tenant_id": tenant_id,
            "force_rerun": force_rerun,
            "replacement_pdf_count": len(files or []),
        },
    )
    for upload in files:
        pdf_name = str(upload.filename or "").strip()
        if not pdf_name.lower().endswith(".pdf"):
            raise HTTPException(
                status_code=422,
                detail=f"'{pdf_name or 'archivo'}' no es un PDF valido.",
            )

    try:
        normalized_run_id = _normalize_required_uuid(run_id, "run_id")
        normalized_project_id = _normalize_project_id(project_id)
        normalized_tenant_id = _normalize_tenant_id(tenant_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    if _RUN_REPOSITORY is None:
        raise HTTPException(status_code=503, detail=USER_START_FAILURE_MESSAGE)

    actor_user_id: str | None = None
    if SUPABASE_ENFORCE_USER_AUTH:
        actor_user_id = _authorize_budget_execution(
            request=request,
            tenant_id=normalized_tenant_id or "",
        )

    run_row = _RUN_REPOSITORY.get_budget_run_by_id(normalized_run_id)
    if not run_row:
        raise HTTPException(status_code=404, detail="No se encontro la ejecucion solicitada.")

    run_project_id = str(run_row.get("project_id") or "").strip()
    run_tenant_id = str(run_row.get("tenant_id") or "").strip()
    if run_project_id != normalized_project_id or run_tenant_id != normalized_tenant_id:
        raise HTTPException(
            status_code=403,
            detail="La ejecucion solicitada no pertenece al proyecto activo.",
        )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    job_id = str(uuid.uuid4())
    workspace_dir, inputs_dir = _create_job_workspace(job_id, timestamp)

    try:
        pauta_path, pdf_paths_by_name, original_pdf_order = _download_rerun_inputs_from_storage(
            run_row=run_row,
            inputs_dir=inputs_dir,
        )
    except Exception as ex:
        cleanup_temp_folder(workspace_dir)
        raise HTTPException(status_code=500, detail=USER_START_FAILURE_MESSAGE) from ex

    try:
        rerun_pdf_filenames_raw = json.loads(rerun_pdf_filenames_json or "[]")
        reuse_pdf_filenames_raw = json.loads(reuse_pdf_filenames_json or "[]")
    except Exception as json_exc:
        cleanup_temp_folder(workspace_dir)
        raise HTTPException(status_code=422, detail="Seleccion de PDFs invalida para rerun.") from json_exc

    rerun_selected = {
        _safe_filename(str(name or "").strip())
        for name in (rerun_pdf_filenames_raw if isinstance(rerun_pdf_filenames_raw, list) else [])
        if str(name or "").strip()
    }
    reuse_selected = {
        _safe_filename(str(name or "").strip())
        for name in (reuse_pdf_filenames_raw if isinstance(reuse_pdf_filenames_raw, list) else [])
        if str(name or "").strip()
    }

    replaced_names: set[str] = set()
    for upload in files:
        incoming_name = _safe_filename(str(upload.filename or "").strip())
        if not incoming_name.lower().endswith(".pdf"):
            continue
        dest_path = pdf_paths_by_name.get(incoming_name) or os.path.join(inputs_dir, incoming_name)
        try:
            await _save_upload_to_disk(
                upload=upload,
                dest_path=dest_path,
                max_bytes=MAX_PDF_UPLOAD_BYTES,
                error_label=f"El PDF {incoming_name}",
            )
            pdf_paths_by_name[incoming_name] = dest_path
            replaced_names.add(incoming_name)
        except Exception as ex:
            cleanup_temp_folder(workspace_dir)
            if isinstance(ex, HTTPException):
                raise
            raise HTTPException(status_code=500, detail=USER_START_FAILURE_MESSAGE) from ex

    final_pdf_names: list[str] = []
    for name in original_pdf_order:
        if name in pdf_paths_by_name:
            final_pdf_names.append(name)
    for name in sorted(pdf_paths_by_name.keys()):
        if name not in final_pdf_names:
            final_pdf_names.append(name)
    pdf_paths = [pdf_paths_by_name[name] for name in final_pdf_names if name in pdf_paths_by_name]
    if not pdf_paths:
        cleanup_temp_folder(workspace_dir)
        raise HTTPException(status_code=422, detail="No hay PDFs validos para ejecutar el rerun.")

    unchanged_names = [name for name in final_pdf_names if name not in replaced_names]
    reusable_existing = {name for name in unchanged_names if name in set(original_pdf_order)}
    rerun_audit_names = [name for name in reusable_existing if name in rerun_selected and name not in reuse_selected]
    reuse_full_names = [name for name in reusable_existing if name in reuse_selected]
    if not rerun_audit_names and not reuse_full_names and not replaced_names and not files:
        cleanup_temp_folder(workspace_dir)
        raise HTTPException(
            status_code=422,
            detail="No has seleccionado ningun PDF para re-ejecutar o reutilizar.",
        )

    reuse_plan = _build_rerun_reuse_plan(
        run_id=normalized_run_id,
        unchanged_pdf_names=sorted(set(rerun_audit_names + reuse_full_names)),
    )
    rerun_cached_names = _materialize_rerun_reuse_artifacts(
        workspace_dir=workspace_dir,
        pdf_paths=pdf_paths,
        reuse_plan=reuse_plan,
        mode="rerun_audit_judge",
    )
    reused_names = _materialize_rerun_reuse_artifacts(
        workspace_dir=workspace_dir,
        pdf_paths=pdf_paths,
        reuse_plan=reuse_plan,
        mode="reuse_full",
    )
    rerun_audit_names = [name for name in rerun_audit_names if name in set(rerun_cached_names)]
    reuse_full_names = [name for name in reuse_full_names if name in set(reused_names)]
    process_pdf_names = [name for name in final_pdf_names if name not in set(reuse_full_names)]
    process_pdf_paths = [pdf_paths_by_name[name] for name in process_pdf_names if name in pdf_paths_by_name]

    uploaded_file_sizes_bytes = [int(max(0, os.path.getsize(pauta_path)))]
    total_uploaded_bytes = int(uploaded_file_sizes_bytes[0])
    for pdf_path in pdf_paths:
        file_size = int(max(0, os.path.getsize(pdf_path)))
        uploaded_file_sizes_bytes.append(file_size)
        total_uploaded_bytes += file_size

    persistence_context: dict[str, Any] | None = None
    persistence_error: str | None = None
    billing_context: dict[str, Any] | None = None

    try:
        billing_context = consume_execution_credits(
            repo=_RUN_REPOSITORY,
            tenant_id=normalized_tenant_id or "",
            project_id=normalized_project_id or "",
            job_id=job_id,
            app_key="comparacion_presupuestos",
            pdf_count=len(pdf_paths),
            total_bytes=total_uploaded_bytes,
            file_sizes_bytes=uploaded_file_sizes_bytes,
            actor_user_id=actor_user_id,
            is_rerun=True,
            source_run_id=normalized_run_id,
        )
    except CreditBalanceError as billing_exc:
        cleanup_temp_folder(workspace_dir)
        raise HTTPException(status_code=402, detail=str(billing_exc)) from billing_exc
    except Exception as billing_exc:
        cleanup_temp_folder(workspace_dir)
        if isinstance(billing_exc, CreditBillingNotInitializedError):
            raise HTTPException(status_code=503, detail=str(billing_exc)) from billing_exc
        raise HTTPException(status_code=503, detail=USER_START_FAILURE_MESSAGE) from billing_exc

    rerun_context_payload = {
        "source_run_id": normalized_run_id,
        "mode": "pdf_selection",
        "changed_pdf_filenames": sorted(replaced_names),
        "rerun_audit_pdf_filenames": sorted(rerun_audit_names),
        "reused_pdf_filenames": sorted(reuse_full_names),
        "added_pdf_filenames": sorted(name for name in final_pdf_names if name not in set(original_pdf_order)),
    }

    try:
        persistence_context = _init_persistence_context(
            job_id=job_id,
            timestamp=timestamp,
            project_id=normalized_project_id,
            expected_tenant_id=normalized_tenant_id,
            actor_user_id=actor_user_id,
            force_rerun=force_rerun,
            pauta_path=pauta_path,
            pdf_paths=pdf_paths,
            rerun_context=rerun_context_payload,
        )
        if not persistence_context:
            raise RuntimeError("Persistence context initialization returned empty context.")
    except ValueError as persist_exc:
        try:
            maybe_refund_execution_credits(
                repo=_RUN_REPOSITORY,
                billing_context=billing_context,
                actor_user_id=actor_user_id,
                reason="startup_persistence_validation_failure",
            )
        except Exception:
            pass
        cleanup_temp_folder(workspace_dir)
        status_code = 404 if "not found" in str(persist_exc).lower() else 422
        raise HTTPException(
            status_code=status_code,
            detail="No se pudo validar el proyecto seleccionado. Vuelve a intentarlo.",
        ) from persist_exc
    except Exception as persist_exc:
        try:
            maybe_refund_execution_credits(
                repo=_RUN_REPOSITORY,
                billing_context=billing_context,
                actor_user_id=actor_user_id,
                reason="startup_persistence_failure",
            )
        except Exception:
            pass
        cleanup_temp_folder(workspace_dir)
        persistence_error = str(persist_exc)
        raise HTTPException(status_code=500, detail=USER_START_FAILURE_MESSAGE) from persist_exc

    JOBS[job_id] = {
        "status": "processing",
        "progress": 0,
        "message": "Iniciando pipeline...",
        "timestamp": timestamp,
        "file_path": None,
        "error": None,
        "workspace_dir": workspace_dir,
        "project_id": normalized_project_id,
        "project_name": (persistence_context or {}).get("project_name"),
        "force_rerun": force_rerun,
        "persistence": persistence_context,
        "persistence_error": persistence_error,
        "billing": billing_context,
        "rerun_context": rerun_context_payload,
    }

    asyncio.create_task(
        run_pipeline_worker(
            job_id,
            workspace_dir,
            pauta_path,
            pdf_paths,
            timestamp,
            process_pdf_paths=process_pdf_paths,
        )
    )
    return {"job_id": job_id}


async def get_job_status(job_id: str):
    """Returns current progress / status for a running or finished job.

    Resolution order (multi-instance safe):
    1. In-memory JOBS dict (fast path for the process that owns the worker).
    2. Supabase budget_runs table (survives process restarts).
    3. Orphan reconciliation + 404.
    """
    job = JOBS.get(job_id)

    # ── Fast-path: job lives in this process ──────────────────────────────────
    if job is not None:
        status = job["status"]
        # For terminal states, also verify against persistence so the client
        # gets consistent data even if the in-memory entry is stale.
        if status in {"completed", "failed"}:
            run_row = _load_budget_run_for_job(job_id)
            if run_row:
                persisted_status = str(run_row.get("status") or "").strip().lower()
                if persisted_status in {"completed", "failed", "cancelled"}:
                    return _build_persisted_response(job_id, persisted_status, run_row)
        return {
            "job_id":   job_id,
            "status":   status,
            "progress": job["progress"],
            "message":  job["message"],
            "error":    job.get("error"),
        }

    # ── Persistence path: job not in this process ─────────────────────────────
    run_row = _load_budget_run_for_job(job_id)

    if run_row:
        persisted_status = str(run_row.get("status") or "").strip().lower()
        if persisted_status in {"running", "queued"}:
            # If the worker is no longer alive for this run, mark it as failed.
            _reconcile_orphan_budget_runs(
                pipeline_job_id=job_id,
                force=False,
            )
            run_row = _load_budget_run_for_job(job_id) or run_row
            persisted_status = str(run_row.get("status") or "").strip().lower()
        return _build_persisted_response(job_id, persisted_status, run_row)

    reconciled = _reconcile_orphan_budget_runs(pipeline_job_id=job_id, force=True)
    if reconciled > 0:
        _logger.warning(
            "Status requested for orphaned job; returning failed state after reconciliation",
            extra={"job_id": job_id, "reconciled_runs": reconciled},
        )
        return {
            "job_id": job_id,
            "status": "failed",
            "progress": 100,
            "message": USER_RUN_FAILURE_MESSAGE,
            "error": USER_RUN_FAILURE_MESSAGE,
        }
    _logger.warning("Status requested for unknown job id", extra={"job_id": job_id})
    raise HTTPException(status_code=404, detail="Job not found.")


# ─── Endpoint 3: DOWNLOAD ─────────────────────────────────────────────────────
async def download_result(job_id: str):
    """Streams the generated Excel for a completed job.

    Resolution order (multi-instance safe):
    1. Supabase Storage (canonical, survives restarts/rebalances).
    2. Local filesystem via in-memory JOBS dict (legacy/fallback).
    """
    _logger.info("Download result requested", extra={"job_id": job_id})
    job = JOBS.get(job_id)
    if job is not None and job.get("status") not in {"completed"}:
        raise HTTPException(status_code=409, detail="Job is not completed yet.")

    # Always attempt persistence-backed download first (multi-instance safe)
    run_row = _load_budget_run_for_job(job_id)
    if run_row:
        run_status = str(run_row.get("status") or "").strip().lower()
        if run_status not in {"completed"}:
            raise HTTPException(status_code=409, detail="Job is not completed yet.")
    storage_ref = _extract_output_storage_ref(run_row)
    if storage_ref and _RUN_REPOSITORY is not None:
        try:
            blob = _RUN_REPOSITORY.download_bytes(
                bucket=storage_ref["bucket"],
                object_path=storage_ref["path"],
            )
            storage_name = os.path.basename(storage_ref["path"]) or "comparativo.xlsx"
            output_filename = _safe_filename(storage_name, max_len=120)
            return Response(
                content=blob,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={
                    "Content-Disposition": f'attachment; filename="{output_filename}"',
                },
            )
        except Exception as ex:
            _logger.warning(
                "Could not stream output from Supabase storage; falling back to local file",
                extra={"job_id": job_id, "error": str(ex)},
            )

    # Local filesystem fallback (same-process jobs only)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found.")
    file_path = job.get("file_path")
    if not file_path or not os.path.exists(file_path):
        raise HTTPException(status_code=500, detail="Output file not found on server.")
    timestamp = job.get("timestamp", datetime.now().strftime("%Y%m%d_%H%M%S"))
    project_name = str(job.get("project_name") or "").strip() or None
    output_filename = _build_project_output_filename(project_name, timestamp)
    _logger.info(
        "Streaming output file from local filesystem",
        extra={"job_id": job_id, "file_path": file_path},
    )
    return FileResponse(
        path=file_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=output_filename,
    )


def _input_media_type(filename: str) -> str:
    lower = filename.lower()
    if lower.endswith(".pdf"):
        return "application/pdf"
    if lower.endswith(".xlsx"):
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    if lower.endswith(".xls"):
        return "application/vnd.ms-excel"
    return "application/octet-stream"


async def download_input_file(
    job_id: str,
    kind: str = Query(..., pattern="^(pauta|pdf)$"),
    filename: str = Query(..., min_length=1),
):
    """Streams one input file (pauta or a PDF offer) for a persisted run."""
    _logger.info(
        "Download input file requested",
        extra={"job_id": job_id, "kind": kind, "requested_filename": filename},
    )
    run_row = _load_budget_run_for_job(job_id)
    if not run_row:
        raise HTTPException(status_code=404, detail="Job not found.")
    if _RUN_REPOSITORY is None:
        raise HTTPException(status_code=503, detail="Storage repository is not configured.")

    request_payload = run_row.get("request_payload")
    if not isinstance(request_payload, dict):
        raise HTTPException(status_code=404, detail="Run request payload is not available.")

    artifacts_prefix = str(request_payload.get("artifacts_prefix") or "").strip().strip("/")
    if not artifacts_prefix:
        raise HTTPException(status_code=404, detail="Run artifacts prefix is missing.")

    selected_filename = ""
    if kind == "pauta":
        pauta_filename = str(request_payload.get("pauta_filename") or "").strip()
        if not pauta_filename:
            raise HTTPException(status_code=404, detail="Run pauta filename is missing.")
        selected_filename = pauta_filename
        object_path = f"{artifacts_prefix}/inputs/pauta/{_safe_filename(selected_filename)}"
    else:
        pdf_filenames_raw = request_payload.get("pdf_filenames")
        if not isinstance(pdf_filenames_raw, list):
            raise HTTPException(status_code=404, detail="Run PDF filenames are missing.")
        candidates = [str(name or "").strip() for name in pdf_filenames_raw if str(name or "").strip()]
        if not candidates:
            raise HTTPException(status_code=404, detail="Run PDF filenames are missing.")

        requested = str(filename or "").strip()
        selected_filename = next((name for name in candidates if name == requested), "")
        if not selected_filename:
            requested_safe = _safe_filename(requested)
            selected_filename = next((name for name in candidates if _safe_filename(name) == requested_safe), "")
        if not selected_filename:
            raise HTTPException(status_code=404, detail="Requested PDF is not available for this run.")

        object_path = f"{artifacts_prefix}/inputs/pdfs/{_safe_filename(selected_filename)}"

    try:
        blob = _RUN_REPOSITORY.download_bytes(
            bucket=_RUN_REPOSITORY.storage_bucket,
            object_path=object_path,
        )
    except Exception as ex:
        _logger.warning(
            "Could not stream input file from storage",
            extra={
                "job_id": job_id,
                "kind": kind,
                "object_path": object_path,
                "error": str(ex),
            },
        )
        raise HTTPException(status_code=404, detail="Input file not found in storage.") from ex

    download_name = _safe_filename(selected_filename, max_len=120)
    _logger.info(
        "Streaming input file from storage",
        extra={"job_id": job_id, "kind": kind, "file_name": selected_filename},
    )
    return Response(
        content=blob,
        media_type=_input_media_type(selected_filename),
        headers={
            "Content-Disposition": f'attachment; filename="{download_name}"',
        },
    )



async def health_check():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


async def get_credit_balance(
    request: Request,
    tenant_id: str = Query(..., description="Tenant id to check credit balance."),
):
    if _RUN_REPOSITORY is None:
        raise HTTPException(status_code=503, detail="Storage repository is not configured.")

    try:
        normalized_tenant_id = _normalize_tenant_id(tenant_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if not normalized_tenant_id:
        raise HTTPException(status_code=422, detail="tenant_id is required and must be a valid UUID.")

    actor_user_id: str | None = None
    if SUPABASE_ENFORCE_USER_AUTH:
        actor_user_id = _authorize_tenant_read_access(
            request=request,
            tenant_id=normalized_tenant_id,
        )

    try:
        return get_tenant_credit_balance(
            repo=_RUN_REPOSITORY,
            tenant_id=normalized_tenant_id,
            actor_user_id=actor_user_id,
        )
    except CreditBillingNotInitializedError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        _logger.warning(
            "Could not resolve tenant credit balance",
            extra={"tenant_id": normalized_tenant_id, "error": str(exc)},
        )
        raise HTTPException(status_code=503, detail="Could not resolve tenant credit balance.") from exc


async def get_credit_estimate(
    request: Request,
    tenant_id: str = Query(..., description="Tenant id used for auth check."),
    pdf_count: int = Query(..., ge=1, le=100, description="Number of PDF files in the execution."),
    total_bytes: int = Query(0, ge=0, description="Total upload size in bytes (Excel + PDFs)."),
    file_sizes_bytes: list[int] = Query(
        default=[],
        description="Optional repeated file sizes in bytes for proportional per-file estimate.",
    ),
    is_rerun: bool = Query(False, description="Set true to estimate rerun credit discount."),
):
    try:
        normalized_tenant_id = _normalize_tenant_id(tenant_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if not normalized_tenant_id:
        raise HTTPException(status_code=422, detail="tenant_id is required and must be a valid UUID.")

    actor_user_id: str | None = None
    if SUPABASE_ENFORCE_USER_AUTH:
        actor_user_id = _authorize_tenant_read_access(
            request=request,
            tenant_id=normalized_tenant_id,
        )

    estimate = estimate_execution_credits(
        pdf_count=int(pdf_count),
        total_bytes=int(total_bytes),
        file_sizes_bytes=[int(max(0, size)) for size in file_sizes_bytes],
        is_rerun=bool(is_rerun),
    )
    return {
        "tenant_id": normalized_tenant_id,
        "requested_by": actor_user_id,
        "estimate": estimate,
    }



