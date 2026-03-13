"""Runtime persistence helpers extracted from runtime monolith."""

import os
import json
import hashlib
import uuid
from datetime import datetime
from typing import Any, Optional

from src.app.runtime import (
    EXTRACTION_BACKEND,
    JOBS,
    LANDING_AI_MODEL,
    LANDING_AI_SCHEMA_PATH,
    SUPABASE_PERSIST_DEBUG_ARTIFACTS,
    SUPABASE_PERSIST_TRACE_ARTIFACTS,
    SUPABASE_RETENTION_CANONICAL_DAYS,
    SUPABASE_RETENTION_DEBUG_DAYS,
    SUPABASE_RETENTION_TRACE_DAYS,
    _RUN_REPOSITORY,
    _safe_dirname,
    _safe_filename,
    _sha256_file,
)


def _build_extraction_signature(
    *,
    file_sha256: str,
    backend: str,
    model: str | None = None,
    schema_path: str | None = None,
) -> str:
    seed = "|".join(
        [
            file_sha256 or "",
            backend or "",
            model or "",
            schema_path or "",
        ]
    )
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def _build_artifact_prefix(tenant_id: str, project_id: str, timestamp: str, job_id: str) -> str:
    return (
        f"tenants/{tenant_id}/projects/{project_id}/budget-runs/"
        f"{timestamp}_{job_id[:8]}"
    )


def _normalize_project_id(raw_project_id: Optional[str]) -> Optional[str]:
    if raw_project_id is None:
        return None
    candidate = raw_project_id.strip()
    if not candidate:
        return None
    try:
        return str(uuid.UUID(candidate))
    except ValueError as exc:
        raise ValueError("project_id must be a valid UUID.") from exc


def _normalize_tenant_id(raw_tenant_id: Optional[str]) -> Optional[str]:
    if raw_tenant_id is None:
        return None
    candidate = raw_tenant_id.strip()
    if not candidate:
        return None
    try:
        return str(uuid.UUID(candidate))
    except ValueError as exc:
        raise ValueError("tenant_id must be a valid UUID.") from exc




def _load_json(path: str):
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        return None




def _upload_artifact(
    *,
    local_path: str,
    object_path: str,
    content_type: str,
) -> dict[str, Any]:
    if _RUN_REPOSITORY is None:
        raise RuntimeError("Supabase repository is not configured.")

    with open(local_path, "rb") as fh:
        payload = fh.read()

    storage_ref = _RUN_REPOSITORY.upload_bytes(
        object_path=object_path,
        data=payload,
        content_type=content_type,
        upsert=True,
    )
    return {
        "storage": storage_ref,
        "size_bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }


def _init_persistence_context(
    *,
    job_id: str,
    timestamp: str,
    project_id: Optional[str],
    expected_tenant_id: Optional[str],
    actor_user_id: Optional[str],
    force_rerun: bool,
    pauta_path: str,
    pdf_paths: list[str],
    rerun_context: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """
    Initializes DB persistence entities for this execution.

    Returns a context dict stored in JOBS[job_id]['persistence'].
    """
    if not project_id:
        return None
    if _RUN_REPOSITORY is None:
        raise RuntimeError(
            "project_id was provided but Supabase persistence is not configured "
            "(SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY missing)."
        )

    project = _RUN_REPOSITORY.get_project_context(project_id)
    if not project:
        raise ValueError(f"Project '{project_id}' not found in Supabase.")
    project_status = str(project.get("status") or "").strip().lower()
    if project_status and project_status != "active":
        raise ValueError(f"Project '{project_id}' is not active (status={project_status}).")

    tenant_id = project.get("tenant_id")
    created_by = actor_user_id or project.get("created_by")
    if not tenant_id:
        raise ValueError(f"Project '{project_id}' has no tenant_id.")
    if not created_by:
        raise ValueError(f"Project '{project_id}' has no created_by owner.")
    if expected_tenant_id and tenant_id != expected_tenant_id:
        raise ValueError(
            f"Project '{project_id}' does not belong to tenant '{expected_tenant_id}'."
        )

    project_name = project.get("name") or project_id
    artifacts_prefix = _build_artifact_prefix(tenant_id, project_id, timestamp, job_id)
    normalized_rerun_context = rerun_context if isinstance(rerun_context, dict) else {}

    pauta_name = os.path.basename(pauta_path)
    pauta_sha = _sha256_file(pauta_path)
    pauta_size = os.path.getsize(pauta_path)
    pdf_input_manifest = []
    for pdf_path in pdf_paths:
        pdf_name = os.path.basename(pdf_path)
        pdf_input_manifest.append(
            {
                "filename": pdf_name,
                "sha256": _sha256_file(pdf_path),
                "size_bytes": int(max(0, os.path.getsize(pdf_path))),
            }
        )

    task_payload = {
        "kind": "budget_execution",
        "job_id": job_id,
        "timestamp": timestamp,
        "force_rerun": force_rerun,
        "rerun_context": normalized_rerun_context,
        "input": {
            "pauta_filename": pauta_name,
            "pdf_count": len(pdf_paths),
        },
        "status_detail": "running",
    }
    task = _RUN_REPOSITORY.create_task_run(
        tenant_id=tenant_id,
        project_id=project_id,
        created_by=created_by,
        title=f"Budget execution {timestamp}",
        payload=task_payload,
    )

    run = _RUN_REPOSITORY.create_budget_run(
        tenant_id=tenant_id,
        project_id=project_id,
        created_by=created_by,
        task_id=task.get("id"),
        pipeline_job_id=job_id,
        force_rerun=force_rerun,
        request_payload={
            "job_id": job_id,
            "timestamp": timestamp,
            "project_name": project_name,
            "pauta_filename": pauta_name,
            "pdf_filenames": [os.path.basename(p) for p in pdf_paths],
            "inputs_manifest": {
                "pauta": {
                    "filename": pauta_name,
                    "sha256": pauta_sha,
                    "size_bytes": int(max(0, pauta_size)),
                },
                "pdfs": pdf_input_manifest,
            },
            "rerun_context": normalized_rerun_context,
            "extraction_backend": EXTRACTION_BACKEND,
            "artifacts_prefix": artifacts_prefix,
            "artifacts_policy": {
                "canonical": True,
                "trace": SUPABASE_PERSIST_TRACE_ARTIFACTS,
                "debug": SUPABASE_PERSIST_DEBUG_ARTIFACTS,
                "retention_days": {
                    "canonical": SUPABASE_RETENTION_CANONICAL_DAYS,
                    "trace": SUPABASE_RETENTION_TRACE_DAYS,
                    "debug": SUPABASE_RETENTION_DEBUG_DAYS,
                },
            },
        },
    )

    pauta_storage = _upload_artifact(
        local_path=pauta_path,
        object_path=f"{artifacts_prefix}/inputs/pauta/{_safe_filename(pauta_name)}",
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    pauta_doc = _RUN_REPOSITORY.create_document(
        tenant_id=tenant_id,
        project_id=project_id,
        created_by=created_by,
        title=pauta_name,
        document_type="pauta_excel",
        status="draft",
        content={
            "filename": pauta_name,
            "size_bytes": pauta_size,
            "sha256": pauta_sha,
            "storage": pauta_storage["storage"],
        },
        source_hash=pauta_sha,
        source_size_bytes=pauta_size,
        source_mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    pdf_docs: dict[str, dict[str, Any]] = {}
    for pdf_path in pdf_paths:
        name = os.path.basename(pdf_path)
        pdf_sha = _sha256_file(pdf_path)
        pdf_size = os.path.getsize(pdf_path)
        pdf_storage = _upload_artifact(
            local_path=pdf_path,
            object_path=f"{artifacts_prefix}/inputs/pdfs/{_safe_filename(name)}",
            content_type="application/pdf",
        )
        pdf_doc = _RUN_REPOSITORY.create_document(
            tenant_id=tenant_id,
            project_id=project_id,
            created_by=created_by,
            title=name,
            document_type="budget_pdf",
            status="draft",
            content={
                "filename": name,
                "size_bytes": pdf_size,
                "sha256": pdf_sha,
                "storage": pdf_storage["storage"],
            },
            source_hash=pdf_sha,
            source_size_bytes=pdf_size,
            source_mime="application/pdf",
        )
        pdf_docs[name] = {
            "id": pdf_doc["id"],
            "sha256": pdf_sha,
            "size_bytes": pdf_size,
            "storage": pdf_storage["storage"],
        }

    return {
        "enabled": True,
        "project_id": project_id,
        "project_name": project_name,
        "tenant_id": tenant_id,
        "created_by": created_by,
        "task_id": task.get("id"),
        "run_id": (run or {}).get("id"),
        "artifacts_prefix": artifacts_prefix,
        "pauta_document_id": pauta_doc.get("id"),
        "pdf_document_ids": pdf_docs,
    }


def _persist_execution_result(
    *,
    job_id: str,
    timestamp: str,
    workspace_dir: str,
    pdf_paths: list[str],
    output_excel_path: Optional[str],
    final_status: str,
    error_message: Optional[str],
    execution_cost: Optional[dict[str, Any]] = None,
) -> None:
    """
    Persists execution outputs into tasks/extractions/variables and budget_runs.
    """
    ctx = JOBS.get(job_id, {}).get("persistence")
    if not ctx or not ctx.get("enabled"):
        return
    if _RUN_REPOSITORY is None:
        return

    tenant_id = ctx["tenant_id"]
    project_id = ctx["project_id"]
    created_by = ctx["created_by"]
    task_id = ctx.get("task_id")
    run_id = ctx.get("run_id")
    artifacts_prefix = str(ctx.get("artifacts_prefix") or "")
    pdf_document_ids: dict[str, dict[str, Any]] = ctx.get("pdf_document_ids", {})

    output_base_dir = os.path.join(workspace_dir, "output")
    extraction_rows = []
    completed_extractions = 0
    failed_extractions = 0
    persistence_errors = 0
    output_excel_storage: dict[str, Any] | None = None
    artifact_retention_by_class = {
        "canonical": SUPABASE_RETENTION_CANONICAL_DAYS,
        "trace": SUPABASE_RETENTION_TRACE_DAYS,
        "debug": SUPABASE_RETENTION_DEBUG_DAYS,
    }

    def _store_artifact(
        *,
        artifact_refs: dict[str, Any],
        artifact_class: str,
        key: str,
        local_path: str,
        object_path: str,
        content_type: str,
        enabled: bool = True,
    ) -> None:
        if not enabled or not os.path.exists(local_path):
            return
        try:
            upload = _upload_artifact(
                local_path=local_path,
                object_path=object_path,
                content_type=content_type,
            )
            artifact_refs.setdefault(artifact_class, {})[key] = {
                **upload["storage"],
                "class": artifact_class,
                "retention_days": artifact_retention_by_class[artifact_class],
            }
        except Exception as ex:
            print(f"⚠️  Could not upload {key}: {ex}")

    for pdf_path in pdf_paths:
        filename = os.path.basename(pdf_path)
        safe_name = _safe_dirname(os.path.splitext(filename)[0])
        pdf_output_dir = os.path.join(output_base_dir, safe_name)
        final_json_path = os.path.join(pdf_output_dir, f"FINAL_{safe_name}.json")
        plan_path = os.path.join(pdf_output_dir, "plan_log.json")
        project_details_path = os.path.join(pdf_output_dir, "project_details.json")
        mapping_batches_dir = os.path.join(pdf_output_dir, "mapping_batches")
        cap_mapping_path = os.path.join(mapping_batches_dir, f"CAP_MAPPING_{safe_name}.json")
        mapping_links_path = os.path.join(mapping_batches_dir, f"MAPPING_LINKS_{safe_name}.json")
        extra_review_path = os.path.join(mapping_batches_dir, f"EXTRA_REVIEW_{safe_name}.json")
        final_mapping_path = os.path.join(pdf_output_dir, f"MAPPING_LINKS_FINAL_{safe_name}.json")
        audit_input_path = os.path.join(pdf_output_dir, "audit_qualitative_input.json")
        audit_output_path = os.path.join(pdf_output_dir, f"AUDITORIA_{safe_name}.json")
        audit_enriched_path = os.path.join(pdf_output_dir, f"AUDITORIA_ENRIQUECIDA_{safe_name}.json")
        audit_validated_path = os.path.join(pdf_output_dir, f"AUDITORIA_VALIDADA_{safe_name}.json")
        chunks_dir = os.path.join(pdf_output_dir, "chunks")
        debug_batches_dir = os.path.join(pdf_output_dir, "debug_batches")

        normalized_payload = _load_json(final_json_path) or {}
        plan_log = _load_json(plan_path) or {}
        project_details = _load_json(project_details_path) or {}
        artifact_refs: dict[str, Any] = {"canonical": {}, "trace": {}, "debug": {}}

        extraction_status = "completed" if normalized_payload else "failed"
        extraction_error = None if normalized_payload else "FINAL extraction JSON not found."
        doc_meta = pdf_document_ids.get(filename) or {}
        doc_sha = str(doc_meta.get("sha256") or _sha256_file(pdf_path))
        extraction_signature = _build_extraction_signature(
            file_sha256=doc_sha,
            backend=EXTRACTION_BACKEND,
            model=LANDING_AI_MODEL if EXTRACTION_BACKEND == "landingai" else "gemini-2.5-pro/flash",
            schema_path=LANDING_AI_SCHEMA_PATH or "",
        )

        # Canonical artifacts (always persisted).
        _store_artifact(
            artifact_refs=artifact_refs,
            artifact_class="canonical",
            key="final_json",
            local_path=final_json_path,
            object_path=f"{artifacts_prefix}/processing/{safe_name}/final.json",
            content_type="application/json",
        )
        _store_artifact(
            artifact_refs=artifact_refs,
            artifact_class="canonical",
            key="mapping_links_final",
            local_path=final_mapping_path,
            object_path=f"{artifacts_prefix}/processing/{safe_name}/mapping_links_final.json",
            content_type="application/json",
        )
        _store_artifact(
            artifact_refs=artifact_refs,
            artifact_class="canonical",
            key="audit_qualitative_input",
            local_path=audit_input_path,
            object_path=f"{artifacts_prefix}/processing/{safe_name}/audit_qualitative_input.json",
            content_type="application/json",
        )
        _store_artifact(
            artifact_refs=artifact_refs,
            artifact_class="canonical",
            key="auditoria_validada",
            local_path=audit_validated_path,
            object_path=f"{artifacts_prefix}/processing/{safe_name}/auditoria_validada.json",
            content_type="application/json",
        )
        _store_artifact(
            artifact_refs=artifact_refs,
            artifact_class="canonical",
            key="project_details",
            local_path=project_details_path,
            object_path=f"{artifacts_prefix}/processing/{safe_name}/project_details.json",
            content_type="application/json",
        )

        # Trace artifacts (optional).
        _store_artifact(
            artifact_refs=artifact_refs,
            artifact_class="trace",
            key="plan_log",
            local_path=plan_path,
            object_path=f"{artifacts_prefix}/processing/{safe_name}/plan_log.json",
            content_type="application/json",
            enabled=SUPABASE_PERSIST_TRACE_ARTIFACTS,
        )
        _store_artifact(
            artifact_refs=artifact_refs,
            artifact_class="trace",
            key="cap_mapping",
            local_path=cap_mapping_path,
            object_path=f"{artifacts_prefix}/processing/{safe_name}/cap_mapping.json",
            content_type="application/json",
            enabled=SUPABASE_PERSIST_TRACE_ARTIFACTS,
        )
        _store_artifact(
            artifact_refs=artifact_refs,
            artifact_class="trace",
            key="mapping_links",
            local_path=mapping_links_path,
            object_path=f"{artifacts_prefix}/processing/{safe_name}/mapping_links.json",
            content_type="application/json",
            enabled=SUPABASE_PERSIST_TRACE_ARTIFACTS,
        )
        _store_artifact(
            artifact_refs=artifact_refs,
            artifact_class="trace",
            key="extra_review",
            local_path=extra_review_path,
            object_path=f"{artifacts_prefix}/processing/{safe_name}/extra_review.json",
            content_type="application/json",
            enabled=SUPABASE_PERSIST_TRACE_ARTIFACTS,
        )
        _store_artifact(
            artifact_refs=artifact_refs,
            artifact_class="trace",
            key="auditoria",
            local_path=audit_output_path,
            object_path=f"{artifacts_prefix}/processing/{safe_name}/auditoria.json",
            content_type="application/json",
            enabled=SUPABASE_PERSIST_TRACE_ARTIFACTS,
        )
        _store_artifact(
            artifact_refs=artifact_refs,
            artifact_class="trace",
            key="auditoria_enriquecida",
            local_path=audit_enriched_path,
            object_path=f"{artifacts_prefix}/processing/{safe_name}/auditoria_enriquecida.json",
            content_type="application/json",
            enabled=SUPABASE_PERSIST_TRACE_ARTIFACTS,
        )

        # Debug artifacts (optional, potentially high volume).
        if SUPABASE_PERSIST_DEBUG_ARTIFACTS and os.path.isdir(chunks_dir):
            for chunk_file in os.listdir(chunks_dir):
                if not chunk_file.lower().endswith(".json"):
                    continue
                chunk_local = os.path.join(chunks_dir, chunk_file)
                _store_artifact(
                    artifact_refs=artifact_refs,
                    artifact_class="debug",
                    key=f"chunk_{chunk_file}",
                    local_path=chunk_local,
                    object_path=f"{artifacts_prefix}/processing/{safe_name}/chunks/{chunk_file}",
                    content_type="application/json",
                    enabled=True,
                )
        if SUPABASE_PERSIST_DEBUG_ARTIFACTS and os.path.isdir(debug_batches_dir):
            for debug_file in os.listdir(debug_batches_dir):
                if not debug_file.lower().endswith(".json"):
                    continue
                debug_local = os.path.join(debug_batches_dir, debug_file)
                _store_artifact(
                    artifact_refs=artifact_refs,
                    artifact_class="debug",
                    key=f"debug_{debug_file}",
                    local_path=debug_local,
                    object_path=f"{artifacts_prefix}/processing/{safe_name}/debug_batches/{debug_file}",
                    content_type="application/json",
                    enabled=True,
                )

        raw_payload = {
            "job_id": job_id,
            "timestamp": timestamp,
            "run_id": run_id,
            "safe_name": safe_name,
            "source_pdf": filename,
            "artifacts": artifact_refs,
            "artifacts_policy": {
                "canonical": True,
                "trace": SUPABASE_PERSIST_TRACE_ARTIFACTS,
                "debug": SUPABASE_PERSIST_DEBUG_ARTIFACTS,
                "retention_days": artifact_retention_by_class,
            },
            "plan_log": plan_log,
            "project_details": project_details,
            "execution_backend": EXTRACTION_BACKEND,
        }

        try:
            row = _RUN_REPOSITORY.create_extraction(
                tenant_id=tenant_id,
                project_id=project_id,
                created_by=created_by,
                document_id=doc_meta.get("id"),
                run_id=run_id,
                extraction_signature=extraction_signature,
                provider=EXTRACTION_BACKEND,
                status=extraction_status,
                raw_payload=raw_payload,
                normalized_payload=normalized_payload,
                warnings=[] if extraction_status == "completed" else ["missing_final_json"],
                error_message=extraction_error,
            )
            extraction_rows.append(row.get("id"))
            if extraction_status == "completed":
                completed_extractions += 1
            else:
                failed_extractions += 1
        except Exception as ex:
            persistence_errors += 1
            print(f"⚠️  Could not persist extraction for {filename}: {ex}")

    if output_excel_path and os.path.exists(output_excel_path):
        try:
            output_upload = _upload_artifact(
                local_path=output_excel_path,
                object_path=f"{artifacts_prefix}/outputs/{_safe_filename(os.path.basename(output_excel_path))}",
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            output_excel_storage = {
                **output_upload["storage"],
                "class": "canonical",
                "retention_days": SUPABASE_RETENTION_CANONICAL_DAYS,
            }
        except Exception as ex:
            print(f"⚠️  Could not upload output Excel for job {job_id}: {ex}")

    task_status = "done" if final_status == "completed" else "cancelled"
    execution_cost_payload = execution_cost if isinstance(execution_cost, dict) else {}
    billing_payload = (
        JOBS.get(job_id, {}).get("billing")
        if isinstance(JOBS.get(job_id, {}).get("billing"), dict)
        else {}
    )
    task_payload = {
        "kind": "budget_execution",
        "job_id": job_id,
        "timestamp": timestamp,
        "status_detail": final_status,
        "error": error_message,
        "result": {
            "output_excel": output_excel_storage,
            "extraction_count": len(extraction_rows),
            "completed_extractions": completed_extractions,
            "failed_extractions": failed_extractions,
            "persistence_errors": persistence_errors,
            "execution_cost": execution_cost_payload,
            "execution_cost_usd": execution_cost_payload.get("total_cost_usd"),
            "billing": billing_payload,
            "artifacts_policy": {
                "canonical": True,
                "trace": SUPABASE_PERSIST_TRACE_ARTIFACTS,
                "debug": SUPABASE_PERSIST_DEBUG_ARTIFACTS,
                "retention_days": artifact_retention_by_class,
            },
        },
    }

    if task_id:
        try:
            _RUN_REPOSITORY.update_task_run(task_id, status=task_status, payload=task_payload)
        except Exception as ex:
            print(f"⚠️  Could not update task {task_id}: {ex}")

    if run_id:
        try:
            _RUN_REPOSITORY.update_budget_run(
                run_id,
                status=final_status,
                result_payload={
                    "output_excel": output_excel_storage,
                    "extraction_ids": extraction_rows,
                    "execution_cost": execution_cost_payload,
                    "execution_cost_usd": execution_cost_payload.get("total_cost_usd"),
                    "billing": billing_payload,
                },
                error_message=error_message,
            )
        except Exception as ex:
            print(f"⚠️  Could not update budget run {run_id}: {ex}")

    try:
        _RUN_REPOSITORY.upsert_variable(
            tenant_id=tenant_id,
            project_id=project_id,
            created_by=created_by,
            variable_key="latest_budget_run",
            value={
                "job_id": job_id,
                "task_id": task_id,
                "run_id": run_id,
                "status": final_status,
                "output_excel": output_excel_storage,
                "updated_at": datetime.now().isoformat(),
            },
            source="budget-comparator-api",
            confidence=1.0,
            metadata={"backend": EXTRACTION_BACKEND},
        )
    except Exception as ex:
        print(f"⚠️  Could not upsert variable latest_budget_run: {ex}")


# ─── Active Gemini file registry ─────────────────────────────────────────────
# Thread-safe set of Gemini file names currently being used by an active worker.
# The pre-upload purge skips any name in this set so parallel PDF workers never
# delete each other's files mid-extraction.
import threading as _threading
_active_gemini_lock  = _threading.Lock()
_active_gemini_files: set[str] = set()

# Serialises the purge → upload_file() sequence.
# Held only for the short critical section (list → delete → upload_file call).
# Released before the slow "wait for ACTIVE" polling so other workers are not
# blocked for the full upload duration.
_upload_slot_lock = _threading.Lock()


