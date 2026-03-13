"""Runtime pipeline orchestration extracted from runtime monolith."""

import os
import json
import shutil
import time
import asyncio
import traceback
from concurrent.futures import ThreadPoolExecutor as _ThreadPoolExecutor
from typing import Any

from src.app.runtime import (
    AUDITOR_BATCH_SIZE,
    EXTRA_REVIEW_BATCH_SIZE,
    EXTRACTION_BACKEND,
    JOBS,
    LANDING_AI_MODEL,
    LANDING_AI_SCHEMA_PATH,
    PDF_SEMAPHORE_LIMIT,
    USER_OUTPUT_FAILURE_MESSAGE,
    USER_RUN_FAILURE_MESSAGE,
    _RUN_REPOSITORY,
    _TOKEN_COOLDOWN_SLEEP,
    _TPM_SOFT_LIMIT,
    _build_project_output_filename,
    _token_lock,
    _token_usage,
    cleanup_temp_folder,
    compute_concurrency,
    _safe_dirname,
)
from src.shared.billing.credit_service import maybe_refund_execution_credits
from src.features.runs.infrastructure.runtime_persistence_service import (
    _active_gemini_files,
    _active_gemini_lock,
    _upload_slot_lock,
    _persist_execution_result,
    _load_json,
)
from src.shared.observability.logger import get_logger

_logger = get_logger(__name__)


def _pipeline_print(*values: Any, sep: str = " ", **_: Any) -> None:
    """Bridge legacy print traces into the structured logger."""
    message = sep.join(str(value) for value in values)
    _logger.info(message)


print = _pipeline_print


def _register_gemini_file(name: str) -> None:
    with _active_gemini_lock:
        _active_gemini_files.add(name)


def _unregister_gemini_file(name: str) -> None:
    with _active_gemini_lock:
        _active_gemini_files.discard(name)


# ─── Pipeline helpers (mirror of main_final.py top-level helpers) ─────────────

# Minimum gap (seconds) enforced between successive upload_file() calls.
# The Gemini File API rate-limits upload session creation; this cooldown
# prevents the second worker from hitting the limit immediately after the first.
_UPLOAD_MIN_INTERVAL_S = 5
_last_upload_time: float = 0.0   # monotonic; protected by _upload_slot_lock


def _upload_pdf_to_gemini(pdf_path: str, genai):
    """Uploads a PDF to Gemini File API and waits until it is ACTIVE.

    Strategy
    --------
    1. Acquire _upload_slot_lock so only one worker runs purge+upload at a time.
    2. Enforce _UPLOAD_MIN_INTERVAL_S since the last upload to avoid Google's
       per-key rate limit on upload session creation (400 "Failed to create file").
    3. Purge orphaned files (not in _active_gemini_files) to stay under quota.
    4. Call upload_file() with up to 3 retries + exponential backoff (15 s base)
       — the 400 is transient; a short wait always resolves it.
    5. Register the file name immediately so the next worker's purge skips it.
    6. Release the lock, then poll for ACTIVE (slow — no lock held).
    """
    import time

    _MAX_UPLOAD_RETRIES  = 3
    _UPLOAD_RETRY_BASE_S = 15   # 15 s, 30 s, 60 s

    with _upload_slot_lock:
        global _last_upload_time

        # ── Rate-limit cooldown ───────────────────────────────────────────────
        elapsed = time.monotonic() - _last_upload_time
        if elapsed < _UPLOAD_MIN_INTERVAL_S:
            wait = _UPLOAD_MIN_INTERVAL_S - elapsed
            print(f"    ⏱️  Waiting {wait:.1f}s before upload (inter-upload cooldown)...")
            time.sleep(wait)

        # ── Pre-upload quota sweep (orphans only) ────────────────────────────
        try:
            existing = list(genai.list_files())
            if existing:
                with _active_gemini_lock:
                    active_snapshot = set(_active_gemini_files)
                orphans = [f for f in existing if f.name not in active_snapshot]
                if orphans:
                    print(
                        f"    🧹 Purging {len(orphans)} orphaned Gemini file(s) "
                        f"before upload ({len(existing) - len(orphans)} in-use kept)..."
                    )
                    for stale in orphans:
                        try:
                            genai.delete_file(stale.name)
                        except Exception:
                            pass
        except Exception as list_err:
            print(f"    ⚠️  Could not list/purge existing Gemini files: {list_err}")

        # ── upload_file() with retry ─────────────────────────────────────────
        # google.generativeai does NOT retry 400s internally; we must.
        print(f"    ☁️  Uploading PDF to Gemini: {pdf_path}...")
        file_ref = None
        last_upload_err = None
        for attempt in range(1, _MAX_UPLOAD_RETRIES + 1):
            try:
                file_ref = genai.upload_file(pdf_path, mime_type="application/pdf")
                _last_upload_time = time.monotonic()
                break  # success
            except Exception as up_err:
                last_upload_err = up_err
                if attempt < _MAX_UPLOAD_RETRIES:
                    delay = _UPLOAD_RETRY_BASE_S * attempt  # 15 s, 30 s
                    print(
                        f"    ⚠️  upload_file() failed (attempt {attempt}/{_MAX_UPLOAD_RETRIES}): "
                        f"{type(up_err).__name__}: {up_err} — retrying in {delay}s..."
                    )
                    time.sleep(delay)
                else:
                    print(
                        f"    ❌ upload_file() failed after {_MAX_UPLOAD_RETRIES} attempts: "
                        f"{last_upload_err}"
                    )
                    raise

        # Register IMMEDIATELY — before ACTIVE — so any concurrent purge skips it.
        _register_gemini_file(file_ref.name)

    # ── Lock released — poll for ACTIVE state ────────────────────────────────
    for _ in range(30):
        file_ref = genai.get_file(file_ref.name)
        if file_ref.state.name == "ACTIVE":
            print(f"    ✅ PDF ready in cloud: {file_ref.name}")
            return file_ref
        if file_ref.state.name == "FAILED":
            _unregister_gemini_file(file_ref.name)
            raise RuntimeError("File processing failed in Google Cloud.")
        print("    ⏳ Waiting for file to become ACTIVE...")
        time.sleep(2)
    _unregister_gemini_file(file_ref.name)
    raise RuntimeError(f"Timeout waiting for PDF to become ACTIVE: {pdf_path}")


def _extract_text_from_pdf(pdf_path: str) -> str:
    import fitz
    doc = fitz.open(pdf_path)
    pages = list(doc)
    return "".join(
        [f"--- PÁGINA {p.number + 1} ---\n{p.get_text()}" for p in pages]
    )


def _load_json(path: str):
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        return None


def _enrich_audit_data(audit_results, context_input_path, save_path=None):
    """Joins audit results with the original pauta descriptions."""
    context_input = _load_json(context_input_path)
    if not context_input:
        return audit_results

    context_map = {}
    for item in context_input:
        ref_obj = item.get("ref")
        if ref_obj and "codigo" in ref_obj:
            context_map[ref_obj["codigo"]] = ref_obj.get("desc", "")

    enriched_data = []
    for finding in audit_results:
        code = finding.get("codigo_pauta")
        if code == "EXTRA":
            finding["descripcion_original_pauta"] = None
        else:
            finding["descripcion_original_pauta"] = context_map.get(
                code, "Descripción no disponible."
            )
        enriched_data.append(finding)

    if save_path:
        try:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            with open(save_path, "w", encoding="utf-8") as f:
                json.dump(enriched_data, f, indent=4, ensure_ascii=False)
        except Exception:
            pass

    return enriched_data

def _normalize_cap_cod(entry: str) -> str:
    """Normalises a codigo_oferta entry to zero-padded "CAP::COD" format.

    Handles the two malformed variants the model occasionally produces:
      - Missing prefix:  "7.2"   → "07::7.2"
      - Unpadded CAP:    "7::7.2" → "07::7.2"
    Entries that already have a 2-digit CAP are returned unchanged.
    """
    parts = entry.split("::", 1)
    if len(parts) == 2:
        cap, cod = parts
        # Zero-pad the CAP part: "7" → "07", "10" stays "10"
        try:
            cap_norm = f"{int(cap):02d}"
        except ValueError:
            cap_norm = cap
        return f"{cap_norm}::{cod}"
    else:
        # No "::" at all — derive CAP from the leading digits before the dot
        cod = parts[0]
        dot_idx = cod.find(".")
        if dot_idx > 0:
            try:
                cap_norm = f"{int(cod[:dot_idx]):02d}"
                return f"{cap_norm}::{cod}"
            except ValueError:
                pass
        return cod  # Can't parse — leave as-is


def _inject_texto_oferta(audit_results, final_json_path):
    """Re-injects texto_oferta from the FINAL_ offer JSON after the audit runs.

    The AuditorAgent returns null for texto_oferta to save output tokens.
    This function reads the consolidated offer (FINAL_*.json), builds a
    lookup keyed by partida codigo, then for every finding maps each entry
    in codigo_oferta (format "CAP::COD") to its full text:
        "[CAP::COD] nombre\\ndescripcion"
    Multiple matches are joined with "\\n\\n---\\n\\n".
    OMISION findings (empty codigo_oferta list) get an empty string.

    Also normalises all codigo_oferta entries in-place to the canonical
    zero-padded "CAP::COD" format so downstream phases always see consistent keys.
    """
    if not audit_results or not final_json_path:
        return audit_results

    offer_data = _load_json(final_json_path)
    if not offer_data:
        return audit_results

    # Build lookup: partida_codigo -> (capitulo_codigo, nombre, descripcion)
    partida_map: dict[str, tuple[str, str, str]] = {}
    for cap in offer_data:
        cap_code = cap.get("capitulo_codigo", "")
        for p in cap.get("partidas", []):
            cod = p.get("codigo", "")
            if cod:
                partida_map[cod] = (
                    cap_code,
                    p.get("nombre", ""),
                    p.get("descripcion", ""),
                )

    for finding in audit_results:
        raw_list = finding.get("codigo_oferta") or []
        if not raw_list:
            finding["texto_oferta"] = ""
            continue

        # Normalise entries to canonical "CAP::COD" format in-place
        normalized_list = [_normalize_cap_cod(e) for e in raw_list]
        finding["codigo_oferta"] = normalized_list

        texts = []
        for entry in normalized_list:
            # entry is now guaranteed to be "CAP::COD"
            parts = entry.split("::", 1)
            raw_cod = parts[1] if len(parts) == 2 else parts[0]
            # Strip leading zeros from each numeric segment so "02.4" matches "2.4"
            def _unpad(cod: str) -> str:
                segs = []
                for seg in cod.split("."):
                    try:
                        segs.append(str(int(seg)))
                    except ValueError:
                        segs.append(seg)
                return ".".join(segs)
            lookup_cod = raw_cod if raw_cod in partida_map else _unpad(raw_cod)
            if lookup_cod in partida_map:
                cap_c, nombre, desc = partida_map[lookup_cod]
                # Use the already-normalised cap from entry, fall back to JSON cap
                cap_prefix = parts[0] if len(parts) == 2 else f"{int(cap_c):02d}"
                full_key = f"{cap_prefix}::{lookup_cod}"
                texts.append(f"[{full_key}] {nombre}\n{desc}" if desc else f"[{full_key}] {nombre}")
            else:
                # Fallback: use the normalised entry string; better than nothing
                texts.append(entry)

        finding["texto_oferta"] = "\n\n---\n\n".join(texts) if texts else ""

    return audit_results


# ─── Async token-rate-limit throttle ─────────────────────────────────────────
async def _throttle_tokens(estimated_tokens: int) -> None:
    """
    Soft-throttle before sending a Gemini request.

    Tracks tokens consumed in the current 60-second window.  If adding
    `estimated_tokens` would exceed _TPM_SOFT_LIMIT, sleeps
    _TOKEN_COOLDOWN_SLEEP seconds (repeating until the window resets or
    consumption falls below the limit).

    Token estimate: caller passes len(text) // 4 as a rough proxy for
    the true token count (1 token ≈ 4 characters for English/Spanish).
    """
    global _token_usage
    while True:
        async with _token_lock:
            now = time.monotonic()
            elapsed = now - _token_usage["window_start"]
            if elapsed >= 60.0:
                # New minute — reset counter
                _token_usage["count"] = 0
                _token_usage["window_start"] = now
                elapsed = 0.0

            projected = _token_usage["count"] + estimated_tokens
            if projected <= _TPM_SOFT_LIMIT:
                _token_usage["count"] = projected
                return  # safe to proceed

            remaining = 60.0 - elapsed
            print(
                f"    🚦 Token throttle: {_token_usage['count']:,} + {estimated_tokens:,} "
                f"≥ {_TPM_SOFT_LIMIT:,} TPM limit. "
                f"Sleeping {_TOKEN_COOLDOWN_SLEEP}s "
                f"({remaining:.0f}s until window resets)..."
            )
        await asyncio.sleep(_TOKEN_COOLDOWN_SLEEP)


# ─── Custom Exception ────────────────────────────────────────────────────────
class PipelineError(Exception):
    """
    Raised when a critical pipeline step fails.
    Carries a human-readable message that is forwarded to the Frontend.
    """
    pass


# ─── Core Pipeline ────────────────────────────────────────────────────────────
def run_pipeline(
    workspace_dir: str,
    pauta_path: str,
    pdf_paths: list,
    process_pdf_paths: list | None = None,
) -> str:
    """
    Entry point called from the FastAPI endpoint (sync context).

    Bootstraps shared state, converts any pauta Excel → JSON ONCE,
    then hands off to the async coordinator which processes PDFs in
    parallel (up to PDF_SEMAPHORE_LIMIT concurrent workers).

    Returns:
        Absolute path to the generated Excel output file.
    """
    started_at = time.perf_counter()
    from dotenv import load_dotenv
    import google.generativeai as genai

    from src.features.pauta.application.pauta_excel_mapper import map_excel_to_json
    from src.features.reporting.application.comparative_excel_builder import generar_comparativo_final

    load_dotenv()
    GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
    if not GOOGLE_API_KEY:
        raise RuntimeError(
            "GOOGLE_API_KEY is required by the current pipeline for Gemini-based "
            "mapping/audit phases (even when EXTRACTION_BACKEND=landingai)."
        )

    genai.configure(api_key=GOOGLE_API_KEY)
    _logger.info(
        "Pipeline execution started",
        extra={
            "workspace_dir": workspace_dir,
            "pauta_path": pauta_path,
            "pdf_count": len(pdf_paths),
            "process_pdf_count": len(process_pdf_paths or pdf_paths),
            "extraction_backend": EXTRACTION_BACKEND,
        },
    )

    # ── PHASE 0: Pauta Excel → JSON (once, shared by all PDF workers) ────────
    print("\n--- 📋 PHASE 0: PREPARING PAUTA ---")
    pauta_json_path = os.path.join(workspace_dir, "mapped_pauta.json")
    map_excel_to_json(pauta_path, pauta_json_path)
    print(f"  ✅ Pauta JSON saved: {pauta_json_path}")

    output_base_dir = os.path.join(workspace_dir, "output")
    os.makedirs(output_base_dir, exist_ok=True)

    # ── Reset per-run cost tracker ────────────────────────────────────────────
    from src.shared.observability.cost_tracker import reset as _reset_cost_tracker
    _reset_cost_tracker()

    # ── Run the async pipeline in a new event loop ────────────────────────────
    # asyncio.run() creates a fresh loop (safe when called from uvicorn's
    # sync thread-pool, which is where FastAPI routes `run_in_threadpool` tasks).
    output_excel_path = asyncio.run(
        _async_pipeline(
            workspace_dir=workspace_dir,
            pauta_json_path=pauta_json_path,
            output_base_dir=output_base_dir,
            pdf_paths=pdf_paths,
            process_pdf_paths=process_pdf_paths or pdf_paths,
            GOOGLE_API_KEY=GOOGLE_API_KEY,
        )
    )
    duration_ms = int((time.perf_counter() - started_at) * 1000)
    _logger.info(
        "Pipeline execution finished",
        extra={
            "duration_ms": duration_ms,
            "workspace_dir": workspace_dir,
            "output_excel_path": output_excel_path,
            "pdf_count": len(pdf_paths),
        },
    )
    return output_excel_path


async def _async_pipeline(
    workspace_dir: str,
    pauta_json_path: str,
    output_base_dir: str,
    pdf_paths: list,
    process_pdf_paths: list,
    GOOGLE_API_KEY: str,
) -> str:
    """
    Async coordinator.

    Launches up to PDF_SEMAPHORE_LIMIT PDF workers concurrently using
    asyncio.gather, then generates the final Excel once all workers finish.
    Each worker runs its blocking Gemini calls inside a ThreadPoolExecutor
    so the event loop is never blocked.
    """
    import google.generativeai as genai
    from src.features.reporting.application.comparative_excel_builder import generar_comparativo_final

    genai.configure(api_key=GOOGLE_API_KEY)

    # Compute per-request concurrency plan.
    # Adapts worker counts to the number of PDFs in THIS request so the total
    # API call rate stays within Gemini Tier-1 limits regardless of batch size.
    concurrency = compute_concurrency(len(process_pdf_paths))

    # Shared thread-pool shared by all workers for blocking I/O
    executor = _ThreadPoolExecutor(max_workers=PDF_SEMAPHORE_LIMIT * 2)
    sem = asyncio.Semaphore(PDF_SEMAPHORE_LIMIT)
    loop = asyncio.get_event_loop()

    print(
        f"\n[parallel] Pipeline: {len(process_pdf_paths)} PDF(s) to process "
        f"({len(pdf_paths)} total in run), "
        f"semaphore={PDF_SEMAPHORE_LIMIT}, extractor={EXTRACTION_BACKEND}"
    )
    _logger.info(
        "Async pipeline coordinator started",
        extra={
            "workspace_dir": workspace_dir,
            "pdf_count": len(pdf_paths),
            "process_pdf_count": len(process_pdf_paths),
            "semaphore_limit": PDF_SEMAPHORE_LIMIT,
            "extraction_backend": EXTRACTION_BACKEND,
        },
    )

    async def process_with_semaphore(idx, pdf_path):
        # Stagger PDF starts so multiple planners don't fire at the exact same
        # instant and stack-burst the per-minute quota.
        # idx is 1-based; first PDF starts immediately, each subsequent one
        # waits an extra 8 s before acquiring the semaphore.
        stagger_delay = (idx - 1) * 8
        if stagger_delay:
            await asyncio.sleep(stagger_delay)

        async with sem:
            filename = os.path.basename(pdf_path)
            print(f"\n{'='*60}")
            print(f"🚀 [{idx}/{len(process_pdf_paths)}] STARTING: {filename}")
            print(f"{'='*60}")
            # Run the synchronous per-PDF function inside a thread so the
            # event loop stays free for other workers and token throttle sleeps.
            try:
                await loop.run_in_executor(
                    executor,
                    _process_single_pdf,
                    idx,
                    len(process_pdf_paths),
                    pdf_path,
                    pauta_json_path,
                    output_base_dir,
                    GOOGLE_API_KEY,
                    loop,
                    concurrency,
                )
            except Exception:
                print(f"\n❌ [{idx}/{len(process_pdf_paths)}] FAILED: {filename}")
                _logger.exception(
                    "Single PDF processing failed",
                    extra={"pdf_filename": filename, "pdf_index": idx, "pdf_total": len(process_pdf_paths)},
                )
                raise

    tasks = [
        process_with_semaphore(idx, pdf_path)
        for idx, pdf_path in enumerate(process_pdf_paths, 1)
    ]

    # ── STRICT BARRIER: await ALL tasks before proceeding ────────────────────
    # return_exceptions=True so one failure does not cancel sibling workers.
    print(f"\n⏳ Waiting for all {len(tasks)} PDF worker(s) to finish...")
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # ── Post-barrier audit ────────────────────────────────────────────────────
    failed:    list[tuple[str, BaseException]] = []
    succeeded: list[str] = []

    for pdf_path, r in zip(pdf_paths, results):
        filename = os.path.basename(pdf_path)
        if isinstance(r, BaseException):
            failed.append((filename, r))
            print(f"  💀 [{filename}] FAILED — {type(r).__name__}: {r}")
        else:
            succeeded.append(filename)
            print(f"  ✅ [{filename}] Completed successfully.")

    # Abort if every single PDF failed — nothing to consolidate
    if len(failed) == len(pdf_paths):
        details = "; ".join(f"{n}: {e}" for n, e in failed)
        _logger.error(
            "All PDF tasks failed; pipeline cannot continue",
            extra={"failed_count": len(failed), "pdf_count": len(pdf_paths), "error": details},
        )
        raise PipelineError(
            f"All {len(pdf_paths)} PDF(s) failed — cannot generate output.\n"
            f"Details: {details}"
        )

    # Warn on partial failure but continue with whatever succeeded
    if failed:
        _logger.warning(
            "Some PDF tasks failed; pipeline will continue with successful files",
            extra={
                "failed_count": len(failed),
                "succeeded_count": len(succeeded),
                "pdf_count": len(pdf_paths),
            },
        )
        print(
            f"\n⚠️  {len(failed)}/{len(pdf_paths)} PDF(s) failed. "
            f"Continuing to Excel with {len(succeeded)} successful result(s): "
            + ", ".join(succeeded)
        )

    # Barrier passed: all surviving workers have finished writing to disk
    print(
        f"\n✅ {len(succeeded)}/{len(pdf_paths)} PDF task(s) completed successfully. "
        f"Starting consolidation phase..."
    )

    # ── Fix: copy mapped_pauta.json into output_base_dir ─────────────────────
    # generar_comparativo_final → cargar_todo() expects mapped_pauta.json to
    # live inside the output directory.  It is written to workspace_dir root
    # by run_pipeline() (Phase 0), so we copy it here — after the barrier,
    # guaranteeing all workers have written their output files first.
    _logger.info(
        "PDF processing barrier completed; starting consolidation phase",
        extra={
            "succeeded_count": len(succeeded),
            "failed_count": len(failed),
            "pdf_count": len(pdf_paths),
        },
    )

    src_pauta = pauta_json_path                               # workspace_dir/mapped_pauta.json
    dst_pauta = os.path.join(output_base_dir, "mapped_pauta.json")

    if not os.path.exists(src_pauta):
        raise FileNotFoundError(
            f"mapped_pauta.json not found at expected path: {src_pauta}\n"
            f"Phase 0 (map_excel_to_json) may have failed silently."
        )

    if not os.path.exists(dst_pauta):
        try:
            shutil.copy(src_pauta, dst_pauta)
            print(f"  📋 Copied mapped_pauta.json → {dst_pauta}")
        except Exception as e:
            raise RuntimeError(
                f"Could not copy mapped_pauta.json to output directory: {e}"
            ) from e
    else:
        print(f"  ♻️  mapped_pauta.json already present in output dir — skipping copy.")

    # Sanity-check: confirm the destination file is readable valid JSON
    try:
        with open(dst_pauta, "r", encoding="utf-8") as _fh:
            json.load(_fh)
        print(f"  ✅ mapped_pauta.json verified (readable + valid JSON).")
    except Exception as e:
        raise RuntimeError(
            f"mapped_pauta.json exists at {dst_pauta} but is not valid JSON: {e}"
        ) from e

    # ── FINAL PHASE: Comparative Excel ───────────────────────────────────────
    print("\n" + "=" * 60)
    print("🔄 FINAL PHASE: GENERATING MASTER COMPARATIVE EXCEL")
    print("=" * 60)

    output_excel_path = os.path.join(workspace_dir, "COMPARATIVO_MAESTRO_FINAL.xlsx")
    try:
        await loop.run_in_executor(
            executor, generar_comparativo_final, output_base_dir, output_excel_path
        )
        print(f"\n🚀 PROCESS COMPLETE!")
        print(f"📄 Output: {output_excel_path}")
        _logger.info(
            "Final comparative Excel generated",
            extra={"workspace_dir": workspace_dir, "output_excel_path": output_excel_path},
        )
        from src.shared.observability.cost_tracker import print_cost_summary as _print_cost
        _print_cost()
    except FileNotFoundError as e:
        print(f"\n❌ FileNotFoundError in final Excel generation:")
        _logger.exception(
            "Final Excel generation failed due to missing input files",
            extra={"workspace_dir": workspace_dir, "error": str(e)},
        )
        raise RuntimeError(
            f"Final Excel generation failed — a required file is missing.\n"
            f"output_base_dir contents: {os.listdir(output_base_dir)}\n"
            f"Original error: {e}"
        ) from e
    except Exception as e:
        print(f"\n❌ Unexpected error in final Excel generation:")
        _logger.exception(
            "Final Excel generation failed with unexpected error",
            extra={"workspace_dir": workspace_dir, "error": str(e)},
        )
        raise RuntimeError(
            f"Error generating comparative Excel: {type(e).__name__}: {e}"
        ) from e
    finally:
        executor.shutdown(wait=False)

    return output_excel_path


def _process_single_pdf(
    idx: int,
    total: int,
    pdf_path: str,
    pauta_json_path: str,
    output_base_dir: str,
    GOOGLE_API_KEY: str,
    loop: asyncio.AbstractEventLoop,
    concurrency: dict,
) -> None:
    """
    Synchronous per-PDF pipeline worker.  Runs in a thread-pool thread.

    Handles phases 1-7.5 for a single PDF:
      1. Planning
      2. Chunk extraction (parallel threads within this function)
      3. Consolidation
      4. Mapping + extra review
      5. Audit input generation
      6. Audit Phase 1
      7. Audit Phase 2 (Judge)
      7.5 Post-audit sanitisation

    Token throttling is invoked via `asyncio.run_coroutine_threadsafe`
    so the async _throttle_tokens coroutine runs on the shared event loop.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import google.generativeai as genai

    from src.features.mapping.application.mapper_agent import MapperAgent
    from src.features.mapping.application.extra_review_agent import ExtraReviewAgent
    from src.features.mapping.application.chapter_mapping_deriver import (
        derive_chapter_mapping_from_links,
    )
    from src.features.audit.application.auditor_agent import AuditorAgent
    from src.features.audit.application.audit_reflector import AuditReflector

    from src.features.extraction.application.chunk_consolidator import consolidate_chunks
    from src.features.mapping.application.mapping_applier import apply_mapping_to_json
    from src.features.audit.application.audit_input_builder import generate_audit_qualitative_input
    from src.features.extraction.infrastructure.ade_client import AdeHTTPError
    from src.features.extraction.application.landing_ai_service import extract_offer_to_internal_payload
    from src.features.extraction.application.chapter_merger import merge_duplicate_chapters
    from src.features.audit.application.post_audit_sanitizer import sanitizar_asignaciones_post_auditoria

    genai.configure(api_key=GOOGLE_API_KEY)

    planner = None
    extractor = None
    mapper        = MapperAgent(GOOGLE_API_KEY)
    extra_reviewer = ExtraReviewAgent(GOOGLE_API_KEY)
    auditor       = AuditorAgent(GOOGLE_API_KEY)
    reflector     = AuditReflector(GOOGLE_API_KEY)

    filename    = os.path.basename(pdf_path)
    pdf_name    = os.path.splitext(filename)[0]
    safe_name   = _safe_dirname(pdf_name)
    pdf_output_dir = os.path.join(output_base_dir, safe_name)
    chunks_dir  = os.path.join(pdf_output_dir, "chunks")
    os.makedirs(chunks_dir, exist_ok=True)

    def _throttle_sync(estimated_tokens: int) -> None:
        """Bridge: calls the async throttle from this sync thread."""
        future = asyncio.run_coroutine_threadsafe(
            _throttle_tokens(estimated_tokens), loop
        )
        future.result()  # block this thread until throttle ok

    def _is_landingai_payment_error(exc: Exception) -> bool:
        current: Exception | None = exc
        depth = 0
        while current is not None and depth < 8:
            if isinstance(current, AdeHTTPError) and getattr(current, "status", None) == 402:
                return True
            text = str(current).lower()
            if "payment required" in text and (
                "insufficient" in text or "balance" in text
            ):
                return True
            next_exc = current.__cause__ or current.__context__
            current = next_exc if isinstance(next_exc, Exception) else None
            depth += 1
        return False

    # ── 1. EXTRACTION ─────────────────────────────────────────────────────────
    plan_path = os.path.join(pdf_output_dir, "plan_log.json")
    final_json_path = None          # set by whichever backend runs
    extraction_plan = None          # set only by Gemini path
    cached_final_json_path = os.path.join(pdf_output_dir, f"FINAL_{safe_name}.json")
    if os.path.exists(cached_final_json_path):
        final_json_path = cached_final_json_path
        print(f"    [{safe_name}] ♻️  FINAL_ found (rerun cache) — skipping extraction.")

    if EXTRACTION_BACKEND == "landingai":
        # ── LandingAI path: ADE Parse + Extract  →  FINAL_ JSON directly ────
        # No planning / chunk-splitting / consolidation needed — ADE returns
        # a single validated JSON with chapters and partidas.
        print(
            f"    [{safe_name}] LandingAI extraction enabled "
            f"(model={LANDING_AI_MODEL}) — replacing planning/chunk extraction."
        )

        landing_api_key = (
            os.getenv("ADE_API_KEY")
            or os.getenv("VISION_AGENT_API_KEY")
        )
        if not landing_api_key:
            raise PipelineError(
                f"[{filename}] EXTRACTION_BACKEND=landingai requires "
                "ADE_API_KEY or VISION_AGENT_API_KEY in the environment."
            )

        final_json_path = os.path.join(pdf_output_dir, f"FINAL_{safe_name}.json")

        if os.path.exists(final_json_path):
            # Re-use cached extraction — skip API call
            print(f"    [{safe_name}] 📂 FINAL_ found (cache) — skipping extraction.")
        else:
            try:
                _extraction_result = extract_offer_to_internal_payload(
                    pdf_path=pdf_path,
                    api_key=landing_api_key,
                    model=LANDING_AI_MODEL,
                    schema_path=LANDING_AI_SCHEMA_PATH,
                )

                # Unpack new return format {"chapters": [...], "project_details": {...}}
                if isinstance(_extraction_result, dict):
                    _chapters_raw    = _extraction_result.get("chapters", [])
                    _project_details = _extraction_result.get("project_details") or {}
                else:
                    _chapters_raw    = _extraction_result  # backward compat
                    _project_details = {}

                # -- Merge duplicate chapters + fill missing partida codes ------
                print(f"    [{safe_name}] [cleanup] Merging duplicate chapters / filling codes...")
                extracted_offer = merge_duplicate_chapters(_chapters_raw)  # always list

                # Persist project_details for downstream use (Excel header, etc.)
                if _project_details:
                    _pd_path = os.path.join(pdf_output_dir, "project_details.json")
                    with open(_pd_path, "w", encoding="utf-8") as f:
                        json.dump(_project_details, f, indent=4, ensure_ascii=False)

                # Write the authoritative FINAL_ file consumed by all later phases
                with open(final_json_path, "w", encoding="utf-8") as f:
                    json.dump(extracted_offer, f, indent=4, ensure_ascii=False)

                # Also write a chunk copy (useful for debugging / manual inspection)
                for chunk_file in os.listdir(chunks_dir):
                    if chunk_file.lower().endswith(".json"):
                        os.remove(os.path.join(chunks_dir, chunk_file))
                landing_chunk_path = os.path.join(chunks_dir, "chunk_landingai_1.json")
                with open(landing_chunk_path, "w", encoding="utf-8") as f:
                    json.dump(extracted_offer, f, indent=4, ensure_ascii=False)

                # Write plan log for traceability
                with open(plan_path, "w", encoding="utf-8") as f:
                    json.dump(
                        {
                            "source": "landingai",
                            "model": LANDING_AI_MODEL,
                            "tasks": [{"id": "landingai_1"}],
                            "chapters_extracted": len(extracted_offer),
                            "total_partidas": sum(
                                len(ch.get("partidas", []))
                                for ch in extracted_offer
                            ),
                        },
                        f,
                        indent=4,
                        ensure_ascii=False,
                    )

                print(
                    f"    [{safe_name}] ✅ LandingAI extracted {len(extracted_offer)} "
                    f"chapter(s), {sum(len(c.get('partidas',[])) for c in extracted_offer)} "
                    f"partida(s) → FINAL_{safe_name}.json"
                )
            except Exception as e:
                if _is_landingai_payment_error(e):
                    print(
                        f"    [{safe_name}] LandingAI no disponible por saldo (402). "
                        "Fallback automatico a extraccion Gemini."
                    )
                    final_json_path = None
                else:
                    raise PipelineError(
                        f"[{filename}] LandingAI extraction failed: {type(e).__name__}: {e}"
                    ) from e

        # Validate that the FINAL_ file is loadable and non-empty
        if final_json_path:
            _final_data = _load_json(final_json_path)
            if not _final_data:
                raise PipelineError(
                    f"[{filename}] FINAL_ JSON is empty or unreadable after LandingAI extraction."
                )
            print(
                f"    [{safe_name}] ✅ FINAL_ verified: "
                f"{len(_final_data)} chapter(s), "
                f"{sum(len(c.get('partidas',[])) for c in _final_data)} partida(s)."
            )
    # ── Gemini path: Planning → Chunk extraction → Consolidation ─────────────
    # gemini_file holds the File API reference once the PDF is uploaded.
    # It is set here (during planning) when a new plan must be generated, so the
    # same reference is reused during chunk extraction without a second upload.
    gemini_file = None
    if final_json_path is None and os.path.exists(plan_path):
        from src.features.extraction.application.planner_agent import PlannerAgent
        from src.features.extraction.application.extractor_agent import ExtractorAgent
        planner = PlannerAgent(GOOGLE_API_KEY)
        extractor = ExtractorAgent(GOOGLE_API_KEY)
        print(f"    [{safe_name}] 📂 Extraction plan found — loading from cache...")
        extraction_plan = _load_json(plan_path)
    elif final_json_path is None:
        from src.features.extraction.application.planner_agent import PlannerAgent
        from src.features.extraction.application.extractor_agent import ExtractorAgent
        planner = PlannerAgent(GOOGLE_API_KEY)
        extractor = ExtractorAgent(GOOGLE_API_KEY)
        print(f"    [{safe_name}] 🧠 Generating extraction plan...")
        try:
            print(f"    [{safe_name}] ⬆️  Uploading PDF to Gemini File API (planner)...")
            gemini_file = _upload_pdf_to_gemini(pdf_path, genai)
            extraction_plan = planner.generate_extraction_plan(gemini_file)
            with open(plan_path, "w", encoding="utf-8") as f:
                json.dump(extraction_plan, f, indent=4, ensure_ascii=False)
            print(f"    [{safe_name}] ✅ Plan generated and saved.")
        except Exception as e:
            raise PipelineError(
                f"[{filename}] Failed to generate extraction plan: {e}"
            ) from e


    #  Gemini-only: plan validation, chunk extraction, consolidation 
    # LandingAI sets final_json_path above; this entire block is skipped.
    if final_json_path is None:
        if extractor is None:
            raise PipelineError(f"[{filename}] Extractor agent not initialized.")
        if not extraction_plan or "tasks" not in extraction_plan:
            raise PipelineError(
                f"[{filename}] Extraction plan is invalid or missing 'tasks'."
            )

        tasks = extraction_plan["tasks"]

        # ── 2. EXTRACTION (CHUNKS) ───────────────────────────────────────────────
        pending_tasks = []
        for task in tasks:
            task_id = task.get("id")
            chunk_path = os.path.join(chunks_dir, f"chunk_{task_id}.json")
            if not os.path.exists(chunk_path):
                pending_tasks.append((task, chunk_path))
            else:
                print(f"      [{safe_name}] ♻️  Chunk {task_id} already exists (skipping)")

        if pending_tasks:
            if gemini_file is None:
                print(f"    [{safe_name}] ⬆️  Uploading PDF to Gemini File API...")
                try:
                    gemini_file = _upload_pdf_to_gemini(pdf_path, genai)
                except Exception as e:
                    raise PipelineError(
                        f"[{filename}] Failed to upload PDF to Gemini File API: {e}"
                    ) from e
            else:
                print(f"    [{safe_name}] ♻️  Reusing PDF already uploaded to Gemini (from planner).")

            # Estimate tokens for all pending chunks and throttle once up front
            total_prompt_chars = sum(
                len(str(t.get("prompt_especifico", ""))) + len(str(t.get("rango_partidas", "")))
                for t, _ in pending_tasks
            )
            _throttle_sync(total_prompt_chars // 4)

            print(
                    f"    [{safe_name}] [parallel] Extracting {len(pending_tasks)} chunks "
                    f"(max {concurrency['extractor_workers']} workers, gemini-2.5-pro)..."
                )

            def _parse_rango_codes(rango) -> list[str]:
                """Parse rango_partidas (string or list) into a flat list of code strings."""
                if isinstance(rango, list):
                    return [str(c).strip() for c in rango if str(c).strip()]
                import re as _re
                parts = _re.split(r"[,;\n]+", str(rango))
                return [p.strip() for p in parts if p.strip()]

            def _make_partial_task(base_task: dict, codes: list, part: int, total_parts: int) -> dict:
                """Clone task with a reduced code list and a self-contained fragment prompt. (Fix 3)"""
                sub = dict(base_task)
                sub["rango_partidas"] = codes
                # Explicit fragment instruction so the model returns a complete independent JSON list
                sub["prompt_especifico"] = (
                    f"[FRAGMENTO {part}/{total_parts}] "
                    f"Esta es una solicitud de extraccion INDEPENDIENTE para un SUBCONJUNTO de partidas. "
                    f"Extrae UNICAMENTE los codigos listados en rango_partidas. "
                    f"Devuelve una lista JSON COMPLETA y VALIDA. "
                    f"NO hagas referencia a otros fragmentos.\n\n"
                    + base_task.get("prompt_especifico", "")
                )
                return sub

            def _is_empty_result(data) -> bool:
                """True when extract_chunk returned the fail-safe empty sentinel {}."""
                if not data:
                    return True
                if isinstance(data, dict) and not any(data.values()):
                    return True
                return False

            def _extend_items(target: list, data) -> None:
                """Flatten a list or dict-of-lists into target, skipping placeholders."""
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and item.get("_skipped"):
                            continue
                        target.append(item)
                elif isinstance(data, dict) and not data.get("_skipped"):
                    for v in data.values():
                        if isinstance(v, list):
                            target.extend(v)

            def extract_and_save(task_tuple, _depth: int = 0):
                """
                Extracts a chunk and saves it.

                Failure strategy (in order):
                  1. extractor.extract_chunk() retries 3x with a 45-second hard timeout each.
                  2. If it returns {} (fail-safe sentinel): split codes in half and retry each
                     half as its own sub-chunk (up to depth 2 = quarters).
                  3. If impossible to split or all sub-chunks also fail: write a placeholder
                     and return (id, False, reason) so the pipeline keeps moving.
                """
                task, chunk_path = task_tuple
                task_id = task.get("id", "?")

                try:
                    chunk_data = extractor.extract_chunk(task, gemini_file)

                    # Fix 4: detect fail-safe empty return → try splitting
                    if _is_empty_result(chunk_data) and _depth < 2:
                        codes = _parse_rango_codes(task.get("rango_partidas", ""))
                        if len(codes) > 1:
                            mid    = len(codes) // 2
                            halves = [codes[:mid], codes[mid:]]
                            print(
                                f"      [{safe_name}] ✂️  Chunk {task_id}: empty result — "
                                f"splitting into 2 parts (depth {_depth+1}): "
                                f"{len(halves[0])} + {len(halves[1])} codes"
                            )
                            merged_items: list = []
                            for i, half_codes in enumerate(halves, 1):
                                suffix         = "abcdefgh"[i - 1]
                                sub_id         = f"{task_id}{suffix}"
                                sub_task       = _make_partial_task(task, half_codes, i, 2)
                                sub_task["id"] = sub_id
                                sub_chunk_path = os.path.join(chunks_dir, f"chunk_{sub_id}.json")
                                if os.path.exists(sub_chunk_path):
                                    print(f"      [{safe_name}] ♻️  Sub-chunk {sub_id} already exists — skipping.")
                                    loaded = _load_json(sub_chunk_path)
                                    if loaded and not _is_empty_result(loaded):
                                        _extend_items(merged_items, loaded)
                                    continue
                                _, sub_ok, _ = extract_and_save(
                                    (sub_task, sub_chunk_path), _depth=_depth + 1
                                )
                                if sub_ok:
                                    loaded = _load_json(sub_chunk_path)
                                    if loaded:
                                        _extend_items(merged_items, loaded)

                            if merged_items:
                                chunk_data = merged_items
                                print(
                                    f"      [{safe_name}] 🔗 Chunk {task_id}: "
                                    f"merged {len(merged_items)} items from sub-chunks."
                                )

                    # Fix 4: still empty → write placeholder, keep pipeline alive
                    if _is_empty_result(chunk_data):
                        placeholder = {"_skipped": True, "chunk_id": task_id, "partidas": []}
                        with open(chunk_path, "w", encoding="utf-8") as f:
                            json.dump(placeholder, f, indent=4, ensure_ascii=False)
                        print(
                            f"      ⚠️  ERROR: Chunk {task_id} skipped — "
                            f"placeholder written so pipeline can continue."
                        )
                        return (task_id, False, "Skipped (persistent AI failure — placeholder written)")

                    with open(chunk_path, "w", encoding="utf-8") as f:
                        json.dump(chunk_data, f, indent=4, ensure_ascii=False)
                    return (task_id, True, None)

                except Exception as unexpected_err:
                    # Last-resort: write placeholder, never crash the worker
                    print(
                        f"      💀 Chunk {task_id}: unexpected exception in extract_and_save "
                        f"(depth={_depth}): {type(unexpected_err).__name__}: {unexpected_err}"
                    )
                    _logger.exception(
                        "Chunk extraction failed with unexpected exception",
                        extra={"task_id": task_id, "pdf_name": safe_name, "error": str(unexpected_err)},
                    )
                    try:
                        with open(chunk_path, "w", encoding="utf-8") as f:
                            json.dump({"_skipped": True, "chunk_id": task_id, "partidas": []}, f, indent=4)
                    except Exception:
                        pass
                    return (task_id, False, str(unexpected_err))

            completed_count = 0
            skipped_chunks:  list[str] = []
            total_pending = len(pending_tasks)
            chunk_executor = ThreadPoolExecutor(max_workers=concurrency["extractor_workers"])
            try:
                futures = {
                    chunk_executor.submit(extract_and_save, t): t[0].get("id")
                    for t in pending_tasks
                }
                for future in as_completed(futures):
                    task_id = futures[future]
                    completed_count += 1
                    try:
                        tid, success, error = future.result(timeout=600)
                        if success:
                            print(
                                f"      [{safe_name}] ✅ Chunk {tid} "
                                f"({completed_count}/{total_pending})"
                            )
                        else:
                            # Placeholder was written — warn but keep going (fail-safe)
                            skipped_chunks.append(str(tid))
                            print(
                                f"      ⚠️  [{safe_name}] Chunk {tid} skipped "
                                f"({completed_count}/{total_pending}): {error}"
                            )
                    except Exception as e:
                        raise PipelineError(
                            f"[{filename}] Unexpected error in chunk {task_id}: {e}"
                        ) from e

                if skipped_chunks:
                    print(
                        f"    [{safe_name}] ⚠️  {len(skipped_chunks)} chunk(s) skipped "
                        f"(placeholder written): {', '.join(skipped_chunks)}"
                    )
                print(
                    f"    [{safe_name}] ✅ Extraction complete "
                    f"({completed_count - len(skipped_chunks)}/{total_pending} OK, "
                    f"{len(skipped_chunks)} skipped)"
                )

            finally:
                chunk_executor.shutdown(wait=False, cancel_futures=True)
                # Guarantee Gemini file deletion even when extraction crashes midway.
                # Unregister first so the file is not mistakenly kept by a concurrent
                # worker's purge sweep while we're deleting it.
                try:
                    gname = (
                        gemini_file["name"]
                        if isinstance(gemini_file, dict)
                        else getattr(gemini_file, "name", None)
                    )
                    if gname:
                        _unregister_gemini_file(gname)
                        genai.delete_file(gname)
                        print(f"    [{safe_name}] 🧹 PDF deleted from Gemini File API.")
                except Exception as _del_err:
                    print(f"    [{safe_name}] ⚠️  Could not delete Gemini file: {_del_err}")

        # ── 3. CONSOLIDATION ─────────────────────────────────────────────────────
        print(f"    [{safe_name}] 📦 Consolidating chunks...")
        final_json_path = consolidate_chunks(safe_name, output_base_dir=output_base_dir)
        if not final_json_path:
            raise PipelineError(
                f"[{filename}] Chunk consolidation failed — no output JSON produced."
            )
        print(f"    [{safe_name}] ✅ Consolidated JSON: {final_json_path}")


    # ── 4. MAPPING ───────────────────────────────────────────────────────────
    print(f"    [{safe_name}] 🗺️  Mapping offer to pauta...")
    mapping_batches_dir = os.path.join(pdf_output_dir, "mapping_batches")
    os.makedirs(mapping_batches_dir, exist_ok=True)

    # ── 4.0 CHAPTER MAPPING ──────────────────────────────────────────────────
    cap_mapping_path = os.path.join(mapping_batches_dir, f"CAP_MAPPING_{safe_name}.json")
    # 4.0 chapter mapping is derived from final partida links (no extra AI call).

    mapping_path = os.path.join(
        mapping_batches_dir, f"MAPPING_LINKS_{safe_name}.json"
    )
    final_mapping_path = os.path.join(
        pdf_output_dir, f"MAPPING_LINKS_FINAL_{safe_name}.json"
    )
    mapping_result = None

    if os.path.exists(mapping_path):
        print(f"    [{safe_name}] 📂 Loading cached mapping...")
        mapping_result = _load_json(mapping_path)
    elif os.path.exists(final_mapping_path):
        print(f"    [{safe_name}] ♻️  Loading final mapping cache...")
        mapping_result = _load_json(final_mapping_path)
        if mapping_result:
            with open(mapping_path, "w", encoding="utf-8") as f:
                json.dump(mapping_result, f, indent=4, ensure_ascii=False)

    if not mapping_result:
        print(f"    [{safe_name}] 🧠 Generating mapping with AI...")
        try:
            mapping_result = mapper.map_offer_to_pauta(
                pauta_json_path, final_json_path,
                max_workers=concurrency["mapper_workers"],
            )
            with open(mapping_path, "w", encoding="utf-8") as f:
                json.dump(mapping_result, f, indent=4, ensure_ascii=False)
            print(f"    [{safe_name}] 💾 Mapping saved: {mapping_path}")
        except Exception as e:
            raise PipelineError(
                f"[{filename}] AI mapping (MapperAgent) failed: {e}"
            ) from e

    # ── 4.1 EXTRA REVIEW ─────────────────────────────────────────────────────
    extra_review_path = os.path.join(
        mapping_batches_dir, f"EXTRA_REVIEW_{safe_name}.json"
    )

    if os.path.exists(extra_review_path):
        print(f"    [{safe_name}] 📂 Loading cached extras review...")
        try:
            with open(extra_review_path, "r", encoding="utf-8") as f:
                review_data = json.load(f)
            decisions = review_data.get("decisions", [])
            for item in decisions:
                if item.get("decision") == "MAP" and item.get("pauta_id"):
                    id_oferta = item.get("id_oferta")
                    mapping_result.setdefault("mapping", {})[id_oferta] = item["pauta_id"]
                    if id_oferta in mapping_result.get("extras", []):
                        mapping_result["extras"].remove(id_oferta)
            print(f"    [{safe_name}] ✅ Extras review loaded from cache.")
        except Exception as e:
            print(f"    [{safe_name}] ⚠️  Error loading extras review: {e}")

    elif mapping_result and mapping_result.get("extras"):
        print(
            f"    [{safe_name}] 🔎 Reviewing "
            f"{len(mapping_result.get('extras', []))} extras with AI..."
        )
        try:
            review_payload = extra_reviewer.review_extras(
                pauta_json_path,
                final_json_path,
                mapping_result,
                output_path=extra_review_path,
                batch_size=EXTRA_REVIEW_BATCH_SIZE,
            )
            mapping_result = review_payload.get("mapping_result", mapping_result)
            with open(mapping_path, "w", encoding="utf-8") as f:
                json.dump(mapping_result, f, indent=4, ensure_ascii=False)
            print(f"    [{safe_name}] ✅ Extras reviewed. Mapping updated.")
        except Exception as e:
            print(f"    [{safe_name}] ⚠️  Error reviewing extras: {e}")
    else:
        print(f"    [{safe_name}] ✅ No extras to review.")

    # ── 4.2 SAVE FINAL MAPPING ────────────────────────────────────────────────
    print(f"    [{safe_name}] 💾 Saving final mapping...")
    try:
        with open(final_mapping_path, "w", encoding="utf-8") as f:
            json.dump(mapping_result, f, indent=4, ensure_ascii=False)
    except Exception as e:
        print(f"    [{safe_name}] ⚠️  Error saving final mapping: {e}")

    try:
        offer_data = _load_json(final_json_path) or []
        cap_mapping_result = derive_chapter_mapping_from_links(
            mapping_result,
            offer_data=offer_data if isinstance(offer_data, list) else None,
        )
        with open(cap_mapping_path, "w", encoding="utf-8") as f:
            json.dump(cap_mapping_result, f, indent=4, ensure_ascii=False)
        print(
            f"    [{safe_name}] 💾 Chapter mapping derived from final links: "
            f"{cap_mapping_path}"
        )
    except Exception as e:
        print(
            f"    [{safe_name}] ⚠️  Could not derive chapter mapping from final links: {e}"
        )

    try:
        stats = apply_mapping_to_json(final_json_path, mapping_result)
    except Exception as e:
        raise PipelineError(
            f"[{filename}] apply_mapping_to_json failed: {e}"
        ) from e
    print(
        f"    [{safe_name}] ✅ Mapping applied: "
        f"{stats['mapped']} linked, {stats['extras']} extras."
    )

    # ── 5. AUDIT QUALITATIVE INPUT ────────────────────────────────────────────
    print(f"    [{safe_name}] 📋 Generating qualitative audit input...")
    audit_input_path = os.path.join(pdf_output_dir, "audit_qualitative_input.json")
    try:
        generate_audit_qualitative_input(
            pauta_json_path,
            final_json_path,
            mapping_result=mapping_result,
            output_path=audit_input_path,
        )
    except Exception as e:
        print(f"    [{safe_name}] ⚠️  Error generating audit_qualitative_input: {e}")

    # ── 6. AUDIT PHASE 1: DETECTION ───────────────────────────────────────────
    audit_output_path = os.path.join(pdf_output_dir, f"AUDITORIA_{safe_name}.json")
    audit_results = []

    if os.path.exists(audit_output_path):
        print(f"    [{safe_name}] ⏩ Audit Phase 1 already exists — loading...")
        audit_results = _load_json(audit_output_path)
    else:
        print(f"    [{safe_name}] 🕵️  Starting Audit Phase 1 (Detection)...")
        try:
            audit_results = auditor.run_full_audit(
                pauta_json_path,
                final_json_path,
                batch_size=AUDITOR_BATCH_SIZE,
                max_concurrency=concurrency["auditor_concurrency"],
            )
            # Save raw results (texto_oferta == null) for cache / re-run
            with open(audit_output_path, "w", encoding="utf-8") as f:
                json.dump(audit_results, f, indent=4, ensure_ascii=False)
            print(
                f"    [{safe_name}] ✅ Phase 1 complete: "
                f"{len(audit_results)} issues detected."
            )
        except Exception as e:
            raise PipelineError(
                f"[{filename}] Audit Phase 1 (AuditorAgent) failed: {e}"
            ) from e

    # ── Post-process: re-inject texto_oferta from FINAL_ JSON ────────────────
    # The auditor model returns null for texto_oferta to save output tokens.
    # We recover the full offer description here by mapping codigo_oferta back
    # to the consolidated offer JSON so downstream phases have rich text.
    if audit_results:
        audit_results = _inject_texto_oferta(audit_results, final_json_path)
        print(f"    [{safe_name}] 🔗 texto_oferta injected from offer JSON.")

    # ── 7. AUDIT PHASE 2: JUDGE (REFLECTOR) ──────────────────────────────────
    final_validated_path = os.path.join(
        pdf_output_dir, f"AUDITORIA_VALIDADA_{safe_name}.json"
    )
    juez_completado = False
    if os.path.exists(final_validated_path):
        existing_data = _load_json(final_validated_path)
        if existing_data and len(existing_data) > 0:
            juez_completado = True
        else:
            print(f"    [{safe_name}] ⚠️  Validated audit file corrupted/empty — regenerating.")

    if juez_completado:
        print(f"    [{safe_name}] ⏩ Audit Phase 2 already exists — skipping...")
    elif not audit_results:
        print(f"    [{safe_name}] ⚠️  Skipping Phase 2 (no Phase 1 results).")
    else:
        print(f"    [{safe_name}] ⚖️  Starting Phase 2: The Judge (PDF verification)...")
        if not os.path.exists(audit_input_path):
            print(f"    [{safe_name}] ⚠️  Missing 'audit_qualitative_input.json'.")

        enriched_save_path = os.path.join(
            pdf_output_dir, f"AUDITORIA_ENRIQUECIDA_{safe_name}.json"
        )
        enriched_audit = _enrich_audit_data(
            audit_results, audit_input_path, save_path=enriched_save_path
        )
        debug_batches_dir = os.path.join(pdf_output_dir, "debug_batches")
        try:
            validated_audit = reflector.review_audit(
                enriched_audit,
                pdf_path,
                debug_folder_path=debug_batches_dir,
                max_concurrency=concurrency["reflector_concurrency"],
            )
            if validated_audit and len(validated_audit) > 0:
                with open(final_validated_path, "w", encoding="utf-8") as f:
                    json.dump(validated_audit, f, indent=4, ensure_ascii=False)
                print(f"    [{safe_name}] 🏆 Validated audit saved.")
            else:
                print(f"    [{safe_name}] ❌ The Judge returned empty data.")
        except Exception as e:
            print(f"    [{safe_name}] ❌ CRITICAL ERROR IN JUDGE AGENT: {e}")

    # ── 7.5 POST-AUDIT SANITISATION ──────────────────────────────────────────
    if os.path.exists(final_validated_path):
        print(f"    [{safe_name}] 🧼 Sanitising duplicate assignments in audit...")
        try:
            san_stats = sanitizar_asignaciones_post_auditoria(
                final_validated_path, final_json_path
            )
            if not san_stats.get("skipped"):
                print(
                    f"    [{safe_name}] 📊 Sanitisation: "
                    f"{san_stats['eliminados']} duplicates, "
                    f"{san_stats['sincronizados']} items re-synced."
                )
        except Exception as e:
            print(f"    [{safe_name}] ⚠️  Sanitisation error (non-blocking): {e}")
    else:
        print(f"    [{safe_name}] ⏩ No validated audit — sanitisation skipped.")

    print(f"\n    [{safe_name}] ✅ PDF processing COMPLETE.")


# ─── Pipeline Worker ──────────────────────────────────────────────────────────
async def run_pipeline_worker(
    job_id: str,
    workspace_dir: str,
    pauta_path: str,
    pdf_paths: list,
    timestamp: str,
    process_pdf_paths: list | None = None,
) -> None:
    """
    Background async worker. Calls the blocking run_pipeline() in a thread-pool
    so the event loop stays free. Updates JOBS[job_id] throughout for polling.
    Launched via asyncio.create_task() — never awaited by the endpoint.
    """

    worker_started_at = time.perf_counter()
    _logger.info(
        "Pipeline worker started",
        extra={
            "job_id": job_id,
            "workspace_dir": workspace_dir,
            "pauta_path": pauta_path,
            "pdf_count": len(pdf_paths),
            "process_pdf_count": len(process_pdf_paths or pdf_paths),
        },
    )

    def _update(progress: int, message: str) -> None:
        JOBS[job_id]["progress"] = progress
        JOBS[job_id]["message"]  = message
        _logger.info(
            "Pipeline worker progress update",
            extra={"job_id": job_id, "progress": progress, "message_text": message},
        )

    def _snapshot_execution_cost() -> dict[str, Any]:
        try:
            from src.shared.observability.cost_tracker import get_cost_summary as _get_cost_summary

            return _get_cost_summary()
        except Exception:
            return {
                "currency": "USD",
                "models": [],
                "total_calls": 0,
                "total_input_tokens": 0,
                "total_output_tokens": 0,
                "total_cost_usd": 0.0,
            }

    _update(3, "Preparando entorno de trabajo...")

    # Heartbeat: gently nudges progress forward while the pipeline runs so the
    # frontend progress bar keeps moving even though we can't inject checkpoints
    # into the blocking run_pipeline() call.
    heartbeat_done = asyncio.Event()

    async def _heartbeat() -> None:
        while not heartbeat_done.is_set():
            await asyncio.sleep(30)
            if not heartbeat_done.is_set():
                current = JOBS[job_id]["progress"]
                if current < 88:
                    JOBS[job_id]["progress"] = min(current + 2, 88)

    heartbeat_task = asyncio.create_task(_heartbeat())

    try:
        to_process = process_pdf_paths or pdf_paths
        _update(5, f"Procesando {len(to_process)} PDF(s) con IA - esto puede tardar varios minutos...")
        loop = asyncio.get_running_loop()
        output_excel_path: str = await loop.run_in_executor(
            None, run_pipeline, workspace_dir, pauta_path, pdf_paths, to_process
        )
    except Exception as exc:
        _logger.exception(
            "Pipeline worker failed while running pipeline",
            extra={"job_id": job_id, "error": str(exc)},
        )
        execution_cost = _snapshot_execution_cost()
        JOBS[job_id].update({
            "status":   "failed",
            "progress": JOBS[job_id].get("progress", 0),
            "message":  USER_RUN_FAILURE_MESSAGE,
            "error":    USER_RUN_FAILURE_MESSAGE,
            "execution_cost": execution_cost,
        })
        try:
            maybe_refund_execution_credits(
                repo=_RUN_REPOSITORY,
                billing_context=JOBS.get(job_id, {}).get("billing"),
                actor_user_id=(JOBS.get(job_id, {}).get("persistence") or {}).get("created_by"),
                reason="pipeline_failure",
            )
        except Exception as refund_exc:
            _logger.warning(
                "Could not refund credits after pipeline failure",
                extra={"job_id": job_id, "error": str(refund_exc)},
            )
        try:
            _persist_execution_result(
                job_id=job_id,
                timestamp=timestamp,
                workspace_dir=workspace_dir,
                pdf_paths=pdf_paths,
                output_excel_path=None,
                final_status="failed",
                error_message=str(exc),
                execution_cost=execution_cost,
            )
        except Exception as persist_err:
            _logger.warning(
                "Persistence finalization failed after worker error",
                extra={"job_id": job_id, "error": str(persist_err)},
            )
        _logger.error(
            "Pipeline worker marked job as failed",
            extra={
                "job_id": job_id,
                "duration_ms": int((time.perf_counter() - worker_started_at) * 1000),
            },
        )
        cleanup_temp_folder(workspace_dir)
        return
    finally:
        heartbeat_done.set()
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass

    if not output_excel_path or not os.path.exists(output_excel_path):
        execution_cost = _snapshot_execution_cost()
        JOBS[job_id].update({
            "status":   "failed",
            "progress": 90,
            "message":  "El pipeline terminó pero no se generó el Excel.",
            "error":    "Output Excel not found after pipeline completion.",
            "execution_cost": execution_cost,
        })
        JOBS[job_id]["message"] = USER_OUTPUT_FAILURE_MESSAGE
        JOBS[job_id]["error"] = USER_OUTPUT_FAILURE_MESSAGE
        try:
            maybe_refund_execution_credits(
                repo=_RUN_REPOSITORY,
                billing_context=JOBS.get(job_id, {}).get("billing"),
                actor_user_id=(JOBS.get(job_id, {}).get("persistence") or {}).get("created_by"),
                reason="pipeline_output_missing",
            )
        except Exception as refund_exc:
            _logger.warning(
                "Could not refund credits after missing output",
                extra={"job_id": job_id, "error": str(refund_exc)},
            )
        try:
            _persist_execution_result(
                job_id=job_id,
                timestamp=timestamp,
                workspace_dir=workspace_dir,
                pdf_paths=pdf_paths,
                output_excel_path=None,
                final_status="failed",
                error_message="Output Excel not found after pipeline completion.",
                execution_cost=execution_cost,
            )
        except Exception as persist_err:
            _logger.warning(
                "Persistence finalization failed when output Excel was missing",
                extra={"job_id": job_id, "error": str(persist_err)},
            )
        _logger.error(
            "Pipeline worker completed without generating output Excel",
            extra={
                "job_id": job_id,
                "workspace_dir": workspace_dir,
                "duration_ms": int((time.perf_counter() - worker_started_at) * 1000),
            },
        )
        cleanup_temp_folder(workspace_dir)
        return

    project_name_for_output = str(JOBS.get(job_id, {}).get("project_name") or "").strip() or None
    desired_output_name = _build_project_output_filename(project_name_for_output, timestamp)
    desired_output_path = os.path.join(os.path.dirname(output_excel_path), desired_output_name)
    if os.path.abspath(output_excel_path) != os.path.abspath(desired_output_path):
        try:
            if os.path.exists(desired_output_path):
                os.remove(desired_output_path)
            os.replace(output_excel_path, desired_output_path)
            output_excel_path = desired_output_path
            _logger.info(
                "Renamed output Excel file",
                extra={"job_id": job_id, "output_filename": os.path.basename(output_excel_path)},
            )
        except Exception as rename_err:
            _logger.warning(
                "Could not rename output Excel file",
                extra={"job_id": job_id, "error": str(rename_err)},
            )

    # Persisted artifacts live in Supabase; keep local workspace only as temp.
    _update(95, "Guardando resultados...")
    final_excel = output_excel_path
    execution_cost = _snapshot_execution_cost()

    # ── Done ──────────────────────────────────────────────────────────────────
    JOBS[job_id].update({
        "status":    "completed",
        "progress":  100,
        "message":   "¡Proceso completado con éxito!",
        "file_path": final_excel,
        "execution_cost": execution_cost,
    })

    try:
        _persist_execution_result(
            job_id=job_id,
            timestamp=timestamp,
            workspace_dir=workspace_dir,
            pdf_paths=pdf_paths,
            output_excel_path=final_excel,
            final_status="completed",
            error_message=None,
            execution_cost=execution_cost,
        )
    except Exception as persist_err:
        _logger.warning(
            "Persistence finalization failed after successful pipeline run",
            extra={"job_id": job_id, "error": str(persist_err)},
        )

    _logger.info(
        "Pipeline worker completed successfully",
        extra={
            "job_id": job_id,
            "duration_ms": int((time.perf_counter() - worker_started_at) * 1000),
            "output_excel_path": final_excel,
        },
    )

    # Always cleanup local temp workspace after completion.
    asyncio.get_running_loop().call_later(5, lambda: cleanup_temp_folder(workspace_dir))


# ─── Endpoint 1: START ────────────────────────────────────────────────────────


