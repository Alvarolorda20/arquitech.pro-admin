"""
Fase 2 — Extracción de chunks
==============================
Input:  - ruta al PDF de la oferta
        - ruta al plan_log.json generado en la Fase 1
Output: {output_dir}/{safe_name}/chunks/chunk_{id}.json  (uno por tarea)

Sube el PDF a la Gemini File API y extrae cada chunk en paralelo.
Los chunks ya existentes se saltan automáticamente (caché).
"""

import os
import sys
import json
import re
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Añadir Backend/ al path ───────────────────────────────────────────────────
_BACKEND_DIR = os.path.join(os.path.dirname(__file__), "..", "..")
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)
_SRC_DIR = os.path.join(os.path.dirname(__file__), "..")
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

from agents.extractor_agent import ExtractorAgent


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _safe_dirname(name: str, max_len: int = 48) -> str:
    safe = re.sub(r'[\\/:*?"<>|&]', '_', name)
    safe = re.sub(r'[\s_]+', '_', safe)
    safe = safe.strip('_') or 'pdf'
    return safe[:max_len].rstrip('_')


def _load_json(path: str):
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        return None


def _upload_pdf_to_gemini(pdf_path: str, genai, max_retries: int = 3):
    """Sube el PDF a la Gemini File API y espera hasta que esté ACTIVE."""
    print(f"  ☁️  Subiendo PDF a Gemini: {os.path.basename(pdf_path)}")
    file_ref = None
    for attempt in range(1, max_retries + 1):
        try:
            file_ref = genai.upload_file(pdf_path, mime_type="application/pdf")
            break
        except Exception as e:
            if attempt < max_retries:
                wait = 15 * attempt
                print(f"  ⚠️  upload_file intento {attempt} falló: {e} — reintentando en {wait}s")
                time.sleep(wait)
            else:
                raise

    for _ in range(30):
        file_ref = genai.get_file(file_ref.name)
        if file_ref.state.name == "ACTIVE":
            print(f"  ✅ PDF listo en la nube: {file_ref.name}")
            return file_ref
        if file_ref.state.name == "FAILED":
            raise RuntimeError("El procesamiento del archivo falló en Google Cloud.")
        print("  ⏳ Esperando que el archivo quede ACTIVE...")
        time.sleep(2)
    raise RuntimeError(f"Timeout esperando que el PDF quede ACTIVE: {pdf_path}")


def _is_empty_result(data) -> bool:
    if not data:
        return True
    if isinstance(data, dict) and not any(data.values()):
        return True
    return False


def _parse_rango_codes(rango) -> list:
    if isinstance(rango, list):
        return [str(c).strip() for c in rango if str(c).strip()]
    import re as _re
    parts = _re.split(r"[,;\n]+", str(rango))
    return [p.strip() for p in parts if p.strip()]


def _extend_items(target: list, data) -> None:
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict) and item.get("_skipped"):
                continue
            target.append(item)
    elif isinstance(data, dict) and not data.get("_skipped"):
        for v in data.values():
            if isinstance(v, list):
                target.extend(v)


# ─── Función principal ────────────────────────────────────────────────────────

def run(
    pdf_path: str,
    plan_path: str,
    output_dir: str,
    api_key: str,
    max_workers: int = 10,
) -> str:
    """
    Ejecuta la Fase 2 para un PDF.

    Parámetros
    ----------
    pdf_path    : ruta al PDF de la oferta
    plan_path   : ruta al plan_log.json (salida de la Fase 1)
    output_dir  : directorio base de salida
    api_key     : GOOGLE_API_KEY
    max_workers : paralelismo de extracción de chunks (por defecto 10)

    Retorna
    -------
    Ruta al directorio de chunks generados.
    """
    import google.generativeai as genai
    genai.configure(api_key=api_key)

    plan = _load_json(plan_path)
    if not plan or "tasks" not in plan:
        raise ValueError(f"plan_log.json inválido o sin 'tasks': {plan_path}")

    filename  = os.path.basename(pdf_path)
    pdf_name  = os.path.splitext(filename)[0]
    safe_name = _safe_dirname(pdf_name)
    pdf_out   = os.path.join(output_dir, safe_name)
    chunks_dir = os.path.join(pdf_out, "chunks")
    os.makedirs(chunks_dir, exist_ok=True)

    tasks = plan["tasks"]
    pending = [
        (t, os.path.join(chunks_dir, f"chunk_{t['id']}.json"))
        for t in tasks
        if not os.path.exists(os.path.join(chunks_dir, f"chunk_{t['id']}.json"))
    ]

    if not pending:
        print(f"[{safe_name}] ♻️  Todos los chunks ya existen — nada que extraer.")
        return chunks_dir

    print(f"[{safe_name}] ⬆️  Subiendo PDF a Gemini File API...")
    gemini_file = _upload_pdf_to_gemini(pdf_path, genai)

    extractor = ExtractorAgent(api_key)

    def extract_and_save(task_tuple, _depth: int = 0):
        task, chunk_path = task_tuple
        task_id = task.get("id", "?")
        try:
            chunk_data = extractor.extract_chunk(task, gemini_file)

            if _is_empty_result(chunk_data) and _depth < 2:
                codes = _parse_rango_codes(task.get("rango_partidas", ""))
                if len(codes) > 1:
                    mid = len(codes) // 2
                    halves = [codes[:mid], codes[mid:]]
                    print(
                        f"  [{safe_name}] ✂️  Chunk {task_id}: resultado vacío — "
                        f"dividiendo en 2 partes (profundidad {_depth+1})"
                    )
                    merged_items: list = []
                    for i, half_codes in enumerate(halves, 1):
                        suffix = "abcdefgh"[i - 1]
                        sub_id = f"{task_id}{suffix}"
                        sub_task = dict(task)
                        sub_task["id"] = sub_id
                        sub_task["rango_partidas"] = half_codes
                        sub_chunk_path = os.path.join(chunks_dir, f"chunk_{sub_id}.json")
                        if os.path.exists(sub_chunk_path):
                            loaded = _load_json(sub_chunk_path)
                            if loaded and not _is_empty_result(loaded):
                                _extend_items(merged_items, loaded)
                            continue
                        _, sub_ok, _ = extract_and_save((sub_task, sub_chunk_path), _depth=_depth + 1)
                        if sub_ok:
                            loaded = _load_json(sub_chunk_path)
                            if loaded:
                                _extend_items(merged_items, loaded)
                    if merged_items:
                        chunk_data = merged_items

            if _is_empty_result(chunk_data):
                placeholder = {"_skipped": True, "chunk_id": task_id, "partidas": []}
                with open(chunk_path, "w", encoding="utf-8") as f:
                    json.dump(placeholder, f, indent=4, ensure_ascii=False)
                print(f"  ⚠️  Chunk {task_id} vaciado — placeholder escrito.")
                return (task_id, False, "Chunk persistentemente vacío")

            with open(chunk_path, "w", encoding="utf-8") as f:
                json.dump(chunk_data, f, indent=4, ensure_ascii=False)
            return (task_id, True, None)

        except Exception as e:
            print(f"  💀 Chunk {task_id} excepción inesperada: {type(e).__name__}: {e}")
            traceback.print_exc()
            try:
                with open(chunk_path, "w", encoding="utf-8") as f:
                    json.dump({"_skipped": True, "chunk_id": task_id, "partidas": []}, f, indent=4)
            except Exception:
                pass
            return (task_id, False, str(e))

    print(f"[{safe_name}] ⚡ Extrayendo {len(pending)} chunks (workers={max_workers})...")
    completed = 0
    skipped = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(extract_and_save, t): t[0].get("id") for t in pending}
        for future in as_completed(futures):
            completed += 1
            tid, success, error = future.result(timeout=600)
            if success:
                print(f"  [{safe_name}] ✅ Chunk {tid} ({completed}/{len(pending)})")
            else:
                skipped.append(str(tid))
                print(f"  ⚠️  [{safe_name}] Chunk {tid} saltado ({completed}/{len(pending)}): {error}")

    try:
        genai.delete_file(gemini_file.name)
        print(f"[{safe_name}] 🗑️  Archivo Gemini eliminado: {gemini_file.name}")
    except Exception:
        pass

    if skipped:
        print(f"[{safe_name}] ⚠️  Chunks con error: {', '.join(skipped)}")
    print(f"[{safe_name}] ✅ Extracción completa → {chunks_dir}")
    return chunks_dir


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # ── CONFIGURA AQUÍ ────────────────────────────────────────────────────────
    PDF_PATH   = r"..\..\temp_processing\request_20260219_143415_92464209\inputs\ricard.pdf"
    PLAN_PATH  = r"..\..\output\test_phases\ricard\plan_log.json"
    OUTPUT_DIR = r"..\..\output\test_phases"
    MAX_WORKERS = 10
    # ─────────────────────────────────────────────────────────────────────────

    from dotenv import load_dotenv
    load_dotenv(os.path.join(_BACKEND_DIR, ".env"))
    API_KEY = os.environ["GOOGLE_API_KEY"]

    _base = os.path.dirname(os.path.abspath(__file__))
    result = run(
        pdf_path=os.path.normpath(os.path.join(_base, PDF_PATH)),
        plan_path=os.path.normpath(os.path.join(_base, PLAN_PATH)),
        output_dir=os.path.normpath(os.path.join(_base, OUTPUT_DIR)),
        api_key=API_KEY,
        max_workers=MAX_WORKERS,
    )
    print(f"\n🏁 Chunks en: {result}")
