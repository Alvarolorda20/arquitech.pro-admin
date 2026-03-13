"""
Fase 1 — Planificación
======================
Input:  ruta al PDF de la oferta
Output: {output_dir}/{safe_name}/plan_log.json

Extrae el texto del PDF y pide al PlannerAgent que genere el plan
de extracción (lista de tareas de chunks).
"""

import os
import sys
import json
import re

# ── Añadir Backend/ al path para que los imports de agents/utils funcionen ───
_BACKEND_DIR = os.path.join(os.path.dirname(__file__), "..", "..")
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)
_SRC_DIR = os.path.join(os.path.dirname(__file__), "..")
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

from agents.planner_agent import PlannerAgent


# ─── Helpers (copiados de server.py) ─────────────────────────────────────────

def _safe_dirname(name: str, max_len: int = 48) -> str:
    safe = re.sub(r'[\\/:*?"<>|&]', '_', name)
    safe = re.sub(r'[\s_]+', '_', safe)
    safe = safe.strip('_') or 'pdf'
    return safe[:max_len].rstrip('_')


def _extract_text_from_pdf(pdf_path: str) -> str:
    import fitz
    doc = fitz.open(pdf_path)
    return "".join(
        [f"--- PÁGINA {p.number + 1} ---\n{p.get_text()}" for p in doc]
    )


# ─── Función principal ────────────────────────────────────────────────────────

def run(pdf_path: str, output_dir: str, api_key: str) -> str:
    """
    Ejecuta la Fase 1 para un PDF.

    Parámetros
    ----------
    pdf_path   : ruta al archivo PDF
    output_dir : directorio base de salida (se creará {output_dir}/{safe_name}/)
    api_key    : GOOGLE_API_KEY

    Retorna
    -------
    Ruta al plan_log.json generado.
    """
    filename  = os.path.basename(pdf_path)
    pdf_name  = os.path.splitext(filename)[0]
    safe_name = _safe_dirname(pdf_name)
    pdf_out   = os.path.join(output_dir, safe_name)
    os.makedirs(pdf_out, exist_ok=True)

    plan_path = os.path.join(pdf_out, "plan_log.json")

    if os.path.exists(plan_path):
        print(f"[{safe_name}] 📂 plan_log.json ya existe — cargando desde caché.")
        return plan_path

    print(f"[{safe_name}] 🔍 Extrayendo texto del PDF...")
    full_text = _extract_text_from_pdf(pdf_path)
    print(f"[{safe_name}] 📝 Texto extraído: {len(full_text):,} caracteres")

    print(f"[{safe_name}] 🧠 Generando plan de extracción...")
    planner = PlannerAgent(api_key)
    plan = planner.generate_extraction_plan(full_text)

    if not plan or "tasks" not in plan:
        raise RuntimeError(f"[{safe_name}] El plan generado no contiene 'tasks'.")

    with open(plan_path, "w", encoding="utf-8") as f:
        json.dump(plan, f, indent=4, ensure_ascii=False)

    n_tasks = len(plan.get("tasks", []))
    print(f"[{safe_name}] ✅ Plan guardado: {plan_path}  ({n_tasks} tareas)")
    return plan_path


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # ── CONFIGURA AQUÍ ────────────────────────────────────────────────────────
    PDF_PATH   = r"..\..\temp_processing\request_20260219_143415_92464209\inputs\ricard.pdf"
    OUTPUT_DIR = r"..\..\output\test_phases"
    # ─────────────────────────────────────────────────────────────────────────

    from dotenv import load_dotenv
    load_dotenv(os.path.join(_BACKEND_DIR, ".env"))
    API_KEY = os.environ["GOOGLE_API_KEY"]

    # Resolver rutas relativas desde la ubicación de este archivo
    _base = os.path.dirname(os.path.abspath(__file__))
    pdf_abs = os.path.normpath(os.path.join(_base, PDF_PATH))
    out_abs = os.path.normpath(os.path.join(_base, OUTPUT_DIR))

    result = run(pdf_abs, out_abs, API_KEY)
    print(f"\n🏁 Resultado: {result}")
