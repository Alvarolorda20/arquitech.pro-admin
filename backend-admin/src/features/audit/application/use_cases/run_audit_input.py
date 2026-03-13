"""
Fase 5 — Generación del input de auditoría cualitativa
========================================================
Input:  - ruta al mapped_pauta.json
        - ruta al FINAL_{safe_name}.json
        - ruta al MAPPING_LINKS_FINAL_{safe_name}.json
Output: {pdf_out}/audit_qualitative_input.json

Genera la lista de comparaciones pauta ↔ oferta que se pasará
al AuditorAgent en la Fase 6.
"""

import os
import sys
import json
import re

# ── Añadir Backend/ al path ───────────────────────────────────────────────────
_BACKEND_DIR = os.path.join(os.path.dirname(__file__), "..", "..")
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)
_SRC_DIR = os.path.join(os.path.dirname(__file__), "..")
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

from src.features.audit.application.audit_input_builder import generate_audit_qualitative_input


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _safe_dirname(name: str, max_len: int = 48) -> str:
    safe = re.sub(r'[\\/:*?"<>|&]', '_', name)
    safe = re.sub(r'[\s_]+', '_', safe)
    safe = safe.strip('_') or 'pdf'
    return safe[:max_len].rstrip('_')


def _safe_path(path: str) -> str:
    """En Windows, añade prefijo \\\\?\\ para rutas que superan los 260 chars."""
    if os.name != "nt":
        return path
    abs_p = os.path.abspath(path)
    if not abs_p.startswith("\\\\?\\") and len(abs_p) >= 260:
        return "\\\\?\\" + abs_p
    return abs_p


def _load_json(path: str):
    safe = _safe_path(path)
    if not os.path.exists(safe):
        return None
    try:
        with open(safe, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        return None


# ─── Función principal ────────────────────────────────────────────────────────

def run(
    pauta_path: str,
    final_json_path: str,
    mapping_links_final_path: str,
) -> str:
    """
    Ejecuta la Fase 5.

    Parámetros
    ----------
    pauta_path               : ruta al mapped_pauta.json
    final_json_path          : ruta al FINAL_{safe_name}.json
    mapping_links_final_path : ruta al MAPPING_LINKS_FINAL_{safe_name}.json

    Retorna
    -------
    Ruta al audit_qualitative_input.json generado.
    """
    pdf_output_dir = os.path.dirname(final_json_path)
    filename = os.path.basename(final_json_path)
    safe_name = re.sub(r'^FINAL_', '', os.path.splitext(filename)[0])

    mapping_result = _load_json(mapping_links_final_path)
    if not mapping_result:
        raise ValueError(
            f"No se pudo cargar el mapping final: {mapping_links_final_path}"
        )

    audit_input_path = os.path.join(pdf_output_dir, "audit_qualitative_input.json")
    audit_input_path_safe = _safe_path(audit_input_path)

    if os.path.exists(audit_input_path_safe):
        print(f"[{safe_name}] ♻️  audit_qualitative_input.json ya existe — regenerando...")

    os.makedirs(_safe_path(pdf_output_dir), exist_ok=True)
    generate_audit_qualitative_input(
        pauta_path,
        final_json_path,
        mapping_result=mapping_result,
        output_path=audit_input_path_safe,
    )

    print(f"[{safe_name}] ✅ Input de auditoría guardado → {audit_input_path}")
    return audit_input_path


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Selección de job: pasar '1' o '2' como primer argumento o mediante env JOB_CHOICE
    JOB = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("JOB_CHOICE", "1")
    BASE_HISTORY = r"..\..\history\20260220_110903_454d025b\output"
    if JOB == "1":
        PAUTA_PATH = os.path.join(BASE_HISTORY, "mapped_pauta.json")
        FINAL_JSON_PATH = os.path.join(BASE_HISTORY, r"P-2025-305-III_DAUFES_-_JANE_FONT_-_GOLFET_-_CAL\FINAL_P-2025-305-III_DAUFES_-_JANE_FONT_-_GOLFET_-_CAL.json")
        MAPPING_FINAL_PATH = os.path.join(BASE_HISTORY, r"P-2025-305-III_DAUFES_-_JANE_FONT_-_GOLFET_-_CAL\MAPPING_LINKS_FINAL_P-2025-305-III_DAUFES_-_JANE_FONT_-_GOLFET_-_CAL.json")
    else:
        PAUTA_PATH = os.path.join(BASE_HISTORY, "mapped_pauta.json")
        FINAL_JSON_PATH = os.path.join(BASE_HISTORY, r"2025-12-05_RCP_Ricard_-_Pressupost_Daufes_Golfet\FINAL_2025-12-05_RCP_Ricard_-_Pressupost_Daufes_Golfet.json")
        MAPPING_FINAL_PATH = os.path.join(BASE_HISTORY, r"2025-12-05_RCP_Ricard_-_Pressupost_Daufes_Golfet\MAPPING_LINKS_FINAL_2025-12-05_RCP_Ricard_-_Pressupost_Daufes_Golfet.json")
    # ─────────────────────────────────────────────────────────────────────────

    _base = os.path.dirname(os.path.abspath(__file__))
    result = run(
        pauta_path=os.path.normpath(os.path.join(_base, PAUTA_PATH)),
        final_json_path=os.path.normpath(os.path.join(_base, FINAL_JSON_PATH)),
        mapping_links_final_path=os.path.normpath(os.path.join(_base, MAPPING_FINAL_PATH)),
    )
    print(f"\n🏁 Resultado: {result}")

