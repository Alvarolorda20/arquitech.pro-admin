"""
Fase 3 — Consolidación de chunks
==================================
Input:  - directorio base de salida que contiene {safe_name}/chunks/chunk_*.json
        - nombre del proveedor (safe_name), o bien se deduce del PDF original
Output: {output_dir}/{safe_name}/FINAL_{safe_name}.json

Fusiona todos los chunks en un único JSON de oferta estructurado.
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

from utils.consolidator import consolidate_chunks


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _safe_dirname(name: str, max_len: int = 48) -> str:
    safe = re.sub(r'[\\/:*?"<>|&]', '_', name)
    safe = re.sub(r'[\s_]+', '_', safe)
    safe = safe.strip('_') or 'pdf'
    return safe[:max_len].rstrip('_')


# ─── Función principal ────────────────────────────────────────────────────────

def run(safe_name: str, output_dir: str) -> str:
    """
    Ejecuta la Fase 3 para un proveedor.

    Parámetros
    ----------
    safe_name  : nombre del proveedor (subdirectorio dentro de output_dir)
    output_dir : directorio base de salida (el que contiene {safe_name}/)

    Retorna
    -------
    Ruta al FINAL_{safe_name}.json generado.
    """
    final_path = consolidate_chunks(safe_name, output_dir)

    print(f"[{safe_name}] ✅ Consolidación completa → {final_path}")
    return final_path


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # ── CONFIGURA AQUÍ ────────────────────────────────────────────────────────
    # safe_name: es el nombre del subdirectorio del proveedor dentro de OUTPUT_DIR
    # (equivale a _safe_dirname(nombre_pdf_sin_extension))
    # Ejemplo: si el PDF se llama "ricard.pdf", SAFE_NAME = "ricard"
    SAFE_NAME  = "ricard"
    OUTPUT_DIR = r"..\..\output\test_phases"
    # ─────────────────────────────────────────────────────────────────────────

    _base = os.path.dirname(os.path.abspath(__file__))
    out_abs = os.path.normpath(os.path.join(_base, OUTPUT_DIR))

    result = run(SAFE_NAME, out_abs)
    print(f"\n🏁 Resultado: {result}")
