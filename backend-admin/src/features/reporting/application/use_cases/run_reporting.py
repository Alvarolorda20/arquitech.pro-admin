"""
Fase 8 — Generación del Excel comparativo maestro
===================================================
Input:  - directorio base de salida (output_base_dir) que contiene:
            {safe_name}/FINAL_{safe_name}.json
            {safe_name}/AUDITORIA_VALIDADA_{safe_name}.json
            {safe_name}/MAPPING_LINKS_FINAL_{safe_name}.json
            {safe_name}/mapping_batches/CAP_MAPPING_{safe_name}.json
            mapped_pauta.json   (en la raíz de output_base_dir)
Output: {output_excel_path}  (por defecto output_base_dir/../COMPARATIVO_MAESTRO_FINAL.xlsx)

Genera el Excel final con una columna por proveedor, totales por capítulo,
y celdas de validación con color según resultado de auditoría.
"""

import os
import sys
import re

# ── Añadir Backend/ al path ───────────────────────────────────────────────────
_BACKEND_DIR = os.path.join(os.path.dirname(__file__), "..", "..")
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)
_SRC_DIR = os.path.join(os.path.dirname(__file__), "..")
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

from src.features.reporting.application.comparative_excel_builder import generar_comparativo_final


# ─── Función principal ────────────────────────────────────────────────────────

def run(output_base_dir: str, output_excel_path: str = None) -> str:
    """
    Ejecuta la Fase 8 (generación del Excel).

    Parámetros
    ----------
    output_base_dir     : directorio que contiene los subdirectorios de cada
                          proveedor más el mapped_pauta.json en su raíz.
    output_excel_path   : ruta completa del archivo Excel de salida.
                          Por defecto se crea en output_base_dir/../COMPARATIVO_MAESTRO_FINAL.xlsx

    Retorna
    -------
    Ruta al Excel generado.
    """
    if output_excel_path is None:
        parent = os.path.dirname(output_base_dir.rstrip(os.sep))
        output_excel_path = os.path.join(parent, "COMPARATIVO_MAESTRO_FINAL.xlsx")

    os.makedirs(os.path.dirname(output_excel_path), exist_ok=True)

    print(f"📊 Generando Excel comparativo...")
    print(f"   Directorio base : {output_base_dir}")
    print(f"   Salida          : {output_excel_path}")

    generar_comparativo_final(output_base_dir, output_excel_path)

    print(f"✅ Excel generado → {output_excel_path}")
    return output_excel_path


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # ── CONFIGURA AQUÍ ────────────────────────────────────────────────────────
    # Directorio que contiene los subdirectorios de cada proveedor
    # y el mapped_pauta.json en su raíz.
    OUTPUT_BASE_DIR   = r"..\..\history\20260220_110903_454d025b\output"
    # Ruta del Excel de salida (dejar en None para que se genere automáticamente)
    OUTPUT_EXCEL_PATH = r"..\output\pruebas\COMPARATIVO_MAESTRO_FINAL_pack1.xlsx"
    # ─────────────────────────────────────────────────────────────────────────

    _base = os.path.dirname(os.path.abspath(__file__))
    base_abs = os.path.normpath(os.path.join(_base, OUTPUT_BASE_DIR))
    excel_abs = (
        os.path.normpath(os.path.join(_base, OUTPUT_EXCEL_PATH))
        if OUTPUT_EXCEL_PATH
        else None
    )

    result = run(base_abs, excel_abs)
    print(f"\n🏁 Excel: {result}")

