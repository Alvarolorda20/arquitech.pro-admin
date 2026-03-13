"""
Fase 7 — Juez (AuditReflector) + Sanitización post-auditoría
==============================================================
Input:  - ruta al AUDITORIA_{safe_name}.json  (salida de Fase 6, con texto_oferta)
        - ruta al PDF original de la oferta
        - ruta al FINAL_{safe_name}.json
        - ruta al audit_qualitative_input.json
Output:
  - {pdf_out}/AUDITORIA_ENRIQUECIDA_{safe_name}.json  (intermedio)
  - {pdf_out}/AUDITORIA_VALIDADA_{safe_name}.json
  - FINAL_{safe_name}.json actualizado (sanitización de asignaciones)

El AuditReflector lee el PDF directamente vía la Gemini File API para verificar
cada incidencia detectada en la Fase 6.
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

from src.features.audit.application.audit_reflector import AuditReflector
from src.features.audit.application.post_audit_sanitizer import sanitizar_asignaciones_post_auditoria


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


# ─── Función principal ────────────────────────────────────────────────────────

def run(
    audit_path: str,
    pdf_path: str,
    final_json_path: str,
    audit_input_path: str,
    api_key: str,
    reflector_concurrency: int = 8,
) -> str:
    """
    Ejecuta la Fase 7 (Juez + sanitización).

    Parámetros
    ----------
    audit_path            : ruta al AUDITORIA_{safe_name}.json
    pdf_path              : ruta al PDF original de la oferta
    final_json_path       : ruta al FINAL_{safe_name}.json
    audit_input_path      : ruta al audit_qualitative_input.json
    api_key               : GOOGLE_API_KEY
    reflector_concurrency : workers paralelos del reflector (por defecto 8)

    Retorna
    -------
    Ruta al AUDITORIA_VALIDADA_{safe_name}.json
    """
    pdf_output_dir = os.path.dirname(audit_path)
    filename = os.path.basename(audit_path)
    # AUDITORIA_{safe_name}.json → safe_name
    safe_name = re.sub(r'^AUDITORIA_', '', os.path.splitext(filename)[0])

    final_validated_path = os.path.join(
        pdf_output_dir, f"AUDITORIA_VALIDADA_{safe_name}.json"
    )
    enriched_save_path = os.path.join(
        pdf_output_dir, f"AUDITORIA_ENRIQUECIDA_{safe_name}.json"
    )

    # ── Verificar si ya está hecho ─────────────────────────────────────────
    if os.path.exists(final_validated_path):
        existing = _load_json(final_validated_path)
        if existing and len(existing) > 0:
            print(f"[{safe_name}] ⏩ AUDITORIA_VALIDADA ya existe — saltando Fase 7.")
            # Ejecutar sanitización igualmente por si hubo cambios
            _run_sanitize(safe_name, final_validated_path, final_json_path)
            return final_validated_path
        print(f"[{safe_name}] ⚠️  AUDITORIA_VALIDADA corrupta/vacía — regenerando.")

    # ── Paso 1: Generar / cargar AUDITORIA_ENRIQUECIDA ───────────────────
    # (input del Juez = AUDITORIA + descripciones de la pauta)
    if os.path.exists(enriched_save_path):
        print(f"[{safe_name}] 📂 AUDITORIA_ENRIQUECIDA ya existe — cargando desde caché...")
        enriched_audit = _load_json(enriched_save_path) or []
        if not enriched_audit:
            print(f"[{safe_name}] ⚠️  AUDITORIA_ENRIQUECIDA vacía — regenerando...")
            enriched_audit = _build_enriched_audit(
                safe_name, audit_path, audit_input_path, enriched_save_path
            )
    else:
        enriched_audit = _build_enriched_audit(
            safe_name, audit_path, audit_input_path, enriched_save_path
        )

    if not enriched_audit:
        raise ValueError(f"[{safe_name}] AUDITORIA_ENRIQUECIDA vacía o no generada.")

    # ── Paso 2: Ejecutar el Juez ──────────────────────────────────────────
    print(f"[{safe_name}] ⚖️  Iniciando Fase 2: El Juez (verificación con PDF)...")
    reflector = AuditReflector(api_key)
    debug_batches_dir = os.path.join(pdf_output_dir, "debug_batches")

    try:
        validated_audit = reflector.review_audit(
            enriched_audit,
            pdf_path,
            debug_folder_path=debug_batches_dir,
            max_concurrency=reflector_concurrency,
        )
    except Exception as e:
        raise RuntimeError(f"[{safe_name}] Error crítico en el Juez: {e}") from e

    if not validated_audit:
        print(f"[{safe_name}] ⚠️  El Juez devolvió datos vacíos — guardando lista vacía igualmente.")
        validated_audit = []

    # Guardar siempre, incluso si está vacío, para tener el archivo en disco
    os.makedirs(pdf_output_dir, exist_ok=True)
    with open(final_validated_path, "w", encoding="utf-8") as f:
        json.dump(validated_audit, f, indent=4, ensure_ascii=False)
    print(f"[{safe_name}] 🏆 Auditoría validada guardada → {final_validated_path} ({len(validated_audit)} ítems)")

    # ── Sanitización post-auditoría ────────────────────────────────────────
    _run_sanitize(safe_name, final_validated_path, final_json_path)

    return final_validated_path


def _build_enriched_audit(
    safe_name: str,
    audit_path: str,
    audit_input_path: str,
    save_path: str,
) -> list:
    """Carga AUDITORIA, enriquece con descripciones de la pauta y guarda AUDITORIA_ENRIQUECIDA."""
    audit_results = _load_json(audit_path)
    if not audit_results:
        raise ValueError(
            f"[{safe_name}] No se pudo cargar el archivo de auditoría: {audit_path}"
        )
    print(f"[{safe_name}] 🔍 Generando AUDITORIA_ENRIQUECIDA ({len(audit_results)} incidencias)...")
    enriched = _enrich_audit_data(audit_results, audit_input_path, save_path=save_path)
    print(f"[{safe_name}] ✅ AUDITORIA_ENRIQUECIDA guardada → {save_path}")
    return enriched


def _enrich_audit_data(audit_results: list, context_input_path: str, save_path: str = None) -> list:
    """Añade descripcion_original_pauta a cada finding (igual que server.py)."""
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
        # Preparar ruta absoluta y, en Windows, usar prefijo extendido para rutas largas
        abs_path = os.path.abspath(save_path)
        write_path = abs_path
        if os.name == "nt":
            # Añadir prefijo extendido si es necesario
            if not abs_path.startswith("\\\\?\\") and len(abs_path) > 260:
                write_path = "\\\\?\\" + abs_path

        dirpath = os.path.dirname(write_path) or os.getcwd()
        os.makedirs(dirpath, exist_ok=True)
        try:
            with open(write_path, "w", encoding="utf-8") as f:
                json.dump(enriched_data, f, indent=4, ensure_ascii=False)
        except Exception as e:
            raise RuntimeError(f"Error guardando AUDITORIA_ENRIQUECIDA en {save_path}: {e}") from e

    return enriched_data


def _run_sanitize(safe_name: str, final_validated_path: str, final_json_path: str) -> None:
    print(f"[{safe_name}] 🧼 Sanitizando asignaciones duplicadas...")
    try:
        san_stats = sanitizar_asignaciones_post_auditoria(
            final_validated_path, final_json_path
        )
        if not san_stats.get("skipped"):
            print(
                f"[{safe_name}] 📊 Sanitización: "
                f"{san_stats['eliminados']} duplicados, "
                f"{san_stats['sincronizados']} ítems re-sincronizados."
            )
    except Exception as e:
        print(f"[{safe_name}] ⚠️  Error en sanitización (no bloqueante): {e}")


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Selección de job: pasar '1' o '2' como primer argumento o mediante env JOB_CHOICE
    JOB = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("JOB_CHOICE", "1")
    BASE_HISTORY = r"..\..\history\20260220_110903_454d025b"
    if JOB == "1":
        AUDIT_PATH = os.path.join(BASE_HISTORY, r"output\P-2025-305-III_DAUFES_-_JANE_FONT_-_GOLFET_-_CAL\AUDITORIA_P-2025-305-III_DAUFES_-_JANE_FONT_-_GOLFET_-_CAL.json")
        PDF_PATH = os.path.join(BASE_HISTORY, r"inputs\P-2025-305-III_DAUFES_-_JANE_FONT_-_GOLFET_-_CAL.pdf")
        FINAL_JSON_PATH = os.path.join(BASE_HISTORY, r"output\P-2025-305-III_DAUFES_-_JANE_FONT_-_GOLFET_-_CAL\FINAL_P-2025-305-III_DAUFES_-_JANE_FONT_-_GOLFET_-_CAL.json")
        AUDIT_INPUT_PATH = os.path.join(BASE_HISTORY, r"output\P-2025-305-III_DAUFES_-_JANE_FONT_-_GOLFET_-_CAL\audit_qualitative_input.json")
    else:
        AUDIT_PATH = os.path.join(BASE_HISTORY, r"output\2025-12-05_RCP_Ricard_-_Pressupost_Daufes_Golfet\AUDITORIA_2025-12-05_RCP_Ricard_-_Pressupost_Daufes_Golfet.json")
        PDF_PATH = os.path.join(BASE_HISTORY, r"inputs\2025-12-05_RCP_Ricard_-_Pressupost_Daufes_Golfet.pdf")
        FINAL_JSON_PATH = os.path.join(BASE_HISTORY, r"output\2025-12-05_RCP_Ricard_-_Pressupost_Daufes_Golfet\FINAL_2025-12-05_RCP_Ricard_-_Pressupost_Daufes_Golfet.json")
        AUDIT_INPUT_PATH = os.path.join(BASE_HISTORY, r"output\2025-12-05_RCP_Ricard_-_Pressupost_Daufes_Golfet\audit_qualitative_input.json")
    REFLECTOR_CONC = 8
    # ─────────────────────────────────────────────────────────────────────────

    from dotenv import load_dotenv
    load_dotenv(os.path.join(_BACKEND_DIR, ".env"))
    API_KEY = os.environ["GOOGLE_API_KEY"]

    _base = os.path.dirname(os.path.abspath(__file__))
    result = run(
        audit_path=os.path.normpath(os.path.join(_base, AUDIT_PATH)),
        pdf_path=os.path.normpath(os.path.join(_base, PDF_PATH)),
        final_json_path=os.path.normpath(os.path.join(_base, FINAL_JSON_PATH)),
        audit_input_path=os.path.normpath(os.path.join(_base, AUDIT_INPUT_PATH)),
        api_key=API_KEY,
        reflector_concurrency=REFLECTOR_CONC,
    )
    print(f"\n🏁 Resultado: {result}")

