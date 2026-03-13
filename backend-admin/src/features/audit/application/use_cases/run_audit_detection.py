"""
Fase 6 — Auditoría Fase 1 (Detección) + inyección de texto_oferta
===================================================================
Input:  - ruta al mapped_pauta.json
        - ruta al FINAL_{safe_name}.json
Output: {pdf_out}/AUDITORIA_{safe_name}.json  (con texto_oferta inyectado)

Ejecuta el AuditorAgent sobre todos los ítems y luego enriquece los resultados
con el texto completo de la oferta (texto_oferta) para que el Judge tenga
contexto completo en la Fase 7.
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

from src.features.audit.application.auditor_agent import AuditorAgent

# ─── Constantes ────────────────────────────────────────────────────────────────
AUDITOR_BATCH_SIZE: int = int(os.environ.get("AUDITOR_BATCH_SIZE", "20"))


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


def _normalize_cap_cod(entry: str) -> str:
    parts = entry.split("::", 1)
    if len(parts) == 2:
        cap, cod = parts
        try:
            cap_norm = f"{int(cap):02d}"
        except ValueError:
            cap_norm = cap
        return f"{cap_norm}::{cod}"
    else:
        cod = parts[0]
        dot_idx = cod.find(".")
        if dot_idx > 0:
            try:
                cap_norm = f"{int(cod[:dot_idx]):02d}"
                return f"{cap_norm}::{cod}"
            except ValueError:
                pass
        return cod


def _inject_texto_oferta(audit_results: list, final_json_path: str) -> list:
    """Re-inyecta texto_oferta desde el FINAL_ JSON (igual que server.py)."""
    offer_data = _load_json(final_json_path)
    if not offer_data:
        return audit_results

    partida_map: dict = {}
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

        normalized_list = [_normalize_cap_cod(e) for e in raw_list]
        finding["codigo_oferta"] = normalized_list

        texts = []
        for entry in normalized_list:
            parts = entry.split("::", 1)
            raw_cod = parts[1] if len(parts) == 2 else parts[0]

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
                cap_prefix = parts[0] if len(parts) == 2 else f"{int(cap_c):02d}"
                full_key = f"{cap_prefix}::{lookup_cod}"
                texts.append(
                    f"[{full_key}] {nombre}\n{desc}" if desc else f"[{full_key}] {nombre}"
                )
            else:
                texts.append(entry)

        finding["texto_oferta"] = "\n\n---\n\n".join(texts) if texts else ""

    return audit_results


# ─── Función principal ────────────────────────────────────────────────────────

def run(
    pauta_path: str,
    final_json_path: str,
    api_key: str,
    batch_size: int = AUDITOR_BATCH_SIZE,
    max_concurrency: int = 20,
) -> str:
    """
    Ejecuta la Fase 6 (Auditoría detección + inyección texto_oferta).

    Parámetros
    ----------
    pauta_path       : ruta al mapped_pauta.json
    final_json_path  : ruta al FINAL_{safe_name}.json
    api_key          : GOOGLE_API_KEY
    batch_size       : ítems por lote del auditor (por defecto 20)
    max_concurrency  : workers paralelos del auditor (por defecto 20)

    Retorna
    -------
    Ruta al AUDITORIA_{safe_name}.json guardado.
    """
    filename  = os.path.basename(final_json_path)
    safe_name = re.sub(r'^FINAL_', '', os.path.splitext(filename)[0])
    pdf_output_dir = os.path.dirname(final_json_path)

    audit_output_path = os.path.join(pdf_output_dir, f"AUDITORIA_{safe_name}.json")

    if os.path.exists(audit_output_path):
        print(f"[{safe_name}] 📂 AUDITORIA ya existe — cargando desde caché...")
        audit_results = _load_json(audit_output_path) or []
    else:
        print(f"[{safe_name}] 🕵️  Iniciando Auditoría Fase 1 (Detección)...")
        auditor = AuditorAgent(api_key)
        audit_results = auditor.run_full_audit(
            pauta_path,
            final_json_path,
            batch_size=batch_size,
            max_concurrency=max_concurrency,
        )
        with open(audit_output_path, "w", encoding="utf-8") as f:
            json.dump(audit_results, f, indent=4, ensure_ascii=False)
        print(
            f"[{safe_name}] ✅ Fase 1 completa: "
            f"{len(audit_results)} incidencias detectadas."
        )

    if audit_results:
        audit_results = _inject_texto_oferta(audit_results, final_json_path)
        # Sobreescribir con texto_oferta enriquecido
        with open(audit_output_path, "w", encoding="utf-8") as f:
            json.dump(audit_results, f, indent=4, ensure_ascii=False)
        print(f"[{safe_name}] 🔗 texto_oferta inyectado y guardado.")

    print(f"[{safe_name}] ✅ AUDITORIA → {audit_output_path}")
    return audit_output_path


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Selección de job: pasar '1' o '2' como primer argumento o mediante env JOB_CHOICE
    JOB = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("JOB_CHOICE", "1")
    BASE_HISTORY = r"..\..\history\20260220_110903_454d025b\output"
    PAUTA_PATH = os.path.join(BASE_HISTORY, "mapped_pauta.json")
    if JOB == "1":
        FINAL_JSON_PATH = os.path.join(BASE_HISTORY, r"P-2025-305-III_DAUFES_-_JANE_FONT_-_GOLFET_-_CAL\FINAL_P-2025-305-III_DAUFES_-_JANE_FONT_-_GOLFET_-_CAL.json")
    else:
        FINAL_JSON_PATH = os.path.join(BASE_HISTORY, r"2025-12-05_RCP_Ricard_-_Pressupost_Daufes_Golfet\FINAL_2025-12-05_RCP_Ricard_-_Pressupost_Daufes_Golfet.json")
    BATCH_SIZE      = 20
    MAX_CONCURRENCY = 20
    # ─────────────────────────────────────────────────────────────────────────

    from dotenv import load_dotenv
    load_dotenv(os.path.join(_BACKEND_DIR, ".env"))
    API_KEY = os.environ["GOOGLE_API_KEY"]

    _base = os.path.dirname(os.path.abspath(__file__))
    result = run(
        pauta_path=os.path.normpath(os.path.join(_base, PAUTA_PATH)),
        final_json_path=os.path.normpath(os.path.join(_base, FINAL_JSON_PATH)),
        api_key=API_KEY,
        batch_size=BATCH_SIZE,
        max_concurrency=MAX_CONCURRENCY,
    )
    print(f"\n🏁 Resultado: {result}")

