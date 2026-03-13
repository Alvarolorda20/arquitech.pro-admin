"""
Fase 4 — Mapping completo (capítulos + oferta→pauta + extras + aplicar)
=========================================================================
Input:  - ruta al mapped_pauta.json
        - ruta al FINAL_{safe_name}.json  (salida de Fase 3)
        - api_key
Output:
  - {pdf_out}/mapping_batches/CAP_MAPPING_{safe_name}.json
  - {pdf_out}/mapping_batches/MAPPING_LINKS_{safe_name}.json
  - {pdf_out}/mapping_batches/EXTRA_REVIEW_{safe_name}.json
  - {pdf_out}/MAPPING_LINKS_FINAL_{safe_name}.json
  - FINAL_{safe_name}.json actualizado con id_pauta_unico

Incluye los 4 sub-pasos:
  4.0  Mapeo de capítulos (derivado desde MAPPING_LINKS_FINAL)
  4.1  Mapeo oferta → pauta (MapperAgent)
  4.2  Revisión de extras (ExtraReviewAgent)
  4.3  Aplicar mapping al JSON final
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

from src.features.mapping.application.mapper_agent import MapperAgent
from src.features.mapping.application.extra_review_agent import ExtraReviewAgent
from src.features.mapping.application.chapter_mapping_deriver import (
    derive_chapter_mapping_from_links,
)
from src.features.mapping.application.mapping_applier import apply_mapping_to_json

# ─── Constantes de batch (pueden sobreescribirse con env vars) ────────────────
EXTRA_REVIEW_BATCH_SIZE: int = int(os.environ.get("EXTRA_REVIEW_BATCH_SIZE", "15"))


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


def _safe_path(path: str) -> str:
    """En Windows, añade prefijo \\\\?\\ para rutas que superan los 260 chars."""
    if os.name != "nt":
        return path
    abs_p = os.path.abspath(path)
    if not abs_p.startswith("\\\\?\\") and len(abs_p) >= 260:
        return "\\\\?\\" + abs_p
    return abs_p


# ─── Función principal ────────────────────────────────────────────────────────

def run(
    pauta_path: str,
    final_json_path: str,
    api_key: str,
    mapper_workers: int = 8,
) -> str:
    """
    Ejecuta la Fase 4 completa (4.0 → 4.3).

    Parámetros
    ----------
    pauta_path       : ruta al mapped_pauta.json
    final_json_path  : ruta al FINAL_{safe_name}.json
    api_key          : GOOGLE_API_KEY
    mapper_workers   : paralelismo del MapperAgent (por defecto 8)

    Retorna
    -------
    Ruta al MAPPING_LINKS_FINAL_{safe_name}.json
    """
    # Inferir safe_name y directorio de salida a partir de final_json_path
    pdf_output_dir = os.path.dirname(final_json_path)
    filename = os.path.basename(final_json_path)
    # FINAL_{safe_name}.json → safe_name
    safe_name = re.sub(r'^FINAL_', '', os.path.splitext(filename)[0])

    mapping_batches_dir = os.path.join(pdf_output_dir, "mapping_batches")
    os.makedirs(mapping_batches_dir, exist_ok=True)

    mapper         = MapperAgent(api_key)
    extra_reviewer = ExtraReviewAgent(api_key)

    # ── 4.0 Mapeo de capítulos ────────────────────────────────────────────────
    cap_mapping_path = os.path.join(mapping_batches_dir, f"CAP_MAPPING_{safe_name}.json")
    # 4.0 chapter mapping is derived after final partida mapping (no extra AI call).

    mapping_path = os.path.join(mapping_batches_dir, f"MAPPING_LINKS_{safe_name}.json")
    mapping_result = None

    if os.path.exists(mapping_path):
        print(f"[{safe_name}] 📂 Cargando mapping desde caché...")
        mapping_result = _load_json(mapping_path)

    if not mapping_result:
        print(f"[{safe_name}] 🧠 Generando mapping con IA...")
        mapping_result = mapper.map_offer_to_pauta(
            pauta_path,
            final_json_path,
            max_workers=mapper_workers,
        )
        os.makedirs(mapping_batches_dir, exist_ok=True)
        with open(_safe_path(mapping_path), "w", encoding="utf-8") as f:
            json.dump(mapping_result, f, indent=4, ensure_ascii=False)
        print(f"[{safe_name}] 💾 Mapping guardado: {mapping_path}")

    # ── 4.2 Revisión de extras ────────────────────────────────────────────────
    extra_review_path = os.path.join(mapping_batches_dir, f"EXTRA_REVIEW_{safe_name}.json")

    if os.path.exists(extra_review_path):
        print(f"[{safe_name}] 📂 Cargando revisión de extras desde caché...")
        review_data = _load_json(extra_review_path) or {}
        decisions = review_data.get("decisions", [])
        for item in decisions:
            if item.get("decision") == "MAP" and item.get("pauta_id"):
                id_oferta = item.get("id_oferta")
                mapping_result.setdefault("mapping", {})[id_oferta] = item["pauta_id"]
                if id_oferta in mapping_result.get("extras", []):
                    mapping_result["extras"].remove(id_oferta)
        print(f"[{safe_name}] ✅ Extras cargados desde caché.")
    elif mapping_result and mapping_result.get("extras"):
        n_extras = len(mapping_result.get("extras", []))
        print(f"[{safe_name}] 🔎 Revisando {n_extras} extras con IA...")
        try:
            review_payload = extra_reviewer.review_extras(
                pauta_path,
                final_json_path,
                mapping_result,
                output_path=extra_review_path,
                batch_size=EXTRA_REVIEW_BATCH_SIZE,
            )
            mapping_result = review_payload.get("mapping_result", mapping_result)
            with open(_safe_path(mapping_path), "w", encoding="utf-8") as f:
                json.dump(mapping_result, f, indent=4, ensure_ascii=False)
            print(f"[{safe_name}] ✅ Extras revisados. Mapping actualizado.")
        except Exception as e:
            print(f"[{safe_name}] ⚠️  Error revisando extras: {e}")
    else:
        print(f"[{safe_name}] ✅ No hay extras que revisar.")

    # ── 4.3 Guardar mapping final ─────────────────────────────────────────────
    final_mapping_path = os.path.join(pdf_output_dir, f"MAPPING_LINKS_FINAL_{safe_name}.json")
    os.makedirs(pdf_output_dir, exist_ok=True)
    with open(_safe_path(final_mapping_path), "w", encoding="utf-8") as f:
        json.dump(mapping_result, f, indent=4, ensure_ascii=False)

    try:
        offer_data = _load_json(final_json_path) or []
        cap_mapping_result = derive_chapter_mapping_from_links(
            mapping_result,
            offer_data=offer_data if isinstance(offer_data, list) else None,
        )
        with open(_safe_path(cap_mapping_path), "w", encoding="utf-8") as f:
            json.dump(cap_mapping_result, f, indent=4, ensure_ascii=False)
        print(f"[{safe_name}] CAP_MAPPING derivado desde MAPPING_LINKS_FINAL.")
    except Exception as e:
        print(f"[{safe_name}] WARNING: no se pudo derivar CAP_MAPPING desde enlaces finales: {e}")

    # ── 4.4 Aplicar mapping al JSON de la oferta ──────────────────────────────
    stats = apply_mapping_to_json(final_json_path, mapping_result)
    print(
        f"[{safe_name}] ✅ Mapping aplicado: "
        f"{stats['mapped']} vinculados, {stats['extras']} extras."
    )
    print(f"[{safe_name}] ✅ Fase 4 completa → {final_mapping_path}")
    return final_mapping_path


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # ── CONFIGURA AQUÍ ────────────────────────────────────────────────────────
    # Selección de job: pasar '1' o '2' como primer argumento o mediante env JOB_CHOICE
    JOB = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("JOB_CHOICE", "1")
    BASE_HISTORY = r"..\..\history\20260220_110903_454d025b\output"
    if JOB == "1":
        # Opción 1: P-2025-305... (folder con nombre largo)
        PAUTA_PATH = os.path.join(BASE_HISTORY, "mapped_pauta.json")
        FINAL_JSON_PATH = os.path.join(BASE_HISTORY, r"P-2025-305-III_DAUFES_-_JANE_FONT_-_GOLFET_-_CAL\FINAL_P-2025-305-III_DAUFES_-_JANE_FONT_-_GOLFET_-_CAL.json")
    else:
        # Opción 2: Ricard
        PAUTA_PATH = os.path.join(BASE_HISTORY, "mapped_pauta.json")
        FINAL_JSON_PATH = os.path.join(BASE_HISTORY, r"2025-12-05_RCP_Ricard_-_Pressupost_Daufes_Golfet\FINAL_2025-12-05_RCP_Ricard_-_Pressupost_Daufes_Golfet.json")
    MAPPER_WORKERS = 8
    # ─────────────────────────────────────────────────────────────────────────

    from dotenv import load_dotenv
    load_dotenv(os.path.join(_BACKEND_DIR, ".env"))
    API_KEY = os.environ["GOOGLE_API_KEY"]

    _base = os.path.dirname(os.path.abspath(__file__))
    result = run(
        pauta_path=os.path.normpath(os.path.join(_base, PAUTA_PATH)),
        final_json_path=os.path.normpath(os.path.join(_base, FINAL_JSON_PATH)),
        api_key=API_KEY,
        mapper_workers=MAPPER_WORKERS,
    )
    print(f"\n🏁 Resultado: {result}")


