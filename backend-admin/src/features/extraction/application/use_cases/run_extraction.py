"""
Fase 1 — Extracción de presupuesto PDF con LandingAI ADE
=========================================================
Input:  ruta al PDF de la oferta
Output: {output_dir}/{safe_name}/FINAL_{safe_name}.json

Ejecuta la extracción completa:
  1. ADE /parse  → Markdown
  2. ADE /extract → JSON estructurado
  3. Validación y normalización con Pydantic
  4. Escritura del FINAL_{safe_name}.json (mismo formato que usa el resto del pipeline)

Uso standalone:
    python phase1_extraction.py

    # O con argumentos:
    python phase1_extraction.py --pdf ../../pdfs/ricard.pdf --output-dir ../../output/test_phases

Validaciones que realiza:
  - El JSON resultante es una lista de capítulos
  - Cada capítulo tiene capitulo_codigo, capitulo_nombre, partidas
  - Cada partida tiene codigo, nombre, unidad, cantidad, precio, total
  - Los números son float (no strings)
  - Las unidades están normalizadas (m², m³, ud, pa, etc.)
"""

import os
import sys
import json
import re
import time
from pathlib import Path

# ── Añadir raíz del proyecto al path ─────────────────────────────────────────
_PHASE_DIR = os.path.dirname(os.path.abspath(__file__))
_SRC_DIR = os.path.join(_PHASE_DIR, "..")
_BACKEND_DIR = os.path.join(_PHASE_DIR, "..", "..")
for p in (_SRC_DIR, _BACKEND_DIR):
    if p not in sys.path:
        sys.path.insert(0, p)


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _safe_dirname(name: str, max_len: int = 48) -> str:
    safe = re.sub(r'[\\/:*?"<>|&]', '_', name)
    safe = re.sub(r'[\s_]+', '_', safe)
    safe = safe.strip('_') or 'pdf'
    return safe[:max_len].rstrip('_')


def _validate_final_json(data: list, strict: bool = True) -> dict:
    """
    Validates the internal JSON structure that the pipeline expects.

    Returns a dict with:
      ok:        bool
      chapters:  int
      partidas:  int
      warnings:  list[str]
      errors:    list[str]
    """
    result = {
        "ok": True,
        "chapters": 0,
        "partidas": 0,
        "warnings": [],
        "errors": [],
    }

    if not isinstance(data, list):
        result["errors"].append(f"Root must be a list, got {type(data).__name__}")
        result["ok"] = False
        return result

    result["chapters"] = len(data)
    if len(data) == 0:
        result["errors"].append("Empty list — no chapters extracted.")
        result["ok"] = False
        return result

    required_chapter_keys = {"capitulo_codigo", "partidas"}
    required_partida_keys = {"codigo"}
    numeric_fields = {"cantidad", "precio", "total"}

    for i, ch in enumerate(data):
        if not isinstance(ch, dict):
            result["errors"].append(f"Chapter [{i}] is not a dict.")
            result["ok"] = False
            continue

        # Chapter-level checks
        missing_ch = required_chapter_keys - set(ch.keys())
        if missing_ch:
            result["errors"].append(
                f"Chapter [{i}] missing keys: {missing_ch}"
            )
            result["ok"] = False

        cap_code = ch.get("capitulo_codigo", f"?_{i}")
        cap_name = ch.get("capitulo_nombre")
        if not cap_name:
            result["warnings"].append(
                f"Chapter [{cap_code}] has empty/null capitulo_nombre."
            )

        total_cap = ch.get("total_capitulo")
        if total_cap is not None and not isinstance(total_cap, (int, float)):
            result["warnings"].append(
                f"Chapter [{cap_code}] total_capitulo is {type(total_cap).__name__}, expected float."
            )

        partidas = ch.get("partidas", [])
        if not isinstance(partidas, list):
            result["errors"].append(
                f"Chapter [{cap_code}] 'partidas' is not a list."
            )
            result["ok"] = False
            continue

        if len(partidas) == 0:
            result["warnings"].append(
                f"Chapter [{cap_code}] has 0 partidas."
            )

        for j, p in enumerate(partidas):
            result["partidas"] += 1
            if not isinstance(p, dict):
                result["errors"].append(
                    f"Chapter [{cap_code}] partida [{j}] is not a dict."
                )
                result["ok"] = False
                continue

            missing_p = required_partida_keys - set(p.keys())
            if missing_p:
                result["errors"].append(
                    f"Chapter [{cap_code}] partida [{j}] missing keys: {missing_p}"
                )
                result["ok"] = False

            # Check numeric fields are float/int/None
            for fld in numeric_fields:
                val = p.get(fld)
                if val is not None and not isinstance(val, (int, float)):
                    if strict:
                        result["errors"].append(
                            f"[{cap_code}/{p.get('codigo','?')}] '{fld}' = {val!r} "
                            f"({type(val).__name__}) — expected float/null"
                        )
                        result["ok"] = False
                    else:
                        result["warnings"].append(
                            f"[{cap_code}/{p.get('codigo','?')}] '{fld}' = {val!r} "
                            f"is not numeric"
                        )

            # nombre/descripcion text
            if not p.get("nombre") and not p.get("descripcion"):
                result["warnings"].append(
                    f"[{cap_code}/{p.get('codigo','?')}] both nombre and descripcion are empty."
                )

    return result


# ─── Run function ─────────────────────────────────────────────────────────────

def run(
    pdf_path: str,
    output_dir: str,
    api_key: str | None = None,
    model: str | None = None,
    schema_path: str | None = None,
    force: bool = False,
    save_markdown: bool = True,
) -> str:
    """
    Ejecuta Phase 1: extracción PDF → FINAL_ JSON con LandingAI ADE.

    Parameters
    ----------
    pdf_path    : ruta al archivo PDF
    output_dir  : directorio base de salida
    api_key     : ADE API key (si None, lee de .env)
    model       : modelo ADE (default: env LANDING_AI_MODEL o 'dpt-2')
    schema_path : ruta a schema JSON personalizado (opcional)
    force       : si True, re-ejecuta aunque FINAL_ ya exista
    save_markdown : si True, guarda el markdown intermedio

    Returns
    -------
    Ruta al FINAL_{safe_name}.json generado.
    """
    from src.features.extraction.infrastructure.ade_client import AdeClient, AdeError
    from src.features.extraction.domain.budget_models import BudgetDocument
    from src.features.extraction.application.chapter_merger import merge_duplicate_chapters

    filename  = os.path.basename(pdf_path)
    pdf_name  = os.path.splitext(filename)[0]
    safe_name = _safe_dirname(pdf_name)
    pdf_out   = os.path.join(output_dir, safe_name)
    os.makedirs(pdf_out, exist_ok=True)

    final_path = os.path.join(pdf_out, f"FINAL_{safe_name}.json")

    if os.path.exists(final_path) and not force:
        print(f"[{safe_name}] 📂 FINAL_ ya existe — cargando desde caché.")
        print(f"    (usa --force para forzar re-extracción)")
        data = json.loads(Path(final_path).read_text(encoding="utf-8"))
        _print_validation(data, safe_name)
        return final_path

    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"PDF no encontrado: {pdf_path}")

    # ── Resolve API key ──────────────────────────────────────────────────────
    api_key = api_key or os.getenv("ADE_API_KEY") or os.getenv("VISION_AGENT_API_KEY")
    if not api_key:
        raise RuntimeError(
            "API key requerida. Configura ADE_API_KEY o VISION_AGENT_API_KEY en .env"
        )
    model = model or os.getenv("LANDING_AI_MODEL", "dpt-2")

    # ── 1. Parse PDF → Markdown ──────────────────────────────────────────────
    client = AdeClient(api_key=api_key, model=model)

    print(f"\n{'='*60}")
    print(f"  PHASE 1: EXTRACCIÓN — {filename}")
    print(f"{'='*60}")
    print(f"  Backend:  LandingAI ADE")
    print(f"  Modelo:   {model}")
    print(f"  PDF:      {pdf_path}")
    print(f"  Output:   {pdf_out}")
    print(f"{'='*60}\n")

    t0 = time.time()
    print(f"[{safe_name}] 📄 ADE /parse — convirtiendo PDF a Markdown...")
    markdown = client.parse_pdf(pdf_path)
    t_parse = time.time() - t0
    print(f"[{safe_name}] ✅ Markdown: {len(markdown):,} caracteres ({t_parse:.1f}s)")

    if save_markdown:
        md_path = os.path.join(pdf_out, f"markdown_{safe_name}.md")
        Path(md_path).write_text(markdown, encoding="utf-8")
        print(f"[{safe_name}] 💾 Markdown guardado: {md_path}")

    # ── 2. Load / generate schema ────────────────────────────────────────────
    if schema_path and os.path.exists(schema_path):
        schema = json.loads(Path(schema_path).read_text(encoding="utf-8"))
        print(f"[{safe_name}] 📋 Schema personalizado: {schema_path}")
    else:
        schema = BudgetDocument.ade_json_schema()
        print(f"[{safe_name}] 📋 Schema Pydantic generado automáticamente")
        # Save schema for reference
        schema_out = os.path.join(pdf_out, "ade_schema.json")
        Path(schema_out).write_text(
            json.dumps(schema, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    # ── 3. Extract → structured JSON ────────────────────────────────────────
    t1 = time.time()
    print(f"[{safe_name}] 🔍 ADE /extract — extrayendo datos estructurados...")
    raw = client.extract(markdown, schema)
    t_extract = time.time() - t1
    print(f"[{safe_name}] ✅ Extracción raw completada ({t_extract:.1f}s)")

    # Save raw ADE response for debugging
    raw_path = os.path.join(pdf_out, f"raw_ade_response_{safe_name}.json")
    Path(raw_path).write_text(
        json.dumps(raw, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"[{safe_name}] 💾 Raw ADE response: {raw_path}")

    # ── 4. Validate + normalize with Pydantic ────────────────────────────────
    print(f"[{safe_name}] 🔧 Normalizando con Pydantic...")
    doc = BudgetDocument.from_ade_response(raw)
    result = doc.to_internal_json()

    if not result:
        raise RuntimeError(
            f"[{safe_name}] La extracción devolvió 0 capítulos después de validación."
        )
    # ── 4.5 Merge duplicate chapters + fill missing partida codes ─────────────
    print(f"[{safe_name}] [cleanup] Fusionando capitulos duplicados y rellenando codigos...")
    result = merge_duplicate_chapters(result)
    # ── 5. Write FINAL_ ──────────────────────────────────────────────────────
    Path(final_path).write_text(
        json.dumps(result, indent=4, ensure_ascii=False), encoding="utf-8"
    )

    t_total = time.time() - t0
    print(f"\n[{safe_name}] 💾 FINAL_ guardado: {final_path}")
    print(f"[{safe_name}] ⏱️  Tiempo total: {t_total:.1f}s (parse={t_parse:.1f}s, extract={t_extract:.1f}s)")

    # ── 6. Validate output ───────────────────────────────────────────────────
    _print_validation(result, safe_name)

    # ── Write plan log ───────────────────────────────────────────────────────
    plan_path = os.path.join(pdf_out, "plan_log.json")
    plan_data = {
        "source": "landingai",
        "model": model,
        "tasks": [{"id": "landingai_1"}],
        "chapters_extracted": len(result),
        "total_partidas": sum(len(ch.get("partidas", [])) for ch in result),
        "timing": {
            "parse_seconds": round(t_parse, 2),
            "extract_seconds": round(t_extract, 2),
            "total_seconds": round(t_total, 2),
        },
    }
    Path(plan_path).write_text(
        json.dumps(plan_data, indent=4, ensure_ascii=False), encoding="utf-8"
    )

    return final_path


def _print_validation(data: list, safe_name: str) -> None:
    """Run validation and print a structured report."""
    v = _validate_final_json(data)

    print(f"\n{'─'*60}")
    print(f"  VALIDACIÓN — {safe_name}")
    print(f"{'─'*60}")
    print(f"  Capítulos:  {v['chapters']}")
    print(f"  Partidas:   {v['partidas']}")
    print(f"  Estado:     {'✅ OK' if v['ok'] else '❌ ERRORES'}")

    if v["warnings"]:
        print(f"\n  ⚠️  Warnings ({len(v['warnings'])}):")
        for w in v["warnings"][:20]:
            print(f"    - {w}")
        if len(v["warnings"]) > 20:
            print(f"    ... y {len(v['warnings']) - 20} más")

    if v["errors"]:
        print(f"\n  ❌ Errors ({len(v['errors'])}):")
        for e in v["errors"][:20]:
            print(f"    - {e}")
        if len(v["errors"]) > 20:
            print(f"    ... y {len(v['errors']) - 20} más")

    # Quick overview: first 5 chapters
    print(f"\n  📊 Resumen de capítulos:")
    for ch in data[:10]:
        n_p = len(ch.get("partidas", []))
        total = ch.get("total_capitulo")
        total_str = f"{total:,.2f}" if isinstance(total, (int, float)) and total else "—"
        print(
            f"    CAP {ch.get('capitulo_codigo', '?'):>5}  "
            f"{n_p:>3} partidas  "
            f"total={total_str:>12}  "
            f"{(ch.get('capitulo_nombre') or '')[:50]}"
        )
    if len(data) > 10:
        print(f"    ... y {len(data) - 10} capítulos más")
    print(f"{'─'*60}\n")


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Phase 1: Extracción de presupuesto PDF con LandingAI ADE",
    )
    parser.add_argument(
        "--pdf",
        default=None,
        help="Ruta al PDF (por defecto: ricard.pdf en pdfs/)",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directorio base de salida (por defecto: output/test_phases)",
    )
    parser.add_argument(
        "--schema",
        default=None,
        help="Ruta a schema JSON personalizado para ADE /extract",
    )
    parser.add_argument(
        "--model",
        default=None,
        help="Modelo ADE (default: env LANDING_AI_MODEL o 'dpt-2')",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Forzar re-extracción aunque FINAL_ ya exista",
    )
    parser.add_argument(
        "--no-markdown",
        action="store_true",
        help="No guardar el markdown intermedio",
    )
    args = parser.parse_args()

    from dotenv import load_dotenv
    load_dotenv(os.path.join(_BACKEND_DIR, ".env"))

    # Default paths relative to project root
    _base = _BACKEND_DIR
    pdf_path = args.pdf or os.path.join(_base, "pdfs", "ricard.pdf")
    output_dir = args.output_dir or os.path.join(_base, "output", "test_phases")

    # Resolve relative paths
    pdf_path = os.path.abspath(pdf_path)
    output_dir = os.path.abspath(output_dir)

    print(f"PDF:        {pdf_path}")
    print(f"Output dir: {output_dir}")

    if not os.path.exists(pdf_path):
        print(f"\n❌ PDF no encontrado: {pdf_path}")
        sys.exit(1)

    try:
        result_path = run(
            pdf_path=pdf_path,
            output_dir=output_dir,
            schema_path=args.schema,
            model=args.model,
            force=args.force,
            save_markdown=not args.no_markdown,
        )
        print(f"\n🏁 Resultado: {result_path}")
    except Exception as exc:
        print(f"\n❌ Error: {type(exc).__name__}: {exc}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

