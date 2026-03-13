#!/usr/bin/env python
"""
CLI para extracción standalone de un presupuesto PDF mediante LandingAI ADE.

Uso:
    python -m scripts.run_extract --pdf ruta/al/presupuesto.pdf
    python -m scripts.run_extract --pdf ruta/al/presupuesto.pdf --out resultado.json
    python -m scripts.run_extract --pdf ruta/al/presupuesto.pdf --schema mi_schema.json

Variables de entorno requeridas (en .env o exportadas):
    ADE_API_KEY  o  VISION_AGENT_API_KEY
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

# Asegurar que el directorio raíz del proyecto esté en sys.path
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env")

from src.features.extraction.infrastructure.ade_client import AdeClient, AdeError  # noqa: E402
from src.features.extraction.domain.budget_models import BudgetDocument  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Extrae un presupuesto PDF usando LandingAI ADE y devuelve JSON interno.",
    )
    p.add_argument(
        "--pdf",
        required=True,
        help="Ruta al archivo PDF del presupuesto.",
    )
    p.add_argument(
        "--out", "-o",
        default=None,
        help="Ruta de salida para el JSON resultante (por defecto: stdout).",
    )
    p.add_argument(
        "--schema",
        default=None,
        help="Ruta a un fichero JSON con schema personalizado para ADE /extract.",
    )
    p.add_argument(
        "--model",
        default=None,
        help="Modelo ADE a utilizar (default: env LANDING_AI_MODEL o 'dpt-2').",
    )
    p.add_argument(
        "--timeout",
        type=int,
        default=None,
        help="Timeout en segundos por request HTTP (default: env ADE_TIMEOUT o 240).",
    )
    p.add_argument(
        "--markdown-out",
        default=None,
        help="Si se pasa, guarda el markdown intermedio en esta ruta.",
    )
    p.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Activa logging detallado (DEBUG).",
    )
    return p


def main() -> None:
    args = _build_parser().parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )
    log = logging.getLogger("run_extract")

    pdf_path = Path(args.pdf)
    if not pdf_path.exists():
        log.error("No se encontró el PDF: %s", pdf_path)
        sys.exit(1)

    # ── Cliente ADE ──────────────────────────────────────────────────
    api_key = os.getenv("ADE_API_KEY") or os.getenv("VISION_AGENT_API_KEY") or ""
    model = args.model or os.getenv("LANDING_AI_MODEL", "dpt-2")

    overrides: dict = {}
    if args.timeout:
        overrides["timeout"] = args.timeout

    client = AdeClient(api_key=api_key, model=model, **overrides)

    try:
        # 1) Parse → Markdown
        log.info("Parseando PDF: %s", pdf_path)
        markdown = client.parse_pdf(str(pdf_path))
        log.info("Markdown obtenido (%d caracteres).", len(markdown))

        if args.markdown_out:
            Path(args.markdown_out).write_text(markdown, encoding="utf-8")
            log.info("Markdown guardado en %s", args.markdown_out)

        # 2) Schema
        if args.schema:
            schema = json.loads(Path(args.schema).read_text(encoding="utf-8"))
            log.info("Schema personalizado cargado desde %s", args.schema)
        else:
            schema = BudgetDocument.ade_json_schema()
            log.info("Schema Pydantic generado automáticamente.")

        # 3) Extract → JSON crudo
        log.info("Extrayendo datos estructurados …")
        raw = client.extract(markdown, schema)

        # 4) Validar y normalizar con Pydantic
        doc = BudgetDocument.from_ade_response(raw)
        result = doc.to_internal_json()

        # ── Resumen rápido ───────────────────────────────────────────
        total_partidas = sum(len(ch.get("partidas", [])) for ch in result)
        log.info(
            "Extracción completa: %d capítulos, %d partidas.",
            len(result),
            total_partidas,
        )

        # 5) Output
        json_str = json.dumps(result, ensure_ascii=False, indent=2)
        if args.out:
            Path(args.out).write_text(json_str, encoding="utf-8")
            log.info("JSON guardado en %s", args.out)
        else:
            print(json_str)

    except AdeError as exc:
        log.error("Error ADE: %s", exc)
        sys.exit(2)
    except Exception as exc:
        log.exception("Error inesperado: %s", exc)
        sys.exit(3)


if __name__ == "__main__":
    main()

