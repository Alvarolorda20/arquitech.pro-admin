"""
LandingAI extraction — thin wrapper around AdeClient + BudgetDocument.

Public API (unchanged for backward compatibility)::

    extract_offer_to_internal_json(
        pdf_path, api_key, model, schema_path, timeout_seconds
    ) -> list[dict]

Internally delegates to:
    1. ``AdeClient.parse_pdf()``  →  markdown
    2. ``AdeClient.extract()``    →  raw JSON
    3. ``BudgetDocument.from_ade_response()``  →  validated Pydantic model
    4. ``.to_internal_json()``  →  list[dict] (pipeline-compatible)
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from src.features.extraction.infrastructure.ade_client import AdeClient
from src.features.extraction.domain.budget_models import BudgetDocument

logger = logging.getLogger(__name__)


def extract_offer_to_internal_payload(
    pdf_path: str,
    api_key: str,
    model: str = "dpt-2",
    schema_path: str | None = None,
    timeout_seconds: int = 300,
    extract_timeout_seconds: int = 900,
) -> dict[str, Any]:
    """
    Run LandingAI ADE Parse + Extract and return the enriched extraction payload.

    Parameters
    ----------
    pdf_path : str
        Path to the PDF file.
    api_key : str
        LandingAI API key.
    model : str
        ADE parse model (default ``dpt-2``).
    schema_path : str | None
        Optional path to a custom JSON schema file.  When ``None`` the
        auto-generated Pydantic schema is used.
    timeout_seconds : int
        HTTP timeout for /parse (PDF upload). Default 300 s.
    extract_timeout_seconds : int
        HTTP timeout for /extract (LLM processing). Default 900 s (15 min)
        because large budget PDFs can produce very long Markdown that the
        LLM takes a long time to process.

    Returns
    -------
    dict with keys:
        ``"chapters"`` — list of chapter dicts in the internal pipeline format::

            [{"capitulo_codigo": "01", "capitulo_nombre": "…",
              "total_capitulo": 2320.0, "partidas": [...]}, ...]

        ``"project_details"`` — dict with project metadata extracted by ADE
        (``promoter_name``, ``technician_name``, ``project_reference``,
        ``project_address``), or ``None`` if not present in the document.
    """
    if not api_key:
        raise ValueError("LandingAI API key is empty.")
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    # --- Build client -------------------------------------------------------
    client = AdeClient(
        api_key=api_key,
        model=model,
        timeout=timeout_seconds,
        extract_timeout=extract_timeout_seconds,
    )

    # --- 1. Parse PDF → Markdown --------------------------------------------
    markdown = client.parse_pdf(pdf_path)

    # --- 2. Load or generate schema -----------------------------------------
    schema = _load_schema(schema_path)

    # --- 3. Extract structured JSON -----------------------------------------
    raw = client.extract(markdown, schema)

    # --- 4. Validate + normalise with Pydantic ------------------------------
    budget = BudgetDocument.from_ade_response(raw)
    result = budget.to_internal_json()

    if not result:
        raise RuntimeError(
            "LandingAI extraction returned zero chapters after validation."
        )

    # --- 5. Extract project_details from the validated ADE model -----------
    from src.features.extraction.domain.budget_models import AdeBudgetExtraction as _AdeBudgetExtraction
    _ade = _AdeBudgetExtraction.model_validate(raw)
    project_details: dict | None = None
    if _ade.project_details:
        _pd = _ade.project_details
        project_details = {
            "promoter_name":     _pd.promoter_name,
            "technician_name":   _pd.technician_name,
            "project_reference": _pd.project_reference,
            "project_address":   _pd.project_address,
        }

    logger.info(
        "Extraction complete: %d chapter(s), %d total item(s)",
        len(result),
        sum(len(ch.get("partidas", [])) for ch in result),
    )
    return {"chapters": result, "project_details": project_details}


def extract_offer_to_internal_json(
    pdf_path: str,
    api_key: str,
    model: str = "dpt-2",
    schema_path: str | None = None,
    timeout_seconds: int = 300,
    extract_timeout_seconds: int = 900,
) -> list[dict[str, Any]]:
    """
    Backward-compatible API that returns only the internal chapter list.
    """
    payload = extract_offer_to_internal_payload(
        pdf_path=pdf_path,
        api_key=api_key,
        model=model,
        schema_path=schema_path,
        timeout_seconds=timeout_seconds,
        extract_timeout_seconds=extract_timeout_seconds,
    )
    return payload.get("chapters", [])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_schema(schema_path: str | None) -> dict[str, Any]:
    """Load a custom schema or fall back to the Pydantic-generated one."""
    if schema_path and os.path.exists(schema_path):
        with open(schema_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            raise ValueError("Schema file must contain a JSON object.")
        logger.info("Using custom schema from %s", schema_path)
        return data

    logger.info("Using auto-generated Pydantic schema")
    return BudgetDocument.ade_json_schema()


