"""
Tests for the ADE extraction pipeline.

Covers:
- Pydantic models (normalisation, validation, schema generation)
- AdeClient (mocked HTTP)
- End-to-end ``extract_offer_to_internal_json`` (mocked HTTP)
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure project root is on the path
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.features.extraction.domain.budget_models import (
    AdeBudgetExtraction,
    AdeChapter,
    AdeItem,
    BudgetDocument,
    Chapter,
    Item,
    _normalise_number,
    _normalise_unit,
    _clean_text,
)
from src.features.extraction.infrastructure.ade_client import (
    AdeClient,
    AdeConfigError,
    AdeHTTPError,
    AdeParseError,
)


# =========================================================================
# Fixtures
# =========================================================================


FAKE_ADE_PARSE_RESPONSE = {
    "markdown": "# Presupuesto\n\n| Código | Descripción | Ud | Cantidad | Precio | Total |\n",
}

FAKE_ADE_EXTRACT_RESPONSE = {
    "document_title": "Presupuesto Test",
    "project_details": None,
    "chapters": [
        {
            "chapter_number": "01",
            "chapter_title": "DEMOLICIONES",
            "chapter_total": 1500.50,
            "item": [
                {
                    "item_code": "01.001",
                    "item_title": "Demolición tabique",
                    "item_description": "Demolición de tabique de ladrillo cerámico",
                    "item_unit": "m2",
                    "item_quantity": 25.0,
                    "item_price": 12.50,
                    "item_total": 312.50,
                    "item_componentes": [
                        {
                            "description_component": "Mano de obra",
                            "quantity_component": 0.3,
                            "price_component": 22.0,
                            "total_component": 6.6,
                        },
                    ],
                },
                {
                    "item_code": "01.002",
                    "item_title": "Retirada escombros",
                    "item_description": "Retirada de escombros a vertedero",
                    "item_unit": "m3",
                    "item_quantity": 10.0,
                    "item_price": 35.00,
                    "item_total": 350.00,
                    "item_componentes": None,
                },
            ],
        },
        {
            "chapter_number": "02",
            "chapter_title": "ALBAÑILERÍA",
            "chapter_total": None,  # deliberately missing → auto-computed
            "item": [
                {
                    "item_code": "02.001",
                    "item_title": "Tabique LH",
                    "item_description": None,
                    "item_unit": "m²",
                    "item_quantity": 40.0,
                    "item_price": 18.0,
                    "item_total": 720.0,
                    "item_componentes": None,
                },
            ],
        },
    ],
}


# =========================================================================
# Unit tests — number / text normalisation helpers
# =========================================================================


class TestNormaliseNumber:
    """Test ``_normalise_number`` with ES/EN decimal formats."""

    def test_none(self):
        assert _normalise_number(None) is None

    def test_empty_string(self):
        assert _normalise_number("") is None

    def test_int(self):
        assert _normalise_number(42) == 42.0

    def test_float(self):
        assert _normalise_number(3.14) == 3.14

    def test_string_plain(self):
        assert _normalise_number("123.45") == 123.45

    def test_es_decimal_comma(self):
        """Spanish format: 1.234,56 → 1234.56"""
        assert _normalise_number("1.234,56") == 1234.56

    def test_en_decimal_dot(self):
        """English format: 1,234.56 → 1234.56"""
        assert _normalise_number("1,234.56") == 1234.56

    def test_comma_only(self):
        """Just a comma decimal: 25,5 → 25.5"""
        assert _normalise_number("25,5") == 25.5

    def test_currency_euro(self):
        assert _normalise_number("1.200,00€") == 1200.0

    def test_currency_eur_prefix(self):
        assert _normalise_number("EUR 500") == 500.0

    def test_bool_ignored(self):
        assert _normalise_number(True) is None

    def test_multiple_dots_thousand_separator(self):
        """1.234.567 → 1234567"""
        assert _normalise_number("1.234.567") == 1234567.0


class TestNormaliseUnit:
    def test_m2_variants(self):
        assert _normalise_unit("m2") == "m2"
        assert _normalise_unit("m²") == "m2"
        assert _normalise_unit("m^2") == "m2"

    def test_m3_variants(self):
        assert _normalise_unit("m3") == "m3"
        assert _normalise_unit("m³") == "m3"

    def test_ud_variants(self):
        assert _normalise_unit("ud") == "ud"
        assert _normalise_unit("u") == "ud"
        assert _normalise_unit("UT") == "ud"

    def test_pa(self):
        assert _normalise_unit("PA") == "pa"
        assert _normalise_unit("P.A") == "pa"

    def test_passthrough(self):
        assert _normalise_unit("METRO LINEAL") == "METRO LINEAL"

    def test_none(self):
        assert _normalise_unit(None) is None


class TestCleanText:
    def test_none(self):
        assert _clean_text(None) is None

    def test_empty_string(self):
        assert _clean_text("  ") is None

    def test_strips(self):
        assert _clean_text("  hello  ") == "hello"


# =========================================================================
# Pydantic models
# =========================================================================


class TestItemModel:
    def test_number_coercion(self):
        item = Item(codigo="X", cantidad="25,5", precio="12.50", total="318,75")
        assert item.cantidad == 25.5
        assert item.precio == 12.5
        assert item.total == 318.75

    def test_unit_normalisation(self):
        item = Item(codigo="X", unidad="m²")
        assert item.unidad == "m2"

    def test_text_cleaning(self):
        item = Item(codigo="X", nombre="  Wall partition  ", descripcion="  long desc  ")
        assert item.nombre == "Wall partition"
        assert item.descripcion == "long desc"


class TestChapterModel:
    def test_auto_total(self):
        """Chapter total auto-computed from items when missing."""
        ch = Chapter(
            capitulo_codigo="01",
            partidas=[
                Item(codigo="01.001", total=100.0),
                Item(codigo="01.002", total=200.5),
            ],
        )
        assert ch.total_capitulo == 300.5

    def test_explicit_total_preserved(self):
        ch = Chapter(
            capitulo_codigo="01",
            total_capitulo=999.0,
            partidas=[Item(codigo="01.001", total=100.0)],
        )
        assert ch.total_capitulo == 999.0


class TestBudgetDocument:
    def test_from_ade_response(self):
        doc = BudgetDocument.from_ade_response(FAKE_ADE_EXTRACT_RESPONSE)
        assert len(doc.chapters) == 2
        assert doc.chapters[0].capitulo_codigo == "01"
        assert doc.chapters[0].capitulo_nombre == "DEMOLICIONES"
        assert len(doc.chapters[0].partidas) == 2
        # Check component
        comps = doc.chapters[0].partidas[0].componentes
        assert comps is not None
        assert len(comps) == 1
        assert comps[0].descripcion == "Mano de obra"

    def test_auto_total_on_missing(self):
        doc = BudgetDocument.from_ade_response(FAKE_ADE_EXTRACT_RESPONSE)
        # Chapter 02 had total=None → computed from items
        ch2 = doc.chapters[1]
        assert ch2.total_capitulo == 720.0

    def test_to_internal_json(self):
        doc = BudgetDocument.from_ade_response(FAKE_ADE_EXTRACT_RESPONSE)
        result = doc.to_internal_json()
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["capitulo_codigo"] == "01"
        assert "partidas" in result[0]
        assert result[0]["partidas"][0]["codigo"] == "01.001"

    def test_ade_json_schema(self):
        schema = BudgetDocument.ade_json_schema()
        assert isinstance(schema, dict)
        assert "properties" in schema
        assert "chapters" in schema["properties"]


class TestBudgetDocumentFallbackCodes:
    def test_missing_chapter_number_gets_fallback(self):
        raw = {
            "chapters": [
                {
                    "chapter_number": None,
                    "chapter_title": "NO NUMBER",
                    "chapter_total": 100.0,
                    "item": [],
                }
            ]
        }
        doc = BudgetDocument.from_ade_response(raw)
        assert doc.chapters[0].capitulo_codigo == "CAP_01"

    def test_missing_item_code_gets_fallback(self):
        raw = {
            "chapters": [
                {
                    "chapter_number": "01",
                    "chapter_title": "CH1",
                    "chapter_total": None,
                    "item": [
                        {
                            "item_code": None,
                            "item_title": "Some item",
                            "item_description": None,
                            "item_unit": None,
                            "item_quantity": None,
                            "item_price": None,
                            "item_total": None,
                            "item_componentes": None,
                        }
                    ],
                }
            ]
        }
        doc = BudgetDocument.from_ade_response(raw)
        assert doc.chapters[0].partidas[0].codigo == "SIN_COD_001"


# =========================================================================
# AdeClient tests (mocked HTTP)
# =========================================================================


class TestAdeClientConfig:
    def test_missing_api_key_raises(self):
        with patch.dict(os.environ, {}, clear=True):
            # Also clear potential env vars
            for k in ("ADE_API_KEY", "VISION_AGENT_API_KEY"):
                os.environ.pop(k, None)
            with pytest.raises(AdeConfigError):
                AdeClient(api_key="")

    def test_explicit_config(self):
        client = AdeClient(
            api_key="test-key",
            base_url="https://example.com",
            timeout=10,
            retries=1,
            model="test-model",
        )
        assert client.api_key == "test-key"
        assert client.base_url == "https://example.com"
        assert client.timeout == 10
        assert client.retries == 1
        assert client.model == "test-model"


class TestAdeClientParsePdf:
    @patch("src.features.extraction.infrastructure.ade_client.requests.post")
    def test_parse_pdf_success(self, mock_post, tmp_path):
        # Create fake PDF file
        pdf = tmp_path / "test.pdf"
        pdf.write_bytes(b"%PDF-1.4 fake")

        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = FAKE_ADE_PARSE_RESPONSE
        mock_post.return_value = mock_resp

        client = AdeClient(api_key="key", retries=1)
        md = client.parse_pdf(str(pdf))

        assert "Presupuesto" in md
        mock_post.assert_called_once()

    def test_parse_pdf_file_not_found(self):
        client = AdeClient(api_key="key", retries=1)
        with pytest.raises(FileNotFoundError):
            client.parse_pdf("/nonexistent/path.pdf")

    @patch("src.features.extraction.infrastructure.ade_client.requests.post")
    def test_parse_pdf_http_error(self, mock_post, tmp_path):
        pdf = tmp_path / "test.pdf"
        pdf.write_bytes(b"%PDF")

        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 500
        mock_resp.text = "Internal Server Error"
        mock_post.return_value = mock_resp

        client = AdeClient(api_key="key", retries=1)
        with pytest.raises(AdeHTTPError) as exc_info:
            client.parse_pdf(str(pdf))
        assert exc_info.value.status == 500

    @patch("src.features.extraction.infrastructure.ade_client.requests.post")
    def test_parse_pdf_missing_markdown(self, mock_post, tmp_path):
        pdf = tmp_path / "test.pdf"
        pdf.write_bytes(b"%PDF")

        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {"something": "else"}
        mock_post.return_value = mock_resp

        client = AdeClient(api_key="key", retries=1)
        with pytest.raises(AdeParseError):
            client.parse_pdf(str(pdf))


class TestAdeClientExtract:
    @patch("src.features.extraction.infrastructure.ade_client.requests.post")
    def test_extract_success(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = FAKE_ADE_EXTRACT_RESPONSE
        mock_post.return_value = mock_resp

        client = AdeClient(api_key="key", retries=1)
        result = client.extract("# Markdown", {"properties": {}})

        assert "chapters" in result
        assert len(result["chapters"]) == 2

    @patch("src.features.extraction.infrastructure.ade_client.requests.post")
    def test_extract_missing_chapters(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {"data": {"no_chapters": True}}
        mock_post.return_value = mock_resp

        client = AdeClient(api_key="key", retries=1)
        with pytest.raises(AdeParseError):
            client.extract("# Markdown", {"properties": {}})

    @patch("src.features.extraction.infrastructure.ade_client.requests.post")
    def test_extract_nested_envelope(self, mock_post):
        """ADE sometimes wraps response in {'data': {...}}."""
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {
            "data": FAKE_ADE_EXTRACT_RESPONSE,
        }
        mock_post.return_value = mock_resp

        client = AdeClient(api_key="key", retries=1)
        result = client.extract("# Markdown", {"properties": {}})
        assert "chapters" in result


class TestAdeClientRetries:
    @patch("src.features.extraction.infrastructure.ade_client.time.sleep")  # don't actually sleep
    @patch("src.features.extraction.infrastructure.ade_client.requests.post")
    def test_retries_on_timeout(self, mock_post, mock_sleep, tmp_path):
        import requests as _req

        pdf = tmp_path / "test.pdf"
        pdf.write_bytes(b"%PDF")

        # First two attempts timeout, third succeeds
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = FAKE_ADE_PARSE_RESPONSE
        mock_post.side_effect = [
            _req.Timeout("timeout"),
            _req.Timeout("timeout"),
            mock_resp,
        ]

        client = AdeClient(api_key="key", retries=3)
        md = client.parse_pdf(str(pdf))
        assert "Presupuesto" in md
        assert mock_post.call_count == 3
        assert mock_sleep.call_count == 2


# =========================================================================
# End-to-end: extract_offer_to_internal_json (mocked HTTP)
# =========================================================================


class TestExtractOfferToInternalJson:
    @patch("src.features.extraction.infrastructure.ade_client.requests.post")
    def test_full_pipeline(self, mock_post, tmp_path):
        """Full extraction pipeline with mocked ADE responses."""
        pdf = tmp_path / "offer.pdf"
        pdf.write_bytes(b"%PDF-1.4 fake")

        # First call → parse, second call → extract
        parse_resp = MagicMock()
        parse_resp.ok = True
        parse_resp.json.return_value = FAKE_ADE_PARSE_RESPONSE

        extract_resp = MagicMock()
        extract_resp.ok = True
        extract_resp.json.return_value = FAKE_ADE_EXTRACT_RESPONSE

        mock_post.side_effect = [parse_resp, extract_resp]

        # Import here so sys.path is set up
        from src.features.extraction.application.landing_ai_service import extract_offer_to_internal_json

        result = extract_offer_to_internal_json(
            pdf_path=str(pdf),
            api_key="test-key",
            model="dpt-2",
            timeout_seconds=30,
        )

        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["capitulo_codigo"] == "01"
        assert result[0]["capitulo_nombre"] == "DEMOLICIONES"
        assert len(result[0]["partidas"]) == 2
        assert result[0]["partidas"][0]["codigo"] == "01.001"
        # Chapter 2 total auto-computed
        assert result[1]["total_capitulo"] == 720.0

