"""
Pydantic v2 models for budget/estimate extraction.

These models serve two purposes:
1. Generate the JSON Schema sent to ADE /extract (via ``model_json_schema()``).
2. Validate + normalise the raw JSON returned by ADE (via ``model_validate()``).

The final ``model_dump()`` output matches the internal offer JSON structure
consumed by the existing mapping / auditing pipeline.
"""

from __future__ import annotations

import re
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_UNIT_NORM: dict[str, str] = {
    "m2": "m2", "m^2": "m2", "m\u00b2": "m2",
    "m3": "m3", "m^3": "m3", "m\u00b3": "m3",
    "ml": "ml", "m.l": "ml", "m/l": "ml",
    "ud": "ud", "u": "ud", "ut": "ud",
    "kg": "kg",
    "pa": "pa", "p.a": "pa",
}


def _normalise_number(value: Any) -> float | None:
    """Coerce a value to ``float`` handling ES/EN decimal formats."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return None

    # Remove currency symbols and non-breaking spaces
    text = text.replace("\u00a0", "").replace(" ", "")
    text = re.sub(r"(?i)eur|€|\$|£", "", text).strip()
    if not text:
        return None

    # ES/EN decimal disambiguation
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(".", "").replace(",", ".")
    elif text.count(".") > 1:
        text = text.replace(".", "")

    filtered = "".join(ch for ch in text if ch in "0123456789.-")
    if filtered in ("", "-", ".", "-."):
        return None
    try:
        return float(filtered)
    except ValueError:
        return None


def _clean_text(value: Any) -> str | None:
    """Return stripped text or ``None``."""
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def _normalise_unit(value: Any) -> str | None:
    raw = _clean_text(value)
    if raw is None:
        return None
    key = raw.lower().replace(" ", "").replace(".", "")
    # Handle unicode superscripts and occasional mojibake variants from OCR/PDF extraction.
    key = key.translate({
        0x00B2: ord("2"),  # ²
        0x00B3: ord("3"),  # ³
    })
    key = (
        key.replace("^2", "2")
        .replace("^3", "3")
        .replace("\u00c2", "")
        .replace("\u00e2", "")
    )
    return _UNIT_NORM.get(key, raw)


# ---------------------------------------------------------------------------
# ADE extraction schema models  (sent to /extract)
# ---------------------------------------------------------------------------
# These carry ``nullable: true`` via ``json_schema_extra`` so ADE can return
# ``null`` for optional fields.

class AdeComponent(BaseModel):
    """Component contributing to an item price (materials, labour, …)."""
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    description_component: str | None = Field(
        None,
        description="Component description (e.g. 'Skilled labor', 'Cement mortar').",
        json_schema_extra={"nullable": True},
    )
    quantity_component: float | None = Field(
        None,
        description="Numeric quantity of the component.",
        json_schema_extra={"nullable": True},
    )
    price_component: float | None = Field(
        None,
        description="Numeric unit price of the component.",
        json_schema_extra={"nullable": True},
    )
    total_component: float | None = Field(
        None,
        description="Numeric total of the component.",
        json_schema_extra={"nullable": True},
    )


class AdeItem(BaseModel):
    """Single line item / partida inside a chapter."""
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    item_code: str | None = Field(
        None,
        description="Item code exactly as shown (e.g. '01.02.003').",
        json_schema_extra={"nullable": True},
    )
    item_unit: str | None = Field(
        None,
        description="Unit of measure (e.g. 'm2', 'kg', 'ud').",
        json_schema_extra={"nullable": True},
    )
    item_title: str | None = Field(
        None,
        description="Short concept/name of the item.",
        json_schema_extra={"nullable": True},
    )
    item_description: str | None = Field(
        None,
        description="Full descriptive text including specs and conditions.",
        json_schema_extra={"nullable": True},
    )
    item_quantity: float | None = Field(
        None,
        description="Numeric quantity (no unit).",
        json_schema_extra={"nullable": True},
    )
    item_price: float | None = Field(
        None,
        description="Numeric unit price.",
        json_schema_extra={"nullable": True},
    )
    item_total: float | None = Field(
        None,
        description="Numeric total amount.",
        json_schema_extra={"nullable": True},
    )
    item_componentes: list[AdeComponent] | None = Field(
        None,
        description="Optional breakdown into components.",
        json_schema_extra={"nullable": True},
    )


class AdeChapter(BaseModel):
    """Budget chapter / section."""
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    chapter_number: str | None = Field(
        None,
        description=(
            "Chapter number exactly as shown (e.g. '01', 'CHAPTER 1'). "
            "Use the most specific structural level available. "
            "If the document has an umbrella chapter (e.g. '22') and real subchapters "
            "('22.1', '22.2'), return each subchapter as its own chapter_number and do not "
            "collapse all items into the umbrella parent unless the parent has direct items."
        ),
        json_schema_extra={"nullable": True},
    )
    chapter_title: str | None = Field(
        None,
        description="Chapter title (e.g. 'Demolitions').",
        json_schema_extra={"nullable": True},
    )
    chapter_total: float | None = Field(
        None,
        description="Total amount for the chapter.",
        json_schema_extra={"nullable": True},
    )
    item: list[AdeItem] = Field(
        default_factory=list,
        description=(
            "Line items included in the chapter. "
            "Assign each item to the nearest matching subchapter heading by code prefix "
            "(e.g. item 22.2.4 belongs to chapter 22.2, not 22)."
        ),
    )


class AdeProjectDetails(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    promoter_name: str | None = Field(None, json_schema_extra={"nullable": True})
    technician_name: str | None = Field(None, json_schema_extra={"nullable": True})
    project_reference: str | None = Field(None, json_schema_extra={"nullable": True})
    project_address: str | None = Field(
        None,
        description=(
            "Full postal address of the project site as written in the document "
            "(street, number, postal code, city, province/country). Extract exactly as shown."
        ),
        json_schema_extra={"nullable": True},
    )


class AdeBudgetExtraction(BaseModel):
    """Top-level model mirroring the ADE extraction schema."""
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    document_title: str | None = Field(None, json_schema_extra={"nullable": True})
    project_details: AdeProjectDetails | None = Field(None, json_schema_extra={"nullable": True})
    chapters: list[AdeChapter] = Field(
        default_factory=list,
        description=(
            "Budget chapters preserving document hierarchy at the most specific level. "
            "When subchapters exist, output separate chapter objects per subchapter."
        ),
    )


# ---------------------------------------------------------------------------
# Internal pipeline models  (validated + normalised)
# ---------------------------------------------------------------------------

class Component(BaseModel):
    """Component / subpartida."""
    descripcion: str | None = None
    cantidad: float | None = None
    unidad: str | None = None
    precio: float | None = None
    total: float | None = None


class Item(BaseModel):
    """Single line item / partida."""
    codigo: str | None = None
    nombre: str | None = None
    descripcion: str | None = None
    unidad: str | None = None
    cantidad: float | None = None
    precio: float | None = None
    total: float | None = None
    componentes: list[Component] | None = None

    @field_validator("cantidad", "precio", "total", mode="before")
    @classmethod
    def _coerce_number(cls, v: Any) -> float | None:
        return _normalise_number(v)

    @field_validator("unidad", mode="before")
    @classmethod
    def _norm_unit(cls, v: Any) -> str | None:
        return _normalise_unit(v)

    @field_validator("nombre", "descripcion", mode="before")
    @classmethod
    def _clean(cls, v: Any) -> str | None:
        return _clean_text(v)


class Chapter(BaseModel):
    """Budget chapter."""
    capitulo_codigo: str | None = None
    capitulo_nombre: str | None = None
    total_capitulo: float | None = None
    partidas: list[Item] = Field(default_factory=list)

    @field_validator("total_capitulo", mode="before")
    @classmethod
    def _coerce_total(cls, v: Any) -> float | None:
        return _normalise_number(v)

    @field_validator("capitulo_nombre", mode="before")
    @classmethod
    def _clean_name(cls, v: Any) -> str | None:
        return _clean_text(v)

    @model_validator(mode="after")
    def _fill_total_from_items(self) -> "Chapter":
        """Compute total from items when the chapter total is missing."""
        if self.total_capitulo is None and self.partidas:
            known = [p.total for p in self.partidas if p.total is not None]
            if known:
                self.total_capitulo = round(sum(known), 2)
        return self


class BudgetDocument(BaseModel):
    """
    Top-level validated budget.

    ``model_dump()`` returns the internal JSON structure expected by the
    pipeline (list of chapter dicts with ``capitulo_codigo``, ``partidas``, …).
    """
    chapters: list[Chapter] = Field(default_factory=list)

    # ------------------------------------------------------------------
    # Conversion helpers
    # ------------------------------------------------------------------

    @classmethod
    def from_ade_response(cls, raw: dict[str, Any]) -> "BudgetDocument":
        """
        Build a ``BudgetDocument`` from the raw ADE extract payload.

        Handles the ADE→internal field mapping so the caller never touches
        the ADE-specific key names.
        """
        ade = AdeBudgetExtraction.model_validate(raw)
        chapters: list[Chapter] = []

        for idx, ch in enumerate(ade.chapters, start=1):
            code = _clean_text(ch.chapter_number) or f"CAP_{idx:02d}"
            name = _clean_text(ch.chapter_title) or f"CAPITULO {code}"
            total = _normalise_number(ch.chapter_total)

            partidas: list[Item] = []
            for it_idx, it in enumerate(ch.item, start=1):
                item_code = _clean_text(it.item_code) or f"SIN_COD_{it_idx:03d}"
                nombre = _clean_text(it.item_title)
                descripcion = _clean_text(it.item_description) or nombre

                comps: list[Component] | None = None
                if it.item_componentes:
                    comps = [
                        Component(
                            descripcion=_clean_text(c.description_component),
                            cantidad=_normalise_number(c.quantity_component),
                            precio=_normalise_number(c.price_component),
                            total=_normalise_number(c.total_component),
                        )
                        for c in it.item_componentes
                    ]

                partidas.append(Item(
                    codigo=item_code,
                    nombre=nombre,
                    descripcion=descripcion,
                    unidad=it.item_unit,
                    cantidad=it.item_quantity,
                    precio=it.item_price,
                    total=it.item_total,
                    componentes=comps if comps else None,
                ))

            chapters.append(Chapter(
                capitulo_codigo=code,
                capitulo_nombre=name,
                total_capitulo=total,
                partidas=partidas,
            ))

        return cls(chapters=chapters)

    def to_internal_json(self) -> list[dict[str, Any]]:
        """
        Serialise to the list-of-chapter-dicts format consumed by the
        mapping / auditing pipeline.
        """
        return [ch.model_dump() for ch in self.chapters]

    @classmethod
    def ade_json_schema(cls) -> dict[str, Any]:
        """Return the JSON Schema to send to ADE ``/extract``."""
        return AdeBudgetExtraction.model_json_schema()
