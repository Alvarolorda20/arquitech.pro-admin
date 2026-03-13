"""Utilities to infer chapter-level mapping from partida-level mapping links."""

from __future__ import annotations

from collections import defaultdict
from typing import Any


def _extract_cap_from_id(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if "::" in raw:
        return raw.split("::", 1)[0].strip()
    return ""


def _score_from_mapping_logic(mapping_result: dict[str, Any]) -> dict[tuple[str, str], float]:
    """Builds a confidence lookup keyed by (id_oferta, pauta_id)."""
    scores: dict[tuple[str, str], float] = {}
    for key in ("alertas_tecnicas", "logica_de_mapeo"):
        rows = mapping_result.get(key)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            id_oferta = str(row.get("id_oferta") or "").strip()
            pauta_id = str(row.get("pauta_id") or "").strip()
            if not id_oferta or not pauta_id:
                continue
            try:
                confidence = float(row.get("confianza") or 0.0)
            except (TypeError, ValueError):
                confidence = 0.0
            key_pair = (id_oferta, pauta_id)
            previous = scores.get(key_pair, 0.0)
            if confidence > previous:
                scores[key_pair] = confidence
    return scores


def _ordered_offer_caps(offer_data: Any) -> list[str]:
    if not isinstance(offer_data, list):
        return []
    ordered: list[str] = []
    seen: set[str] = set()
    for chapter in offer_data:
        if not isinstance(chapter, dict):
            continue
        cap = str(chapter.get("capitulo_codigo") or "").strip()
        if not cap or cap in seen:
            continue
        seen.add(cap)
        ordered.append(cap)
    return ordered


def derive_chapter_mapping_from_links(
    mapping_result: dict[str, Any] | None,
    *,
    offer_data: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Derives a CAP_MAPPING-style payload from partida links.

    Rules:
    - Each mapped partida votes for (offer_cap -> pauta_cap).
    - A chapter match is the pauta chapter with highest weighted vote.
    - Weight = 1 + max confidence for that mapping pair (if present).
    - ``extra_cap_mapping`` contains offer chapters with no mapped partidas.
    """
    payload = mapping_result if isinstance(mapping_result, dict) else {}
    raw_mapping = payload.get("mapping")
    if not isinstance(raw_mapping, dict):
        raw_mapping = {}

    pair_scores = _score_from_mapping_logic(payload)
    votes: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    mapped_offer_caps: set[str] = set()

    for offer_id_raw, pauta_id_raw in raw_mapping.items():
        offer_id = str(offer_id_raw or "").strip()
        pauta_id = str(pauta_id_raw or "").strip()
        offer_cap = _extract_cap_from_id(offer_id)
        pauta_cap = _extract_cap_from_id(pauta_id)
        if not offer_cap or not pauta_cap:
            continue
        mapped_offer_caps.add(offer_cap)
        weight = 1.0 + max(0.0, pair_scores.get((offer_id, pauta_id), 0.0))
        votes[offer_cap][pauta_cap] += weight

    cap_mapping: dict[str, str] = {}
    for offer_cap, pauta_scores in votes.items():
        if not pauta_scores:
            continue
        best_pauta_cap = max(
            pauta_scores.items(),
            key=lambda item: (item[1], item[0]),
        )[0]
        cap_mapping[offer_cap] = best_pauta_cap

    ordered_caps = _ordered_offer_caps(offer_data)
    if ordered_caps:
        ordered_cap_mapping: dict[str, str] = {}
        for cap in ordered_caps:
            if cap in cap_mapping:
                ordered_cap_mapping[cap] = cap_mapping[cap]
        cap_mapping = ordered_cap_mapping
        extra_cap_mapping = [cap for cap in ordered_caps if cap not in mapped_offer_caps]
    else:
        extras_raw = payload.get("extras")
        extra_caps = set()
        if isinstance(extras_raw, list):
            for offer_id in extras_raw:
                cap = _extract_cap_from_id(offer_id)
                if cap:
                    extra_caps.add(cap)
        extra_cap_mapping = sorted(cap for cap in extra_caps if cap not in mapped_offer_caps)

    return {
        "cap_mapping": cap_mapping,
        "extra_cap_mapping": extra_cap_mapping,
    }

