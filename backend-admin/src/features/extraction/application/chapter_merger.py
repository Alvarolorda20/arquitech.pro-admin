"""
chapter_merger.py — Fusión de capítulos duplicados en el JSON interno del pipeline.
======================================================================================

Problema: LandingAI ADE a veces divide un capítulo largo en dos entradas con el
mismo ``capitulo_codigo``.  El pipeline downstream (mapper, auditor…) necesita
exactamente UN capítulo por código.

Este módulo expone una única función pública::

    merge_duplicate_chapters(chapters: list[dict]) -> list[dict]

Qué hace:
    1. Agrupa capítulos por ``capitulo_codigo`` (normalizado: stripped + lower).
    2. Para cada grupo con más de 1 entrada, fusiona en un único capítulo:
       - ``capitulo_nombre``:  primer valor no nulo/vacío del grupo.
       - ``partidas``:         concatenación de todas las partidas de todos los
                               capítulos del grupo, en orden.
       - ``total_capitulo``:   recalculado como suma de todos los ``total`` de las
                               partidas resultantes (más fiable que combinar los
                               totales de capítulo que pueden ser null o parciales).
    3. Rellena ``codigo`` nulo/vacío en cualquier partida con ``no_code_1``,
       ``no_code_2``, … (numeración global, única en todo el documento).

Uso::

    from src.utils.chapter_merger import merge_duplicate_chapters

    cleaned = merge_duplicate_chapters(raw_chapters)
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _unwrap_chapters(data: Any) -> list[dict[str, Any]]:
    """
    Accepts any of the following shapes and always returns a plain list of
    chapter dicts:

    1. Already a list  →  returned as-is.
    2. Raw ADE envelope  ``{"extraction": {"chapters": [...]}}``.
    3. Any dict with a top-level ``"chapters"`` key.
    4. Any other dict  →  ValueError.
    """
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        # Shape 2 — raw ADE response
        if "extraction" in data and isinstance(data["extraction"], dict):
            inner = data["extraction"]
            if "chapters" in inner and isinstance(inner["chapters"], list):
                return inner["chapters"]
        # Shape 3 — simple dict wrapper
        if "chapters" in data and isinstance(data["chapters"], list):
            return data["chapters"]
    raise ValueError(
        "Cannot extract a chapter list from the provided data. "
        "Expected a list[dict], or a dict with 'chapters' / 'extraction.chapters'."
    )


def _normalise_chapter(ch: dict[str, Any]) -> dict[str, Any]:
    """
    Convert a raw ADE chapter (``chapter_number`` / ``item``) to the
    internal pipeline format (``capitulo_codigo`` / ``partidas``).
    If the chapter already uses the internal format it is returned unchanged.
    """
    # Already internal format
    if "capitulo_codigo" in ch or "partidas" in ch:
        return ch

    # Raw ADE format
    partidas_raw = ch.get("item") or []
    partidas: list[dict[str, Any]] = []
    for it in partidas_raw:
        if not isinstance(it, dict):
            continue
        partidas.append({
            "codigo":       it.get("item_code"),
            "nombre":       it.get("item_title") or it.get("item_description", ""),
            "descripcion":  it.get("item_description", ""),
            "unidad":       it.get("item_unit"),
            "cantidad":     it.get("item_quantity"),
            "precio":       it.get("item_price"),
            "total":        it.get("item_total"),
            "componentes":  it.get("item_componentes") or [],
        })

    total_raw = ch.get("chapter_total")
    return {
        "capitulo_codigo": ch.get("chapter_number"),
        "capitulo_nombre": ch.get("chapter_title"),
        "total_capitulo":  total_raw if isinstance(total_raw, (int, float)) else None,
        "partidas":        partidas,
    }


def _fix_bleeding_descriptions(chapters: list[dict[str, Any]]) -> None:
    """
    In-place fix for partidas whose ``descripcion`` embeds text that actually
    belongs to the *previous* partida.

    Pattern produced by ADE when a page break splits an item description:

        <trailing text of item N>  <code of item N+1>  <description of item N+1>

    ADE places everything in item N+1's ``descripcion``.  We:
      1. Search for the partida's own ``codigo`` inside its ``descripcion``.
      2. Move anything *before* the code to the end of the previous partida's
         ``descripcion``.
      3. Strip the code itself (and surrounding whitespace) from the description.
      4. Leave the text *after* the code as the cleaned ``descripcion``.

    Partidas with ``no_code_*`` codes are skipped.
    """
    for ch in chapters:
        partidas = ch.get("partidas") or []
        for i, partida in enumerate(partidas):
            codigo = (partida.get("codigo") or "").strip()
            if not codigo or codigo.startswith("no_code_"):
                continue
            descripcion = partida.get("descripcion") or ""
            if not descripcion:
                continue

            idx = descripcion.find(codigo)
            if idx == -1:
                continue  # code not present in description — nothing to do

            prefix = descripcion[:idx].strip()
            remainder = descripcion[idx + len(codigo):].strip()

            # Append orphan prefix to previous partida's description
            if prefix and i > 0:
                prev = partidas[i - 1]
                prev_desc = (prev.get("descripcion") or "").rstrip()
                sep = " " if prev_desc and not prev_desc.endswith(" ") else ""
                prev["descripcion"] = prev_desc + sep + prefix

            partida["descripcion"] = remainder


def _absorb_no_code_partidas(chapters: list[dict[str, Any]]) -> None:
    """
    In-place: every ``no_code_*`` partida is converted into a component of the
    closest preceding partida that has a real code, then removed from the
    ``partidas`` list.  After absorption, the host partida's ``total`` and the
    chapter's ``total_capitulo`` are recomputed from scratch.

    If a no_code partida appears before any real partida in a chapter it is
    left untouched.
    """
    for ch in chapters:
        partidas = ch.get("partidas") or []
        to_remove: list[int] = []
        last_real_idx: int | None = None

        for i, partida in enumerate(partidas):
            codigo = (partida.get("codigo") or "").strip()
            if codigo.startswith("no_code_"):
                if last_real_idx is not None:
                    real = partidas[last_real_idx]
                    comp: dict[str, Any] = {
                        "description_component": (
                            partida.get("nombre")
                            or partida.get("descripcion")
                            or codigo
                        ),
                        "quantity_component": partida.get("cantidad"),
                        "price_component":    partida.get("precio"),
                        "total_component":    partida.get("total"),
                    }
                    real.setdefault("componentes", []).append(comp)
                    to_remove.append(i)
                # else: no preceding real partida — leave as-is
            else:
                last_real_idx = i

        # Remove absorbed partidas (reverse order keeps indices valid)
        for i in reversed(to_remove):
            partidas.pop(i)
        ch["partidas"] = partidas

        if not to_remove:
            continue

        # Recompute totals for every partida that gained new components,
        # then recompute the chapter total from scratch.
        for partida in partidas:
            componentes = partida.get("componentes") or []
            if not componentes:
                continue
            comp_sum = sum(
                c["total_component"]
                for c in componentes
                if isinstance(c.get("total_component"), (int, float))
            )
            own_price = partida.get("precio")
            own_price = own_price if isinstance(own_price, (int, float)) else 0
            partida["total"] = round(comp_sum + own_price, 2)

        totals = [
            p.get("total")
            for p in partidas
            if isinstance(p.get("total"), (int, float))
        ]
        if totals:
            ch["total_capitulo"] = round(sum(totals), 2)


def _recalculate_totals(chapters: list[dict[str, Any]]) -> None:
    """
    In-place recalculation of totals at partida and chapter level.

    Rules:
    1. Partidas WITH ``componentes``:
       ``total`` = sum(component ``total_component``) + partida ``precio``
       (partida ``precio`` covers any base/installation cost not in components;
       if it is None or 0 the result is just the component sum.)

    2. Partidas WITHOUT ``componentes`` — if exactly 2 of {cantidad, precio,
       total} are numeric, the missing third is inferred:
         • missing total  → total  = cantidad × precio
         • missing precio → precio = total / cantidad
         • missing cantidad → cantidad = total / precio

    3. Chapter ``total_capitulo`` is always recomputed as the sum of all
       partida ``total`` values (after steps 1/2).
    """
    for ch in chapters:
        partidas = ch.get("partidas") or []
        chapter_sum = 0.0
        chapter_has_total = False

        for partida in partidas:
            componentes = partida.get("componentes") or []

            if componentes:
                # Rule 1
                comp_sum = sum(
                    c["total_component"]
                    for c in componentes
                    if isinstance(c.get("total_component"), (int, float))
                )
                own_price = partida.get("precio")
                own_price = own_price if isinstance(own_price, (int, float)) else 0
                partida["total"] = round(comp_sum + own_price, 2)
            else:
                # Rule 2
                cantidad = partida.get("cantidad")
                precio   = partida.get("precio")
                total    = partida.get("total")
                qty_ok = isinstance(cantidad, (int, float))
                prc_ok = isinstance(precio,   (int, float))
                tot_ok = isinstance(total,    (int, float))

                if not tot_ok and qty_ok and prc_ok:
                    partida["total"] = round(cantidad * precio, 2)
                elif not prc_ok and qty_ok and tot_ok and cantidad != 0:
                    partida["precio"] = round(total / cantidad, 4)
                elif not qty_ok and prc_ok and tot_ok and precio != 0:
                    partida["cantidad"] = round(total / precio, 4)

            t = partida.get("total")
            if isinstance(t, (int, float)):
                chapter_sum += t
                chapter_has_total = True

        if chapter_has_total:
            ch["total_capitulo"] = round(chapter_sum, 2)


def merge_duplicate_chapters(chapters: Any) -> list[dict[str, Any]]:
    """
    Elimina capítulos duplicados y rellena códigos de partida vacíos.

    Acepta tanto el formato interno del pipeline como la respuesta RAW de
    ADE (``{"extraction": {"chapters": [...]}}``) o una lista directa de
    capítulos en cualquiera de los dos formatos.

    Parameters
    ----------
    chapters : list[dict] | dict
        Lista de capítulos (formato interno o raw ADE) o un dict envolvente.

    Returns
    -------
    list[dict]
        Lista limpia en formato interno con un único capítulo por código y
        ``codigo`` de partida garantizado no nulo.
    """
    raw_list = _unwrap_chapters(chapters)
    if not raw_list:
        return []

    # Normalise every chapter to internal format
    normalised = [_normalise_chapter(ch) for ch in raw_list if isinstance(ch, dict)]

    # ── 1. Agrupar por capitulo_codigo (preservar orden de primera aparición) ─
    seen_order: list[str] = []                         # códigos en orden de aparición
    groups: dict[str, list[dict]] = {}                 # codigo → [cap1, cap2, …]

    for ch in normalised:
        raw_code = ch.get("capitulo_codigo") or ""
        key = raw_code.strip().upper()          # normalise for matching
        if not key:
            key = "__NO_CODE__"

        if key not in groups:
            seen_order.append(key)
            groups[key] = []
        groups[key].append(ch)

    # ── 2. Fusionar grupos con más de 1 capítulo ──────────────────────────────
    merged: list[dict] = []
    n_merged = 0
    n_total_before = len(normalised)

    for key in seen_order:
        group = groups[key]

        if len(group) == 1:
            merged.append(group[0])
            continue

        # Multiple chapters with same code → fuse
        n_merged += len(group) - 1
        logger.info(
            "Merging %d chapters with capitulo_codigo=%r into one.",
            len(group), key,
        )

        # capitulo_nombre: first non-null non-empty
        nombre = next(
            (g.get("capitulo_nombre") for g in group if g.get("capitulo_nombre")),
            None,
        )

        # capitulo_codigo: use original value from first entry (preserves casing)
        original_code = group[0].get("capitulo_codigo") or key

        # partidas: concatenate all
        all_partidas: list[dict] = []
        for g in group:
            partidas = g.get("partidas") or []
            all_partidas.extend(partidas)

        # total_capitulo: sum of partida totals (recomputed for reliability)
        totals = [p.get("total") for p in all_partidas if isinstance(p.get("total"), (int, float))]
        total_cap = round(sum(totals), 2) if totals else None

        fused: dict[str, Any] = {
            "capitulo_codigo": original_code,
            "capitulo_nombre": nombre,
            "total_capitulo": total_cap,
            "partidas": all_partidas,
        }
        # Preserve any extra keys that might exist (e.g. mapping fields added later)
        for key_extra in group[0]:
            if key_extra not in fused:
                fused[key_extra] = group[0][key_extra]

        merged.append(fused)

    # ── 3. Corregir descripciones "sangradas" (texto que pertenece a la partida anterior) ──
    _fix_bleeding_descriptions(merged)

    # ── 4. Recalcular totales de partidas y capítulos ─────────────────────────
    _recalculate_totals(merged)

    # ── 5. Rellenar códigos de partida vacíos ─────────────────────────────────
    no_code_counter = 0
    for ch in merged:
        for partida in ch.get("partidas") or []:
            if not (partida.get("codigo") or "").strip():
                no_code_counter += 1
                partida["codigo"] = f"no_code_{no_code_counter}"

    # ── 6. Absorber partidas no_code_ como componentes de la partida anterior ─
    _absorb_no_code_partidas(merged)

    # ── 7. Log summary ────────────────────────────────────────────────────────
    n_total_after = len(merged)
    if n_merged > 0 or no_code_counter > 0:
        print(
            f"  [chapter_merger] {n_total_before} -> {n_total_after} chapter(s) "
            f"({n_merged} duplicate(s) merged); "
            f"{no_code_counter} partida(s) with missing code filled."
        )
    else:
        print(
            f"  [chapter_merger] no duplicates found "
            f"({n_total_after} chapter(s), {no_code_counter} codes filled)."
        )

    # Preserve original input shape: if caller passed a dict envelope containing
    # the chapters (e.g. ADE's raw response with 'extraction' or a simple wrapper
    # with top-level 'chapters') return the same envelope with the merged list
    # substituted. This prevents callers from losing other keys such as
    # 'project_details' or 'document_title' when they expect the full payload
    # back (see server pipeline usage).
    if isinstance(chapters, list):
        return merged

    if isinstance(chapters, dict):
        out = dict(chapters)  # shallow copy to avoid mutating caller data
        # ADE raw envelope: {"extraction": {"chapters": [...] , ...}, ...}
        if "extraction" in chapters and isinstance(chapters["extraction"], dict) and \
           "chapters" in chapters["extraction"]:
            out_ex = dict(chapters["extraction"])
            out_ex["chapters"] = merged
            out["extraction"] = out_ex
            return out

        # Simple wrapper: {"chapters": [...], ...}
        if "chapters" in chapters and isinstance(chapters["chapters"], list):
            out["chapters"] = merged
            return out

    # Fallback: return the merged list
    return merged


# ---------------------------------------------------------------------------
# CLI (optional quick test)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json
    import sys

    if len(sys.argv) < 2:
        print("Usage: python chapter_merger.py <path_to_final.json> [output.json]")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else None

    with open(input_path, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    result = merge_duplicate_chapters(data)

    out = json.dumps(result, indent=4, ensure_ascii=False)
    if output_path:
        with open(output_path, "w", encoding="utf-8") as fh:
            fh.write(out)
        print(f"Written to {output_path}")
    else:
        print(out)
