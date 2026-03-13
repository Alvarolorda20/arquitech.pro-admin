"""
generar_comparativo_final.py  ·  v4 – Formato Profesional Jerarquizado
=======================================================================
ALGORITMO:
  1. Carga dinámica de FINAL_{proveedor}.json + AUDITORIA_VALIDADA_*.json
  2. Filas de metadatos (Título, Ubicación, Fecha) en las primeras 3 filas
  3. Super-encabezados: bloque ESTIMACIÓN + bloque por proveedor
  4. Nombres de columna: Código, Nat, Ut, Resumen, Amid., Preu, Import
     + Amid. / Preu / Import / Comentarios por proveedor
  5. Columna Nat identifica automáticamente Capítulo / Partida / Extra
  6. Partidas 1-to-MANY: bloques jerárquicos con sub-filas en tono gris
  7. EXTRAS al final de cada capítulo en filas amarillas
  8. Pie de página automático: TOTAL NETO, IVA, TOTAL IVA, IMPREVISTOS,
     TOTAL FINAL — con fórmulas activas y porcentajes configurables
"""

import json
import os
import glob
import math
import re
import textwrap
from statistics import median
import xlsxwriter
from xlsxwriter.utility import xl_rowcol_to_cell
from src.features.mapping.application.chapter_mapping_deriver import (
    derive_chapter_mapping_from_links,
)


# ------------------------------------------------------------------------------
# ESTILO NEUTRO ÚNICO — sin colores por proveedor, tabla limpia y legible
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# CONFIGURACIÓN GLOBAL DEL REPORTE  (sobreescribible via variables de entorno)
# ------------------------------------------------------------------------------
PROYECTO_TITULO: str    = os.environ.get("PROYECTO_TITULO",    "COMPARATIVO DE PRESUPUESTOS")
PROYECTO_UBICACION: str = os.environ.get("PROYECTO_UBICACION", "")
IVA_PCT: float          = float(os.environ.get("PRESUPUESTO_IVA",          "10"))  # %
IMPREVISTOS_PCT: float  = float(os.environ.get("PRESUPUESTO_IMPREVISTOS",   "5"))  # %


# ------------------------------------------------------------------------------
# CARGA DE DATOS
# ------------------------------------------------------------------------------
MEDIAN_ALERT_THRESHOLD = 0.20
MEDIAN_MIN_SAMPLES = 2
OUTLIER_ORANGE = "#FCE4D6"


def _cargar_json(ruta: str) -> list:
    with open(ruta, "r", encoding="utf-8") as f:
        return json.load(f)


def cargar_todo(directorio: str):
    """
    Carga mapped_pauta.json, todos los FINAL_*.json y AUDITORIA_VALIDADA_*.json.
    Retorna:
        pauta           – lista de capítulos de la pauta
        proveedores     – lista ordenada de nombres de proveedor
        ofertas_idx     – { proveedor: { id_pauta_unico: [partida_dict, ...] } }   lista (1-to-many)
        extras_idx      – { proveedor: { cap_cod: [partidas extra] } }
        auditorias_idx  – { proveedor: { codigo_pauta: entry_dict } }
        totales_cap_idx – { proveedor: { cap_cod: total_capitulo (float) } }
    """

    # ── Pauta ──────────────────────────────────────────────────────────────────
    pauta_path = os.path.join(directorio, "mapped_pauta.json")
    if not os.path.exists(pauta_path):
        raise FileNotFoundError(f"No se encontró mapped_pauta.json en: {directorio}")
    pauta = _cargar_json(pauta_path)

    # ── Detectar proveedores ───────────────────────────────────────────────────
    archivos_finales = glob.glob(
        os.path.join(directorio, "**/FINAL_*.json"), recursive=True
    )
    if not archivos_finales:
        raise FileNotFoundError(
            f"No se encontraron archivos FINAL_*.json en: {directorio}"
        )

    ofertas_raw    = {}
    auditorias_raw = {}
    subdirs        = {}  # safe_name -> subdirectorio
    cap_mappings   = {}  # safe_name -> {offer_cap_norm: pauta_cap_norm}

    for ruta in sorted(archivos_finales):
        nombre = os.path.basename(ruta)
        m = re.match(r"FINAL_(.+)\.json", nombre, re.IGNORECASE)
        if not m:
            continue
        proveedor     = m.group(1)
        subdirectorio = os.path.dirname(ruta)
        subdirs[proveedor] = subdirectorio

        ofertas_raw[proveedor] = _cargar_json(ruta)

        # ── Resolver mapeo de capítulos ───────────────────────────────────────
        # Fuente primaria: MAPPING_LINKS_FINAL_{proveedor}.json (más fiel al
        # mapeo final de partidas). Fallback: CAP_MAPPING_{proveedor}.json.
        raw_cmap: dict = {}
        mapping_final_path = os.path.join(
            subdirectorio,
            f"MAPPING_LINKS_FINAL_{proveedor}.json",
        )
        if os.path.exists(mapping_final_path):
            try:
                mapping_result = _cargar_json(mapping_final_path)
                derived = derive_chapter_mapping_from_links(
                    mapping_result if isinstance(mapping_result, dict) else None,
                    offer_data=ofertas_raw[proveedor] if isinstance(ofertas_raw[proveedor], list) else None,
                )
                raw_cmap = (
                    derived.get("cap_mapping", {})
                    if isinstance(derived, dict)
                    else {}
                )
            except Exception:
                raw_cmap = {}
        if not raw_cmap:
            cap_map_path = os.path.join(subdirectorio, "mapping_batches", f"CAP_MAPPING_{proveedor}.json")
            if os.path.exists(cap_map_path):
                try:
                    raw_cmap = _cargar_json(cap_map_path).get("cap_mapping", {})
                except Exception:
                    pass
        # Normalizar solo la clave de la OFERTA para búsquedas robustas.
        # El valor (código de la PAUTA) se guarda SIN normalizar para preservar
        # la distinción entre códigos como "011" y "11", que son capítulos distintos.
        cap_mappings[proveedor] = {_norm_cap(k): v for k, v in raw_cmap.items()}

        arr = glob.glob(
            os.path.join(subdirectorio, f"AUDITORIA_VALIDADA_{proveedor}*.json")
        )
        if not arr:
            arr = glob.glob(
                os.path.join(directorio, f"**/AUDITORIA_VALIDADA_{proveedor}*.json"),
                recursive=True,
            )
        auditorias_raw[proveedor] = _cargar_json(arr[0]) if arr else []

    proveedores = list(ofertas_raw.keys())

    # ── Resolver nombres de display desde plan_log.json ───────────────────────
    # Si el plan del planificador incluye un campo "proveedor", se usa como
    # nombre visible en las cabeceras del Excel en lugar del nombre de archivo.
    display_names: dict[str, str] = {}
    for prov, subdir in subdirs.items():
        plan_log_path = os.path.join(subdir, "plan_log.json")
        display = prov  # fallback: nombre seguro del archivo PDF
        if os.path.exists(plan_log_path):
            try:
                with open(plan_log_path, "r", encoding="utf-8") as _fp:
                    _plog = json.load(_fp)
                _name = (_plog.get("proveedor") or "").strip()
                if _name:
                    display = _name
            except Exception:
                pass
        display_names[prov] = display

    # ── Construir índices ──────────────────────────────────────────────────────
    ofertas_idx     = {}
    extras_idx      = {}
    auditorias_idx  = {}
    totales_cap_idx = {}

    for proveedor in proveedores:
        oferta_id  = {}
        extras_cap = {}
        totales    = {}
        cmap       = cap_mappings.get(proveedor, {})  # {offer_cap_norm → pauta_cap_norm}

        for capitulo in ofertas_raw[proveedor]:
            cap_cod     = str(capitulo.get("capitulo_codigo", "")).strip()
            cap_cod_raw = _norm_cap(cap_cod)
            # Traducir código del capítulo de la oferta al código del capítulo de la pauta.
            # Si no existe mapeo explícito se usa el código normalizado de la oferta
            # (los números idénticos con distinto zero-padding quedan cubiertos por _norm_cap).
            pauta_cap_key = cmap.get(cap_cod_raw, cap_cod_raw)
            totales[pauta_cap_key] = capitulo.get("total_capitulo", 0.0)

            for partida in capitulo.get("partidas", []):
                if partida.get("es_extra", False):
                    extras_cap.setdefault(pauta_cap_key, []).append(partida)
                else:
                    id_unico = partida.get("id_pauta_unico")
                    # Defensive: if a list slipped through, take the first element
                    if isinstance(id_unico, list):
                        id_unico = id_unico[0] if id_unico else None
                    if id_unico:
                        # ── Cross-chapter guard ───────────────────────────────
                        # If the individual partida mapping points to a DIFFERENT
                        # pauta chapter than the one derived from the chapter-level
                        # cap_mapping, treat the partida as an EXTRA in the
                        # chapter-mapped chapter (pauta_cap_key), not in the
                        # chapter where the partida mapping says it belongs.
                        # Example: cap_mapping says offer-02 = pauta-01, but
                        # MapperAgent assigned id_pauta_unico="03::XX".  The
                        # partida should appear as extra under pauta cap 01.
                        id_cap = id_unico.split("::")[0].strip() if "::" in id_unico else ""
                        if id_cap and _norm_cap(id_cap) != _norm_cap(pauta_cap_key):
                            # Chapter mismatch → redirect to extras of the
                            # chapter-mapped chapter
                            extras_cap.setdefault(pauta_cap_key, []).append(partida)
                        else:
                            # 1-to-many: acumular todas las sub-partidas de la oferta
                            oferta_id.setdefault(id_unico, []).append(partida)

        ofertas_idx[proveedor]     = oferta_id
        extras_idx[proveedor]      = extras_cap
        totales_cap_idx[proveedor] = totales

        audit_map = {}  # { key: [entry, ...] }  — keeps ALL entries per codigo_pauta
        for entry in auditorias_raw[proveedor]:
            raw = str(entry.get("codigo_pauta", "")).strip()
            if raw == "EXTRA":  # extra entries have no pauta code → skip
                continue
            audit_map.setdefault(raw, []).append(entry)
            corto = raw.split("::")[-1].strip()
            if corto and corto != raw:
                audit_map.setdefault(corto, []).append(entry)
        auditorias_idx[proveedor] = audit_map

    # ── Recollir project_address des dels fitxers project_details.json ────────────
    project_address: str = ""
    for _prov, _subdir in subdirs.items():
        _pd_path = os.path.join(_subdir, "project_details.json")
        if os.path.exists(_pd_path):
            try:
                with open(_pd_path, "r", encoding="utf-8") as _fp:
                    _pd_data = json.load(_fp)
                _addr = (_pd_data.get("project_address") or "").strip()
                if _addr:
                    project_address = _addr
                    break
            except Exception:
                pass

    # ── QA consola ────────────────────────────────────────────────────────────
    print(f"\nProveedores detectados : {', '.join(proveedores)}")
    for prov in proveedores:
        dn = display_names.get(prov, prov)
        label = f"{prov} → '{dn}'" if dn != prov else prov
        print(f"   📛 {label}")
    print(f"📄 Total partidas pauta   : {sum(len(c.get('partidas',[])) for c in pauta)}")
    for prov in proveedores:
        n = sum(len(v) for v in extras_idx[prov].values())
        print(f"🔧 Extras {display_names.get(prov, prov):<20}: {n}")

    if project_address:
        print(f"\U0001f4cd Adreça projecte        : {project_address}")

    return pauta, proveedores, ofertas_idx, extras_idx, auditorias_idx, totales_cap_idx, display_names, project_address


# ------------------------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------------------------

def _row_height(texts_and_widths: list, line_pts: float = 15.0, min_pts: float = 18.0) -> float:
    """
    Estima l'altària de fila necessari per mostrar el contingut embolcallat.
    texts_and_widths: llista de (text, amplada_en_caracters)
    """
    max_lines = 1
    for text, col_w in texts_and_widths:
        if not text:
            continue
        usable_width = max(8, int(col_w) - 2)
        for segment in str(text).split("\n"):
            wrapped = textwrap.wrap(
                segment,
                width=usable_width,
                break_long_words=True,
                break_on_hyphens=False,
                replace_whitespace=False,
                drop_whitespace=False,
            )
            lines = max(1, len(wrapped))
            max_lines = max(max_lines, lines)
    return max(min_pts, max_lines * line_pts)


def _val(v):
    """None si el valor es 0/nulo → celda vacía."""
    if v is None or v == "" or v == 0 or v == 0.0:
        return None
    return v


def _buscar_ofertas(proveedor, cap_cod, cod_pauta, ofertas_idx) -> list:
    """Devuelve la lista de sub-partidas que el proveedor ha ofertado para este id_pauta_unico.
    Siempre retorna una lista (puede ser vacía)."""
    id_unico = f"{cap_cod}::{cod_pauta}"
    lista = ofertas_idx[proveedor].get(id_unico)
    if lista is None:
        # fallback: buscar cualquier clave que termine en ::{cod_pauta}
        for k, v in ofertas_idx[proveedor].items():
            if k.endswith(f"::{cod_pauta}"):
                lista = v
                break
    return lista or []


def _buscar_auditorias(proveedor, cap_cod, cod_pauta, auditorias_idx) -> list:
    """Retorna TOTES les entrades d'auditoria per a aquest id_pauta_unico (pot ser >1)."""
    d = auditorias_idx[proveedor]
    return d.get(f"{cap_cod}::{cod_pauta}") or d.get(cod_pauta) or []


def _norm_cap(cod: str) -> str:
    """Normaliza código de capítulo para comparación: '01' == '1' == ' 1 '.
    Elimina espacios y ceros a la izquierda; '0' solo se conserva como '0'."""
    return cod.strip().lstrip("0") or "0"


def _num_or_blank(ws, row, col, value, fmt_num, fmt_blank):
    if value is not None:
        ws.write_number(row, col, value, fmt_num)
    else:
        ws.write(row, col, "", fmt_blank)


def _importe_partida(partida: dict):
    can = _val(partida.get("cantidad"))
    pr = _val(partida.get("precio"))
    if can is None or pr is None:
        return None
    try:
        return float(can) * float(pr)
    except (TypeError, ValueError):
        return None


def _mediana_y_alertas_por_proveedor(matches: dict):
    """
    Retorna:
      - mediana de importes totales por proveedor (None si no hay muestra minima)
      - totales por proveedor {prov: total|None}
      - outliers por proveedor {prov: bool} segun desviacion vs mediana
    """
    totales_por_proveedor = {}
    valores = []

    for prov, partidas in matches.items():
        total = 0.0
        has_value = False
        for partida in partidas:
            importe = _importe_partida(partida)
            if importe is not None:
                total += importe
                has_value = True
        if has_value:
            totales_por_proveedor[prov] = total
            valores.append(total)
        else:
            totales_por_proveedor[prov] = None

    if len(valores) < MEDIAN_MIN_SAMPLES:
        return None, totales_por_proveedor, {prov: False for prov in matches.keys()}

    mediana = median(valores)
    outliers = {}

    if mediana <= 0:
        for prov, total in totales_por_proveedor.items():
            outliers[prov] = bool(total is not None and total > 0)
        return mediana, totales_por_proveedor, outliers

    for prov, total in totales_por_proveedor.items():
        if total is None:
            outliers[prov] = False
            continue
        desviacion_rel = abs(total - mediana) / mediana
        outliers[prov] = desviacion_rel > MEDIAN_ALERT_THRESHOLD

    return mediana, totales_por_proveedor, outliers


def _analisi_preus_unitaris(pr_pauta, preus_prov: dict) -> dict:
    """
    Anàlisi jeràrquica de preus unitaris en tres passos (Pas A/B/C).

    Retorna: { prov: {"color": None|"red"|"orange"|"yellow", "value": float|None} }
    On "value" és el preu a mostrar (pot ser el màxim imputat en cas vermell).
    """
    from statistics import median as _med

    DESV_B = 0.60   # Pas B: >±60% de la mediana → taronja
    DESV_C = 0.20   # Pas C: >±20% de la mediana neta → groc

    result = {prov: {"color": None, "value": v} for prov, v in preus_prov.items()}

    # ── PAS A: Omissions (vermell) ────────────────────────────────────────────
    tots_valids = [v for v in preus_prov.values() if v is not None and v > 0]
    tots_amb_pauta = (tots_valids + [pr_pauta]
                      if pr_pauta is not None and pr_pauta > 0
                      else tots_valids)
    max_preu = max(tots_amb_pauta) if tots_amb_pauta else None

    manquen: set = set()
    for prov, v in preus_prov.items():
        if v is None or v <= 0:
            result[prov]["color"] = "red"
            result[prov]["value"] = max_preu
            manquen.add(prov)

    # ── PAS B: Errors de bulto (taronja) ─────────────────────────────────────
    # Mediana: tots els proveïdors vàlids (no vermells) + SEMPRE la Pauta
    vals_b = [preus_prov[p] for p in preus_prov
              if p not in manquen and preus_prov[p] is not None and preus_prov[p] > 0]
    if pr_pauta is not None and pr_pauta > 0:
        vals_b = vals_b + [pr_pauta]

    taronges: set = set()
    if len(vals_b) >= 2:
        med_b = _med(vals_b)
        if med_b > 0:
            for prov in preus_prov:
                if prov in manquen:
                    continue
                v = preus_prov[prov]
                if v is None or v <= 0:
                    continue
                if abs(v - med_b) / med_b > DESV_B:
                    result[prov]["color"] = "orange"
                    taronges.add(prov)

    # ── PAS C: Desviació comercial (groc) ─────────────────────────────────────
    vals_c = [preus_prov[p] for p in preus_prov
              if p not in manquen and p not in taronges
              and preus_prov[p] is not None and preus_prov[p] > 0]

    if len(vals_c) >= 2:
        # ≥2 proveïdors vàlids: mediana dels vàlids + Pauta (mai pinta la Pauta)
        vals_c_med = vals_c + ([pr_pauta] if pr_pauta is not None and pr_pauta > 0 else [])
        med_c = _med(vals_c_med) if vals_c_med else None
    elif len(vals_c) == 1 and pr_pauta is not None and pr_pauta > 0:
        # 1 sol proveïdor vàlid: compara directament contra la Pauta
        med_c = pr_pauta
    else:
        med_c = None

    if med_c is not None and med_c > 0:
        for prov in preus_prov:
            if prov in manquen or prov in taronges:
                continue
            v = preus_prov[prov]
            if v is None or v <= 0:
                continue
            if abs(v - med_c) / med_c > DESV_C:
                result[prov]["color"] = "yellow"

    return result


# Elimina prefijos de auditoría como '✅ VERIFICADO (2): ' o ' CONFIRMADO (3): '
_AUDIT_PREFIX_RE = re.compile(
    r'^[^\w]*'            # emojis / símbolos iniciales
    r'[A-ZÉÓÚÑ]'       # primera mayúscula
    r'[A-Z ÉÓÚÑ]+'     # resto de palabras en mayúscula
    r'(?:\s*\([^)]*\))?'  # paréntesis opcional: (2), (pag. 3), etc.
    r'\s*:\s*',           # dos puntos separadores
    re.UNICODE,
)


def _strip_audit_prefix(text: str) -> str:
    """Devuelve sólo el comentario limpio, sin el prefijo de estado del juez."""
    if not text:
        return text
    return _AUDIT_PREFIX_RE.sub("", text, count=1)


def _format_validacion(audit: dict) -> str:
    """Retorna el comentari_tecnic net d'una entrada d'auditoria."""
    raw = _strip_audit_prefix(audit.get("comentario_tecnico", "") or "").strip()
    if not raw or raw.lower() in ("correcte", "correcto", "correcto confirmado", "correct"):
        return ""
    return raw


def _format_combined_validacion(audits_list: list) -> str:
    """Combina els comentaris de TOTES les entrades d'auditoria per a la mateixa partida.
    Elimina duplicats i entrades buides. Uneix amb salt de línia."""
    seen = []
    for audit in audits_list:
        c = _format_validacion(audit)
        if c and c not in seen:
            seen.append(c)
    return "\n".join(seen)


# ------------------------------------------------------------------------------
# GENERACIÓN DEL EXCEL
# ------------------------------------------------------------------------------

def generar_comparativo_final(
    directorio: str,
    archivo_salida: str = "COMPARATIVO_MAESTRO_FINAL.xlsx",
):
    """Genera el Excel comparativo con encabezados de metadatos, super-headers,
    columna Nat, jerarquía 1-to-many y pie de página con fórmulas activas."""

    import datetime as _dt

    (pauta, proveedores,
     ofertas_idx, extras_idx,
     auditorias_idx, totales_cap_idx,
     display_names, project_address) = cargar_todo(directorio)

    n_provs = len(proveedores)
    print(f"\nGenerando Excel: {archivo_salida}")

    wb = xlsxwriter.Workbook(archivo_salida)
    ws = wb.add_worksheet("Comparativo Maestro")

    # 
    # CONSTANTES DE LAYOUT
    # 
    # Bloque ESTIMACIÓN (pauta)
    COL_CODIGO  = 0   # Código
    COL_NAT     = 1   # Nat  (Capítulo / Partida / Extra)
    COL_UD      = 2   # Ut
    COL_RESUMEN = 3   # Resumen
    COL_CAN     = 4   # Amid.
    COL_PR      = 5   # Preu
    COL_IMP     = 6   # Import
    N_PAUTA     = 7   # número total de columnas del bloque pauta

    # Sub-columnas por proveedor
    SC_CAN = 0   # Amid.
    SC_PR  = 1   # Preu
    SC_IMP = 2   # Import
    SC_VAL = 3   # Comentarios
    N_PROV = 4

    def cp(idx_p: int, sc: int) -> int:
        """Columna absoluta para proveedor idx_p, sub-columna sc."""
        return N_PAUTA + idx_p * N_PROV + sc

    LAST_COL = cp(n_provs - 1, N_PROV - 1) if n_provs > 0 else N_PAUTA - 1

    # Filas de layout
    ROW_TITLE   = 0
    ROW_LOC     = 1
    ROW_DATE    = 2
    ROW_SUPER   = 3   # super-encabezados (ESTIMACIÓN | PROVEEDOR X …)
    ROW_COLS    = 4   # nombres de columna
    ROW_DATA    = 5   # primera fila de datos

    # Anchos de columnas con texto largo (para estimación de altura de filas)
    RESUMEN_COL_WIDTH = 55
    PROV_COMMENT_COL_WIDTH = 42
    DATA_ROW_MIN_HEIGHT = 18.0
    DATA_ROW_LINE_HEIGHT = 15.0

    # 
    # ANCHOS DE COLUMNA
    # 
    ws.set_column(COL_CODIGO,  COL_CODIGO,  10)
    ws.set_column(COL_NAT,     COL_NAT,      8)
    ws.set_column(COL_UD,      COL_UD,       5)
    ws.set_column(COL_RESUMEN, COL_RESUMEN, RESUMEN_COL_WIDTH)
    ws.set_column(COL_CAN,     COL_CAN,     12)
    ws.set_column(COL_PR,      COL_PR,      12)
    ws.set_column(COL_IMP,     COL_IMP,     12)

    for idx_p in range(n_provs):
        ws.set_column(cp(idx_p, SC_CAN), cp(idx_p, SC_CAN), 12)
        ws.set_column(cp(idx_p, SC_PR),  cp(idx_p, SC_PR),  12)
        ws.set_column(cp(idx_p, SC_IMP), cp(idx_p, SC_IMP), 12)
        ws.set_column(cp(idx_p, SC_VAL), cp(idx_p, SC_VAL), PROV_COMMENT_COL_WIDTH)

    # 
    # CONSTANTES DE FORMATO
    # 
    FONT        = "Calibri"
    CURRENCY    = r'_(* #,##0.00 €_);_(* (#,##0.00 €);_(* "-"??_);_(@_)'
    QTY_FMT     = "#,##0.00"
    THIN_BORDER   = {"border": 1, "border_color": "#BFBFBF"}
    FOOTER_BORDER = {"left": 1, "right": 1, "bottom": 1, "top": 6, "border_color": "#BFBFBF"}

    # ── Capçalera (sin fondo, sin bordes) ────────────────────────────────────
    fmt_title = wb.add_format({
        "font_name": FONT, "bold": True, "font_size": 15,
        "valign": "vcenter",
    })
    fmt_header_text = wb.add_format({
        "font_name": FONT, "font_size": 10,
        "valign": "vcenter",
    })
    fmt_header_date = wb.add_format({
        "font_name": FONT, "font_size": 10,
        "align": "right", "valign": "vcenter",
    })

    # ── Super-header ESTIMACIÓN ────────────────────────────────────────────────
    fmt_sh_estim = wb.add_format({
        "font_name": FONT, "bold": True, "font_size": 10,
        "bg_color": "#F2F2F2", "font_color": "#000000",
        "align": "center", "valign": "vcenter",
        "left": 2, "right": 2, "top": 2, "bottom": 1,
        "border_color": "#BFBFBF",
    })

    # ── Nombres de columna ESTIMACIÓN ────────────────────────────────────────
    fmt_col_estim = wb.add_format({
        "font_name": FONT, "bold": True, "font_size": 9,
        "bg_color": "#F2F2F2", "font_color": "#000000",
        "align": "center", "valign": "vcenter",
        "border": 1, "border_color": "#BFBFBF", "bottom": 5,
    })

    # ── Fila capítulo ─────────────────────────────────────────────────────────
    fmt_chap = wb.add_format({
        "font_name": FONT, "bold": True, "font_size": 11,
        "bg_color": "#EAEAEA", "font_color": "#000000",
        "valign": "vcenter", **THIN_BORDER,
    })
    fmt_chap_price = wb.add_format({
        "font_name": FONT, "bold": True, "font_size": 11,
        "bg_color": "#EAEAEA", "font_color": "#000000",
        "valign": "vcenter", "num_format": CURRENCY, **THIN_BORDER,
    })
    fmt_chap_nat = wb.add_format({
        "font_name": FONT, "bold": True, "font_size": 9, "italic": True,
        "bg_color": "#EAEAEA", "font_color": "#404040",
        "align": "center", "valign": "vcenter", **THIN_BORDER,
    })

    # ── Celdas de datos ESTIMACIÓN ────────────────────────────────────────────
    fmt_normal = wb.add_format({
        "font_name": FONT, "font_size": 9, "valign": "top", **THIN_BORDER,
    })
    fmt_center = wb.add_format({
        "font_name": FONT, "font_size": 9, "valign": "top",
        "align": "center", **THIN_BORDER,
    })
    fmt_wrap = wb.add_format({
        "font_name": FONT, "font_size": 9, "valign": "top",
        "text_wrap": True, **THIN_BORDER,
    })
    fmt_price = wb.add_format({
        "font_name": FONT, "font_size": 9, "valign": "top",
        "num_format": CURRENCY, **THIN_BORDER,
    })
    fmt_qty = wb.add_format({
        "font_name": FONT, "font_size": 9, "valign": "top",
        "num_format": QTY_FMT, **THIN_BORDER,
    })
    fmt_nat_partida = wb.add_format({
        "font_name": FONT, "font_size": 9, "italic": True, "valign": "top",
        "font_color": "#595959", "align": "center", **THIN_BORDER,
    })

    # Sub-filas i>0 (sub-partida oferta – fondo blanco)
    fmt_sub_row = wb.add_format({
        "font_name": FONT, "font_size": 9, "italic": True,
        "text_wrap": True, "indent": 1,
        "valign": "top", **THIN_BORDER,
    })
    fmt_sub_row_blank = wb.add_format({
        "font_name": FONT, "font_size": 9,
        "valign": "top", **THIN_BORDER,
    })

    # Extra row (verde claro)
    EXTRA_YELLOW = "#d9f2d0"
    fmt_extra_yellow = wb.add_format({
        "font_name": FONT, "font_size": 9, "italic": True,
        "bg_color": EXTRA_YELLOW, "valign": "top", **THIN_BORDER,
    })
    fmt_extra_yellow_wrap = wb.add_format({
        "font_name": FONT, "font_size": 9, "italic": True,
        "bg_color": EXTRA_YELLOW, "text_wrap": True, "valign": "top",
        **THIN_BORDER,
    })
    fmt_extra_yellow_qty = wb.add_format({
        "font_name": FONT, "font_size": 9, "italic": True,
        "bg_color": EXTRA_YELLOW, "num_format": QTY_FMT,
        "valign": "top", **THIN_BORDER,
    })
    fmt_extra_yellow_price = wb.add_format({
        "font_name": FONT, "font_size": 9, "italic": True,
        "bg_color": EXTRA_YELLOW, "num_format": CURRENCY,
        "valign": "top", **THIN_BORDER,
    })

    # Código fusionado (bloque 1-to-many)
    fmt_merged_codigo = wb.add_format({
        "font_name": FONT, "font_size": 9,
        "valign": "vcenter", "align": "center",
        "bold": False, **THIN_BORDER,
    })

    # ── Pie de página (borde superior doble) ─────────────────────────────────
    fmt_footer_label = wb.add_format({
        "font_name": FONT, "bold": True, "font_size": 9,
        "bg_color": "#D9D9D9", "font_color": "#000000",
        "valign": "vcenter", **FOOTER_BORDER,
    })
    fmt_footer_price = wb.add_format({
        "font_name": FONT, "bold": True, "font_size": 9,
        "bg_color": "#D9D9D9", "num_format": CURRENCY,
        "valign": "vcenter", **FOOTER_BORDER,
    })
    fmt_footer_blank = wb.add_format({
        "font_name": FONT, "font_size": 9,
        "bg_color": "#D9D9D9", "valign": "vcenter", **FOOTER_BORDER,
    })
    fmt_footer_total_label = wb.add_format({
        "font_name": FONT, "bold": True, "font_size": 10,
        "bg_color": "#595959", "font_color": "#FFFFFF",
        "valign": "vcenter", **FOOTER_BORDER,
    })
    fmt_footer_total_price = wb.add_format({
        "font_name": FONT, "bold": True, "font_size": 10,
        "bg_color": "#595959", "font_color": "#FFFFFF",
        "num_format": CURRENCY, "valign": "vcenter", **FOOTER_BORDER,
    })

    # ── Formatos neutros únicos para todos los proveedores ───────────────────
    def _pborder(extra: dict | None = None) -> dict:
        base = {"left": 1, "right": 1, "top": 1, "bottom": 1,
                "border_color": "#BFBFBF"}
        if extra:
            base.update(extra)
        return base

    _pf = {
        # Super-header
        "sh": wb.add_format({
            "font_name": FONT, "bold": True, "font_size": 10,
            "bg_color": "#F2F2F2", "font_color": "#000000",
            "align": "center", "valign": "vcenter",
            "left": 2, "right": 2, "top": 2, "bottom": 1,
            "border_color": "#BFBFBF",
        }),
        # Fila nombres de columna
        "col_hdr": wb.add_format({
            "font_name": FONT, "bold": True, "font_size": 9,
            "bg_color": "#F2F2F2", "font_color": "#000000",
            "align": "center", "valign": "vcenter",
            "border": 1, "border_color": "#BFBFBF", "bottom": 5,
        }),
        # Fila capítulo: total proveedor
        "ch_blank": wb.add_format({
            "font_name": FONT, "bold": True, "font_size": 10,
            "bg_color": "#EAEAEA", "font_color": "#000000",
            "valign": "vcenter", **THIN_BORDER,
        }),
        "ch_price": wb.add_format({
            "font_name": FONT, "bold": True, "font_size": 10,
            "bg_color": "#EAEAEA", "font_color": "#000000",
            "num_format": CURRENCY, "valign": "vcenter", **THIN_BORDER,
        }),
        # Celdas de datos normales (primera columna del bloque con borde izq. grueso)
        "normal": wb.add_format({
            "font_name": FONT, "font_size": 9, "valign": "top",
            **_pborder({"left": 2, "border_color": "#BFBFBF"}),
        }),
        "qty": wb.add_format({
            "font_name": FONT, "font_size": 9, "valign": "top",
            "num_format": QTY_FMT, **THIN_BORDER,
        }),
        "price": wb.add_format({
            "font_name": FONT, "font_size": 9, "valign": "top",
            "num_format": CURRENCY, **THIN_BORDER,
        }),
        "price_alert": wb.add_format({
            "font_name": FONT, "font_size": 9, "valign": "top",
            "num_format": CURRENCY, "bg_color": "#FCE4D6", **THIN_BORDER,
        }),
        # Columna Comentaris (borde derecho grueso = cierre del bloque)
        "wrap": wb.add_format({
            "font_name": FONT, "font_size": 9, "valign": "top",
            "text_wrap": True,
            **_pborder({"right": 2, "border_color": "#BFBFBF"}),
        }),
        "wrap_disc": wb.add_format({
            "font_name": FONT, "font_size": 9, "valign": "top",
            "text_wrap": True, "bg_color": "#FDE9E9",
            **_pborder({"right": 2, "border_color": "#BFBFBF"}),
        }),
        "wrap_merge": wb.add_format({
            "font_name": FONT, "font_size": 9, "valign": "top",
            "text_wrap": True,
            **_pborder({"right": 2, "border_color": "#BFBFBF"}),
        }),
        "wrap_disc_merge": wb.add_format({
            "font_name": FONT, "font_size": 9, "valign": "top",
            "text_wrap": True, "bg_color": "#FDE9E9",
            **_pborder({"right": 2, "border_color": "#BFBFBF"}),
        }),
        # Sub-filas i>0 (fondo blanco – sin color)
        "sub_normal": wb.add_format({
            "font_name": FONT, "font_size": 9, "valign": "top",
            **_pborder({"left": 2, "border_color": "#BFBFBF"}),
        }),
        "sub_qty": wb.add_format({
            "font_name": FONT, "font_size": 9,
            "num_format": QTY_FMT, "valign": "top", **THIN_BORDER,
        }),
        "sub_price": wb.add_format({
            "font_name": FONT, "font_size": 9,
            "num_format": CURRENCY, "valign": "top", **THIN_BORDER,
        }),
        "sub_price_alert": wb.add_format({
            "font_name": FONT, "font_size": 9, "bg_color": "#FCE4D6",
            "num_format": CURRENCY, "valign": "top", **THIN_BORDER,
        }),
        "sub_wrap": wb.add_format({
            "font_name": FONT, "font_size": 9, "valign": "top",
            "text_wrap": True,
            **_pborder({"right": 2, "border_color": "#BFBFBF"}),
        }),
        # Fila extra proveedor (verde menta #d9f2d0 – único color de la tabla)
        "extra_yellow": wb.add_format({
            "font_name": FONT, "font_size": 9, "italic": True,
            "bg_color": EXTRA_YELLOW, "valign": "top",
            **_pborder({"left": 2, "border_color": "#BFBFBF"}),
        }),
        "extra_yellow_wrap": wb.add_format({
            "font_name": FONT, "font_size": 9, "italic": True,
            "bg_color": EXTRA_YELLOW, "text_wrap": True, "valign": "top",
            **_pborder({"right": 2, "border_color": "#BFBFBF"}),
        }),
        "extra_yellow_qty": wb.add_format({
            "font_name": FONT, "font_size": 9, "italic": True,
            "bg_color": EXTRA_YELLOW, "num_format": QTY_FMT,
            "valign": "top", **THIN_BORDER,
        }),
        "extra_yellow_price": wb.add_format({
            "font_name": FONT, "font_size": 9, "italic": True,
            "bg_color": EXTRA_YELLOW, "num_format": CURRENCY,
            "valign": "top", **THIN_BORDER,
        }),
        # ── Anàlisi de preus (Pas A / B / C) ─────────────────────────────────
        "price_red": wb.add_format({
            "font_name": FONT, "font_size": 9, "valign": "top",
            "num_format": CURRENCY, "bg_color": "#FF7474", **THIN_BORDER,
        }),
        "price_orange": wb.add_format({
            "font_name": FONT, "font_size": 9, "valign": "top",
            "num_format": CURRENCY, "bg_color": "#FFC000", **THIN_BORDER,
        }),
        "price_yellow": wb.add_format({
            "font_name": FONT, "font_size": 9, "valign": "top",
            "num_format": CURRENCY, "bg_color": "#FFFF00", **THIN_BORDER,
        }),
        "imp_red": wb.add_format({
            "font_name": FONT, "font_size": 9, "valign": "top",
            "num_format": CURRENCY, "bg_color": "#FF7474", **THIN_BORDER,
        }),
        "imp_orange": wb.add_format({
            "font_name": FONT, "font_size": 9, "valign": "top",
            "num_format": CURRENCY, "bg_color": "#FFC000", **THIN_BORDER,
        }),
        "imp_yellow": wb.add_format({
            "font_name": FONT, "font_size": 9, "valign": "top",
            "num_format": CURRENCY, "bg_color": "#FFFF00", **THIN_BORDER,
        }),
        # Pie de página por proveedor (con borde superior doble)
        "footer_price": wb.add_format({
            "font_name": FONT, "bold": True, "font_size": 9,
            "bg_color": "#D9D9D9", "num_format": CURRENCY,
            "valign": "vcenter", **FOOTER_BORDER,
        }),
        "footer_blank": wb.add_format({
            "font_name": FONT, "font_size": 9,
            "bg_color": "#D9D9D9", "valign": "vcenter", **FOOTER_BORDER,
        }),
        "footer_total_price": wb.add_format({
            "font_name": FONT, "bold": True, "font_size": 10,
            "bg_color": "#595959", "font_color": "#FFFFFF",
            "num_format": CURRENCY, "valign": "vcenter", **FOOTER_BORDER,
        }),
        "footer_total_blank": wb.add_format({
            "font_name": FONT, "bold": True, "font_size": 10,
            "bg_color": "#595959", "font_color": "#FFFFFF",
            "valign": "vcenter", **FOOTER_BORDER,
        }),
    }
    # Todos los proveedores comparten el mismo estilo neutro
    prov_fmts = [_pf] * n_provs

    # 
    # FILA 0 – T?TULO
    # 
    fecha_str = _dt.date.today().strftime("%d/%m/%Y")

    # -- Fila 1: titol fix de l'informe
    ws.set_row(ROW_TITLE, 26)
    ws.write(ROW_TITLE, 0, "Comparatiu de pressupostos", fmt_title)

    # -- Fila 2: nom del projecte/carrer  +  data a la dreta
    ws.set_row(ROW_LOC, 16)
    ws.write(ROW_LOC, 0, PROYECTO_TITULO, fmt_header_text)
    ws.write(ROW_LOC, LAST_COL, fecha_str, fmt_header_date)

    # -- Fila 3: ubicació — etiqueta i valor (preferim la direcció extreta)
    ws.set_row(ROW_DATE, 16)
    # Etiqueta fixa a la primera cel·la
    ws.write(ROW_DATE, 0, "Ubicació:", fmt_header_text)
    # Valor a la cel·la següent: project_address si existeix, sinó PROYECTO_UBICACION
    ws.write(ROW_DATE, 1, project_address if project_address else PROYECTO_UBICACION, fmt_header_text)

    # 
    # FILA 3 – SUPER-ENCABEZADOS  (ESTIMACIÓN | PROVEEDOR…)
    # 
    ws.set_row(ROW_SUPER, 22)
    # Cols 0-3 (Código/Nat/Ut/Resumen) — buit sense estil al super-header
    fmt_sh_blank = wb.add_format({"font_name": FONT, "font_size": 9})
    for _c in range(COL_CAN):
        ws.write(ROW_SUPER, _c, "", fmt_sh_blank)
    # ESTIMACIÓ només sobre Amid./Preu/Import (cols 4-6)
    ws.merge_range(ROW_SUPER, COL_CAN, ROW_SUPER, COL_IMP, "ESTIMACIÓ", fmt_sh_estim)
    for idx_p, prov in enumerate(proveedores):
        label = display_names.get(prov, prov).upper()
        ws.merge_range(
            ROW_SUPER, cp(idx_p, 0),
            ROW_SUPER, cp(idx_p, N_PROV - 1),
            label,
            prov_fmts[idx_p]["sh"],
        )

    # 
    # FILA 4 – NOMBRES DE COLUMNA
    # 
    ws.set_row(ROW_COLS, 30)
    for i, h in enumerate(["Código", "Nat", "Ut", "Resumen", "Amid.", "Preu", "Import"]):
        ws.write(ROW_COLS, i, h, fmt_col_estim)
    for idx_p in range(n_provs):
        for sc, h in enumerate(["Amid.", "Preu", "Import", "Comentaris"]):
            ws.write(ROW_COLS, cp(idx_p, sc), h, prov_fmts[idx_p]["col_hdr"])

    # Congelar cabeceras (filas 0-4) + columnas Código y Resumen (cols 0-3)
    ws.freeze_panes(ROW_DATA, COL_CAN)

    # 
    # ITERACIÓN PRINCIPAL  (datos desde fila ROW_DATA)
    # 
    fila      = ROW_DATA
    qa_pauta  = 0
    qa_extras = {p: 0 for p in proveedores}
    pauta_cap_keys  = set()
    chapter_imp_rows: list[int] = []          # filas donde van los totales de capítulo
    extra_counter_global = {p: 0 for p in proveedores}
    row_heights: dict[int, float] = {}

    def _set_row_height_at_least(row: int, height: float) -> None:
        current = row_heights.get(row)
        if current is None or height > current:
            ws.set_row(row, height)
            row_heights[row] = height

    # ── Helper: fila extra amarilla ───────────────────────────────────────────
    def _write_extra_row(extra: dict, prov: str, _extra_cod: str) -> None:
        nonlocal fila
        nombre_ex = extra.get("nombre", extra.get("name", ""))
        unidad_ex = extra.get("unidad", "")
        can_ex    = _val(extra.get("cantidad"))
        pr_ex     = _val(extra.get("precio"))

        ws.write(fila, COL_CODIGO,  "",          fmt_extra_yellow)
        ws.write(fila, COL_NAT,     "Extra",     fmt_extra_yellow)
        ws.write(fila, COL_UD,      unidad_ex,   fmt_extra_yellow)
        ws.write(fila, COL_RESUMEN, nombre_ex,   fmt_extra_yellow_wrap)
        ws.write(fila, COL_CAN,     "",          fmt_extra_yellow)
        ws.write(fila, COL_PR,      "",          fmt_extra_yellow)
        ws.write(fila, COL_IMP,     "",          fmt_extra_yellow)

        for idx_q, prov_q in enumerate(proveedores):
            fq = prov_fmts[idx_q]
            if prov_q == prov:
                _num_or_blank(ws, fila, cp(idx_q, SC_CAN), can_ex,
                              fq["extra_yellow_qty"],   fq["extra_yellow"])
                _num_or_blank(ws, fila, cp(idx_q, SC_PR),  pr_ex,
                              fq["extra_yellow_price"], fq["extra_yellow"])
                if can_ex is not None and pr_ex is not None:
                    cc = xl_rowcol_to_cell(fila, cp(idx_q, SC_CAN))
                    pc = xl_rowcol_to_cell(fila, cp(idx_q, SC_PR))
                    ws.write_formula(fila, cp(idx_q, SC_IMP),
                                     f"={cc}*{pc}", fq["extra_yellow_price"])
                else:
                    ws.write(fila, cp(idx_q, SC_IMP), "", fq["extra_yellow"])
                ws.write(fila, cp(idx_q, SC_VAL),
                         "Partida afegida pel constructor", fq["extra_yellow_wrap"])
            else:
                for sc in range(N_PROV):
                    f = fq["extra_yellow_wrap"] if sc == SC_VAL else fq["extra_yellow"]
                    ws.write(fila, cp(idx_q, sc), "", f)

        _set_row_height_at_least(
            fila,
            _row_height(
                [(nombre_ex, RESUMEN_COL_WIDTH)],
                line_pts=DATA_ROW_LINE_HEIGHT,
                min_pts=DATA_ROW_MIN_HEIGHT,
            ),
        )
        fila += 1
        qa_extras[prov] += 1

    # ─────────────────────────────────────────────────────────────────────────
    for capitulo in pauta:
        cap_cod    = str(capitulo.get("capitulo_codigo", "")).strip()
        cap_nombre = capitulo.get("capitulo_nombre", "").upper()
        pauta_cap_keys.add(cap_cod)
        pauta_cap_keys.add(_norm_cap(cap_cod))

        fila_capitulo = fila

        # ── FILA CAP?TULO ──────────────────────────────────────────────────
        ws.set_row(fila, 22)
        ws.write(fila, COL_CODIGO,  cap_cod,     fmt_chap)
        ws.write(fila, COL_NAT,     "Capítulo",  fmt_chap_nat)
        ws.write(fila, COL_UD,      "",           fmt_chap)
        ws.write(fila, COL_RESUMEN, cap_nombre,   fmt_chap)
        ws.write(fila, COL_CAN,     "",           fmt_chap)
        ws.write(fila, COL_PR,      "",           fmt_chap)
        ws.write(fila, COL_IMP,     "",           fmt_chap)   # → formula al final

        for idx_p, _ in enumerate(proveedores):
            fp = prov_fmts[idx_p]
            ws.write(fila, cp(idx_p, SC_CAN), "", fp["ch_blank"])
            ws.write(fila, cp(idx_p, SC_PR),  "", fp["ch_blank"])
            ws.write(fila, cp(idx_p, SC_IMP), "", fp["ch_blank"])  # → formula al final
            ws.write(fila, cp(idx_p, SC_VAL), "", fp["ch_blank"])

        chapter_imp_rows.append(fila)
        fila += 1

        last_cod_p = cap_cod

        # ── BUCLE A: PARTIDAS DE LA PAUTA ─────────────────────────────────
        for partida in capitulo.get("partidas", []):
            cod_p   = str(partida.get("codigo_pauta", "")).strip()
            nombre  = partida.get("nombre", "")
            unidad  = partida.get("unidad", "")
            can_p   = _val(partida.get("cantidad"))
            pr_p    = _val(partida.get("precio"))
            if cod_p:
                last_cod_p = cod_p

            matches = {
                prov: _buscar_ofertas(prov, cap_cod, cod_p, ofertas_idx)
                for prov in proveedores
            }
            audits = {
                prov: _buscar_auditorias(prov, cap_cod, cod_p, auditorias_idx)
                for prov in proveedores
            }
            max_filas = max(1, max(len(lst) for lst in matches.values()))
            fila_start_partida = fila
            val_for_prov: dict = {}

            # ── Anàlisi de preus unitaris (Pas A / B / C) ─────────────────────
            preus_unit_0 = {
                prov: _val(matches[prov][0].get("precio")) if matches[prov] else None
                for prov in proveedores
            }
            analisi_preus = _analisi_preus_unitaris(pr_p, preus_unit_0)
            # Pas A: afegir "Falta preu" al resum de la pauta si hi ha manques
            if any(analisi_preus[p]["color"] == "red" for p in proveedores):
                nom_disp = (nombre.strip() + "  [Falta preu]") if nombre.strip() else "[Falta preu]"
            else:
                nom_disp = nombre

            for i in range(max_filas):
                # Columnas ESTIMACIÓN
                if i == 0:
                    ws.write(fila, COL_CODIGO,  cod_p,    fmt_normal)
                    ws.write(fila, COL_NAT,     "Partida", fmt_nat_partida)
                    ws.write(fila, COL_UD,      unidad,    fmt_center)
                    ws.write(fila, COL_RESUMEN, nom_disp,  fmt_wrap)
                    _num_or_blank(ws, fila, COL_CAN, can_p, fmt_qty,   fmt_normal)
                    _num_or_blank(ws, fila, COL_PR,  pr_p,  fmt_price, fmt_normal)
                    if can_p is not None and pr_p is not None:
                        cc = xl_rowcol_to_cell(fila, COL_CAN)
                        pc = xl_rowcol_to_cell(fila, COL_PR)
                        ws.write_formula(fila, COL_IMP, f"={cc}*{pc}", fmt_price)
                    else:
                        ws.write(fila, COL_IMP, "", fmt_normal)
                else:
                    nombres_oferta = []
                    for _prov in proveedores:
                        if i < len(matches[_prov]):
                            _o = matches[_prov][i]
                            _n = (_o.get("nombre") or _o.get("name") or "").strip()
                            if _n:
                                nombres_oferta.append(_n)
                    desc_alt = " / ".join(nombres_oferta)
                    ws.write(fila, COL_CODIGO,  cod_p,    fmt_sub_row_blank)
                    ws.write(fila, COL_NAT,     "",        fmt_sub_row_blank)
                    ws.write(fila, COL_UD,      "",        fmt_sub_row_blank)
                    ws.write(fila, COL_RESUMEN, desc_alt,  fmt_sub_row)
                    ws.write(fila, COL_CAN,     "",        fmt_sub_row_blank)
                    ws.write(fila, COL_PR,      "",        fmt_sub_row_blank)
                    ws.write(fila, COL_IMP,     "",        fmt_sub_row_blank)

                # Columnas PROVEEDOR
                for idx_p, prov in enumerate(proveedores):
                    fp   = prov_fmts[idx_p]
                    lst  = matches[prov]
                    is_sub = (i > 0)
                    f_normal = fp["sub_normal"] if is_sub else fp["normal"]
                    f_qty    = fp["sub_qty"]    if is_sub else fp["qty"]
                    f_price  = fp["sub_price"]  if is_sub else fp["price"]
                    f_wrap   = fp["sub_wrap"]   if is_sub else fp["wrap"]

                    if i < len(lst):
                        oferta    = lst[i]
                        audit_lst = audits[prov]
                        can_o = _val(oferta.get("cantidad"))
                        pr_o  = _val(oferta.get("precio"))

                        val = _format_combined_validacion(audit_lst) if i == 0 else ""

                        # ── Seleccionar format i valor de preu (Pas A/B/C) ───
                        if i == 0:
                            an   = analisi_preus.get(prov, {"color": None, "value": pr_o})
                            pcol = an["color"]   # "red"|"orange"|"yellow"|None
                            pval = an["value"]   # preu a mostrar (pot ser imputat)
                        else:
                            pcol = None
                            pval = pr_o

                        # Preu Unitari — format de color (Pas A/B/C)
                        # L'Import sempre usa format neutre: cap alerta d'import total.
                        if pcol == "red":
                            f_pr_fmt  = fp["price_red"]
                            f_imp_fmt = fp["imp_red"]
                        elif pcol == "orange":
                            f_pr_fmt  = fp["price_orange"]
                            f_imp_fmt = fp["imp_orange"]
                        elif pcol == "yellow":
                            f_pr_fmt  = fp["price_yellow"]
                            f_imp_fmt = fp["imp_yellow"]
                        else:
                            f_pr_fmt  = f_price
                            f_imp_fmt = f_price

                        _num_or_blank(ws, fila, cp(idx_p, SC_CAN), can_o, f_qty, f_normal)
                        # Preu unitari: cel·la vermella → valor imputat (estàtic)
                        if pcol == "red":
                            ws.write(fila, cp(idx_p, SC_PR),
                                     pval if pval is not None else "", f_pr_fmt)
                            if can_o is not None and pval is not None:
                                ws.write(fila, cp(idx_p, SC_IMP), can_o * pval, f_imp_fmt)
                            else:
                                ws.write(fila, cp(idx_p, SC_IMP), "", f_normal)
                        else:
                            _num_or_blank(ws, fila, cp(idx_p, SC_PR), pr_o, f_pr_fmt, f_normal)
                            if can_o is not None and pval is not None:
                                cc = xl_rowcol_to_cell(fila, cp(idx_p, SC_CAN))
                                pc = xl_rowcol_to_cell(fila, cp(idx_p, SC_PR))
                                ws.write_formula(fila, cp(idx_p, SC_IMP),
                                                 f"={cc}*{pc}", f_imp_fmt)
                            else:
                                ws.write(fila, cp(idx_p, SC_IMP), "", f_normal)

                        ws.write(fila, cp(idx_p, SC_VAL), val,
                                 fp["wrap_disc"] if val else f_wrap)
                        if i == 0:
                            val_for_prov[prov] = val
                    else:
                        ws.write(fila, cp(idx_p, SC_CAN), "", f_normal)
                        ws.write(fila, cp(idx_p, SC_PR),  "", f_normal)
                        ws.write(fila, cp(idx_p, SC_IMP), "", f_normal)
                        ws.write(fila, cp(idx_p, SC_VAL), "", f_wrap)

                _tw = [(nom_disp if i == 0 else desc_alt, RESUMEN_COL_WIDTH)]  # type: ignore[possibly-undefined]
                if i == 0:
                    for _p in proveedores:
                        if _p in val_for_prov:
                            _tw.append((val_for_prov[_p], PROV_COMMENT_COL_WIDTH))
                _set_row_height_at_least(
                    fila,
                    _row_height(
                        _tw,
                        line_pts=DATA_ROW_LINE_HEIGHT,
                        min_pts=DATA_ROW_MIN_HEIGHT,
                    ),
                )
                fila += 1

            # Merges 1-to-many
            if max_filas > 1:
                fila_end_partida = fila - 1
                ws.merge_range(
                    fila_start_partida, COL_CODIGO,
                    fila_end_partida,   COL_CODIGO,
                    cod_p, fmt_merged_codigo,
                )
                for idx_p, prov in enumerate(proveedores):
                    if len(matches[prov]) > 1:
                        fp    = prov_fmts[idx_p]
                        val_c = val_for_prov.get(prov, "")
                        fmt_v = fp["wrap_disc_merge"] if val_c else fp["wrap_merge"]
                        ws.merge_range(
                            fila_start_partida, cp(idx_p, SC_VAL),
                            fila_end_partida,   cp(idx_p, SC_VAL),
                            val_c, fmt_v,
                        )

                # El texto en celdas combinadas no autoajusta altura: elevamos
                # la altura total del bloque para que el comentario técnico
                # quede visible completo.
                current_total_height = sum(
                    row_heights.get(r, DATA_ROW_MIN_HEIGHT)
                    for r in range(fila_start_partida, fila_end_partida + 1)
                )
                required_total_height = current_total_height
                for prov in proveedores:
                    val_c = val_for_prov.get(prov, "")
                    if not val_c:
                        continue
                    required_total_height = max(
                        required_total_height,
                        _row_height(
                            [(val_c, PROV_COMMENT_COL_WIDTH)],
                            line_pts=DATA_ROW_LINE_HEIGHT,
                            min_pts=DATA_ROW_MIN_HEIGHT,
                        ),
                    )

                if required_total_height > current_total_height:
                    extra_per_row = (required_total_height - current_total_height) / max_filas
                    for r in range(fila_start_partida, fila_end_partida + 1):
                        _set_row_height_at_least(
                            r,
                            row_heights.get(r, DATA_ROW_MIN_HEIGHT) + extra_per_row,
                        )

            qa_pauta += max_filas

        # ── BUCLE B: EXTRAS ───────────────────────────────────────────────
        for idx_p, prov in enumerate(proveedores):
            for extra in (extras_idx[prov].get(cap_cod)
                          or extras_idx[prov].get(_norm_cap(cap_cod), [])):
                extra_counter_global[prov] += 1
                _write_extra_row(extra, prov,
                                 f"{last_cod_p}.{extra_counter_global[prov]:02d}")

        # ── TOTALES DIN?MICOS DE CAP?TULO ─────────────────────────────────
        fila_ini = fila_capitulo + 1
        fila_fin = fila - 1
        if fila_fin >= fila_ini:
            ini_c = xl_rowcol_to_cell(fila_ini, COL_IMP)
            fin_c = xl_rowcol_to_cell(fila_fin, COL_IMP)
            ws.write_formula(fila_capitulo, COL_IMP,
                             f"=SUM({ini_c}:{fin_c})", fmt_chap_price)
            for idx_p in range(n_provs):
                col_i = cp(idx_p, SC_IMP)
                ini_o = xl_rowcol_to_cell(fila_ini, col_i)
                fin_o = xl_rowcol_to_cell(fila_fin, col_i)
                ws.write_formula(fila_capitulo, col_i,
                                 f"=SUM({ini_o}:{fin_o})",
                                 prov_fmts[idx_p]["ch_price"])

    # ── EXTRAS ORFES (capítulos que no existen en la pauta) ───────────────
    for idx_p, prov in enumerate(proveedores):
        for cap_key, extra_list in extras_idx[prov].items():
            if cap_key not in pauta_cap_keys:
                for extra in extra_list:
                    extra_counter_global[prov] += 1
                    _write_extra_row(extra, prov,
                                     f"{cap_key}.{extra_counter_global[prov]:02d}")

    # 
    # PIE DE P?GINA — fórmulas activas
    # 
    fila += 1   # fila en blanco de separación

    def _chap_sum_formula(col: int) -> str:
        """Construye =R1+R2+… con las celdas de capítulo para la columna col."""
        if not chapter_imp_rows:
            return "0"
        parts = [xl_rowcol_to_cell(r, col) for r in chapter_imp_rows]
        return "=" + "+".join(parts)

    footer_rows = {
        "TOTAL NETO":                        (fmt_footer_label,       fmt_footer_price,       "neto"),
        f"IVA ({IVA_PCT:.0f}%)":              (fmt_footer_label,       fmt_footer_price,       "iva"),
        "TOTAL + IVA":                       (fmt_footer_label,       fmt_footer_price,       "total_iva"),
        f"IMPREVISTOS ({IMPREVISTOS_PCT:.0f}%)": (fmt_footer_label,   fmt_footer_price,       "imprevistos"),
        "TOTAL + IMPREVISTOS":               (fmt_footer_total_label, fmt_footer_total_price, "total_final"),
    }

    # Almacena la fila de cada concepto para poder referenciarla en fórmulas
    footer_fila: dict[str, int] = {}

    for label, (lbl_fmt, val_fmt, key) in footer_rows.items():
        ws.set_row(fila, 20)

        # Columnas ESTIMACIÓN
        ws.merge_range(fila, 0, fila, COL_IMP - 1, label, lbl_fmt)

        # Importe pauta (COL_IMP)
        if key == "neto":
            pauta_formula = _chap_sum_formula(COL_IMP)
        elif key == "iva":
            neto_cell = xl_rowcol_to_cell(footer_fila["neto"], COL_IMP)
            pauta_formula = f"={neto_cell}*{IVA_PCT / 100}"
        elif key == "total_iva":
            neto_cell = xl_rowcol_to_cell(footer_fila["neto"], COL_IMP)
            iva_cell  = xl_rowcol_to_cell(footer_fila["iva"],  COL_IMP)
            pauta_formula = f"={neto_cell}+{iva_cell}"
        elif key == "imprevistos":
            tiva_cell = xl_rowcol_to_cell(footer_fila["total_iva"], COL_IMP)
            pauta_formula = f"={tiva_cell}*{IMPREVISTOS_PCT / 100}"
        else:  # total_final
            tiva_cell = xl_rowcol_to_cell(footer_fila["total_iva"],   COL_IMP)
            imp_cell  = xl_rowcol_to_cell(footer_fila["imprevistos"], COL_IMP)
            pauta_formula = f"={tiva_cell}+{imp_cell}"

        ws.write_formula(fila, COL_IMP, pauta_formula, val_fmt)

        # Columnas por proveedor
        for idx_p in range(n_provs):
            fp  = prov_fmts[idx_p]
            col_i = cp(idx_p, SC_IMP)
            f_blank = fp["footer_total_blank"] if key == "total_final" else fp["footer_blank"]
            f_price = fp["footer_total_price"] if key == "total_final" else fp["footer_price"]

            if key == "neto":
                formula = _chap_sum_formula(col_i)
            elif key == "iva":
                nc = xl_rowcol_to_cell(footer_fila["neto"], col_i)
                formula = f"={nc}*{IVA_PCT / 100}"
            elif key == "total_iva":
                nc = xl_rowcol_to_cell(footer_fila["neto"], col_i)
                ic = xl_rowcol_to_cell(footer_fila["iva"],  col_i)
                formula = f"={nc}+{ic}"
            elif key == "imprevistos":
                tc = xl_rowcol_to_cell(footer_fila["total_iva"], col_i)
                formula = f"={tc}*{IMPREVISTOS_PCT / 100}"
            else:  # total_final
                tc = xl_rowcol_to_cell(footer_fila["total_iva"],   col_i)
                ic = xl_rowcol_to_cell(footer_fila["imprevistos"], col_i)
                formula = f"={tc}+{ic}"

            ws.write(fila, cp(idx_p, SC_CAN), "", f_blank)
            ws.write(fila, cp(idx_p, SC_PR),  "", f_blank)
            ws.write_formula(fila, col_i, formula, f_price)
            ws.write(fila, cp(idx_p, SC_VAL), "", f_blank)

        footer_fila[key] = fila
        fila += 1

    # ── LLEGENDA DE COLORS ─────────────────────────────────────────────────
    fila += 2
    fmt_leg_title = wb.add_format({
        "font_name": FONT, "bold": True, "font_size": 10,
        "bg_color": "#D9D9D9", "valign": "vcenter",
        "border": 1, "border_color": "#BFBFBF",
    })
    if LAST_COL > 0:
        ws.merge_range(fila, 0, fila, LAST_COL, "LLEGENDA DE COLORS", fmt_leg_title)
    else:
        ws.write(fila, 0, "LLEGENDA DE COLORS", fmt_leg_title)
    fila += 1

    llegenda_colors = [
        ("#FF7474", "VERMELL",
         "Preu Unitari no ofertado (0 o buit). S'imputa el valor màxim dels proveïdors i s'indica [Falta preu] al resum."),
        ("#FFC000", "TARONJA",
         "Error de bulto: desviació >±60% sobre la mediana del Preu Unitari (tots els proveïdors vàlids + Pauta)."),
        ("#FFFF00", "GROC",
         "Fora de rang de mercat: desviació >±20% sobre la mediana del Preu Unitari (proveïdors vàlids + Pauta)."),
    ]
    for color_hex, nom_color, desc in llegenda_colors:
        ws.set_row(fila, 28)
        fmt_sw = wb.add_format({
            "bg_color": color_hex,
            "border": 1, "border_color": "#BFBFBF",
        })
        fmt_lbl = wb.add_format({
            "font_name": FONT, "bold": True, "font_size": 9,
            "bg_color": color_hex, "valign": "vcenter",
            "border": 1, "border_color": "#BFBFBF",
        })
        fmt_desc = wb.add_format({
            "font_name": FONT, "font_size": 9, "valign": "vcenter",
            "text_wrap": True, "border": 1, "border_color": "#BFBFBF",
        })
        ws.write(fila, 0, "", fmt_sw)
        ws.write(fila, 1, nom_color, fmt_lbl)
        end_desc_col = max(2, LAST_COL)
        if end_desc_col > 2:
            ws.merge_range(fila, 2, fila, end_desc_col, desc, fmt_desc)
        else:
            ws.write(fila, 2, desc, fmt_desc)
        fila += 1

    wb.close()

    # ── Resumen QA ────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("📊 RESUMEN DE CONTROL DE CALIDAD")
    print("=" * 60)
    print(f"✅ Archivo generado       : {archivo_salida}")
    print(f"   Total filas de datos   : {fila - ROW_DATA}")
    print(f"   Partidas pauta (fijas) : {qa_pauta}")
    for prov in proveedores:
        print(f"   Extras {prov:<15}: {qa_extras[prov]}")
    print("=" * 60)
