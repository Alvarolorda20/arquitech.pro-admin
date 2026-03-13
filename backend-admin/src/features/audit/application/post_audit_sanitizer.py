"""
sanitizer.py
============
Utilidades de saneamiento post-auditoría.

Función principal:
    sanitizar_asignaciones_post_auditoria(ruta_auditoria, ruta_final_json)

    Elimina asignaciones duplicadas del AUDITORIA_VALIDADA_{proveedor}.json
    y sincroniza id_pauta_unico en el FINAL_{proveedor}.json.

    Problema que resuelve:
        Una misma codigo_oferta (ej: "03::03.12") puede aparecer como
        propietario exclusivo en una entrada (1:1) Y dentro de una lista
        grupal en otra entrada (1:N). Esta función retira ese código de la
        lista grupal y actualiza id_pauta_unico en el FINAL JSON.

    Nota sobre claves:
        - En la auditoría, codigo_oferta puede ser "03::03.12" (forma larga)
          o "03.12" (forma corta).
        - En el FINAL JSON, el campo "codigo" siempre es la forma corta "03.12".
        - La sincronización busca coincidencias usando ambas formas.
"""

import json
import os


def sanitizar_asignaciones_post_auditoria(
    ruta_auditoria: str,
    ruta_final_json: str,
) -> dict:
    """
    Limpia asignaciones duplicadas en AUDITORIA_VALIDADA y sincroniza FINAL JSON.

    Retorna dict con estadísticas:
        {"eliminados": int, "sincronizados": int, "cambios": dict, "skipped": bool}
    """
    if not os.path.exists(ruta_auditoria):
        print(f"    ⚠️  Auditoría no encontrada para sanear: {ruta_auditoria}")
        return {"skipped": True, "eliminados": 0, "sincronizados": 0, "cambios": {}}

    with open(ruta_auditoria, "r", encoding="utf-8") as f:
        auditoria = json.load(f)

    def _to_list(val) -> list:
        """Normaliza codigo_oferta a lista (str o list → list)."""
        if val is None:
            return []
        return val if isinstance(val, list) else [str(val)]

    def _short(cod: str) -> str:
        """Devuelve la forma corta de un código ('03::03.12' → '03.12')."""
        return cod.split("::")[-1].strip() if "::" in cod else cod.strip()

    # ── Paso 1: Detectar propietarios exclusivos (entradas 1:1) ───────────────
    # Indexamos tanto la forma larga como la corta para facilitar búsquedas.
    propietario_exclusivo: dict[str, str] = {}  # { cod_oferta (str): cod_pauta }

    for entry in auditoria:
        codigos = _to_list(entry.get("codigo_oferta"))
        if len(codigos) == 1:
            cod     = codigos[0]
            pauta   = str(entry.get("codigo_pauta", ""))
            propietario_exclusivo[cod]          = pauta   # forma original
            propietario_exclusivo[_short(cod)]  = pauta   # forma corta

    if not propietario_exclusivo:
        print("    ℹ️  Sin propietarios exclusivos detectados, saneamiento omitido.")
        return {"eliminados": 0, "sincronizados": 0, "cambios": {}, "skipped": False}

    # ── Paso 2: Limpiar listas grupales (1:N) ─────────────────────────────────
    n_eliminados = 0
    # cambios: { cod_oferta_original: cod_pauta_correcto }
    # También guardamos la forma corta para la sincronización posterior.
    cambios: dict[str, str] = {}

    for entry in auditoria:
        cod_pauta   = str(entry.get("codigo_pauta", ""))
        codigos     = _to_list(entry.get("codigo_oferta"))

        if len(codigos) <= 1:
            continue   # exclusivo o vacío, no tocar

        limpios = []
        for cod_of in codigos:
            propietario = propietario_exclusivo.get(cod_of) \
                       or propietario_exclusivo.get(_short(cod_of))

            if propietario and propietario != cod_pauta:
                # Tiene propietario exclusivo en otra entrada → retirar
                cambios[cod_of]          = propietario
                cambios[_short(cod_of)]  = propietario   # también la forma corta
                n_eliminados += 1
                print(
                    f"    🧹 [{cod_of}] retirado de '{cod_pauta}' "
                    f"→ propietario exclusivo: '{propietario}'"
                )
            else:
                limpios.append(cod_of)

        # Preservar tipo original (str / list)
        original = entry.get("codigo_oferta")
        if isinstance(original, list):
            entry["codigo_oferta"] = limpios
        else:
            entry["codigo_oferta"] = limpios[0] if limpios else ""

    # ── Paso 3: Guardar auditoría saneada ─────────────────────────────────────
    with open(ruta_auditoria, "w", encoding="utf-8") as f:
        json.dump(auditoria, f, indent=4, ensure_ascii=False)
    print(
        f"    ✅ Auditoría saneada — "
        f"{n_eliminados} asignación(es) duplicada(s) eliminada(s)."
    )

    # ── Paso 4: Sincronizar id_pauta_unico en FINAL JSON ──────────────────────
    n_sync = 0

    if not cambios:
        print("    ℹ️  Sin cambios que sincronizar en FINAL JSON.")
        return {"eliminados": n_eliminados, "sincronizados": 0, "cambios": cambios, "skipped": False}

    if not ruta_final_json or not os.path.exists(ruta_final_json):
        print(f"    ⚠️  FINAL JSON no encontrado: {ruta_final_json}")
        return {"eliminados": n_eliminados, "sincronizados": 0, "cambios": cambios, "skipped": False}

    with open(ruta_final_json, "r", encoding="utf-8") as f:
        final_data = json.load(f)

    for capitulo in final_data:
        cap_cod = str(capitulo.get("capitulo_codigo", "")).strip()

        for partida in capitulo.get("partidas", []):
            cod_corto = str(partida.get("codigo", "")).strip()
            cod_largo = f"{cap_cod}::{cod_corto}"

            # Buscar usando forma larga primero, luego corta
            nuevo_pauta = cambios.get(cod_largo) or cambios.get(cod_corto)

            if nuevo_pauta:
                old = partida.get("id_pauta_unico", "")
                if old != nuevo_pauta:
                    partida["id_pauta_unico"] = nuevo_pauta
                    n_sync += 1
                    print(
                        f"    🔄 Partida [{cod_corto}]: "
                        f"id_pauta_unico {old!r} → {nuevo_pauta!r}"
                    )

    with open(ruta_final_json, "w", encoding="utf-8") as f:
        json.dump(final_data, f, indent=4, ensure_ascii=False)

    print(f"    🔄 {n_sync} partida(s) sincronizada(s) en FINAL JSON.")

    return {
        "eliminados":    n_eliminados,
        "sincronizados": n_sync,
        "cambios":       cambios,
        "skipped":       False,
    }
