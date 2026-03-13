import os
import json

def consolidate_chunks(pdf_name, output_base_dir="./output"):
    """
    Unifica los archivos JSON de la carpeta chunks. 
    Maneja estructura dict/list y metadatos.
    """
    chunks_dir = os.path.join(output_base_dir, pdf_name, "chunks")
    final_output_path = os.path.join(output_base_dir, pdf_name, f"FINAL_{pdf_name}.json")
    
    if not os.path.exists(chunks_dir):
        print(f"❌ No se encontró la carpeta: {chunks_dir}")
        return None

    consolidated_data = {}
    chunk_files = sorted([f for f in os.listdir(chunks_dir) if f.endswith(".json")])
    
    for file_name in chunk_files:
        try:
            with open(os.path.join(chunks_dir, file_name), "r", encoding="utf-8") as f:
                content = json.load(f)
                
                # --- CORRECCIÓN CRÍTICA ---
                # Si el chunk se guardó como lista directa (el fix del main.py), lo tratamos como tal.
                if isinstance(content, list):
                    capitulos = content
                else:
                    capitulos = content.get('capitulos', [])
                # --------------------------
                
                for cap in capitulos:
                    cap_id = cap.get('capitulo_codigo', 'UNKNOWN') # .get por seguridad
                    
                    if cap_id not in consolidated_data:
                        consolidated_data[cap_id] = {
                            "capitulo_codigo": cap_id,
                            "capitulo_nombre": cap.get('capitulo_nombre', ''),
                            "total_capitulo": 0.0,
                            "partidas": {}
                        }
                    
                    # MEJORA LÓGICA:
                    # Si este chunk tiene un total mayor que el que teníamos guardado, lo actualizamos.
                    # Esto arregla casos donde el primer chunk dice "0" y el segundo trae el total real.
                    current_total = cap.get('total_capitulo', 0.0)
                    if current_total > consolidated_data[cap_id]["total_capitulo"]:
                        consolidated_data[cap_id]["total_capitulo"] = current_total

                    # Unificamos partidas por su código único
                    for p in cap.get('partidas', []):
                        p_id = p.get('codigo')
                        if p_id and p_id not in consolidated_data[cap_id]["partidas"]:
                            consolidated_data[cap_id]["partidas"][p_id] = p
                            
        except Exception as e:
            print(f"⚠️ Error al consolidar {file_name}: {e}")

    # Reestructurar a lista final
    final_list = []
    for cap_id in sorted(consolidated_data.keys()):
        cap_info = consolidated_data[cap_id]
        # Ordenamos partidas por código y convertimos dict de partidas a lista
        cap_info["partidas"] = [cap_info["partidas"][k] for k in sorted(cap_info["partidas"].keys())]
        final_list.append(cap_info)

    with open(final_output_path, "w", encoding="utf-8") as f:
        json.dump(final_list, f, indent=4, ensure_ascii=False)
    
    return final_output_path

def apply_mapping_to_json(offer_json_path, mapping_result):
    """
    Aplica el resultado del MapperAgent al JSON consolidado de la oferta.
    Maneja llaves compuestas 'CAPITULO::CODIGO'.
    """
    with open(offer_json_path, 'r', encoding='utf-8') as f:
        offer_data = json.load(f)
    
    raw_map_dict = mapping_result.get("mapping", {}) or {}
    # extras_list puede ser una lista o None si la IA devolvió null
    extras_raw = mapping_result.get("extras", [])
    extras_list = list(extras_raw) if isinstance(extras_raw, list) else []

    # ── Sanear mapping: cualquier clave cuyo valor sea null/None se promueve a extra.
    # Esto ocurre cuando Gemini devuelve {"CAP::COD": null} en lugar de mover la
    # partida a "extras". Funcionalmente es incorrecto tenerla en mapping con valor
    # nulo, así que la tratamos como extra para que el comparativo la muestre bien.
    map_dict = {}
    for k, v in raw_map_dict.items():
        if v:
            map_dict[k] = v
        else:
            if k not in extras_list:
                extras_list.append(k)

    stats = {"mapped": 0, "extras": 0}

    for cap in offer_data:
        cap_code_offer = str(cap.get('capitulo_codigo', '')).strip()
        
        for item in cap.get("partidas", []):
            item_code_offer = str(item.get("codigo", "")).strip()
            
            # Reconstruimos el ID único de la oferta (el mismo que usó el Mapper)
            unique_id_offer = f"{cap_code_offer}::{item_code_offer}"
            
            # 1. ¿Está mapeado? (map_dict ya no contiene valores null)
            if unique_id_offer in map_dict:
                pauta_composite_id = map_dict[unique_id_offer]

                # El mapper puede devolver una lista para mapeos 1:N (una partida oferta
                # cubre varias de pauta). Usamos el primer elemento como ID canónico;
                # el resto se almacena en id_pauta_unico_extra para trazabilidad.
                if isinstance(pauta_composite_id, list):
                    extra_ids = pauta_composite_id[1:] if len(pauta_composite_id) > 1 else []
                    pauta_composite_id = pauta_composite_id[0] if pauta_composite_id else unique_id_offer
                    if extra_ids:
                        item["id_pauta_unico_extra"] = extra_ids
                else:
                    extra_ids = []

                # Descomponemos "05::01.01" -> "01.01" para visualización humana
                try:
                    _, pauta_cod = str(pauta_composite_id).split("::", 1)
                except (ValueError, AttributeError):
                    pauta_cod = str(pauta_composite_id)
                
                item["codigo_pauta"] = pauta_cod        # Visual (Ej: 01.01)
                item["id_pauta_unico"] = pauta_composite_id # Técnico (Ej: 01::01.01)
                stats["mapped"] += 1
            
            # 2. ¿Es Extra? (incluye las promovidas desde mapping null)
            elif unique_id_offer in extras_list:
                item["codigo_pauta"] = "EXTRA"
                item["es_extra"] = True
                stats["extras"] += 1
            
            # 3. Fallback (Si la IA no dijo nada, asumimos mapeo directo)
            elif not item.get("codigo_pauta"):
                item["codigo_pauta"] = item_code_offer
                item["id_pauta_unico"] = unique_id_offer

    # Sobrescribimos el archivo con los datos enriquecidos
    with open(offer_json_path, 'w', encoding='utf-8') as f:
        json.dump(offer_data, f, indent=4, ensure_ascii=False)
        
    return stats


def generate_audit_qualitative_input(pauta_path, offer_json_path, mapping_result=None, output_path=None):
    """
    Genera el audit_qualitative_input.json DESPUÉS de que el mapper ha generado el mapeo.
    
    Este archivo contiene la información de mapeo: para cada partida de pauta,
    indica qué partida(s) de oferta le corresponde(n).
    
    Args:
        pauta_path: Ruta al mapped_pauta.json
        offer_json_path: Ruta al JSON consolidado de la oferta (CON mapeo ya aplicado)
        mapping_result: Dict con keys "mapping" y "extras" (resultado del mapper)
        output_path: Donde guardar. Si None, se infiere del directorio de offer_json_path
    
    Returns:
        comparison_queue: Lista de items para comparación
    """
    
    # Cargar datos
    with open(pauta_path, 'r', encoding='utf-8') as f:
        pauta_data = json.load(f)
    
    with open(offer_json_path, 'r', encoding='utf-8') as f:
        offer_data = json.load(f)
    
    # Helper: unir nombre + descripción + componentes
    def get_full_desc(item, include_components=True):
        """Une título, descripción y componentes para que la IA tenga todo el contexto."""
        nombre = str(item.get('nombre', '')).strip()
        descripcion = str(item.get('descripcion', '')).strip()
        
        if not nombre and not descripcion: 
            base_desc = "Sense descripció"
        elif not nombre: 
            base_desc = descripcion
        elif not descripcion: 
            base_desc = nombre
        # Si el título ya está contenido al inicio, no duplicar
        elif nombre.lower() in descripcion.lower()[:len(nombre)+5]: 
            base_desc = descripcion
        else:
            base_desc = f"{nombre}\n{descripcion}"
        
        # Añadir componentes si existen
        if include_components:
            componentes = item.get('componentes')
            if componentes and isinstance(componentes, list) and len(componentes) > 0:
                comp_lines = ["\n[COMPONENTES DESGLOSADOS]:"]
                for comp in componentes:
                    comp_desc = comp.get('descripcion', '?')
                    comp_lines.append(f"  - {comp_desc}")
                base_desc += "\n".join(comp_lines)
        
        return base_desc
    
    # Extraer info del mapping
    map_dict = mapping_result.get("mapping", {}) if mapping_result else {}
    extras_list = mapping_result.get("extras", []) if mapping_result else []
    
    # --- PASO 1: INDEXAR LA OFERTA POR id_pauta_unico ---
    # Para búsquedas rápidas: dado un id_pauta_unico, obtener las partidas de oferta que le corresponden
    pauta_to_offer_map = {}
    
    for cap in offer_data:
        cap_id = str(cap.get('capitulo_codigo', '')).strip()
        
        for item in cap.get('partidas', []):
            # El JSON YA tiene id_pauta_unico si el mapeo fue aplicado
            pauta_unique_id = item.get('id_pauta_unico')
            
            # Fallback: construir desde código_pauta
            if not pauta_unique_id:
                codigo_pauta = item.get('codigo_pauta')
                if codigo_pauta and codigo_pauta != "EXTRA":
                    pauta_unique_id = f"{cap_id}::{codigo_pauta}"
            
            # Mapear: pauta_unique_id -> lista de (item, cap_id) tuplas
            if pauta_unique_id:
                if pauta_unique_id not in pauta_to_offer_map:
                    pauta_to_offer_map[pauta_unique_id] = []
                pauta_to_offer_map[pauta_unique_id].append((item, cap_id))
    
    comparison_queue = []
    
    # --- PASO 2: BARRIDO DE LA PAUTA ---
    # Para cada partida en pauta, buscar sus contrapartes en oferta usando el mapeo
    for cap_pauta in pauta_data:
        cap_pauta_id = str(cap_pauta.get('capitulo_codigo', '')).strip()
        cap_pauta_nombre = cap_pauta.get('capitulo_nombre', 'Sin Nombre')
        
        for item_pauta in cap_pauta.get('partidas', []):
            code_pauta = item_pauta.get('codigo_pauta') or item_pauta.get('codigo')
            pauta_unique_id = f"{cap_pauta_id}::{code_pauta}"
            
            # Buscar qué partidas de oferta corresponden a esta pauta
            found_items = pauta_to_offer_map.get(pauta_unique_id, [])
            
            found_obj = None
            if found_items:
                # Concatenar descripciones si hay múltiples (desgloses)
                full_desc_list = []
                codes_list = []
                
                for found_item_tuple in found_items:
                    # Desempacar la tupla (item, cap_id)
                    found_item, found_cap_id = found_item_tuple
                    found_code = found_item.get('codigo', '?')
                    found_desc = get_full_desc(found_item)
                    
                    # Formato: Capítulo::Código
                    code_key = f"{found_cap_id}::{found_code}"
                    full_desc_list.append(f"[{code_key}] {found_desc}")
                    codes_list.append(code_key)
                
                found_obj = {
                    "codigo_oferta": codes_list if len(codes_list) > 1 else (codes_list[0] if codes_list else None),
                    "desc": "\n---\n".join(full_desc_list)
                }
            
            # Construir el par pauta-oferta
            pair = {
                "capitulo_contexto": f"{cap_pauta_id} - {cap_pauta_nombre}",
                "ref": {
                    "codigo": pauta_unique_id,
                    "desc": get_full_desc(item_pauta)
                },
                "found": found_obj
            }
            comparison_queue.append(pair)
    
    # --- PASO 3: DETECTAR EXTRAS (partidas en oferta que son extras) ---
    for cap_offer in offer_data:
        cap_offer_id = str(cap_offer.get('capitulo_codigo', '')).strip()
        cap_offer_nombre = cap_offer.get('capitulo_nombre', 'Extras')
        
        for item_offer in cap_offer.get('partidas', []):
            # Si la partida está marcada como EXTRA o su codigo_pauta es "EXTRA"
            if item_offer.get('es_extra') or item_offer.get('codigo_pauta') == "EXTRA":
                # Filtrar items vacíos
                if item_offer.get('descripcion') or item_offer.get('precio', 0) > 0:
                    offer_code = item_offer.get('codigo', '?')
                    
                    pair = {
                        "capitulo_contexto": f"{cap_offer_id} - {cap_offer_nombre} (POSIBLE EXTRA)",
                        "ref": None,
                        "found": {
                            "codigo_oferta": f"{cap_offer_id}::{offer_code}",
                            "desc": get_full_desc(item_offer)
                        }
                    }
                    comparison_queue.append(pair)
    
    # --- PASO 4: GUARDAR ---
    if output_path is None:
        # Inferir ruta desde el directorio de offer_json_path
        output_dir = os.path.dirname(offer_json_path)
        output_path = os.path.join(output_dir, "audit_qualitative_input.json")
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(comparison_queue, f, indent=4, ensure_ascii=False)
    
    print(f"    📝 Cola de comparación generada ({len(comparison_queue)} items) en: {output_path}")
    
    return comparison_queue