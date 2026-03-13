import json
import os

def load_audit_data(pauta_path, extracted_path):
    """Carga y valida la existencia de los archivos JSON."""
    if not os.path.exists(pauta_path) or not os.path.exists(extracted_path):
        return None, None
    with open(pauta_path, 'r', encoding='utf-8') as f: pauta_data = json.load(f)
    with open(extracted_path, 'r', encoding='utf-8') as f: extracted_data = json.load(f)
    return pauta_data, extracted_data

def prepare_comparison_queue(pauta_data, extracted_data, save_path=None):
    """
    Genera la cola de comparación para el Auditor.
    1. Alinea Pauta vs Oferta.
    2. Detecta Extras.
    3. Une Nombre y Descripción.
    4. Agrupa desgloses (N:1) concatenando textos.
    """
    
    # --- HELPER: UNIR NOMBRE + DESCRIPCIÓN ---
    def get_full_desc(item):
        """Une título y descripción para que la IA tenga todo el contexto."""
        nombre = str(item.get('nombre', '')).strip()
        descripcion = str(item.get('descripcion', '')).strip()
        
        if not nombre and not descripcion: return "Sense descripció"
        if not nombre: return descripcion
        if not descripcion: return nombre
        
        # Si el título ya está contenido al inicio, no duplicar
        if nombre.lower() in descripcion.lower()[:len(nombre)+5]: 
            return descripcion
            
        return f"{nombre}\n{descripcion}"

    # --- PASO 1: INDEXAR LA OFERTA ---
    extracted_map = {}
    used_extracted_ids = set() 
    
    for cap in extracted_data:
        cap_id = str(cap.get('capitulo_codigo', '')).strip()
        
        for item in cap.get('partidas', []):
            p_code = item.get('codigo_pauta') 
            r_code = item.get('codigo')       
            
            unique_item_id = f"{cap_id}|{r_code}"
            item['_unique_id'] = unique_item_id 
            # Guardar cap_id en el item para acceder después
            item['_cap_id'] = cap_id
            
            # A. Indexar por ID PAUTA ÚNICO
            pauta_unique_id = item.get('id_pauta_unico')
            if not pauta_unique_id and p_code and p_code != "EXTRA":
                pauta_unique_id = f"{cap_id}::{p_code}"
            
            if pauta_unique_id:
                key = f"PAUTA_ID::{pauta_unique_id}"
                if key not in extracted_map: extracted_map[key] = []
                extracted_map[key].append(item)

            # B. Indexar por RAW CODE
            if r_code:
                key_raw = f"RAW::{cap_id}|{r_code}"
                if key_raw not in extracted_map: extracted_map[key_raw] = []
                extracted_map[key_raw].append(item)


    comparison_queue = []

    # --- PASO 2: BARRIDO DE LA PAUTA ---
    for cap in pauta_data:
        cap_id = str(cap.get('capitulo_codigo', '')).strip()
        cap_nombre = cap.get('capitulo_nombre', 'Sin Nombre')
        
        for item_pauta in cap.get('partidas', []):
            code_ref = item_pauta.get('codigo_pauta') or item_pauta.get('codigo')
            
            target_pauta_id = f"{cap_id}::{code_ref}"
            
            found_items = extracted_map.get(f"PAUTA_ID::{target_pauta_id}")
            if not found_items:
                found_items = extracted_map.get(f"RAW::{cap_id}|{code_ref}")
            
            found_obj = None
            if found_items:
                # Si hay múltiples partidas de oferta para una de pauta (Desglose):
                # Concatenamos las descripciones y listamos los códigos.
                full_desc_list = []
                codes_list = []
                pauta_id_used = None  # Guardar el id_pauta_unico del mapping
                
                for fi in found_items:
                    used_extracted_ids.add(fi['_unique_id'])
                    
                    # Obtener directamente del mapping: id_pauta_unico es la CLAVE del mapping
                    pauta_id_used = fi.get('id_pauta_unico')
                    
                    # Para la oferta, construimos el formato capitulo::partida
                    fi_cap_id = fi.get('_cap_id', cap_id)
                    fi_code = fi.get('codigo', '?')
                    fi_desc = get_full_desc(fi)
                    full_desc_list.append(f"[{fi_cap_id}::{fi_code}] {fi_desc}")
                    
                    # Usar formato capitulo::partida para la oferta
                    codes_list.append(f"{fi_cap_id}::{fi_code}")
                
                found_obj = {
                    "codigo_oferta": codes_list if len(codes_list) > 1 else codes_list[0],
                    # Unimos todo con separadores claros
                    "desc": "\n---\n".join(full_desc_list)
                }

            pair = {
                "capitulo_contexto": f"{cap_id} - {cap_nombre}",
                "ref": {
                    # Si existe id_pauta_unico del mapping, lo usamos (es la clave del mapping)
                    # Si no, construimos el formato manualmente
                    "codigo": pauta_id_used if pauta_id_used else f"{cap_id}::{code_ref}",
                    "desc": get_full_desc(item_pauta)
                },
                "found": found_obj
            }
            comparison_queue.append(pair)

    # --- PASO 3: BARRIDO DE EXTRAS ---
    for cap in extracted_data:
        cap_id = str(cap.get('capitulo_codigo', '')).strip()
        cap_nombre = cap.get('capitulo_nombre', 'Extras / Sin Capitulo')
        
        for item in cap.get('partidas', []):
            unique_item_id = item.get('_unique_id')
            
            if unique_item_id and unique_item_id not in used_extracted_ids:
                # Filtrar items vacíos
                if item.get('descripcion') or item.get('precio', 0) > 0:
                    item_code = item.get('codigo')
                    
                    pair = {
                        "capitulo_contexto": f"{cap_id} - {cap_nombre} (POSIBLE EXTRA)",
                        "ref": None, 
                        "found": {
                            "codigo_oferta": f"{cap_id}::{item_code}",
                            "desc": get_full_desc(item)
                        }
                    }
                    comparison_queue.append(pair)

    # --- PASO 4: GUARDAR ---
    if save_path:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(comparison_queue, f, indent=4, ensure_ascii=False)
        print(f"    📝 Cola de comparación guardada ({len(comparison_queue)} items) en: {save_path}")
            
    return comparison_queue