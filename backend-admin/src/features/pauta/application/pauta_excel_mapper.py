import pandas as pd
import json
import os

def safe_float(value):
    """Convierte de forma segura un valor a float."""
    try:
        if pd.isna(value) or value == '':
            return 0.0
        # Si el valor viene como string con formato europeo (1.250,50)
        if isinstance(value, str):
            value = value.replace('.', '').replace(',', '.')
        return float(value)
    except:
        return 0.0

def map_excel_to_json(excel_path, output_json_path):
    """
    Lee el Excel de la pauta y genera un JSON estructurado siguiendo el formato 
    de auditoría (incluyendo cantidades, precios y códigos de pauta).
    """
    # 1. Carga inicial
    if excel_path.endswith('.csv'):
        df_raw = pd.read_csv(excel_path, header=None)
    else:
        df_raw = pd.read_excel(excel_path, header=None)
    
    # 2. Buscar la fila de cabecera
    header_row = 0
    for i, row in df_raw.iterrows():
        row_values = [str(val) for val in row.values if pd.notna(val)]
        row_str = " ".join(row_values).upper()
        if 'CÓDIGO' in row_str or 'RESUMEN' in row_str or 'NAT' in row_str:
            header_row = i
            break
            
    # 3. Leer con la cabecera detectada
    if excel_path.endswith('.csv'):
        df = pd.read_csv(excel_path, header=header_row)
    else:
        df = pd.read_excel(excel_path, header=header_row)
    
    df.columns = [str(c).strip() for c in df.columns]

    pauta_estructurada = []
    capitulo_actual = None
    partida_actual = None

    for _, row in df.iterrows():
        codigo = str(row.get('Código', '')).strip()
        nat = str(row.get('Nat', '')).strip()
        resumen = str(row.get('Resumen', '')).strip()

        # --- DETECTAR CAPÍTULO ---
        if 'Capítol' in nat:
            capitulo_actual = {
                "capitulo_codigo": codigo,
                "capitulo_nombre": resumen,
                "total_capitulo": safe_float(row.get('ImpPres', 0.0)),
                "partidas": []
            }
            pauta_estructurada.append(capitulo_actual)
            partida_actual = None
        
        # --- DETECTAR PARTIDA ---
        elif 'Partida' in nat:
            partida_actual = {
                "codigo_pauta": codigo, # En la pauta, el código es su propia referencia
                "nombre": resumen,
                "descripcion": "",
                "unidad": str(row.get('Ut', '')).strip() if pd.notna(row.get('Ut')) else "",
                "cantidad": safe_float(row.get('CanPres', 0.0)),
                "precio": safe_float(row.get('PrPres', 0.0)),
                "total": safe_float(row.get('ImpPres', 0.0))
            }
            if capitulo_actual:
                capitulo_actual["partidas"].append(partida_actual)
        
        # --- DETECTAR DESCRIPCIÓN (Filas subordinadas) ---
        elif partida_actual and resumen and resumen.lower() != 'nan' and resumen != '':
            # Si la fila no tiene naturaleza (Nat vacía), es parte de la descripción
            if partida_actual["descripcion"]:
                partida_actual["descripcion"] += "\n" + resumen
            else:
                partida_actual["descripcion"] = resumen

    # Limpieza final de espacios
    for cap in pauta_estructurada:
        for p in cap["partidas"]:
            p["descripcion"] = p["descripcion"].strip()

    # Guardar JSON
    os.makedirs(os.path.dirname(output_json_path), exist_ok=True)
    with open(output_json_path, "w", encoding="utf-8") as f:
        json.dump(pauta_estructurada, f, indent=4, ensure_ascii=False)
        
    return pauta_estructurada