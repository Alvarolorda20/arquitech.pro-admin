import json
import google.generativeai as genai
import os
import re
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.shared.observability.cost_tracker import record_usage

class MapperAgent:
    def __init__(self, api_key):
        genai.configure(api_key=api_key)
        # Cost/quality tradeoff:
        # - default: flash (lower cost)
        # - override: set MAPPER_MODEL=gemini-2.5-pro for maximum reasoning depth
        self.model_name = (
            os.getenv("MAPPER_MODEL", "gemini-2.5-flash").strip()
            or "gemini-2.5-flash"
        )
        self.model = genai.GenerativeModel(
            model_name=self.model_name,
            generation_config={
                "response_mime_type": "application/json",
                "temperature": 0.0,
                "max_output_tokens": 16384,  # mapping + logica_de_mapeo per batch
            }
        )

    def map_offer_to_pauta(self, pauta_path, offer_json_path, max_workers: int = 4):
        """
        Genera un mapeo usando IDs ÚNICOS (Capítulo::Código).
        EJECUTA LLAMADAS EN PARALELO CON GUARDADO INCREMENTAL DE BATCHES.

        Args:
            max_workers:  LLM worker threads. Passed from server.compute_concurrency()
                          so the value auto-scales with the number of concurrent PDFs.
        """
        # 1. Cargar datos
        with open(pauta_path, 'r', encoding='utf-8') as f: pauta_data = json.load(f)
        with open(offer_json_path, 'r', encoding='utf-8') as f: offer_data = json.load(f)

        # 2. Aplanar datos
        pauta_flat = self._flatten_json(pauta_data, source="PAUTA")
        offer_flat = self._flatten_json(offer_data, source="OFERTA")

        total_offer = len(offer_flat)
        print(f"🗺️  El Mapper va a procesar {total_offer} partidas en PARALELO...")

        # --- CONFIGURACIÓN DE PARALELISMO ---
        # max_workers is passed by the caller (server.py) via compute_concurrency()
        # so it automatically scales with the number of PDFs in the current
        # request. Default=4 for standalone usage.
        # Shutdown uses cancel_futures=True so 429-sleeping threads don't block exit.
        BATCH_SIZE = 20
        MAX_WORKERS = max_workers
        
        # --- SETUP DE ALMACENAMIENTO INCREMENTAL ---
        # Crear carpeta de batches en el directorio del proveedor
        provider_dir = os.path.dirname(offer_json_path)
        batches_dir = os.path.join(provider_dir, "mapping_batches")
        os.makedirs(batches_dir, exist_ok=True)
        
        # Crear definición de batches
        batches = []
        batches_to_process = []  # Solo los que NO están completos
        
        for i in range(0, total_offer, BATCH_SIZE):
            batch = offer_flat[i : i + BATCH_SIZE]
            batch_id = (i // BATCH_SIZE) + 1
            batches.append((batch_id, batch))
            
            # Verificar si este batch ya fue procesado
            batch_file = os.path.join(batches_dir, f"mapping_batch_{batch_id}.json")
            if not os.path.exists(batch_file):
                batches_to_process.append((batch_id, batch))
            else:
                print(f"    ♻️  Batch {batch_id} ya procesado (saltando)...")
        
        total_batches = len(batches)
        pending_batches = len(batches_to_process)
        
        if pending_batches == 0:
            print(f"✅ Todos los {total_batches} batches ya existen. Cargando resultados...")
        else:
            print(f"🚀 Procesando {pending_batches}/{total_batches} batches pendientes ({total_batches - pending_batches} ya completos)...")
            print(f"[parallel] Procesamiento en paralelo (hasta {MAX_WORKERS} batches simultaneos)")
        
        # --- EJECUCIÓN EN PARALELO ---
        full_mapping = {}
        full_extras = []
        full_alerts = []
        
        if batches_to_process:
            completed_count = 0
            # Usamos shutdown explícito con cancel_futures=True para que los hilos que
            # se queden bloqueados en el backoff de 429 sean cancelados al salir del
            # bloque, evitando que el proceso se quede "pillado" esperando su fin.
            executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
            try:
                future_to_batch = {
                    executor.submit(self._process_batch, pauta_flat, batch, batch_id, total_batches): batch_id
                    for batch_id, batch in batches_to_process
                }

                for future in as_completed(future_to_batch):
                    batch_id = future_to_batch[future]
                    completed_count += 1
                    try:
                        result = future.result(timeout=180)  # 3 min timeout per batch
                        if result and result.get("mapping"):
                            # 📝 GUARDAR BATCH INMEDIATAMENTE
                            batch_file = os.path.join(batches_dir, f"mapping_batch_{batch_id}.json")
                            with open(batch_file, 'w', encoding='utf-8') as f:
                                json.dump({
                                    "batch_id": batch_id,
                                    "timestamp": datetime.now().isoformat(),
                                    "result": result
                                }, f, indent=2, ensure_ascii=False)

                            print(f"    ✅ Batch {batch_id} → Guardado ({completed_count}/{pending_batches})")
                        else:
                            print(f"    ⚠️  Batch {batch_id} devolvió vacío o error.")

                    except Exception as exc:
                        print(f"    ❌ Batch {batch_id} generó excepción: {exc}")
                        print(f"    💾 Los batches completados están guardados. Puede reintentar después.")
            finally:
                # cancel_futures=True cancela los futuros pendientes en la cola y no
                # espera a los hilos que ya están ejecutándose (durmiendo en 429-backoff).
                executor.shutdown(wait=False, cancel_futures=True)

            print(f"    ✅ ThreadPool finalizado ({completed_count}/{pending_batches} completados)")
        
        # --- CONSOLIDAR TODOS LOS BATCHES (completos + nuevos) ---
        print(f"\n📦 Consolidando {total_batches} batches...")
        for batch_id in range(1, total_batches + 1):
            batch_file = os.path.join(batches_dir, f"mapping_batch_{batch_id}.json")
            if os.path.exists(batch_file):
                try:
                    with open(batch_file, 'r', encoding='utf-8') as f:
                        batch_data = json.load(f)
                        result = batch_data.get("result", {})
                        
                        full_mapping.update(result.get("mapping", {}))
                        batch_extras = result.get("extras", [])
                        full_extras.extend([x for x in batch_extras if x not in full_extras])

                        batch_alerts = result.get("logica_de_mapeo") or result.get("alertas_criticas", [])
                        if isinstance(batch_alerts, list):
                            full_alerts.extend(batch_alerts)
                except Exception as e:
                    print(f"    ⚠️  Error consolidando batch {batch_id}: {e}")
        
        # 3. Limpieza final: Si un ID está en mapping, no debería estar en extras
        full_extras = [e for e in full_extras if e not in full_mapping]

        print(f"✨ Mapeo finalizado. {len(full_mapping)} partidas vinculadas, {len(full_extras)} extras detectados.")
        print(f"📂 Batches individuales guardados en: {batches_dir}")

        return {
            "mapping": full_mapping,
            "extras": full_extras,
            "alertas_tecnicas": full_alerts,
        }

    def _process_batch(self, pauta_full, offer_batch, batch_id, total_batches=1):
        """
        Función que ejecuta cada hilo con PROMPT SEMÁNTICO REFORZADO.

        Args:
            pauta_full:   Lista aplanada de partidas de pauta
            offer_batch:  Lote de partidas de oferta para procesar
            batch_id:     ID del batch actual (1, 2, 3...)
            total_batches: Total de batches
        """
        system_prompt = """
        PERFIL: Actúa como un Ingeniero de Edificación / Quantity Surveyor experto en auditoría de costes y comparativos de obra. Tu capacidad analítica se centra en la equivalencia técnica y la composición de precios unitarios.

        OBJETIVO: Vincular cada partida de la LISTA_OFERTA con registro(s) correspondiente(s) de la LISTA_PAUTA. El mapeo es N:M.

        ═══════════════════════════════════════════════════════════════
        📋 ESTRUCTURA DE DATOS DE ENTRADA
        ═══════════════════════════════════════════════════════════════
        Cada ITEM: {"id": "CAPITULO::CODIGO", "cod_vis": "CODIGO", "desc": "[CAP NOMBRE] Descripción", "precio": 1000}
        - PAUTA: Especificaciones que el cliente pidió
        - OFERTA: Lo que el proveedor ofrece

        ⚠️ REGLA ABSOLUTA DE FIDELIDAD DE CÓDIGOS:
        Los valores que uses en el output (claves y valores del mapping, lista extras)
        deben ser COPIAS LITERALES del campo "id" de cada item tal como aparece en la entrada.
        NUNCA reformatees, normalices, añadas puntos, ceros ni alteres los códigos de ninguna manera.
        Ejemplos de lo que NUNCA debes hacer:
          "13009" → NO lo conviertas en "13.009"
          "101"   → NO lo conviertas en "10.1"
          "03.02" → NO lo conviertas en "3.2"
        Si el código en la entrada es "13::13009", en el output debe aparecer exactamente "13::13009".

        ═══════════════════════════════════════════════════════════════
        🧩 PROTOCOLO DE RAZONAMIENTO (8 PASOS)
        ═══════════════════════════════════════════════════════════════

        PASO 0: NOMBRE IDÉNTICO = EQUIVALENTE DIRECTO
        Si el nombre de la partida de la OFERTA es muy parecido (iguales menos algun carácter que varíe)
        al nombre de una partida de la PAUTA, MAPEAR DIRECTAMENTE sin análisis adicional.
        No se requiere verificar unidad, capítulo ni descripción. Es una coincidencia exacta garantizada.

        PASO 1: ANÁLISIS DE EQUIVALENCIA TÉCNICA
        Busca Material + Función + Ubicación. Sinónimos: "Hormigón Armado" = "H.A."
        Cambios de dimension, marca o detalle tecnico NO invalidan el mapeo si el alcance funcional es el mismo.
        Esas diferencias se dejan para el auditor.

        PASO 2: UNIDAD DE MEDIDA COMO FILTRO
        m² con m² = compatible | m² con ud = revisar alcance antes de marcar EXTRA.

        PASO 3: REGLA N:1 (Múltiple Oferta → 1 Pauta)
        Si múltiples partidas oferta describen componentes de UNA pauta:
        OFERTA: "Adreçat" + "Impermeabilizante" → PAUTA: "Impermeabilización" MAPEAR AMBAS

        PASO 4: REGLA 1:N (1 Pauta → Múltiple Oferta)
        Si UNA pauta se desglosa en varios componentes oferta:
        PAUTA: "Cerrajería completa" → OFERTA: "Cerradura" + "Manilla" + "Bisagras" MAPEAR TODAS

        PASO 5: GESTIÓN DE COSTES INDIRECTOS
        "Limpieza", "Transporte", "Implantación" → Busca en PAUTA (Gastos Generales, Limpieza, etc.)
        Si NO existe → CREAR EXTRA (NO fuerces a primera partida)

        PASO 6: GESTIÓN DE CONFLICTOS
        Si múltiples candidatos igualmente válidos → Elige el MÁS ESPECÍFICO
        Si ambos igual de válidos → Reporta "conflicto" en logica_de_mapeo

        REGLA CLAVE: Si existe una partida de pauta con el MISMO ALCANCE FUNCIONAL,
        debes MAPEAR aunque haya diferencias de dimensiones, marca, espesor, unidad o detalle tecnico.
        Solo marca EXTRA cuando NO exista ninguna partida de pauta razonablemente equivalente.

        ═══════════════════════════════════════════════════════════════
        🚫 DEFINICIÓN ESTRICTA DE EXTRA (Ambas condiciones):
        - No tiene correspondencia funcional en PAUTA
        - Añade funcionalidad NO SOLICITADA (ni explícita ni implícitamente)

        VÁLIDO: "Canal desagüe inox316" (no en pauta, mejora extra)
        NO VÁLIDO: "Masterseal 550" para "Impermeabilización" (es marca, mismo concepto)
        NO VÁLIDO: "Lana roca 80mm" vs pauta "Aislamiento lana roca" (diferencia de espesor)

        ═══════════════════════════════════════════════════════════════
        📤 FORMATO DE SALIDA (JSON PURO, SIN MARKDOWN)
        ═══════════════════════════════════════════════════════════════
        {
            "mapping": {"05::01.01": "03::03.02", "29::29.02.02": "31::102SS"},
            "extras": ["12::12.13", "07::07.05"],
            "logica_de_mapeo": [
                {
                    "id_oferta": "05::01.01",
                    "tipo_match": "1:1" | "N:1" | "1:N" | "COMPONENTE" | "EXTRA",
                    "pauta_id": "03::03.02 o null si EXTRA",
                    "confianza": 0.85,
                    "argumento": "Material + función idéntica. Unidad m² coincide. (máx 50 chars)"
                }
            ]
        }

        CONFIANZA:
        - 0.95-1.0: Certeza alta (especificación idéntica)
        - 0.80-0.95: Confianza media (equivalencia técnica clara)
        - 0.60-0.80: Confianza baja (similitud pero dudas)
        - < 0.70: Solo EXTRA si no hay equivalencia funcional

        ═══════════════════════════════════════════════════════════════
        ⚠️ RESTRICCIONES CRÍTICAS
        ═══════════════════════════════════════════════════════════════
        1. NO MARQUES EXTRA si existe equivalencia funcional clara
        2. NO MAPEES por similitud léxica sola (requiere Función + Alcance)
        3. NO IGNORES UNIDADES (m² ≠ ud, m³ ≠ kg) sin evaluar alcance
        4. NO DUPLICES (cada ID_OFERTA → máximo 1 ID_PAUTA primary)
        5. NO INVENTES PAUTA (EXTRA si no existe match)
        6. SIGUE EL ORDEN: Equivalencia → Desgloses → Componentes → Costes indirectos → EXTRA
        7. COPIA LOS CÓDIGOS LITERALMENTE: copia el campo "id" exactamente como aparece en los datos
           de entrada. NO añadas puntos, NO elimines ceros, NO reformatees. "13009" es "13009",
           no "13.009". "101" es "101", no "10.1". Cualquier alteración de un código produce
           un error crítico que rompe el pipeline.

        NOTA SOBRE LOGICA_DE_MAPEO:
        Si recibe instrucción "skip_logica_de_mapeo": true en los datos de entrada,
        devuelve SOLO "mapping" y "extras" (omite logica_de_mapeo para ahorrar tokens).
        """
        
        # Determinar si enviar logica_de_mapeo
        skip_logica = True

        user_payload: dict = {
            "LISTA_PAUTA_COMPLETA": pauta_full,
            "LISTA_OFERTA_PARCIAL": offer_batch,
            "skip_logica_de_mapeo": skip_logica,
        }

        user_content = json.dumps(user_payload, ensure_ascii=False)

        try:
            # Llamada API
            response = self.model.generate_content([system_prompt, user_content])
            
            # Logging de tokens para monitoreo
            if hasattr(response, 'usage_metadata') and response.usage_metadata:
                prompt_tokens = int(getattr(response.usage_metadata, "prompt_token_count", 0) or 0)
                output_tokens = int(getattr(response.usage_metadata, "candidates_token_count", 0) or 0)
                total_tokens = prompt_tokens + output_tokens
                print(f"    📊 Lote {batch_id} - Tokens: {prompt_tokens} INPUT + {output_tokens} OUTPUT = {total_tokens} TOTAL")
                if total_tokens > 15000:
                    print(f"    ⚠️  LÍMITE ALTO: Considera reducir BATCH_SIZE si esto se repite")
                record_usage(self.model_name, prompt_tokens, output_tokens)
            clean_text = self._clean_json_string(response.text)
            return json.loads(clean_text)

        except Exception as e:
            # BACKOFF EXPONENCIAL para Rate Limiting
            error_str = str(e)
            if "429" in error_str or "quota" in error_str.lower():
                # Extraer segundos a esperar del error si está disponible
                import re as re_module
                wait_match = re_module.search(r'Please retry in (\d+)', error_str)
                wait_seconds = int(wait_match.group(1)) if wait_match else 60
                
                print(f"    🔴 RATE LIMIT DETECTADO en Lote {batch_id}")
                print(f"    ⏳ Esperando {wait_seconds} segundos antes de reintentar...")
                print(f"    💡 Tip: Considera usar una API key de pago para evitar limits")
                
                time.sleep(wait_seconds)
                print(f"    🔄 Reintentando Lote {batch_id}...")
                
                try:
                    response = self.model.generate_content([system_prompt, user_content])
                    if hasattr(response, "usage_metadata") and response.usage_metadata:
                        record_usage(
                            self.model_name,
                            int(getattr(response.usage_metadata, "prompt_token_count", 0) or 0),
                            int(getattr(response.usage_metadata, "candidates_token_count", 0) or 0),
                        )
                    clean_text = self._clean_json_string(response.text)
                    return json.loads(clean_text)
                except Exception as retry_error:
                    print(f"    ❌ Reintento falló: {retry_error}")
            else:
                print(f"    ❌ Error en Lote {batch_id}: {e}")
            
            return {"mapping": {}, "extras": [], "logica_de_mapeo": []}

    def _clean_json_string(self, text):
        """Limpia la respuesta del LLM."""
        text = text.strip().replace("```json", "").replace("```", "")
        text = re.sub(r'//.*', '', text) 
        text = re.sub(r',(\s*[\}\]])', r'\1', text) 
        return text

    def _flatten_json(self, data, source):
        """Aplana creando IDs compuestos únicos: CAPITULO::CODIGO"""
        flat_list = []
        for cap in data:
            cap_code = str(cap.get('capitulo_codigo', '')).strip()
            cap_name = cap.get('capitulo_nombre', '')
            
            for item in cap.get("partidas", []):
                raw_code = str(item.get("codigo") or item.get("codigo_pauta")).strip()
                unique_id = f"{cap_code}::{raw_code}"
                
                # Aplanar descripción para facilitar búsqueda semántica
                flat_item = {
                    "id": unique_id, 
                    "cod_vis": raw_code,
                    "desc": f"[{cap_code} {cap_name}] " + (item.get("descripcion") or item.get("nombre", "")),
                    "precio": item.get("precio", 0)
                }
                
                if len(flat_item["desc"]) > 350:
                    flat_item["desc"] = flat_item["desc"][:350]
                
                flat_list.append(flat_item)
                
        return flat_list
