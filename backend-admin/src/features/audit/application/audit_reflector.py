import json
import google.generativeai as genai
import time
import os
import concurrent.futures
from src.shared.observability.cost_tracker import record_usage

class AuditReflector:
    def __init__(self, api_key):
        genai.configure(api_key=api_key)
        # Cost/quality tradeoff:
        # - default: flash (lower cost)
        # - override: set REFLECTOR_MODEL=gemini-2.5-pro for maximum depth
        self.model_name = (
            os.getenv("REFLECTOR_MODEL", "gemini-2.5-flash").strip()
            or "gemini-2.5-flash"
        )
        self.model = genai.GenerativeModel(
            model_name=self.model_name,
            generation_config={
                "response_mime_type": "application/json",
                "temperature": 0.0,
                "max_output_tokens": 16384,  # verdicts for up to 12 items/batch
            }
        )

    def _upload_pdf_to_gemini(self, pdf_path):
        print(f"📤 Subiendo PDF al Juez (Gemini): {os.path.basename(pdf_path)}...")
        pdf_file = genai.upload_file(pdf_path, mime_type="application/pdf")
        
        while pdf_file.state.name == "PROCESSING":
            print(".", end="", flush=True)
            time.sleep(1)
            pdf_file = genai.get_file(pdf_file.name)
        
        print(f" ✅ PDF Listo.")
        return pdf_file

    def _review_batch(self, batch_items, pdf_file):
        """
        Procesa un lote verificando incidencias contra el PDF original.
        """
        system_prompt = """
        ACTÚA COMO: Juez Árbitro de Licitaciones (Senior Quantity Surveyor).
        
        CONTEXTO: Un auditor previo analizó un presupuesto y detectó posibles incidencias.
        TÚ TIENES ACCESO AL PDF ORIGINAL DE LA OFERTA y debes verificar si las acusaciones son correctas.
        
        IMPORTANTE: El auditor trabajó con un JSON extraído que puede tener errores de extracción.
        Tu ventaja es que puedes buscar DIRECTAMENTE en el PDF original.

        INPUT QUE RECIBES PARA CADA INCIDENCIA:
        - "codigo_pauta": ID de la partida de referencia (lo que pidió el cliente)
        - "descripcion_original_pauta": Texto completo de lo que PEDÍA el cliente
        - "texto_oferta": Lo que el auditor VIO en la oferta (puede estar incompleto)
        - "tipo_incidencia": El tipo de problema detectado
        - "comentario_tecnico": Explicación del auditor de por qué es incidencia
        
        TU MISIÓN:
        1. Lee la acusación del auditor
        2. BUSCA EN EL PDF ORIGINAL esa partida o concepto
        3. Compara lo que encuentras en el PDF vs lo que pedía la pauta
        4. Decide si el auditor tenía razón o se equivocó

        CASOS ESPECÍFICOS:

        OMISION (Auditor dice que falta algo):
        → Busca exhaustivamente en TODO el PDF (incluye anexos, cuadros de precios, otras secciones)
        → Si encuentras el concepto aunque tenga otro código o esté agrupado: FALSO_POSITIVO
        → Si tras buscar no está: CONFIRMADO

        CAMBIO_ESPECIFICACION / ALCANCE_REDUCIDO:
        → Busca la partida en el PDF y lee la descripción COMPLETA
        → Si el PDF tiene info adicional que el extractor no capturó: FALSO_POSITIVO
        → Si el PDF confirma que falta especificación: CONFIRMADO

        PARTIDA_EXTRA:
        → Busca si esa "extra" realmente corresponde a algo de la pauta mal mapeado
        → Si encuentras correspondencia: FALSO_POSITIVO (era mapeo incorrecto)
        → Si es genuinamente nueva: CONFIRMADO

        CORRECTO (El auditor dijo que está bien):
        → Verifica en el PDF que realmente cumple con la pauta
        → Si está bien: veredicto "CONFIRMADO" (confirmas que es correcto)
        → Si encuentras problemas: veredicto "INCIDENCIA_DETECTADA" + indica tipo_incidencia_nuevo

        OUTPUT (JSON sin comentarios):
        [
            {
                "codigo_pauta": "string",
                "veredicto": "CONFIRMADO" | "FALSO_POSITIVO" | "INCIDENCIA_DETECTADA",
                "tipo_incidencia_nuevo": "OMISION" | "CAMBIO_ESPECIFICACION" | "ALCANCE_REDUCIDO" | null,
                "correccion": {
                    "codigo_encontrado_pdf": "string",
                    "pagina_pdf": "string",
                    "explicacion": "string"
                }
            }
        ]
        
        NOTAS:
        - "tipo_incidencia_nuevo" solo se usa cuando veredicto es "INCIDENCIA_DETECTADA"
        - FALSO_POSITIVO = el auditor se equivocó, no hay incidencia
        - CONFIRMADO = el auditor tenía razón (incidencia real O correcto confirmado)
        - INCIDENCIA_DETECTADA = el auditor dijo CORRECTO pero tú encontraste problemas
        
        REGLA DE ORO: Busca activamente DEFENDER al proveedor si hay evidencia en el PDF.
        Solo confirma incidencia si tras búsqueda exhaustiva no hay justificación.

        EQUIVALÈNCIES D'UNITATS — TRACTA COM A SINÒNIMS (no confirmis incidència per diferència d'unitats equivalents):
        - ud = u = ut = un = und = unitat = unidad (unitat discreta, peça)
        - ml = m.l. = m.lin. = ML = metre lineal / metro lineal
        - m2 = m² = M2 = m.q. = metre quadrat / metro cuadrado
        - m3 = m³ = M3 = m.c. = metre cúbic / metro cúbico
        - m = metre lineal quan no s'indica altra dimensió
        - pa = PA = p.a. = P.A. = partida alçada / partida alzada (import global)
        - h = hr = h/dia = hora
        - kg = Kg = KG = quilogram / kilogramo
        - t = Tn = TN = tona / tonelada
        - l = lt = lts = L = litre / litro
        - joc = jto = juego = conjunt complet

        IDIOMA DE SORTIDA:
        Escriu el camp 'correccion.explicacion' en CATALÀ, MOLT CONCÍS (màx. 1-2 línies).
        Usa el format corresponent al veredicte:
        - FALSO_POSITIVO:        "Correcte: [raó breu de per què no és incidència]"
        - CONFIRMADO (incid.):   conserva el comentari de l'auditor tal com és (ja concís)
        - INCIDENCIA_DETECTADA:  usa el format:
            OMISION/ALCANCE_REDUCIDO  →  No inclou: "[element concret]"
            CAMBIO_ESPECIFICACION     →  Modificació: "[trobat]" per "[requerit]"
        Cita les frases textuals del PDF en l'idioma original del document.
        NO menciones cap agent anterior, auditoria prèvia ni procés intern.
        Sense rodeos, directe al gra.
        """
        # ------------------------------------------

        data_str = json.dumps(batch_items, indent=2, ensure_ascii=False)
        
        try:
            # Llamada al modelo con el prompt y los datos del lote
            response = self.model.generate_content([system_prompt, data_str, pdf_file])
            # ── Track API cost ──────────────────────────────────────────────
            if hasattr(response, "usage_metadata") and response.usage_metadata:
                record_usage(
                    self.model_name,
                    int(getattr(response.usage_metadata, "prompt_token_count", 0) or 0),
                    int(getattr(response.usage_metadata, "candidates_token_count", 0) or 0),
                )
            clean_text = response.text.strip().replace("```json", "").replace("```", "")
            return json.loads(clean_text)
        except Exception as e:
            print(f"⚠️ Error en lote del Juez: {e}")
            return []

    def review_audit(self, enriched_audit_data, pdf_path, debug_folder_path=None, max_concurrency=5):
        """
        Ejecuta la revisión del Juez en PARALELO con feedback en tiempo real.
        Revisa TODAS las partidas (incidencias y correctos) para validar el trabajo del auditor.

        max_concurrency (int): workers simultáneos del modelo configurado
          (REFLECTOR_MODEL). Default 5.
        """
        # 1. Revisamos TODAS las partidas, no solo incidencias
        items_to_review = enriched_audit_data

        if not items_to_review:
            print("✨ El Juez no tiene casos que revisar.")
            return enriched_audit_data

        # 2. Subimos el PDF una sola vez (Esto no se puede paralelizar, es necesario para todos)
        pdf_file = self._upload_pdf_to_gemini(pdf_path)
        
        # 3. Preparar carpeta de debug
        if debug_folder_path:
            os.makedirs(debug_folder_path, exist_ok=True)
            print(f" 📂 Guardando batches parciales en: {debug_folder_path}")

        # 4. PREPARACIÓN DE LOTES (Pre-calculamos todos los batches)
        BATCH_SIZE = 12
        total_items = len(items_to_review)
        all_batches = []
        
        # Creamos la lista de listas (lotes)
        for i in range(0, total_items, BATCH_SIZE):
            batch_id = (i // BATCH_SIZE) + 1
            all_batches.append((batch_id, items_to_review[i : i + BATCH_SIZE]))
            
        total_batches = len(all_batches)
        all_verdicts = []
        completed_count = 0

        print(f"⚖️  El Juez revisará {total_items} conflictos en {total_batches} lotes (Paralelo: {max_concurrency} hilos)...")

        # 5. EJECUCIÓN PARALELA CON as_completed (feedback en tiempo real)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrency) as executor:
            future_to_batch = {
                executor.submit(self._review_batch, batch_data, pdf_file): batch_id
                for batch_id, batch_data in all_batches
            }
            
            for future in concurrent.futures.as_completed(future_to_batch):
                batch_id = future_to_batch[future]
                completed_count += 1
                
                try:
                    batch_verdicts = future.result(timeout=180)  # 3 min timeout
                    print(f"    ✅ Lote {batch_id}/{total_batches} juzgado ({completed_count}/{total_batches})")

                    # --- GUARDADO PARCIAL ---
                    if debug_folder_path and batch_verdicts:
                        batch_filename = f"batch_{batch_id:03d}.json"
                        batch_path = os.path.join(debug_folder_path, batch_filename)
                        try:
                            with open(batch_path, 'w', encoding='utf-8') as f:
                                json.dump(batch_verdicts, f, indent=4, ensure_ascii=False)
                        except Exception as e:
                            print(f"⚠️ No se pudo guardar el batch {batch_id}: {e}")
                    
                    all_verdicts.extend(batch_verdicts)
                    
                except Exception as e:
                    print(f"    ❌ Lote {batch_id} falló: {e}")

        # 6. Aplicar resultados
        return self._apply_verdicts(enriched_audit_data, all_verdicts)

    def _apply_verdicts(self, audit_data, verdicts):
        verdict_map = {v["codigo_pauta"]: v for v in verdicts}
        
        for item in audit_data:
            verdict = verdict_map.get(item["codigo_pauta"])
            if verdict:
                correccion = verdict.get("correccion", {})
                found_code = correccion.get("codigo_encontrado_pdf", "")
                pagina = correccion.get("pagina_pdf", "")
                reason = correccion.get("explicacion", "")
                ubicacion = f" ({pagina})" if pagina else ""
                
                if verdict["veredicto"] == "FALSO_POSITIVO":
                    # El auditor se equivocó - no hay incidencia
                    item["tipo_incidencia"] = "CORRECTO"
                    item["gravedad"] = "BAJA"
                    item["comentario_tecnico"] = "Correcte"
                    if found_code:
                        item["codigo_oferta"] = [found_code]

                elif verdict["veredicto"] == "INCIDENCIA_DETECTADA":
                    # El auditor dijo CORRECTO pero el Juez encontró problemas;
                    # mostrar la discrepància tècnica directament, sense mencionar l'auditor
                    nuevo_tipo = verdict.get("tipo_incidencia_nuevo", "CAMBIO_ESPECIFICACION")
                    item["tipo_incidencia"] = nuevo_tipo
                    item["gravedad"] = "ALTA"
                    item["comentario_tecnico"] = reason  # ja en català per instrucció del prompt

                elif verdict["veredicto"] == "CONFIRMADO":
                    # El auditor tenía razón (sea incidencia o correcto)
                    if item.get("tipo_incidencia") == "CORRECTO":
                        item["comentario_tecnico"] = "Correcte"
                    else:
                        # Mantenir el comentari tècnic de l'auditor (ja en català)
                        item["comentario_tecnico"] = item.get("comentario_tecnico", reason)
                    
        return audit_data
