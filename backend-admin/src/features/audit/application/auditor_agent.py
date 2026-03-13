import json
import time
import google.generativeai as genai
import math
import os
import concurrent.futures
from src.features.audit.application.audit_utils import load_audit_data, prepare_comparison_queue
from src.shared.observability.cost_tracker import record_usage

class AuditorAgent:
    def __init__(self, api_key):
        genai.configure(api_key=api_key)
        # gemini-2.5-flash: optimal for this highly-structured classification task.
        # The 7-type incidence rubric is explicit enough that Flash follows it
        # reliably at temperature 0.0.  max_output_tokens guards against silent
        # truncation when batch_size=20 generates ~200 tokens/item (~4 000 total).
        self.model = genai.GenerativeModel(
            model_name="gemini-2.5-flash",
            generation_config={
                "response_mime_type": "application/json",
                "temperature": 0.0,
                # gemini-2.5-flash supports up to 65 536 output tokens.
                # With full pauta descriptions in the input and ~400-600 output
                # tokens per item, a batch of 20 items needs ~8 000-12 000 tokens.
                # The old 8 192 cap was below that ceiling, causing every batch to
                # hit MAX_TOKENS and recurse down to size-1 — still failing.
                # 65 536 gives ~100-160 items of headroom; the auto-split mechanism
                # in _execute_batch_with_split remains as a safety net.
                "max_output_tokens": 65536,
            }
        )

    # Retry config for _generate_audit_batch
    _BATCH_MAX_RETRIES = 3
    _BATCH_BASE_DELAY  = 8   # seconds; doubled each attempt for transient errors

    def _generate_audit_batch(self, batch_items):
        """
        Envía un lote de partidas al LLM con el rol de Auditor Técnico Cualitativo.

        Returns:
            list  — audit results for this batch (zero or more items)
            None  — MAX_TOKENS hit; caller must split the batch and retry halves
        """
        # Convertimos a string asegurando que caracteres especiales se mantengan
        batch_str = json.dumps(batch_items, indent=2, ensure_ascii=False)

        system_prompt = """
        ACTÚA COMO: Un Auditor Forense de Construcción especializado en Comparativas Técnicas (Qualitative Surveyor).
        
        DIRECTRIZ SUPREMA: IGNORA TOTALMENTE LOS PRECIOS, IMPORTES Y CANTIDADES (MEDICIONES).
        No quiero saber si es más caro o barato, ni si faltan metros.
        Tu ÚNICO objetivo es comparar la DESCRIPCIÓN TÉCNICA (Semántica) de la Pauta (REF) vs la Oferta (FOUND).

        CONTEXTO:
        El cliente quiere saber si le están ofertando EXACTAMENTE lo que pidió a nivel de materiales, ejecución y alcance.
        
        INPUT:
        Recibes una lista: {"capitulo": "...", "ref": {..DESCRIPCIÓN_PAUTA..}, "found": {..DESCRIPCIÓN_OFERTA..} }
        NOTA: 
        - Si 'found' es null: El proveedor no lo ha ofertado.
        - Si 'ref' es null: El proveedor ha añadido algo que no estaba en el proyecto (Extra).
        - Si 'found' contiene [COMPONENTES DESGLOSADOS]: Son los elementos que componen la partida (equipos, materiales, mano de obra).
          Usa esta info para verificar si el desglose cumple con lo solicitado en 'ref'.

        TU MISIÓN ES DETECTAR DIFERENCIAS EN EL TEXTO ("LA LETRA PEQUEÑA"):
        Analiza palabra por palabra las descripciones para encontrar estas incidencias:

        1. OMISION (No ofertado):
           - Si "ref" existe pero "found" es null o vacío -> OMISION.

        2. CAMBIO_ESPECIFICACION (Calidad Inferior / Diferente):
           - La Pauta pide "Acero Inoxidable" y Ofertan "Acero Galvanizado".
           - La Pauta pide "Espesor 100mm" y Ofertan "80mm".
           - La Pauta pide "Madera Roble" y Ofertan "Melamina imitación".
           - La Pauta pide "Doble vidrio bajo emisivo" y Ofertan "Vidrio simple".
           - També inclou l'especificació de marques (ej: Hikvision) o models quan la pauta és genèrica, o l'addició de prestacions tècniques no demanades.

        3. ALCANCE_REDUCIDO (Falta de trabajos):
           - La Pauta dice "Suministro e Instalación" y la Oferta dice "Solo Suministro".
           - La Pauta incluye "Ayudas de albañilería" y la Oferta no (o las excluye).
           - La Pauta incluye "Remates y sellados" y la Oferta es genérica.
           - Indefinició per asimetria: si la oferta és molt més complexa que la pauta, podria estar amagant serveis no demanats que el client haurà de pagar.
           - OJO: Si la oferta inclou serveis addicionals (instal·lació, programació) que la pauta no esmenta, marca-ho com a incidència per "excés de serveis no sol·licitats".

        4. MARCA_NO_RESPETADA (Cambio de fabricante):
           - La Pauta especifica marca (ej: "Roca", "Cortizo", "Pladur", "Schindler") y la oferta es genérica ("Simil", "Marca blanca" o no menciona marca).

        5. EXCLUSION_EXPLICITA (La trampa legal):
           - Busca activamente frases en la oferta como: "No se incluye...", "Excluido...", "A cargo del cliente...", "Sin medios auxiliares".

        6. PARTIDA_EXTRA (No solicitada / Mejora):
           - Si "ref" es null y "found" tiene datos -> PARTIDA_EXTRA.
           - Son partidas que el proveedor incluye pero no estaban en la Pauta original.

        7. COMPONENTE_INADECUADO (Desglose con problemas):
           - Si [COMPONENTES DESGLOSADOS] existe, verifica que cada elemento cumple con la pauta.
           - Ej: La pauta pide "Grabador 8 cámaras" y el componente dice "Grabador 4 cámaras".
           - Ej: La pauta pide "Cámara IP 4K" y el componente dice "Cámara analógica HD".

        OUTPUT OBLIGATORIO (JSON):
        Devuelve una lista de objetos:
        [
            {
                "codigo_pauta": "string", // Si 'ref' existe, usa su código. Si 'ref' es null (Extra), pon "EXTRA".
                "codigo_oferta": ["string"] | [], // Lista de códigos de la oferta (found). [] si es OMISION.
                "tipo_incidencia": "CORRECTO" | "OMISION" | "CAMBIO_ESPECIFICACION" | "ALCANCE_REDUCIDO" | "MARCA_NO_RESPETADA" | "EXCLUSION_DETECTADA" | "PARTIDA_EXTRA" | "COMPONENTE_INADECUADO",
                "gravedad": "ALTA" (si afecta funcionalidad/calidad/omisión) | "MEDIA" (marca/estética) | "BAJA" (detalles menores o extras informativos),
                "comentario_tecnico": "string", // EXPLICACIÓN CRÍTICA: Cita textualmente la diferencia. Si es EXTRA, indica 'Partida incluida en oferta no presente en proyecto'. Si es COMPONENTE_INADECUADO, indica qué componente falla.
                "texto_oferta": null,  // siempre null — el sistema lo inyecta en post-proceso
                "confidence": float // 0.0 a 1.0
            }
        ]

        INSTRUCCIONES PARA 'codigo_oferta':
            - FORMATO OBLIGATORIO: "CAP::COD" donde CAP es el número de capítulo SIEMPRE con 2 dígitos (zero-padded) y COD es el código exacto de la partida tal como aparece en la oferta.
            - EJEMPLOS CORRECTOS:  partida "07.2"  en capítulo 7  → "07::07.2"
                                   partida "06.9"  en capítulo 6  → "06::06.9"
                                   partida "10.3" en capítulo 10 → "10::10.3"
            - EJEMPLOS INCORRECTOS (NUNCA HAGAS ESTO): "07.2" (sin CAP::), "07::07.2" (CAP sin zero-pad), "07::07.2" (COD alterado).
            - Si el proveedor ha cambiado la numeración (ej: Pauta 01.01 → Oferta 02.05), pon "02::02.05" (capítulo donde vive la partida en la oferta).
            - Si es una OMISIÓN (found is null), devuelve una lista vacía [].
            - Nunca omitas el prefijo CAP:: aunque la partida sea del mismo capítulo que la pauta.

        INSTRUCCIÓN PARA 'texto_oferta':
            - Devuelve SIEMPRE null. El sistema inyecta el texto automáticamente en post-proceso.

        INSTRUCCIÓ CRÍTICA PER A 'comentario_tecnico' — FORMAT OBLIGATORI:
        El camp ha de ser MÉS CURT POSSIBLE. Màxim 1-2 línies. Sense rodeos.
        Usa el format corresponent al tipus d'incidència:

        OMISION / ALCANCE_REDUCIDO  →  No inclou: "[element concret que manca]"
        PARTIDA_EXTRA               →  Afegeix: "[descripció breu de la partida extra]"
        CAMBIO_ESPECIFICACION       →  Modificació: afegeix "[marca/servei/component]" no sol·licitat a la pauta.
        COMPONENTE_INADECUADO       →  Modificació: "[component trobat]" per "[component requerit]"
        MARCA_NO_RESPETADA          →  Modificació: marca "[trobada/genèrica]" per "[marca requerida]"
        EXCLUSION_DETECTADA         →  Exclou: "[frase textual de l'exclusió]"
        CORRECTO                    →  "Correcte"

        MAI no escriguis frases llargues com "La pauta especifica X mentre que l'oferta indica Y...".
        Cita ÚNICAMENT les paraules clau del document original entre cometes.

        INSTRUCCIÓ PARA 'texto_oferta':
            - Devuelve SIEMPRE null. El sistema inyecta el texto automáticamente en post-proceso.

        NOTAS PARA EL ANÁLISIS:
        - Si la descripción de la oferta es muy breve o vaga ("Partida alzada a justificar") vs una descripción muy detallada en pauta -> Márcalo como ALCANCE_REDUCIDO (Indefinición).
        - El detall excessiu és una alerta: Si la pauta diu "Porta automàtica" i l'oferta detalla marca, cables, suports i "posta en marxa", NO és CORRECTE. És CAMBIO_ESPECIFICACION o ALCANCE_REDUCIDO per falta de coherència. Només accepta com a sinònims abreviatures tècniques (H.A. = Hormigón Armado).

        EQUIVALÈNCIES D'UNITATS — TRACTA COM A SINÒNIMS (no marquis incidència per diferència d'unitats equivalents):
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
        Escriu el camp 'comentario_tecnico' en CATALÀ.
        Cita les frases clau del document en l'idioma original (castellà, català o altres),
        però l'explicació i el raonament han d'estar sempre en CATALÀ.
        """

        user_content = f"ANALIZA TÉCNICAMENTE ESTE LOTE DE {len(batch_items)} PARTIDAS:\n{batch_str}"

        last_exc = None
        for attempt in range(1, self._BATCH_MAX_RETRIES + 1):
            try:
                response = self.model.generate_content([system_prompt, user_content])

                # ── Track API cost ──────────────────────────────────────────
                if hasattr(response, "usage_metadata") and response.usage_metadata:
                    record_usage(
                        "gemini-2.5-flash",
                        response.usage_metadata.prompt_token_count  or 0,
                        response.usage_metadata.candidates_token_count or 0,
                    )

                # ── MAX_TOKENS guard ─────────────────────────────────────────
                # If the output was truncated mid-JSON (finish_reason == 2) the
                # json.loads below would raise an "Unterminated string" error.
                # Detect it here and return None so run_full_audit can split
                # the batch in half and retry each half independently — exactly
                # the same pattern used by the extractor auto-split logic.
                candidates = getattr(response, "candidates", None)
                if candidates:
                    finish_reason_code = getattr(candidates[0], "finish_reason", None)
                    if int(finish_reason_code or 0) == 2:  # MAX_TOKENS
                        print(
                            f"⚠️ Lote auditor — MAX_TOKENS: batch of {len(batch_items)} items "
                            f"exceeds output limit. Returning None to trigger auto-split."
                        )
                        return None  # caller must split

                # Limpieza básica por si el modelo devuelve markdown
                clean_text = response.text.strip().replace("```json", "").replace("```", "")
                return json.loads(clean_text)
            except json.JSONDecodeError as e:
                last_exc = e
                print(f"⚠️ Lote auditor — JSON inválido (attempt {attempt}/{self._BATCH_MAX_RETRIES}): {e}")
            except Exception as e:
                last_exc = e
                is_timeout = "DeadlineExceeded" in type(e).__name__ or "504" in str(e) or "timed out" in str(e).lower()
                is_rate    = "429" in str(e) or "quota" in str(e).lower() or "RESOURCE_EXHAUSTED" in str(e)
                delay = self._BATCH_BASE_DELAY * (2 ** (attempt - 1))  # 8 s, 16 s, 32 s
                if is_rate:
                    # Honour the Retry-After hint when present
                    import re as _re
                    m = _re.search(r"retry.*?(\d+)", str(e), _re.IGNORECASE)
                    delay = int(m.group(1)) if m else max(delay, 60)
                print(
                    f"⚠️ Lote auditor — {type(e).__name__} "
                    f"(attempt {attempt}/{self._BATCH_MAX_RETRIES}): {e}"
                    + (f" — esperando {delay}s antes de reintentar..." if attempt < self._BATCH_MAX_RETRIES else "")
                )
                if attempt < self._BATCH_MAX_RETRIES:
                    time.sleep(delay)

        print(f"❌ Lote auditor: {self._BATCH_MAX_RETRIES} intentos fallidos. Último error: {last_exc}. Lote omitido.")
        return []

    def run_full_audit(self, pauta_path, extracted_path, batch_size=20, max_concurrency=16):
        """
        Orquesta la auditoría cualitativa completa en PARALELO.
        
        FLUJO OPTIMIZADO:
        1. Intenta leer audit_qualitative_input.json que fue generado por consolidator
        2. Si no existe, lo genera como fallback (para compatibilidad con workflows antiguos)
        
        Args:
            max_concurrency (int): Llamadas Flash simultáneas (default 16).
              gemini-2.5-flash permite 2000 RPM; 16 workers × 3 PDFs = 48 conc.
              A 5-15 s/req → 192-576 RPM, muy por debajo del límite.
        """
        # --- RUTA DEL ARCHIVO PRECOMPILADO ---
        supplier_folder = os.path.dirname(extracted_path)
        validation_path = os.path.join(supplier_folder, "audit_qualitative_input.json")

        comparison_queue = None
        
        # 1. INTENTAR CARGAR el archivo ya generado por consolidator
        if os.path.exists(validation_path):
            try:
                with open(validation_path, 'r', encoding='utf-8') as f:
                    comparison_queue = json.load(f)
                print(f"    ✅ Usando audit_qualitative_input.json precompilado ({len(comparison_queue)} items).")
            except Exception as e:
                print(f"    ⚠️  Error cargando audit_qualitative_input precompilado: {e}")
                comparison_queue = None
        
        # 2. FALLBACK: Regenerar si no existe o si falló la carga
        if comparison_queue is None:
            print(f"    🔄 Regenerando cola de comparación (fallback)...")
            pauta_data, extracted_data = load_audit_data(pauta_path, extracted_path)
            if pauta_data is None:
                print("❌ Error: No se encuentran los archivos para auditar.")
                return []

            comparison_queue = prepare_comparison_queue(
                pauta_data, 
                extracted_data, 
                save_path=validation_path
            )

        total_items = len(comparison_queue)
        
        # 3. PREPARAR TODOS LOS LOTES DE ANTEMANO
        all_batches = []
        for i in range(0, total_items, batch_size):
            batch_id = (i // batch_size) + 1
            all_batches.append((batch_id, comparison_queue[i : i + batch_size]))
            
        total_batches = len(all_batches)
        print(f"\n⚡ AUDITORÍA CUALITATIVA PARALELA: {total_items} partidas en {total_batches} lotes.")
        print(f"🚀 Ejecutando con {max_concurrency} hilos simultáneos (Workers)...")

        final_report = []
        completed_count = 0

        def _execute_batch_with_split(batch_id_: str, items: list) -> list:
            """
            Calls _generate_audit_batch; on MAX_TOKENS (returns None) splits the
            item list in half and recurses on each half independently.  Recursion
            bottoms out at size-1 batches — a single item can never exceed the
            output limit.  No item is ever dropped or duplicated:
              - The split is a clean [0:mid] / [mid:] slice (no overlap).
              - Results from both halves are concatenated before returning.
            """
            result = self._generate_audit_batch(items)
            if result is not None:
                return result  # success (may be empty list on repeated hard errors)

            # MAX_TOKENS: split and recurse
            if len(items) == 1:
                # Single-item batch still hits the limit — extremely unlikely but
                # guard against infinite recursion.
                ref = items[0].get("ref") or {}
                codigo = ref.get("codigo", "?") if isinstance(ref, dict) else "?"
                print(
                    f"    ⚠️  Lote {batch_id_}: MAX_TOKENS en item individual "
                    f"(codigo={codigo}) — omitido."
                )
                return []

            mid = len(items) // 2
            left, right = items[:mid], items[mid:]
            print(
                f"    ✂️  Lote {batch_id_}: MAX_TOKENS — dividiendo en "
                f"{len(left)} + {len(right)} items y reintentando..."
            )
            merged: list = []
            merged.extend(_execute_batch_with_split(f"{batch_id_}a", left))
            merged.extend(_execute_batch_with_split(f"{batch_id_}b", right))
            return merged

        # 4. EJECUCIÓN EN PARALELO CON as_completed (feedback en tiempo real)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrency) as executor:
            future_to_batch = {
                executor.submit(_execute_batch_with_split, str(batch_id), batch_data): batch_id
                for batch_id, batch_data in all_batches
            }
            
            for future in concurrent.futures.as_completed(future_to_batch):
                batch_id = future_to_batch[future]
                completed_count += 1
                try:
                    batch_result = future.result(timeout=180)  # 3 min timeout per batch
                    final_report.extend(batch_result)
                    print(f"    ✅ Lote {batch_id}/{total_batches} completado ({completed_count}/{total_batches})")
                except Exception as e:
                    print(f"    ❌ Lote {batch_id} falló: {e}")

        print(f"\n🏁 Auditoría finalizada. Total incidencias analizadas: {len(final_report)}")
        return final_report

