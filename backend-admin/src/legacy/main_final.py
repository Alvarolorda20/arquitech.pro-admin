import os
import json
from dotenv import load_dotenv
import fitz  # PyMuPDF
from concurrent.futures import ThreadPoolExecutor, as_completed
import google.generativeai as genai
import time
import shutil  # UNUSED - kept for compatibility
def upload_pdf_to_gemini(pdf_path, mime_type="application/pdf"):
    """Sube el PDF a Gemini File API y espera a que esté activo."""
    print(f"    ☁️  Subiendo PDF a Gemini: {pdf_path}...")
    file_ref = genai.upload_file(pdf_path, mime_type=mime_type)
    
    # Esperar a que el archivo sea procesado
    import time
    for _ in range(30):
        # Actualizamos el estado consultando de nuevo
        file_ref = genai.get_file(file_ref.name)
        
        # CORRECCIÓN AQUÍ: Usamos .state.name en lugar de .get()
        if file_ref.state.name == "ACTIVE":
            print(f"    ✅ PDF Listo en nube: {file_ref.name}")
            return file_ref
        
        if file_ref.state.name == "FAILED":
            raise Exception("El procesamiento del archivo falló en Google Cloud.")
            
        print("    ⏳ Esperando procesamiento...")
        time.sleep(2)
        
    raise RuntimeError(f"Timeout esperando que el archivo esté activo: {pdf_path}")

# --- AGENTES ---
from agents.planner_agent import PlannerAgent
from agents.extractor_agent import ExtractorAgent
from agents.mapper_agent import MapperAgent
from agents.extra_review_agent import ExtraReviewAgent
from agents.auditor_agent import AuditorAgent
from agents.reviewer_agent import AuditReflector 

# --- UTILS ---
from utils.consolidator import consolidate_chunks
from utils.consolidator import apply_mapping_to_json
from utils.consolidator import generate_audit_qualitative_input
from utils.pauta_extractor import map_excel_to_json
from utils.generar_excel_comparativo import GeneradorExcelComparativo
from utils.generar_comparativo_final import generar_comparativo_final
from utils.sanitizer import sanitizar_asignaciones_post_auditoria


load_dotenv()
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# --- FUNCIONES AUXILIARES ---

def extract_text_from_pdf(pdf_path):
    """Extrae todo el texto del PDF para planificación."""
    doc = fitz.open(pdf_path)
    return "".join([f"--- PÁGINA {p.number + 1} ---\n{p.get_text()}" for p in doc])

def load_json(path):
    """Carga un JSON de forma segura."""
    if not os.path.exists(path): return None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError:
        return None 

def enrich_audit_data(audit_results, context_input_path, save_path=None):
    """
    Une el resultado de la auditoría con la descripción original.
    Mantiene separada la lógica de Pauta vs Oferta.
    """
    context_input = load_json(context_input_path)
    if not context_input: return audit_results

    # 1. Crear mapa rápido
    context_map = {}
    for item in context_input:
        ref_obj = item.get("ref")
        if ref_obj and "codigo" in ref_obj:
            context_map[ref_obj["codigo"]] = ref_obj.get("desc", "")

    # 2. Inyectar datos
    enriched_data = []
    for finding in audit_results:
        code = finding.get("codigo_pauta")
        
        # Añadir descripción de la pauta (null si es EXTRA)
        if code == "EXTRA":
             finding["descripcion_original_pauta"] = None 
        else:
             finding["descripcion_original_pauta"] = context_map.get(code, "Descripción no disponible.")
             
        enriched_data.append(finding)

    # 3. Guardar
    if save_path:
        try:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            with open(save_path, 'w', encoding='utf-8') as f:
                json.dump(enriched_data, f, indent=4, ensure_ascii=False)
        except Exception: pass
        
    return enriched_data

# --- MAIN ---

def main():
    if not GOOGLE_API_KEY: return print("Error: Falta GOOGLE_API_KEY en .env")

    data_dir, output_base_dir = "./data", "./output"
    pauta_excel_path = 'data/06 08 2025 PRESSUPOST EXCEL DAUFES.xlsx'
    pauta_json_path = 'output/mapped_pauta.json'
    
    # --- FASE 0: PREPARACIÓN DE LA PAUTA ---
    print("\n--- 📋 FASE 0: PREPARANDO PAUTA ---")
    if not os.path.exists(pauta_excel_path):
        return print(f"❌ No se encuentra el Excel de pauta en: {pauta_excel_path}")
        
    # Convertimos Excel -> JSON Pauta Maestro
    full_pauta = map_excel_to_json(pauta_excel_path, pauta_json_path)

    # --- INICIALIZACIÓN DE AGENTES ---
    planner = PlannerAgent(GOOGLE_API_KEY)
    extractor = ExtractorAgent(GOOGLE_API_KEY)
    mapper = MapperAgent(GOOGLE_API_KEY)      # <--- NUEVO
    extra_reviewer = ExtraReviewAgent(GOOGLE_API_KEY)
    auditor = AuditorAgent(GOOGLE_API_KEY)
    reflector = AuditReflector(GOOGLE_API_KEY) 

    if not os.path.exists(data_dir):
        return print(f"❌ No existe directorio data: {data_dir}")

    pdf_files = [f for f in os.listdir(data_dir) if f.endswith(".pdf")]
    if not pdf_files:
        return print("❌ No hay PDFs en la carpeta data.")

    # --- BUCLE PRINCIPAL POR CADA PDF ---

    for idx, filename in enumerate(pdf_files, 1):
        pdf_name = os.path.splitext(filename)[0]
        pdf_path = os.path.join(data_dir, filename)

        # Estructura de carpetas: output/nombre_pdf/
        pdf_output_dir = os.path.join(output_base_dir, pdf_name)
        chunks_dir = os.path.join(pdf_output_dir, "chunks")
        os.makedirs(chunks_dir, exist_ok=True)

        print(f"\n🚀 [{idx}/{len(pdf_files)}] PROCESANDO PROYECTO: {filename}")

        full_text = None

        # ======================================================================
        # 1. PLANIFICACIÓN
        # ======================================================================
        plan_path = os.path.join(pdf_output_dir, "plan_log.json")
        extraction_plan = None

        if os.path.exists(plan_path):
            print(f"    📂 Plan de extracción encontrado (Cargando de caché)...")
            extraction_plan = load_json(plan_path)
        else:
            print(f"    🧠 Generando nuevo plan de extracción...")
            try:
                full_text = extract_text_from_pdf(pdf_path)
                extraction_plan = planner.generate_extraction_plan(full_text)
                with open(plan_path, 'w', encoding='utf-8') as f:
                    json.dump(extraction_plan, f, indent=4, ensure_ascii=False)
                print("    ✅ Plan generado y guardado.")
            except Exception as e:
                print(f"    ❌ Error generando plan: {e}")
                continue

        # ======================================================================
        # 2. EXTRACCIÓN (CHUNKS) usando Gemini File API
        # ======================================================================
        if not extraction_plan or "tasks" not in extraction_plan:
            print("    ❌ El plan de extracción es inválido.")
            continue

        tasks = extraction_plan["tasks"]

        # Filtrar solo chunks pendientes
        pending_tasks = []
        for task in tasks:
            task_id = task.get("id")
            chunk_path = os.path.join(chunks_dir, f"chunk_{task_id}.json")
            if not os.path.exists(chunk_path):
                pending_tasks.append((task, chunk_path))
            else:
                print(f"      ♻️  Chunk {task_id} ya existe (saltando)")

        if pending_tasks:
            print(f"    ⬆️ Subiendo PDF a Gemini File API...")
            try:
                gemini_file = upload_pdf_to_gemini(pdf_path)
            except Exception as e:
                print(f"    ❌ Error subiendo PDF a Gemini: {e}")
                continue

            print(f"    ⚡ Extrayendo {len(pending_tasks)} chunks en paralelo (max 10 workers)...")

            def extract_and_save(task_tuple):
                task, chunk_path = task_tuple
                task_id = task.get("id")
                try:
                    chunk_data = extractor.extract_chunk(task, gemini_file)
                    with open(chunk_path, 'w', encoding='utf-8') as f:
                        json.dump(chunk_data, f, indent=4, ensure_ascii=False)
                    return (task_id, True, None)
                except Exception as e:
                    return (task_id, False, str(e))

            MAX_WORKERS = 10  # Reducido para evitar límites de RPM
            completed_count = 0
            total_pending = len(pending_tasks)

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(extract_and_save, t): t[0].get("id") for t in pending_tasks}

                for future in as_completed(futures):
                    task_id = futures[future]
                    completed_count += 1
                    try:
                        tid, success, error = future.result(timeout=300)  # 5min timeout per task
                        if success:
                            print(f"      ✅ Chunk {tid} extraído ({completed_count}/{total_pending})")
                        else:
                            print(f"      ❌ Error en Chunk {tid}: {error}")
                    except Exception as e:
                        print(f"      ❌ Excepción en Chunk {task_id}: {e}")

            print(f"    ✅ Extracción paralela completada ({completed_count}/{total_pending} chunks)")

            # Eliminar el archivo de Gemini File API para limpiar
            try:
                genai.delete_file(gemini_file["name"] if isinstance(gemini_file, dict) else getattr(gemini_file, "name", None))
                print(f"    🧹 Archivo PDF eliminado de Gemini File API.")
            except Exception as e:
                print(f"    ⚠️  No se pudo eliminar el archivo de Gemini: {e}")

        print(f"    📦 Consolidando chunks...")

        # ==============================================================================
        # 3. CONSOLIDACIÓN
        # ==============================================================================
        final_json_path = consolidate_chunks(pdf_name) 
        if not final_json_path:
            print("❌ Error: No se pudo consolidar el JSON. Saltando proyecto.")
            continue

        # # ==============================================================================
        # # 4. MAPEO INTELIGENTE (MAPPER AGENT)
        # # ==============================================================================
        # print(f"    🗺️  Verificando Mapeo Pauta <-> Oferta...")
        # mapping_batches_dir = os.path.join(pdf_output_dir, "mapping_batches")
        # os.makedirs(mapping_batches_dir, exist_ok=True)
        # mapping_path = os.path.join(mapping_batches_dir, f"MAPPING_LINKS_{pdf_name}.json")
        # mapping_result = None

        # if os.path.exists(mapping_path):
        #     print(f"    📂 Cargando mapeo existente...")
        #     mapping_result = load_json(mapping_path)
        
        # if not mapping_result:
        #     print(f"    🧠 Generando mapeo con IA (Global Context)...")
        #     try:
        #         mapping_result = mapper.map_offer_to_pauta(pauta_json_path, final_json_path)
                
        #         # Guardamos el resultado intermedio
        #         with open(mapping_path, 'w', encoding='utf-8') as f:
        #             json.dump(mapping_result, f, indent=4, ensure_ascii=False)
        #         print(f"    💾 Mapeo guardado en: {mapping_path}")
        #     except Exception as e:
        #         print(f"    ❌ Error crítico en Mapper: {e}")
        #         mapping_result = {"mapping": {}, "extras": []}

        # # ======================================================================
        # # 4.1. REVISIÓN DE EXTRAS (EXTRA REVIEW AGENT)
        # # ======================================================================
        # extra_review_path = os.path.join(mapping_batches_dir, f"EXTRA_REVIEW_{pdf_name}.json")
        
        # if os.path.exists(extra_review_path):
        #     print(f"    📂 Cargando revisión de extras existente...")
        #     try:
        #         with open(extra_review_path, 'r', encoding='utf-8') as f:
        #             review_data = json.load(f)
        #         # Reconstruir mapping_result con los extras ya revisados
        #         # Los decisions contienen las reasignaciones
        #         decisions = review_data.get("decisions", [])
        #         for item in decisions:
        #             if item.get("decision") == "MAP" and item.get("pauta_id"):
        #                 id_oferta = item.get("id_oferta")
        #                 mapping_result.setdefault("mapping", {})[id_oferta] = item["pauta_id"]
        #                 if id_oferta in mapping_result.get("extras", []):
        #                     mapping_result["extras"].remove(id_oferta)
        #         print(f"    ✅ Revisión de extras cargada de caché.")
        #     except Exception as e:
        #         print(f"    ⚠️  Error cargando revisión de extras: {e}")
        # elif mapping_result and mapping_result.get("extras"):
        #     print(f"    🔎 Revisando {len(mapping_result.get('extras', []))} extras con IA...")
        #     try:
        #         review_payload = extra_reviewer.review_extras(
        #             pauta_json_path,
        #             final_json_path,
        #             mapping_result,
        #             output_path=extra_review_path,
        #             batch_size=10
        #         )
        #         mapping_result = review_payload.get("mapping_result", mapping_result)

        #         # Guardar mapeo actualizado (con extras revisados)
        #         with open(mapping_path, 'w', encoding='utf-8') as f:
        #             json.dump(mapping_result, f, indent=4, ensure_ascii=False)
        #         print(f"    ✅ Extras revisados. Mapeo actualizado.")
        #     except Exception as e:
        #         print(f"    ⚠️  Error revisando extras: {e}")
        # else:
        #     print(f"    ✅ No hay extras para revisar. Saltando revisión.")

        # # ======================================================================
        # # 4.2. GUARDAR MAPPING FINAL (POST-REVISIÓN DE EXTRAS)
        # # ======================================================================
        # print(f"    💾 Guardando mapeo final...")
        # final_mapping_path = os.path.join(pdf_output_dir, f"MAPPING_LINKS_FINAL_{pdf_name}.json")
        # try:
        #     with open(final_mapping_path, 'w', encoding='utf-8') as f:
        #         json.dump(mapping_result, f, indent=4, ensure_ascii=False)
        #     print(f"    💾 Mapeo final guardado en: {final_mapping_path}")
        # except Exception as e:
        #     print(f"    ⚠️  Error guardando mapping final: {e}")

        # # Aplicamos el mapeo al archivo consolidado (Esto inyecta 'id_pauta_unico')
        # stats = apply_mapping_to_json(final_json_path, mapping_result)
        # print(f"    ✅ Mapeo aplicado: {stats['mapped']} vinculadas, {stats['extras']} extras.")

        # # ==============================================================================
        # # 5. GENERACIÓN DEL AUDIT_QUALITATIVE_INPUT (DESPUÉS DEL MAPPER)
        # # ==============================================================================
        # # Ahora el JSON consolidado YA tiene el mapeo aplicado (id_pauta_unico)
        # # y podemos generar audit_qualitative_input con la información del mapping
        # print(f"    📋 Generando entrada cualitativa para auditoría (con mapeo)...")
        # try:
        #     audit_input_path = os.path.join(pdf_output_dir, "audit_qualitative_input.json")
        #     generate_audit_qualitative_input(pauta_json_path, final_json_path, 
        #                                    mapping_result=mapping_result, 
        #                                    output_path=audit_input_path)
        # except Exception as e:
        #     print(f"    ⚠️  Error generando audit_qualitative_input: {e}")

        # # ==============================================================================
        # # 6. AUDITORÍA FASE 1: DETECCIÓN (AUDITOR AGENT)
        # # ==============================================================================
        # audit_output_path = os.path.join(pdf_output_dir, f"AUDITORIA_{pdf_name}.json")
        # audit_results = []

        # if os.path.exists(audit_output_path):
        #     print(f"    ⏩ Auditoría Fase 1 ya existe. Cargando...")
        #     audit_results = load_json(audit_output_path)
        # else:
        #     print(f"    🕵️  Iniciando Auditoría Fase 1 (Detección)...")
        #     try:
        #         # El auditor lee el audit_qualitative_input que generamos en paso 5
        #         audit_results = auditor.run_full_audit(pauta_json_path, final_json_path, batch_size=15)
                
        #         with open(audit_output_path, "w", encoding="utf-8") as f:
        #             json.dump(audit_results, f, indent=4, ensure_ascii=False)
        #         print(f"    ✅ Fase 1 completada: {len(audit_results)} incidencias detectadas.")
        #     except Exception as e:
        #         print(f"    ❌ Error crítico en Auditoría Fase 1: {e}")
        #         audit_results = [] 

        # # ==============================================================================
        # # 7. AUDITORÍA FASE 2: VERIFICACIÓN (JUEZ / REFLEXIÓN)
        # # ==============================================================================
        # final_validated_path = os.path.join(pdf_output_dir, f"AUDITORIA_VALIDADA_{pdf_name}.json")
        
        # # Verificación de integridad del archivo final
        # juez_completado = False
        # if os.path.exists(final_validated_path):
        #     existing_data = load_json(final_validated_path)
        #     if existing_data and len(existing_data) > 0:
        #         juez_completado = True
        #     else:
        #         print("    ⚠️  Archivo validado corrupto/vacío. Se regenerará.")

        # if juez_completado:
        #     print(f"    ⏩ Auditoría Fase 2 (Validada) ya existe. Saltando...")
        
        # elif not audit_results:
        #     print("    ⚠️  Saltando Fase 2 (Sin resultados previos).")
            
        # else:
        #     print(f"    ⚖️  Iniciando Fase 2: El Juez (Verificación PDF)...")
            
        #     # Contexto para el Juez
        #     context_input_path = os.path.join(pdf_output_dir, "audit_qualitative_input.json")
        #     if not os.path.exists(context_input_path):
        #         print("    ⚠️  Alerta: Falta 'audit_qualitative_input.json'.")
            
        #     print("    🔗 Cruzando datos (Acusación + Pauta)...")

        #     enriched_save_path = os.path.join(pdf_output_dir, f"AUDITORIA_ENRIQUECIDA_{pdf_name}.json")
            
        #     enriched_audit = enrich_audit_data(
        #         audit_results, 
        #         context_input_path, 
        #         save_path=enriched_save_path  
        #     )
        #     # Carpeta para guardar los batches del Juez
        #     debug_batches_dir = os.path.join(pdf_output_dir, "debug_batches")
            
        #     try:
        #         # # Llamada al Juez con guardado incremental por batches
        #         validated_audit = reflector.review_audit(
        #             enriched_audit, 
        #             pdf_path, 
        #             debug_folder_path=debug_batches_dir 
        #         )
                
        #         if validated_audit and len(validated_audit) > 0:
        #             with open(final_validated_path, "w", encoding="utf-8") as f:
        #                 json.dump(validated_audit, f, indent=4, ensure_ascii=False)
        #             print(f"    🏆 Auditoría Blindada guardada en: {final_validated_path}")
        #         else:
        #             print("    ❌ El Juez devolvió datos vacíos.")

        #     except Exception as e:
        #         print(f"    ❌ ERROR CRÍTICO EN EL AGENTE JUEZ: {e}")
        #         print("    ⚠️  Omitida generación de archivo final para evitar corrupción.")

    #     # ==============================================================================
    #     # 7.5 SANEAMIENTO DE ASIGNACIONES DUPLICADAS POST-AUDITORÍA
    #     # ==============================================================================
    #     if os.path.exists(final_validated_path):
    #         print(f"    🧼 Saneando asignaciones duplicadas en auditoría...")
    #         try:
    #             stats = sanitizar_asignaciones_post_auditoria(final_validated_path, final_json_path)
    #             if not stats.get("skipped"):
    #                 print(f"    📊 Saneamiento: {stats['eliminados']} duplicados, "
    #                       f"{stats['sincronizados']} partidas sincronizadas.")
    #         except Exception as e:
    #             print(f"    ⚠️  Error en saneamiento (no bloqueante): {e}")
    #     else:
    #         print(f"    ⏩ Sin auditoría validada disponible, saneamiento omitido.")

    #     # ==============================================================================
    #     # 8. GENERACIÓN ENTREGABLE (COMPARATIVO MAESTRO DESDE JSON)
    #     # ==============================================================================
    # print("\n" + "="*50)
    # print("🔄 FASE FINAL: GENERACIÓN COMPARATIVO MAESTRO DESDE JSON")
    # print("="*50)

    # archivo_maestro = os.path.join(output_base_dir, "COMPARATIVO_MAESTRO_FINAL.xlsx")
    # try:
    #     generar_comparativo_final(output_base_dir, archivo_maestro)
    #     print(f"\n🚀 ¡PROCESO COMPLETADO CON ÉXITO!")
    #     print(f"📄 Archivo Maestro Final: {archivo_maestro}")
    # except FileNotFoundError as e:
    #     print(f"❌ Archivos JSON no encontrados: {e}")
    # except Exception as e:
    #     print(f"❌ Error generando comparativo: {e}")
    #     import traceback
    #     traceback.print_exc()

if __name__ == "__main__":
    main()