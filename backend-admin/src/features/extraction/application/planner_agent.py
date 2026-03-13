import os
import json
import time
import random
import google.generativeai as genai
from src.shared.observability.cost_tracker import record_usage

class PlannerAgent:
    # Hard caps enforced both in the prompt and in post-hoc Python validation.
    # The extractor runs with a 65 536 max-output-token budget, so these
    # limits stay generous while keeping chunk output bounded.
    MAX_CODES_PER_TASK = 45
    MAX_WEIGHT_PER_TASK = 80

    # Retry / fallback config for generate_extraction_plan
    _MAX_RETRIES  = 5
    _BASE_DELAY   = 15   # seconds; doubled each attempt (15, 30, 60, 120, 240)

    # Ordered fallback list appended after the model selected via PLANNER_MODEL. , "gemini-1.5-pro"
    _MODEL_FALLBACKS = ["gemini-2.5-flash", "gemini-2.0-flash"]  

    def __init__(self, api_key):
        genai.configure(api_key=api_key)
        self.primary_model = (
            os.getenv("PLANNER_MODEL", "gemini-2.5-flash").strip()
            or "gemini-2.5-flash"
        )
        self.model_cascade = [self.primary_model]
        for fallback in self._MODEL_FALLBACKS:
            if fallback not in self.model_cascade:
                self.model_cascade.append(fallback)

        self.system_prompt = """
        Eres un Arquitecto de Datos experto en fragmentación de presupuestos de construcción.
        Tu misión es producir un plan de extracción EXHAUSTIVO y EQUILIBRADO: ningún código
        de partida debe quedar sin asignar y ninguna tarea debe exceder la capacidad del
        extractor de IA corriente abajo.

        ═══════════════════════════════════════════════════════════════
        🔍 PASO 1: IDENTIFICAR Y PESAR CADA PARTIDA
        ═══════════════════════════════════════════════════════════════

        Recorre el presupuesto DE PRINCIPIO A FIN y asigna un peso a cada código:

        PESO 1 — Línea simple
          • código + descripción corta (< 60 chars) + cantidad + precio + total
          • Sin subcomponentes ni notas técnicas adicionales
          • Ejemplo: "01.01  Excavació manual  12,00 m³  18,50  222,00"

        PESO 2 — Línea media
          • Descripción larga (60-150 chars) o 2-4 líneas de detalle bajo la partida
          • Ejemplo: partida con especificaciones de materiales en 2-3 líneas

        PESO 3 — Línea compleja
          • Descripción > 150 chars, o 5+ subcomponentes/líneas de detalle
          • Kits, desgloses de instalación, tablas de componentes
          • Ejemplo: "22.04 Videovigilancia CCTV" con 6 equipos desglosados

        ⚠️ QUÉ CUENTA COMO CÓDIGO DE PARTIDA:
          • Patrón: número(s) separados por puntos, opcionalmente seguido de letra
            → válidos: "01.01", "02.03", "12.08b", "29.02.01"
          • Aparecen al principio de línea o indentados, seguidos de descripción + cifras
          • EXCLUIR: medidas aisladas (0,50m), precios sueltos, años (2024), nº página,
            códigos de capítulo ("CAPITOL 1" sin subpartidas propias)

        Incluye ABSOLUTAMENTE todos los códigos, incluso los de capítulos con pocas partidas.
        Anota la página aproximada de cada código ("pag_aprox": int).

        REGLA DE SUBCAPITULOS (OBLIGATORIA):
          Cuando exista jerarquia de encabezados (ej. 15 -> 15.1 y 15.2),
          trata cada subcapitulo como bloque independiente de extraccion.
          No mezcles en una misma tarea codigos de subcapitulos distintos si
          pueden separarse sin romper los limites de peso/codigos.
          El capitulo padre contenedor (ej. 15) NO cuenta como bloque extraible
          si no tiene partidas propias; su funcion es solo agrupar.

        Emite los resultados como array de objetos:
          "todos_los_codigos_identificados": [
            {"codigo": "01.01", "peso": 1, "pag_aprox": 3},
            ...
          ]

        ═══════════════════════════════════════════════════════════════
        📋 PASO 2: AGRUPACIÓN EN TAREAS
        ═══════════════════════════════════════════════════════════════

        REGLA PRINCIPAL — el extractor usa 65 536 tokens de salida:
          Cada tarea debe tener PESO TOTAL ≤ 80 puntos.
          Esto equivale a ~6 000 tokens de JSON de salida, bien dentro de la capacidad.

        TABLA DE CAPACIDAD ORIENTATIVA:
          • Solo peso 1:          hasta 40-45 códigos por tarea
          • Mix peso 1+2:         hasta 25-35 códigos por tarea
          • Mix peso 2+3:         hasta 15-20 códigos por tarea
          • Solo peso 3:          hasta 8-12 códigos por tarea

        REGLA DE CORTE ABSOLUTA:
          • NUNCA más de 45 códigos por tarea
          • NUNCA peso total > 80 por tarea
          • Si un capítulo es muy largo, divídelo en sub-tareas consecutivas
          • Capítulos muy cortos (≤ 4 partidas, peso ≤ 8) PUEDEN fusionarse con el
            adyacente siempre que el total combinado no supere 45 códigos ni peso 80

        REGLA ANTI-SOBRECARGA — instalaciones:
          Capítulos de electricidad, climatización, fontanería, ACS,
          telecomunicaciones o seguridad → peso mínimo 2 por partida, aunque
          la línea parezca simple. Suelen tener subcomponentes ocultos.

        AGRUPACIÓN POR AFINIDAD:
          Dentro de un mismo capítulo intenta no cruzar tareas a no ser que sea
          necesario por el límite de peso. Si divides un capítulo, el campo
          "capitulos" de cada sub-tarea debe incluir el mismo nombre con el
          sufijo " (parte 1/N)", " (parte 2/N)", etc.

        Añade a cada tarea el rango de páginas aproximado donde se concentran
        sus partidas: "paginas_aprox": "X-Y"

        ═══════════════════════════════════════════════════════════════
        ✅ PASO 3: VERIFICACIÓN FINAL (OBLIGATORIA — no omitir)
        ═══════════════════════════════════════════════════════════════

        Antes de generar la respuesta JSON, comprueba mentalmente:
        □ ¿Cada código de "todos_los_codigos_identificados" aparece en exactamente
          un "rango_partidas" de alguna tarea?
        □ ¿Ninguna tarea supera 45 códigos?
        □ ¿Ninguna tarea supera peso total 80?
        □ ¿Hay códigos duplicados entre tareas? (NO debe haberlos)
        □ ¿Algún capítulo quedó sin asignar? (NO debe quedarse ninguno)

        Si detectas una violación, corrige el plan antes de responder.

        ═══════════════════════════════════════════════════════════════
        📤 FORMATO DE SALIDA ESTRICTO (JSON)
        ═══════════════════════════════════════════════════════════════

        {
          "todos_los_codigos_identificados": [
            {"codigo": "01.01", "peso": 1, "pag_aprox": 3},
            {"codigo": "01.02", "peso": 2, "pag_aprox": 4},
            ...
          ],
          "total_partidas_detectadas": int,
          "total_tareas": int,
          "proveedor": "nombre del proveedor o contratista",
          "tasks": [
            {
              "id": 1,
              "capitulos": ["CAPITOL 01 - MOVIMENT DE TERRES"],
              "rango_partidas": "01.01, 01.02, 01.03, 01.04, 01.05",
              "num_partidas_estimadas": 5,
              "peso_total_estimado": 8,
              "paginas_aprox": "3-5",
              "prompt_especifico": "Extrae las partidas del CAPITOL 01 - MOVIMENT DE TERRES. Códigos a extraer: 01.01, 01.02, 01.03, 01.04, 01.05. Estas partidas aparecen aproximadamente en las páginas 3-5 del PDF."
            }
          ]
        }

        ═══════════════════════════════════════════════════════════════
        ⚠️ RESTRICCIONES ABSOLUTAS — NO NEGOCIABLES
        ═══════════════════════════════════════════════════════════════

        • MÁXIMO 45 códigos por tarea
        • MÁXIMO peso total 80 por tarea
        • NO omitir ningún código de partida real
        • NO incluir medidas, precios, cantidades ni encabezados de capítulo como códigos
        • NO mezclar partidas de subcapitulos distintos (ej. 15.1.* y 15.2.*) en una
          misma salida de capitulo final cuando el documento los separa explicitamente
        • NO dejar ningún capítulo o bloque de partidas sin asignar
        • "rango_partidas" debe ser una cadena de códigos separados por coma y espacio
        • "prompt_especifico" DEBE mencionar el nombre del capítulo, los códigos exactos
          y el rango de páginas aproximado
        """

    def generate_extraction_plan(self, gemini_file_ref):
        """Genera el plan de extracción a partir del PDF subido a Gemini File API.

        Parameters
        ----------
        gemini_file_ref : google.generativeai.types.File
            Referencia del PDF ya subido con genai.upload_file().
            El modelo accede al documento nativo (tablas, layout) en lugar de
            texto plano extraído localmente.
        """
        user_content = f"""Ejecuta los 3 pasos del sistema sobre el presupuesto del documento PDF adjunto:

        PASO 1 — Recorre CADA página y lista TODOS los códigos de partida con su peso
                 (1/2/3) y página aproximada. No te saltes ningún capítulo.

        PASO 2 — Agrupa los códigos en tareas: máx {self.MAX_CODES_PER_TASK} códigos
                 y peso ≤ {self.MAX_WEIGHT_PER_TASK} por tarea.
                 Los capítulos de instalaciones (electricidad, climatización,
                 fontanería, ACS, telecomunicaciones, seguridad) tienen peso mínimo 2.

        PASO 3 — Verifica que ningún código queda sin asignar y que no hay duplicados.
                 Corrige el plan antes de responder si encuentras una violación."""

        last_exc = None

        for model_name in self.model_cascade:
            model = genai.GenerativeModel(
                model_name=model_name,
                generation_config={
                    "response_mime_type": "application/json",
                    "temperature": 0.0,
                },
            )
            if model_name != self.primary_model:
                print(f"    ⚠️  PlannerAgent: falling back to {model_name}")

            for attempt in range(1, self._MAX_RETRIES + 1):
                try:
                    response = model.generate_content([self.system_prompt, gemini_file_ref, user_content])
                    # ── Track API cost ───────────────────────────────────────
                    if hasattr(response, "usage_metadata") and response.usage_metadata:
                        record_usage(
                            model_name,
                            int(getattr(response.usage_metadata, "prompt_token_count", 0) or 0),
                            int(getattr(response.usage_metadata, "candidates_token_count", 0) or 0),
                        )
                    plan = json.loads(response.text.strip())
                    break  # success — exit retry loop

                except json.JSONDecodeError as e:
                    last_exc = e
                    print(f"    ⚠️  PlannerAgent JSON inválido (model={model_name}, "
                          f"attempt {attempt}/{self._MAX_RETRIES}): {e}")
                    if attempt < self._MAX_RETRIES:
                        time.sleep(self._BASE_DELAY)
                    continue

                except Exception as e:
                    last_exc = e
                    err_str = str(e)
                    is_rate_or_quota = (
                        "429" in err_str
                        or "quota" in err_str.lower()
                        or "RESOURCE_EXHAUSTED" in err_str
                        or "ResourceExhausted" in type(e).__name__
                    )
                    is_daily_quota = (
                        "generate_requests_per_model_per_day" in err_str.lower()
                        or "GenerateRequestsPerDay" in err_str
                        or "limit: 0" in err_str
                    )

                    print(f"    ⚠️  PlannerAgent error (model={model_name}, "
                          f"attempt {attempt}/{self._MAX_RETRIES}): {type(e).__name__}")

                    if is_daily_quota:
                        # Daily cap exhausted for this model — skip to next in cascade
                        print(f"    💀  Daily quota exhausted for {model_name} — "
                              f"switching to next model in cascade.")
                        break  # break inner retry loop → try next model

                    if is_rate_or_quota and attempt < self._MAX_RETRIES:
                        # Transient RPM throttle — honour Retry-After if present
                        import re as _re
                        m = _re.search(r"retry.*?after.*?(\d+)", err_str, _re.IGNORECASE)
                        delay = int(m.group(1)) if m else self._BASE_DELAY * (2 ** (attempt - 1))
                        delay = min(delay, 300)  # cap at 5 min
                        jitter = random.uniform(0, delay * 0.1)
                        print(f"    ⏳  Rate-limited — waiting {delay:.0f}s "
                              f"(+{jitter:.1f}s jitter) before retry…")
                        time.sleep(delay + jitter)
                        continue

                    if attempt < self._MAX_RETRIES:
                        delay = self._BASE_DELAY * (2 ** (attempt - 1))
                        print(f"    ⏳  Retrying in {delay}s…")
                        time.sleep(delay)
                        continue

                    # Max retries for this model — try next in cascade
                    break

            else:
                # All retries for this model exhausted without success — try next
                continue

            # If we get here via `break` from the retry loop we need to check
            # whether `plan` was actually set.  A `break` inside the except block
            # means we should try the next model; a `break` after `json.loads`
            # means success.
            try:
                plan  # noqa: just check it is defined
            except NameError:
                continue

            # ── plan is defined — run post-processing ────────────────────────
            # Normalise todos_los_codigos_identificados
            raw_codes = plan.get("todos_los_codigos_identificados", [])
            if raw_codes and isinstance(raw_codes[0], str):
                raw_codes = [{"codigo": c, "peso": 1, "pag_aprox": 0} for c in raw_codes]
                plan["todos_los_codigos_identificados"] = raw_codes
            all_codes = {entry["codigo"] for entry in raw_codes if isinstance(entry, dict)}

            # Post-hoc hard-cap enforcement
            enforced_tasks = []
            next_id = 1
            for task in plan.get("tasks", []):
                rango = task.get("rango_partidas", "")
                codes = [c.strip() for c in rango.split(",") if c.strip()]
                if len(codes) <= self.MAX_CODES_PER_TASK:
                    task["id"] = next_id
                    enforced_tasks.append(task)
                    next_id += 1
                else:
                    chunks = [
                        codes[i : i + self.MAX_CODES_PER_TASK]
                        for i in range(0, len(codes), self.MAX_CODES_PER_TASK)
                    ]
                    print(
                        f"    ⚙️  Post-hoc split: task with {len(codes)} codes "
                        f"→ {len(chunks)} sub-tasks of ≤{self.MAX_CODES_PER_TASK}"
                    )
                    for chunk_codes in chunks:
                        sub = dict(task)
                        sub["id"] = next_id
                        sub["rango_partidas"] = ", ".join(chunk_codes)
                        sub["num_partidas_estimadas"] = len(chunk_codes)
                        sub["prompt_especifico"] = (
                            task.get("prompt_especifico", "")
                            + f" [Subconjunto: {chunk_codes[0]} … {chunk_codes[-1]}]"
                        )
                        enforced_tasks.append(sub)
                        next_id += 1

            plan["tasks"] = enforced_tasks
            plan["total_tareas"] = len(enforced_tasks)

            # Coherence check
            task_codes: set[str] = set()
            seen: set[str] = set()
            duplicates: list[str] = []
            for task in enforced_tasks:
                for c in task.get("rango_partidas", "").split(","):
                    c = c.strip()
                    if c:
                        if c in seen:
                            duplicates.append(c)
                        seen.add(c)
                        task_codes.add(c)

            missing = all_codes - task_codes
            if missing:
                print(
                    f"    ⚠️  Auto-verificación: {len(missing)} código(s) identificados "
                    f"no asignados: {sorted(missing)[:10]}{'...' if len(missing) > 10 else ''}"
                )
            if duplicates:
                print(
                    f"    ⚠️  Auto-verificación: {len(duplicates)} código(s) duplicados "
                    f"entre tareas: {sorted(set(duplicates))[:10]}"
                )
            if not missing and not duplicates:
                sizes = [
                    len([c for c in t.get("rango_partidas", "").split(",") if c.strip()])
                    for t in enforced_tasks
                ]
                print(
                    f"    ✅ Plan validado ({model_name}): {len(all_codes)} códigos → "
                    f"{len(enforced_tasks)} tareas "
                    f"(sizes: min={min(sizes)}, max={max(sizes)}, avg={sum(sizes) // len(sizes)})"
                )

            return plan

        # All models in the cascade failed
        print(f"❌ PlannerAgent: todos los modelos del cascade fallaron. Último error: {last_exc}")
        raise last_exc
