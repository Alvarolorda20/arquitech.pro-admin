import os
import json
import re
import time
import traceback
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold
from src.shared.observability.cost_tracker import record_usage


# ─── Human-readable labels for finish_reason codes ────────────────────────────
_FINISH_REASON_LABELS = {
    0: "FINISH_REASON_UNSPECIFIED",
    1: "STOP (completed but returned no content — likely an empty JSON response)",
    2: "MAX_TOKENS",
    3: "SAFETY (blocked by safety filters)",
    4: "RECITATION",
    5: "OTHER",
}

# Safety settings that disable all automatic content blocking so that technical
# construction budget PDFs are never misidentified as harmful.
_SAFETY_SETTINGS = {
    HarmCategory.HARM_CATEGORY_HARASSMENT:        HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_HATE_SPEECH:       HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
}


class ExtractorAgent:
    MAX_RETRIES = 3
    RETRY_DELAY = 10     # base seconds between retry attempts
    # Large extraction responses can take 3-5 min for 65 536 output tokens.
    # chunk.  120 s was too tight and caused 504s on the first attempt.  600 s
    # matches the Gemini SDK default and covers worst-case inference time.
    # The outer future.result(timeout=600) in server.py is the safety net.
    REQUEST_TIMEOUT = 600

    def __init__(self, api_key):
        genai.configure(api_key=api_key)
        self.model_name = (
            os.getenv("EXTRACTOR_MODEL", "gemini-2.5-flash").strip()
            or "gemini-2.5-flash"
        )
        # Model configured via EXTRACTOR_MODEL.
        # This lets each chunk carry many codes per request,
        # reducing total extraction calls across the pipeline.
        self.model = genai.GenerativeModel(
            model_name=self.model_name,
            generation_config={
                "response_mime_type": "application/json",
                "temperature": 0.0,
                "max_output_tokens": 65536,
            },
            safety_settings=_SAFETY_SETTINGS,
        )
    
    def _clean_json_string(self, raw: str) -> str:
        """Strips BOM, markdown fences and stray backticks so json.loads always gets clean input."""
        raw = raw.strip().lstrip("\ufeff")
        raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"\s*```$", "", raw)
        raw = raw.strip("`").strip()
        return raw

    def extract_chunk(self, task, gemini_file_ref):
        system_prompt = """
        Eres un Extractor de Datos "Espejo" de alta precisión. Tu única función es transcribir información técnica de presupuestos a formato JSON sin alterar valores ni realizar cálculos.

        ═══════════════════════════════════════════════════════════════
        📋 REGLAS DE ORO DE EXTRACCIÓN
        ═══════════════════════════════════════════════════════════════

        1. TRANSCRIPCIÓN LITERAL
           Extrae QUANTITAT, PREU y TOTAL exactamente como aparecen en el PDF.
           NO sumes, multipliques ni valides. Copia los valores tal cual.

        2. FILTRADO POR CÓDIGO
           Solo genera partidas para los códigos del CHECKLIST.
           Si un código no está en el checklist, ignóralo.

        3. TOTAL CAPÍTULO
           Busca "TOTAL CAPITOL X..." y copia ese valor.
           Si no aparece en tu fragmento, pon null (no 0.0).

        4. KITS Y DESGLOSES CON SUBCOMPONENTES
           Si una partida tiene líneas de detalle debajo (equipos, materiales, mano de obra)
           que tienen cantidad, precio y total PERO NO tienen código propio:
           - Crea un array "componentes" dentro de la partida
           - Cada componente: {{"descripcion": "...", "cantidad": X, "unidad": "UT", "precio": Y, "total": Z}}
           - El "total" de la partida padre es la SUMA de todos los componentes (cópialo del PDF)
           - NO sumes tú los componentes, copia el total que aparece en el PDF

        5. VALORES FALTANTES
           - Si falta cantidad: null
           - Si falta precio: null
           - Si falta total: null
           NO inventes valores. null es mejor que 0.0 cuando no hay dato.

        6. SUBCAPITULOS Y JERARQUIA (OBLIGATORIO)
           Si detectas un capitulo contenedor (ej. 22) con subcapitulos explicitos
           (ej. 22.1, 22.2, 22.3), NO mezcles sus partidas en un unico capitulo padre.
           Debes devolver un objeto de capitulo independiente por cada subcapitulo real:
           - 22.1.* -> capitulo_codigo "22.1"
           - 22.2.* -> capitulo_codigo "22.2"
           - 22.3.* -> capitulo_codigo "22.3"
           Regla de asignacion: cada partida va al capitulo con el prefijo mas especifico
           que coincida con su codigo.
           Si el capitulo padre (22) solo actua como encabezado agrupador y no tiene
           partidas propias con ese mismo nivel, NO lo incluyas como capitulo de salida.
           Solo incluyelo si realmente tiene partidas propias (no de subcapitulos).

        ═══════════════════════════════════════════════════════════════
        🔢 FORMATEO NUMÉRICO (Formato Español)
        ═══════════════════════════════════════════════════════════════

        Entrada PDF (español)  →  Salida JSON (float)
        ─────────────────────────────────────────────
        "1.250,50"             →  1250.50
        "12,50"                →  12.50
        "1.000"                →  1000.0
        "0,75"                 →  0.75
        "12.345.678,90"        →  12345678.90

        REGLA: El punto (.) es separador de miles, la coma (,) es decimal.
        Elimina puntos de miles, reemplaza coma por punto decimal.

        ═══════════════════════════════════════════════════════════════
        📐 NORMALIZACIÓN DE UNIDADES
        ═══════════════════════════════════════════════════════════════

        Entrada PDF   →  Salida JSON
        ─────────────────────────────
        "m²", "M2", "m2"      →  "m²"
        "m³", "M3", "m3"      →  "m³"
        "ml", "ML", "m.l."    →  "ml"
        "ud", "UD", "Ud", "UT" →  "ud"
        "kg", "KG", "Kg"      →  "kg"
        "pa", "PA", "P.A."    →  "pa"

        ═══════════════════════════════════════════════════════════════
        📊 CONFIDENCE_GLOBAL (0.0 - 1.0)
        ═══════════════════════════════════════════════════════════════

        Evalúa la calidad de extracción del capítulo:
        - 1.0: Todos los campos extraídos, formato claro
        - 0.8-0.9: Algunos campos con valores null pero estructura clara
        - 0.6-0.8: Estructura ambigua, varios campos faltantes
        - < 0.6: Extracción dudosa, revisar manualmente

        CHECKLIST DE CÓDIGOS OBLIGATORIOS:
        {checklist}

        ═══════════════════════════════════════════════════════════════
        📤 FORMATO JSON DE SALIDA (ESTRICTO)
        ═══════════════════════════════════════════════════════════════

        [
          {{
            "confidence_global": float,
            "capitulo_codigo": "string",
            "capitulo_nombre": "string",
            "total_capitulo": float | null,
            "partidas": [
              {{
                "codigo": "string",
                "nombre": "string",
                "descripcion": "string",
                "unidad": "string",
                "cantidad": float | null,
                "precio": float | null,
                "total": float | null,
                "componentes": [
                  {{
                    "descripcion": "string",
                    "cantidad": float,
                    "unidad": "string",
                    "precio": float,
                    "total": float
                  }}
                ] | null
              }}
            ]
          }}
        ]

        ═══════════════════════════════════════════════════════════════
        📦 EJEMPLO DE PARTIDA CON COMPONENTES
        ═══════════════════════════════════════════════════════════════

        PDF dice:
        22.04 Videovigilància (CCTV):
          Grabador HD Acusense HIKVISION 4 càmares...   1,00 UT   316,70    316,70
          Disc dur intern WD PURPLE 3.5"...             1,00 UT   133,35    133,35
          Càmera Bullet Hikvision PRO 4 en 1...         4,00 UT   257,80  1.031,20
          Font d'alimentació conmutada 12V...           1,00 UT    51,15     51,15
          Instal.lació, programació i posta en marxa    1,00 UT   488,90    488,90

        JSON esperado:
        {{
          "codigo": "22.04",
          "nombre": "Videovigilancia (CCTV)",
          "descripcion": "Sistema de videovigilancia CCTV completo",
          "unidad": "ud",
          "cantidad": 1,
          "precio": null,
          "total": 2286.35,
          "componentes": [
            {{"descripcion": "Grabador HD Acusense HIKVISION 4 camares", "cantidad": 1, "unidad": "ud", "precio": 316.70, "total": 316.70}},
            {{"descripcion": "Disc dur intern WD PURPLE 3.5 SATA III", "cantidad": 1, "unidad": "ud", "precio": 133.35, "total": 133.35}},
            {{"descripcion": "Camera Bullet Hikvision PRO 4 en 1", "cantidad": 4, "unidad": "ud", "precio": 257.80, "total": 1031.20}},
            {{"descripcion": "Font alimentacio conmutada 12V 12.5A", "cantidad": 1, "unidad": "ud", "precio": 51.15, "total": 51.15}},
            {{"descripcion": "Installacio programacio i posta en marxa", "cantidad": 1, "unidad": "ud", "precio": 488.90, "total": 488.90}}
          ]
        }}

        NOTA: Si la partida NO tiene subcomponentes, el campo "componentes" debe ser null o no existir.
        """.format(
            checklist=task.get('rango_partidas', 'Lista de códigos')
        )

        paginas_hint = task.get("paginas_aprox", "")
        page_note = f" Las partidas se encuentran aproximadamente en las páginas {paginas_hint} del PDF." if paginas_hint else ""
        user_content = (
            f"INSTRUCCIÓN DE TAREA: {task['prompt_especifico']}{page_note}\n\n"
            "Consulta el documento PDF adjunto y extrae ÚNICAMENTE las partidas del "
            "checklist anterior. No inventes códigos que no aparezcan en el PDF."
        )

        task_id = task.get('id', 'unknown')

        debug_dir = "./output/debug_chunks"
        os.makedirs(debug_dir, exist_ok=True)

        last_exception = None
        raw_text = "(not available)"

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                print(f"      [DEBUG] Chunk {task_id}: Calling API (attempt {attempt}/{self.MAX_RETRIES}, timeout={self.REQUEST_TIMEOUT}s)...")
                response = self.model.generate_content(
                    [system_prompt, gemini_file_ref, user_content],
                    safety_settings=_SAFETY_SETTINGS,
                    request_options={"timeout": self.REQUEST_TIMEOUT},  # Fix 2: hard timeout
                )
                print(f"      [DEBUG] Chunk {task_id}: Response received, validating...")

                # ── Guard: check candidates exist and are non-empty ───────────
                candidates = getattr(response, "candidates", None)
                if not candidates:
                    prompt_feedback = getattr(response, "prompt_feedback", None)
                    block_reason = (
                        getattr(prompt_feedback, "block_reason", "unknown")
                        if prompt_feedback
                        else "no candidates returned"
                    )
                    raise ValueError(
                        f"Gemini returned no candidates. "
                        f"Prompt block reason: {block_reason}"
                    )

                candidate = candidates[0]
                finish_reason_code = getattr(candidate, "finish_reason", None)
                finish_reason_name = _FINISH_REASON_LABELS.get(
                    int(finish_reason_code) if finish_reason_code is not None else -1,
                    f"UNKNOWN ({finish_reason_code})",
                )

                # MAX_TOKENS (code 2): the chunk's output exceeds the model's hard
                # token ceiling (65 536 max_output_tokens). Retrying with the same
                # chunk will always reproduce the same failure, so return {} immediately
                # so extract_and_save can split the chunk in half and retry each half.
                if int(finish_reason_code or 0) == 2:
                    print(
                        f"      ⚠️  Chunk {task_id}: MAX_TOKENS — output too large for "
                        f"this chunk size. Returning empty sentinel to trigger auto-split."
                    )
                    with open(f"{debug_dir}/raw_chunk_{task_id}.txt", "w", encoding="utf-8") as f:
                        f.write(f"FINISH REASON: {finish_reason_name} (MAX_TOKENS — chunk too large)\n")
                    return {}  # triggers split in extract_and_save

                parts = getattr(candidate.content, "parts", []) if getattr(candidate, "content", None) else []
                has_text = any(getattr(p, "text", None) for p in parts)

                if not has_text:
                    raise ValueError(
                        f"Gemini candidate has no text parts. "
                        f"finish_reason={finish_reason_name}. "
                        f"Safety ratings: {getattr(candidate, 'safety_ratings', 'n/a')}"
                    )

                raw_text = response.text  # safe to access now

                # ── Track API cost ──────────────────────────────────────────
                if hasattr(response, "usage_metadata") and response.usage_metadata:
                    record_usage(
                        self.model_name,
                        int(getattr(response.usage_metadata, "prompt_token_count", 0) or 0),
                        int(getattr(response.usage_metadata, "candidates_token_count", 0) or 0),
                    )

                # ── Debug: always save raw response ──────────────────────────
                with open(f"{debug_dir}/raw_chunk_{task_id}.txt", "w", encoding="utf-8") as f:
                    f.write(f"FINISH REASON: {finish_reason_name}\n")
                    f.write(f"RESPONSE TEXT (first 10000 chars):\n{raw_text[:10000]}")

                print(f"      [DEBUG] Chunk {task_id}: Parsing JSON...")
                clean_text = self._clean_json_string(raw_text)
                result = json.loads(clean_text)
                print(f"      [DEBUG] Chunk {task_id}: JSON parsed OK")
                return result

            except (json.JSONDecodeError, ValueError) as e:
                last_exception = e
                with open(f"{debug_dir}/failed_chunk_{task_id}_attempt{attempt}.txt", "w", encoding="utf-8") as f:
                    if isinstance(e, json.JSONDecodeError):
                        f.write(f"ERROR: {str(e)}\nERROR POS: {e.pos}\nERROR MSG: {e.msg}\n\n")
                    else:
                        f.write(f"ERROR: {str(e)}\n\n")
                    f.write(f"RESPONSE TEXT:\n{raw_text[:10000]}")
                if attempt < self.MAX_RETRIES:
                    print(
                        f"      ⚠️  Chunk {task_id}: Invalid JSON/value on attempt "
                        f"{attempt}/{self.MAX_RETRIES} — retrying in {self.RETRY_DELAY}s...\n"
                        f"         Error: {e}"
                    )
                    time.sleep(self.RETRY_DELAY)
                else:
                    print(
                        f"      ❌ Chunk {task_id}: All {self.MAX_RETRIES} attempts exhausted. "
                        f"Raw (first 500 chars): {raw_text[:500]!r}"
                    )

            except Exception as e:
                last_exception = e
                with open(f"{debug_dir}/error_chunk_{task_id}_attempt{attempt}.txt", "w", encoding="utf-8") as f:
                    f.write(f"EXCEPTION TYPE: {type(e).__name__}\n")
                    f.write(f"EXCEPTION MSG: {str(e)}\n")
                # DeadlineExceeded (504) is a transient server overload — wait longer
                # before retrying so the model has time to recover.  Other errors
                # use the standard base delay.
                is_timeout = "DeadlineExceeded" in type(e).__name__ or "504" in str(e) or "timed out" in str(e).lower()
                retry_delay = self.RETRY_DELAY * (3 if is_timeout else 1) * attempt
                if attempt < self.MAX_RETRIES:
                    print(
                        f"      ⚠️  Chunk {task_id}: Unexpected error on attempt "
                        f"{attempt}/{self.MAX_RETRIES} — retrying in {retry_delay}s...\n"
                        f"         {type(e).__name__}: {e}"
                    )
                    time.sleep(retry_delay)
                else:
                    print(f"      ❌ Chunk {task_id}: All {self.MAX_RETRIES} attempts exhausted.")
                    traceback.print_exc()

        # Fix 4: FAIL-SAFE — never raise, return empty dict so extract_and_save
        # can decide whether to split or write a placeholder and continue.
        print(
            f"      ❌ ERROR: Chunk {task_id} skipped due to persistent AI failure "
            f"after {self.MAX_RETRIES} attempts. Last error: {last_exception}"
        )
        return {}  # empty sentinel — caller checks for this
