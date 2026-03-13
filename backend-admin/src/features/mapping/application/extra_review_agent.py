import json
import os
import re
import unicodedata
import google.generativeai as genai
from src.shared.observability.cost_tracker import record_usage


class ExtraReviewAgent:
    def __init__(self, api_key):
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(
            model_name="gemini-2.5-flash",
            generation_config={
                "response_mime_type": "application/json",
                "temperature": 0.0
            }
        )

    def review_extras(self, pauta_path, offer_json_path, mapping_result, output_path=None, batch_size=10):
        """
        Review extras from mapping_result. Decide if each extra maps to a pauta item or stays EXTRA.
        Returns updated mapping_result and decisions.
        """
        if not mapping_result:
            return {"mapping_result": {"mapping": {}, "extras": []}, "decisions": [], "stats": {"mapped": 0, "extras": 0}}

        extras_list = list(mapping_result.get("extras", []))
        if not extras_list:
            return {"mapping_result": mapping_result, "decisions": [], "stats": {"mapped": 0, "extras": 0}}

        with open(pauta_path, "r", encoding="utf-8") as f:
            pauta_data = json.load(f)
        with open(offer_json_path, "r", encoding="utf-8") as f:
            offer_data = json.load(f)

        pauta_flat = self._flatten_pauta(pauta_data)

        offer_map = self._offer_map_by_id(offer_data)

        decisions = []
        mapped_count = 0
        confirmed_extras = 0

        batches = [extras_list[i:i + batch_size] for i in range(0, len(extras_list), batch_size)]
        print(f"      📋 Procesando {len(batches)} batches de extras...")
        for batch_idx, batch_ids in enumerate(batches, 1):
            print(f"      🔄 Batch {batch_idx}/{len(batches)} ({len(batch_ids)} items)...")
            payload_items = []
            for extra_id in batch_ids:
                offer_item = offer_map.get(extra_id)
                if not offer_item:
                    continue

                payload_items.append({
                    "id_oferta": extra_id,
                    "oferta": {
                        "capitulo": offer_item["capitulo"],
                        "codigo": offer_item["codigo"],
                        "desc": offer_item["desc"],
                        "precio": offer_item.get("precio")
                    }
                })

            if not payload_items:
                print(f"      ⏭️  Batch {batch_idx} vacío, saltando...")
                continue

            result = self._review_batch(payload_items, pauta_flat, batch_idx, len(batches))
            batch_mapped = 0
            batch_extras = 0
            for item in result:
                decision = item.get("decision")
                id_oferta = item.get("id_oferta")
                pauta_id = item.get("pauta_id")

                if not id_oferta:
                    continue

                if decision == "MAP" and pauta_id:
                    mapping_result.setdefault("mapping", {})[id_oferta] = pauta_id
                    if id_oferta in mapping_result.get("extras", []):
                        mapping_result["extras"].remove(id_oferta)
                    mapped_count += 1
                    batch_mapped += 1
                else:
                    confirmed_extras += 1
                    batch_extras += 1

                decisions.append(item)
            
            print(f"      ✅ Batch {batch_idx} completado: {batch_mapped} mapeados, {batch_extras} confirmados extra")

        stats = {"mapped": mapped_count, "extras": confirmed_extras}

        if output_path:
            try:
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump({"decisions": decisions, "stats": stats}, f, indent=2, ensure_ascii=False)
            except Exception:
                pass

        return {"mapping_result": mapping_result, "decisions": decisions, "stats": stats}

    def _review_batch(self, items, pauta_full, batch_idx, total_batches):
        system_prompt = (
            "ROL: Actua como perito especialista en presupuestos de obra, mediciones y comparativos. "
            "Tu tarea es revisar partidas marcadas como EXTRA en la oferta y decidir si corresponden a "
            "una partida de la pauta o si realmente son extras.\n\n"
            "DISPONES DE LA LISTA_PAUTA_COMPLETA. Debes elegir el pauta_id exacto de esa lista o "
            "confirmar EXTRA si no hay correspondencia clara.\n\n"
            "REGLA CLAVE: Si hay diferencias de dimension, marca, espesor, unidad o detalle tecnico, "
            "NO marques EXTRA si la partida pertenece al mismo sistema/alcance que existe en pauta. "
            "Esas diferencias se dejaran para el auditor. En ese caso, debes MAPEAR a la partida de pauta "
            "mas cercana por alcance tecnico.\n\n"
            "CRITERIOS TECNICOS (aplica todos):\n"
            "1) Alcance tecnico: mismo sistema, unidad funcional y ambito de trabajo.\n"
            "2) Materiales/solucion: si difiere en dimensiones o especificacion, aun puede mapear si el alcance es equivalente.\n"
            "3) Medicion y unidad: diferencias de unidad no invalidan el mapeo si el alcance es equivalente.\n"
            "4) Solo es EXTRA si no hay partida en pauta con el mismo alcance funcional.\n"
            "5) Si el match es dudoso o parcial, PREFIERE MAPEAR al candidato mas plausible y explica la diferencia.\n\n"
            "REGLAS DE DECISION:\n"
            "- Solo puedes elegir un pauta_id que exista en la LISTA_PAUTA_COMPLETA.\n"
            "- Si hay un candidato del mismo alcance, decide MAP aunque existan diferencias tecnicas.\n"
            "- Solo decide EXTRA si no hay ningun candidato razonable en toda la pauta.\n"
            "- No inventes codigos ni reasignes a capitulos no listados.\n\n"
            "FORMATO DE SALIDA (JSON estricto):\n"
            "{\"decisions\": [\n"
            "  {\"id_oferta\": \"CAP::COD\", \"decision\": \"MAP\"|\"EXTRA\", "
            "\"pauta_id\": \"CAP::COD\"|null, \"confianza\": 0.0-1.0, \"argumento\": \"...\"}\n"
            "]}\n"
            "- 'argumento' debe ser tecnico y breve (1-2 frases) y mencionar diferencias si las hay."
        )

        user_content = {
            "batch_id": batch_idx,
            "total_batches": total_batches,
            "lista_pauta_completa": pauta_full,
            "items": items
        }

        response = self.model.generate_content([system_prompt, json.dumps(user_content, ensure_ascii=False)])
        # ── Track API cost ──────────────────────────────────────────────────
        if hasattr(response, "usage_metadata") and response.usage_metadata:
            record_usage(
                "gemini-2.5-flash",
                response.usage_metadata.prompt_token_count  or 0,
                response.usage_metadata.candidates_token_count or 0,
            )
        clean_text = self._clean_json_string(response.text)
        data = json.loads(clean_text)
        return data.get("decisions", [])

    def _flatten_pauta(self, pauta_data):
        flat = []
        for cap in pauta_data:
            cap_id = str(cap.get("capitulo_codigo", "")).strip()
            for item in cap.get("partidas", []):
                code = item.get("codigo_pauta") or item.get("codigo")
                if not code:
                    continue
                flat.append({
                    "id": f"{cap_id}::{code}",
                    "capitulo": cap_id,
                    "codigo": code,
                    "desc": self._get_full_desc(item)
                })
        return flat

    def _offer_map_by_id(self, offer_data):
        offer_map = {}
        for cap in offer_data:
            cap_id = str(cap.get("capitulo_codigo", "")).strip()
            for item in cap.get("partidas", []):
                code = str(item.get("codigo", "")).strip()
                if not code:
                    continue
                unique_id = f"{cap_id}::{code}"
                offer_map[unique_id] = {
                    "capitulo": cap_id,
                    "codigo": code,
                    "desc": self._get_full_desc(item),
                    "precio": item.get("precio")
                }
        return offer_map

    def _get_full_desc(self, item):
        nombre = str(item.get("nombre", "")).strip()
        descripcion = str(item.get("descripcion", "")).strip()
        if not nombre and not descripcion:
            return "Sin descripcion"
        if not nombre:
            return descripcion
        if not descripcion:
            return nombre
        if nombre.lower() in descripcion.lower()[: len(nombre) + 5]:
            return descripcion
        return f"{nombre}\n{descripcion}"

    def _top_candidates(self, offer_desc, candidates_pool, max_candidates=5):
        scored = []
        for cand in candidates_pool:
            score = self._jaccard(offer_desc, cand.get("desc", ""))
            scored.append((score, cand))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [c for _, c in scored[:max_candidates]]

    def _jaccard(self, a, b):
        tokens_a = set(self._tokenize(a))
        tokens_b = set(self._tokenize(b))
        if not tokens_a or not tokens_b:
            return 0.0
        inter = tokens_a.intersection(tokens_b)
        union = tokens_a.union(tokens_b)
        return len(inter) / max(len(union), 1)

    def _tokenize(self, text):
        text = unicodedata.normalize("NFKD", text)
        text = "".join([c for c in text if not unicodedata.combining(c)])
        text = text.lower()
        return re.findall(r"[a-z0-9]+", text)

    def _truncate(self, text, max_len):
        text = text or ""
        if len(text) <= max_len:
            return text
        return text[: max_len - 3] + "..."

    def _clean_json_string(self, raw_text):
        text = raw_text.strip()
        text = re.sub(r"^```(?:json)?", "", text, flags=re.IGNORECASE).strip()
        text = re.sub(r"```$", "", text, flags=re.IGNORECASE).strip()
        return text

