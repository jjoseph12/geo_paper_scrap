from __future__ import annotations

import json
import logging
from typing import Dict, List, Optional, Tuple

from .config import PaperConfig
from .cost_logger import CostLogger
from .llm_client import LLMClient
from .paper_sections import Snippet
from .util_text import normalize_quotes

LOGGER = logging.getLogger(__name__)

LLM_FIELDS = [
    "pregnancy_trimester",
    "birthweight_provided",
    "ga_at_delivery_provided",
    "ga_at_delivery_weeks",
    "ga_at_collection_provided",
    "ga_at_collection_weeks",
    "sex_of_offspring_provided",
    "parity_provided",
    "gravidity_provided",
    "num_offspring_per_pregnancy_provided",
    "race_ethnicity_provided",
    "genetic_ancestry_or_strain_provided",
    "maternal_height_provided",
    "maternal_prepreg_weight_provided",
    "paternal_height_provided",
    "paternal_weight_provided",
    "maternal_age_at_collection_provided",
    "paternal_age_at_collection_provided",
    "samples_from_pregnancy_complications_collected",
    "mode_of_delivery_provided",
    "pregnancy_complications_list",
    "fetal_complications_listed",
    "hospital_center",
    "country_of_collection",
]

LLM_SCHEMA = {
    "name": "clinical_extraction",
    "schema": {
        "type": "object",
        "properties": {
            "pregnancy_trimester": {"type": ["string", "null"]},
            "birthweight_provided": {"type": ["string", "null"]},
            "ga_at_delivery_provided": {"type": ["string", "null"]},
            "ga_at_delivery_weeks": {"type": ["number", "null"]},
            "ga_at_collection_provided": {"type": ["string", "null"]},
            "ga_at_collection_weeks": {"type": ["number", "null"]},
            "sex_of_offspring_provided": {"type": ["string", "null"]},
            "parity_provided": {"type": ["string", "null"]},
            "gravidity_provided": {"type": ["string", "null"]},
            "num_offspring_per_pregnancy_provided": {"type": ["string", "null"]},
            "race_ethnicity_provided": {"type": ["string", "null"]},
            "genetic_ancestry_or_strain_provided": {"type": ["string", "null"]},
            "maternal_height_provided": {"type": ["string", "null"]},
            "maternal_prepreg_weight_provided": {"type": ["string", "null"]},
            "paternal_height_provided": {"type": ["string", "null"]},
            "paternal_weight_provided": {"type": ["string", "null"]},
            "maternal_age_at_collection_provided": {"type": ["string", "null"]},
            "paternal_age_at_collection_provided": {"type": ["string", "null"]},
            "samples_from_pregnancy_complications_collected": {"type": ["string", "null"]},
            "mode_of_delivery_provided": {"type": ["string", "null"]},
            "pregnancy_complications_list": {"type": ["array", "null"], "items": {"type": "string"}},
            "fetal_complications_listed": {"type": ["string", "null"]},
            "hospital_center": {"type": ["string", "null"]},
            "country_of_collection": {"type": ["string", "null"]},
            "evidence_quotes": {
                "type": "array",
                "items": {"type": "string"},
            },
            "confidence": {"type": ["number", "null"]},
        },
        "required": ["confidence", "evidence_quotes"],
    },
}

SYSTEM_PROMPT = "You are a careful information extractor. Use ONLY the provided snippets."


def _build_user_prompt(fields: List[str], snippets: List[Snippet]) -> str:
    snippet_parts = []
    for idx, snip in enumerate(snippets, start=1):
        snippet_parts.append(
            f"--- SNIPPET {idx} (locator: {snip.locator} | section: {snip.section_title}) ---\n{snip.text}"
        )
    schema_summary = json.dumps({field: "..." for field in fields}, indent=2)
    return (
        "TASK: Fill the following fields strictly from the excerpts.\n"
        f"SCHEMA (names only):\n{schema_summary}\n"
        "EXCERPTS:\n" + "\n".join(snippet_parts)
    )


def _normaliseYesNo(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = value.strip().lower()
    if value in {"yes", "no"}:
        return value
    return None


def fill_fields(
    gse: str,
    cfg: PaperConfig,
    snippets: List[Snippet],
    missing_fields: List[str],
    primary_client: Optional[LLMClient],
    fallback_client: Optional[LLMClient],
    cost_logger: Optional[CostLogger],
) -> Dict[str, Dict]:
    if not missing_fields or not snippets:
        return {}
    if primary_client is None or not primary_client.available:
        LOGGER.info("LLM disabled or unavailable; skipping LLM fill for %s", gse)
        return {}

    # Pick top snippets (already filtered per field). We just pass all.
    user_prompt = _build_user_prompt(missing_fields, snippets)

    try:
        parsed, usage = primary_client.complete_json(SYSTEM_PROMPT, user_prompt, LLM_SCHEMA)
        if cost_logger and usage:
            cost_logger.log(gse, primary_client.provider, primary_client.model, usage)
    except Exception as exc:
        LOGGER.warning("Primary LLM failed for %s: %s", gse, exc)
        parsed = None

    if parsed:
        parsed = _post_process(parsed)
        confidence = float(parsed.get("confidence")) if parsed.get("confidence") is not None else 0.0
        if confidence < cfg.escalate_confidence and fallback_client and fallback_client.available:
            LOGGER.info("Escalating %s to fallback model", gse)
            try:
                parsed_fb, usage_fb = fallback_client.complete_json(SYSTEM_PROMPT, user_prompt, LLM_SCHEMA)
                if cost_logger and usage_fb:
                    cost_logger.log(gse, fallback_client.provider, fallback_client.model, usage_fb)
                parsed_fb = _post_process(parsed_fb)
                if (parsed_fb.get("confidence") or 0) > confidence:
                    parsed = parsed_fb
                    confidence = parsed_fb.get("confidence") or 0
            except Exception as exc:
                LOGGER.warning("Fallback LLM failed for %s: %s", gse, exc)
        return {"values": parsed, "source": "llm"}
    return {}


def _post_process(data: Dict) -> Dict:
    evidence = data.get("evidence_quotes") or []
    evidence = [normalize_quotes(e.strip()) for e in evidence if isinstance(e, str) and e.strip()]
    data["evidence_quotes"] = evidence
    confidence = data.get("confidence") or 0.0
    try:
        confidence = float(confidence)
    except Exception:
        confidence = 0.0
    if evidence:
        data["confidence"] = max(0.0, min(1.0, confidence))
    else:
        data["confidence"] = 0.3
    for field in LLM_FIELDS:
        if field.endswith("_provided"):
            data[field] = _normaliseYesNo(data.get(field))
    return data
