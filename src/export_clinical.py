from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .extract_rules import RuleHit
from .paper_sections import Snippet
from .util_text import unique_preserve_order


CLINICAL_COLUMN_MAP = {
    "pregnancy_trimester": "Pregnancy trimester (1st, 2nd, 3rd, term (for full-term delivery), premature (for early delivery due to complications)",
    "birthweight_provided": "Birthweight of offspring provided (yes/no)",
    "ga_at_delivery_provided": "Gestational Age at delivery provided (yes/no)",
    "ga_at_delivery_weeks": "GA at delivery (weeks)",
    "ga_at_collection_provided": "Gestational Age at sample collection provided (yes/no)",
    "ga_at_collection_weeks": "GA at sample collection (weeks)",
    "sex_of_offspring_provided": "Sex of Offspring Provided (yes/no)",
    "parity_provided": "Parity provided (yes/no)",
    "gravidity_provided": "Gravidity provided (yes/no)",
    "num_offspring_per_pregnancy_provided": "Number of offspring per pregnancy provided (yes/no)",
    "race_ethnicity_provided": "Self-reported race/ethnicity of mother provided (yes/no)",
    "genetic_ancestry_or_strain_provided": "Genetic ancestry or genetic strain provided (yes/no)",
    "maternal_height_provided": "Maternal Height provided (yes/no)",
    "maternal_prepreg_weight_provided": "Maternal Pre-pregnancy Weight provided (yes/no)",
    "paternal_height_provided": "Paternal Height provided (yes/no)",
    "paternal_weight_provided": "Paternal Weight provided (yes/no)",
    "maternal_age_at_collection_provided": "Maternal age at sample collection provided (yes/no)",
    "paternal_age_at_collection_provided": "Paternal age at sample collection provided (yes/no)",
    "samples_from_pregnancy_complications_collected": "Samples from pregnancy complications collected",
    "mode_of_delivery_provided": "Mode of delivery provided (yes/no)",
    "pregnancy_complications_list": "Pregnancy complications in data set (list)",
    "fetal_complications_listed": "Fetal complications listed (yes/no)",
    "fetal_complications": "Fetal complications in data set (list)",
    "hospital_center": "Hospital/Center where samples were collected",
    "country_of_collection": "Country where samples were collected",
}

YES_NO_FIELDS = {
    key
    for key in [
        "birthweight_provided",
        "ga_at_delivery_provided",
        "ga_at_collection_provided",
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
        "fetal_complications_listed",
    ]
}

EVIDENCE_COLUMN = "Evidence (clinical)"
SOURCE_COLUMN = "Source (clinical)"
CONFIDENCE_COLUMN = "Confidence (clinical)"


def _normalise_yes_no(value: Optional[str]) -> str:
    if not value:
        return "No"
    if isinstance(value, str) and value.strip().lower() == "yes":
        return "Yes"
    return "No"


def merge_clinical(
    row: Dict[str, object],
    rule_hits: List[RuleHit],
    llm_result: Optional[Dict[str, Dict]],
    snippets: List[Snippet],
    clinical_problems: List[str],
) -> Tuple[Dict[str, object], Dict[str, Dict]]:
    field_data: Dict[str, Dict] = {}

    for hit in rule_hits:
        field_data[hit.field] = {
            "value": hit.value if hit.value is not None else ("yes" if hit.provided else None),
            "evidence": hit.evidence,
            "source": hit.source or "rule",
            "locator": hit.locator,
            "confidence": hit.confidence,
        }

    llm_values: Dict = llm_result.get("values") if llm_result else {}
    llm_confidence = 0.0
    llm_evidence = []
    if llm_values:
        llm_confidence = float(llm_values.get("confidence") or 0.0)
        llm_evidence = llm_values.get("evidence_quotes") or []
        for field, column in CLINICAL_COLUMN_MAP.items():
            if field in field_data:
                continue
            value = llm_values.get(field)
            if value is None or value == "null":
                continue
            field_data[field] = {
                "value": value,
                "evidence": "; ".join(llm_evidence) if llm_evidence else "LLM inference",
                "source": "llm",
                "locator": "llm",
                "confidence": llm_confidence,
            }

    # populate defaults
    for field, column in CLINICAL_COLUMN_MAP.items():
        info = field_data.get(field)
        if field in YES_NO_FIELDS:
            value = _normalise_yes_no(info.get("value")) if info else "No"
            row[column] = value
        else:
            value = info.get("value") if info else ""
            if field in {"pregnancy_complications_list", "fetal_complications"} and isinstance(value, (list, set)):
                value = ", ".join(sorted({str(v) for v in value}))
            row[column] = value or ""
            if not value and field in {"pregnancy_trimester", "hospital_center"}:
                clinical_problems.append(f"Missing {column}")

    evidences = []
    sources = []
    confidences = []
    for info in field_data.values():
        if info.get("evidence"):
            evidences.append(str(info["evidence"]))
        if info.get("source"):
            sources.append(str(info["source"]))
        if info.get("confidence") is not None:
            confidences.append(float(info["confidence"]))

    if llm_evidence:
        evidences.extend(llm_evidence)

    row[EVIDENCE_COLUMN] = "; ".join(unique_preserve_order(evidences))
    row[SOURCE_COLUMN] = "; ".join(unique_preserve_order(sources))
    row[CONFIDENCE_COLUMN] = max(confidences) if confidences else ""
    return row, field_data


def write_artifacts(out_dir: Path, gse: str, snippets: List[Snippet], field_data: Dict[str, Dict]) -> None:
    series_dir = out_dir / "artifacts" / gse
    series_dir.mkdir(parents=True, exist_ok=True)
    snippets_path = series_dir / "snippets.jsonl"
    with snippets_path.open("w", encoding="utf-8") as fh:
        for snip in snippets:
            fh.write(
                json.dumps(
                    {
                        "gse": snip.gse,
                        "field_group": snip.field_group,
                        "source": snip.source,
                        "section_title": snip.section_title,
                        "locator": snip.locator,
                        "text": snip.text,
                    }
                )
                + "\n"
            )
    extracted_path = series_dir / "extracted_fields.json"
    payload = {key: value for key, value in field_data.items()}
    extracted_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
