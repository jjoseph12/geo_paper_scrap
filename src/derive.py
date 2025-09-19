from collections import Counter, defaultdict
import re

from .tissues import TISSUE_SYNONYMS, NEGATIVE_TISSUE_HINTS
from .datatype import map_datatype


def _norm(s):
    return (s or "").strip().lower()


def _unique(seq):
    seen = set()
    out = []
    for item in seq:
        if not item:
            continue
        key = item.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(item.strip())
    return out


def _has_keyword(text: str, keywords) -> bool:
    blob = text.lower()
    return any(k in blob for k in keywords)


def _collect_keyword_matches(text: str, keyword_map) -> list:
    blob = text.lower()
    hits = []
    for label, variants in keyword_map.items():
        if any(v in blob for v in variants):
            hits.append(label)
    return sorted(set(hits))


def _tissue_hit(text):
    t = _norm(text)
    for bad in NEGATIVE_TISSUE_HINTS:
        if bad in t:
            return None
    for key, syns in TISSUE_SYNONYMS.items():
        for syn in syns:
            if syn in t:
                return key, syn
    return None


def _summarize_characteristics(char_map):
    parts = []
    for tag in sorted(char_map.keys()):
        vals = sorted(char_map[tag])
        if not vals:
            continue
        if tag and tag != "value":
            parts.append(f"{tag}: {', '.join(vals)}")
        else:
            parts.append(", ".join(vals))
    return "; ".join(parts)


def _placenta_sampling(placenta_chars, decidua_chars):
    tissue_vals = sorted(placenta_chars.get("tissue", []))
    base = ""
    if tissue_vals:
        base = "; ".join([v for v in tissue_vals if v])
    if not base:
        base = "placenta"

    notes = []
    for tag, vals in placenta_chars.items():
        if tag.lower() in {"tissue", "source_name"}:
            continue
        if not vals:
            continue
        notes.append(f"{tag}: {', '.join(sorted(vals))}")

    if decidua_chars:
        for tag, vals in decidua_chars.items():
            if not vals:
                continue
            notes.append(f"decidua {tag}: {', '.join(sorted(vals))}")

    if notes:
        return f"{base}; additional notes: {'; '.join(notes)}"
    return base


def _infer_trimester(samples, blob_text):
    blob = blob_text.lower()
    if "first trimester" in blob:
        return "1st trimester"
    if "second trimester" in blob:
        return "2nd trimester"
    if "third trimester" in blob:
        return "3rd trimester"
    if "full-term" in blob or "full term" in blob or "term delivery" in blob:
        return "term"
    if "preterm" in blob or "premature" in blob:
        return "premature"

    gestational_weeks = []
    for sample in samples:
        for tag, val in sample.get("characteristics", []):
            text = f"{tag} {val}".lower()
            if "gestational" not in text:
                continue
            nums = re.findall(r"\d+(?:\.\d+)?", text)
            for num_str in nums:
                try:
                    num = float(num_str)
                except ValueError:
                    continue
                if "day" in text or "d gest" in text or "dga" in text:
                    gestational_weeks.append(num / 7.0)
                else:
                    gestational_weeks.append(num)
    if gestational_weeks:
        avg = sum(gestational_weeks) / len(gestational_weeks)
        if avg <= 13.0:
            return "1st trimester"
        if avg <= 27.0:
            return "2nd trimester"
        return "3rd trimester"
    return ""


def derive_fields(series, samples):
    organism_values = []
    seen_orgs = set()
    for sample in samples:
        for org in [sample.get("organism", "")] + sample.get("channel_organisms", []):
            if not org:
                continue
            key = org.strip().lower()
            if key in seen_orgs:
                continue
            seen_orgs.add(key)
            organism_values.append(org.strip())
    series_organism = "; ".join(organism_values)

    plats = _unique([sample.get("platform_id", "") for sample in samples])
    insts = _unique(
        inst
        for sample in samples
        for inst in sample.get("instrument_models", [])
    )
    lib_strats = _unique(sample.get("library_strategy", "") for sample in samples)
    lib_sources = _unique(sample.get("library_source", "") for sample in samples)
    lib_selections = _unique(sample.get("library_selection", "") for sample in samples)

    molecules = _unique(
        mol for sample in samples for mol in sample.get("molecules", [])
    )
    extraction_protocols = _unique(
        prot
        for sample in samples
        for prot in sample.get("extraction_protocols", [])
    )
    data_processing_notes = _unique(
        sample.get("data_processing", "") for sample in samples
    )
    sample_descriptions = _unique(sample.get("description", "") for sample in samples)

    placenta_count = 0
    decidua_count = 0
    other_tissue_hits = Counter()
    placenta_chars = defaultdict(set)
    decidua_chars = defaultdict(set)
    all_char_map = defaultdict(set)
    text_blobs = [series.get("title", ""), series.get("summary", ""), series.get("overall_design", "")]

    relation_texts = []

    sra_studies = set()
    sra_experiments = set()
    sra_runs = set()

    def _collect_sra_tokens(text: str):
        if not text:
            return
        for token in re.findall(r"SRP\d+", text):
            sra_studies.add(token)
        for token in re.findall(r"SRX\d+", text):
            sra_experiments.add(token)
        for token in re.findall(r"SRR\d+", text):
            sra_runs.add(token)

    for sample in samples:
        char_pairs = []
        for tag, val in sample.get("characteristics", []):
            tag_clean = (tag or "").strip()
            val_clean = (val or "").strip()
            if not tag_clean and not val_clean:
                continue
            char_pairs.append(f"{tag_clean}: {val_clean}" if tag_clean else val_clean)
            key = tag_clean if tag_clean else "value"
            if val_clean:
                all_char_map[key].add(val_clean)
        blob = " ".join(char_pairs)
        if blob:
            text_blobs.append(blob)
        hit = _tissue_hit(blob)
        if hit:
            if hit[0] == "placenta":
                placenta_count += 1
                for tag, val in sample.get("characteristics", []):
                    key = (tag or "value").strip() or "value"
                    val_clean = (val or "").strip()
                    if val_clean:
                        placenta_chars[key].add(val_clean)
            elif hit[0] == "decidua":
                decidua_count += 1
                for tag, val in sample.get("characteristics", []):
                    key = (tag or "value").strip() or "value"
                    val_clean = (val or "").strip()
                    if val_clean:
                        decidua_chars[key].add(val_clean)
            else:
                other_tissue_hits[hit[0]] += 1
        if sample.get("data_processing"):
            text_blobs.append(sample["data_processing"])
        if sample.get("description"):
            text_blobs.append(sample["description"])
        for prot in sample.get("extraction_protocols", []):
            text_blobs.append(prot)

        for rel in sample.get("relations", []):
            combined = " ".join(
                filter(
                    None,
                    [
                        rel.get("type", ""),
                        rel.get("value", ""),
                        rel.get("target", ""),
                    ],
                )
            )
            _collect_sra_tokens(combined)
            target = rel.get("target", "") or ""
            _collect_sra_tokens(target)
            if "term=" in target:
                term_part = target.split("term=")[-1]
                _collect_sra_tokens(term_part)

    other_tissues_list = [f"{k}:{v}" for k, v in other_tissue_hits.most_common()]
    total_samples = len(samples)

    is_superseries = 0
    subseries = set()
    parent = None
    biosample_ids = set()
    bioproject_ids = set()
    for rel in series.get("relations", []):
        combined = " ".join(
            filter(
                None,
                [
                    rel.get("type") if isinstance(rel, dict) else rel[0],
                    rel.get("value") if isinstance(rel, dict) else rel[1],
                    rel.get("target") if isinstance(rel, dict) else "",
                ],
            )
        )
        relation_texts.append(combined)
        low = combined.lower()
        if "superseries" in low:
            is_superseries = 1
        if "subseries of" in low:
            parent_hits = re.findall(r"(GSE\d+)", combined)
            if parent_hits:
                parent = parent_hits[0]
        if any(k in low for k in ["contains", "series:", "includes", "superseries of"]):
            subseries.update(re.findall(r"(GSE\d+)", combined))
        _collect_sra_tokens(combined)
        biosample_ids.update(re.findall(r"SAM[ENDPRC]\d+", combined))
        bioproject_ids.update(re.findall(r"PRJ[A-Z]+\d+", combined))

    subseries = sorted(subseries)

    supplementary_files = []
    for item in series.get("supplementary_data", []):
        if isinstance(item, dict):
            entry = item.get("value", "")
            if item.get("type"):
                entry = f"{item['type']}: {entry}" if entry else item["type"]
        else:
            entry = str(item)
        if entry:
            supplementary_files.append(entry)

    primary_dt, additional_dt, dt_problems = map_datatype(
        series.get("title", ""),
        series.get("types", []),
        lib_strats,
        plats,
        insts,
        text_blobs,
    )

    problems = list(dt_problems)
    if placenta_count == 0 and decidua_count == 0:
        problems.append("No placenta/decidua detected; verify relevance")

    placenta_sampling = _placenta_sampling(placenta_chars, decidua_chars)
    decidua_sampling = _summarize_characteristics(decidua_chars)
    characteristics_summary = _summarize_characteristics(all_char_map)

    blob_text = "\n".join(
        filter(
            None,
            text_blobs
            + molecules
            + extraction_protocols
            + data_processing_notes
            + sample_descriptions
            + supplementary_files
            + relation_texts,
        )
    )

    boolean_keywords = {
        "birthweight": ["birthweight", "birth weight"],
        "gestational_age_delivery": ["gestational age at delivery", "ga at delivery"],
        "gestational_age_sample": ["gestational age", "gestational week", "gestational day", "weeks of gestation"],
        "sex_offspring": ["fetal sex", "sex:", "sex of fetus", "sex ("],
        "parity": ["parity"],
        "gravidity": ["gravidity", "gravida"],
        "offspring_number": ["singleton", "twins", "triplet", "number of fetuses", "number of offspring"],
        "race_ethnicity": ["race", "ethnicity"],
        "genetic_ancestry": ["ancestry", "strain"],
        "maternal_height": ["maternal height"],
        "maternal_weight": ["pre-pregnancy weight", "prepregnancy weight", "maternal weight"],
        "paternal_height": ["paternal height"],
        "paternal_weight": ["paternal weight"],
        "maternal_age": ["maternal age"],
        "paternal_age": ["paternal age"],
        "mode_of_delivery": ["mode of delivery", "cesarean", "c-section", "vaginal delivery"],
    }

    flags = {}
    for key, kws in boolean_keywords.items():
        flags[key] = "Yes" if _has_keyword(blob_text, kws) else "No"

    pregnancy_complication_terms = {
        "ectopic pregnancy": ["ectopic pregnancy"],
        "preeclampsia": ["preeclampsia", "pre-eclampsia"],
        "gestational diabetes": ["gestational diabetes"],
        "preterm birth": ["preterm", "premature birth"],
        "placenta previa": ["placenta previa"],
        "placental abruption": ["placental abruption"],
        "hypertension": ["gestational hypertension", "pregnancy-induced hypertension"],
    }
    pregnancy_complications = _collect_keyword_matches(blob_text, pregnancy_complication_terms)

    fetal_complication_terms = {
        "fetal growth restriction": ["fetal growth restriction", "iugr", "intrauterine growth restriction"],
        "congenital anomaly": ["congenital anomaly", "birth defect", "malformation"],
    }
    fetal_complications = _collect_keyword_matches(blob_text, fetal_complication_terms)

    flags["pregnancy_complications_samples"] = "Yes" if pregnancy_complications else "No"
    flags["fetal_complications_listed"] = "Yes" if fetal_complications else "No"

    trimester = _infer_trimester(samples, blob_text)

    exclude_tags = {"tissue", "source_name", "organism", "source", "characteristic"}
    phenotype_parts = []
    for tag, vals in all_char_map.items():
        if tag.lower() in exclude_tags:
            continue
        if "gestational" in tag.lower():
            continue
        phenotype_parts.append(f"{tag}: {', '.join(sorted(vals))}")
    other_phenotypes = "; ".join(sorted(phenotype_parts))

    out = {
        "organism": series_organism,
        "platform_ids": "; ".join(plats),
        "instrument_models": "; ".join(insts),
        "library_strategies": "; ".join(lib_strats),
        "library_sources": "; ".join(lib_sources),
        "library_selections": "; ".join(lib_selections),
        "extracted_molecules": "; ".join(molecules),
        "extraction_protocols": "\n\n".join(extraction_protocols),
        "data_processing": "\n\n".join(data_processing_notes),
        "sample_descriptions": "; ".join(sample_descriptions),
        "total_samples": total_samples,
        "placenta_count": placenta_count,
        "decidua_count": decidua_count,
        "placenta_sampling": placenta_sampling,
        "decidua_sampling": decidua_sampling,
        "other_tissues": "; ".join(other_tissues_list),
        "is_superseries": is_superseries,
        "subseries": "; ".join(subseries),
        "parent_gse": parent or "",
        "primary_data_type": primary_dt,
        "additional_data_types": "; ".join(additional_dt),
        "problems": "; ".join(problems),
        "supplementary_files": "; ".join(supplementary_files),
        "sra_studies": "; ".join(sorted(sra_studies)),
        "sra_experiments": "; ".join(sorted(sra_experiments)),
        "sra_runs": "; ".join(sorted(sra_runs)),
        "biosample_ids": "; ".join(sorted(biosample_ids)),
        "bioproject_ids": "; ".join(sorted(bioproject_ids)),
        "characteristics_summary": characteristics_summary,
        "assay_description": series.get("overall_design") or series.get("summary") or "",
        "flags": flags,
        "pregnancy_complications": "; ".join(pregnancy_complications),
        "fetal_complications": "; ".join(fetal_complications),
        "other_phenotypes": other_phenotypes,
        "pregnancy_trimester": trimester,
    }
    return out
