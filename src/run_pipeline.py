from __future__ import annotations

import argparse
import csv
import logging
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from tqdm import tqdm

from src.fetch_miniml import extract_xml, fetch_miniml
from .config import PaperConfig
from .cost_logger import CostLogger
from .derive import derive_fields
from .export import SERIES_COLUMNS, to_series_row
from .export_clinical import merge_clinical, write_artifacts
from .extract_rules import apply_rules
from .link_paper import link_paper
from .llm_client import LLMClient
from .llm_fill import LLM_FIELDS, fill_fields
from .paper_fetch import get_paper_assets
from .paper_sections import find_snippets
from .parse_miniml import parse_miniml
from .sra_fetch import resolve_sra_studies

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

SAMPLE_COLUMNS = [
    "GSE",
    "GSM",
    "Title",
    "Organism",
    "Library Strategy",
    "Library Source",
    "Library Selection",
    "Platform ID",
    "Instrument Models",
    "Tissue/Characteristics",
]

PROBLEM_COLUMNS = ["GSE", "Problem"]


def _missing_llm_fields(rule_hits_fields: List[str]) -> List[str]:
    missing = []
    for field in LLM_FIELDS:
        if field not in rule_hits_fields:
            missing.append(field)
    return missing


def _sample_rows(series: Dict, samples: List[Dict], derived: Dict) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for sample in samples:
        characteristics = "; ".join(
            [f"{tag}:{val}" if tag else val for tag, val in sample.get("characteristics", []) if val]
        )
        rows.append(
            {
                "GSE": series.get("gse", ""),
                "GSM": sample.get("gsm", ""),
                "Title": sample.get("title", ""),
                "Organism": sample.get("organism", ""),
                "Library Strategy": sample.get("library_strategy", ""),
                "Library Source": sample.get("library_source", ""),
                "Library Selection": sample.get("library_selection", ""),
                "Platform ID": sample.get("platform_id", ""),
                "Instrument Models": "; ".join(sample.get("instrument_models", [])),
                "Tissue/Characteristics": characteristics,
            }
        )
    return rows


def process_one(
    gse: str,
    cache_dir: Path,
    cfg: PaperConfig,
    enable_papers: bool,
    enable_llm: bool,
    primary_client: Optional[LLMClient],
    fallback_client: Optional[LLMClient],
    cost_logger: Optional[CostLogger],
    out_dir: Path,
) -> Tuple[Dict[str, object], List[Dict[str, object]], List[Tuple[str, str]]]:
    problems: List[Tuple[str, str]] = []
    try:
        tgz = fetch_miniml(gse, cache_dir)
        xml = extract_xml(tgz)
        series, samples = parse_miniml(xml)
        derived = derive_fields(series, samples)

        # Resolve SRA study IDs if missing
        existing_studies = {s.strip() for s in derived.get("sra_studies", "").split(";") if s.strip()}
        experiment_tokens = {s.strip() for s in derived.get("sra_experiments", "").split(";") if s.strip()}
        if experiment_tokens:
            mapping = resolve_sra_studies(gse, experiment_tokens, cfg)
            for studies in mapping.values():
                existing_studies.update(studies)
        if existing_studies:
            derived["sra_studies"] = "; ".join(sorted(existing_studies))

        clinical_problems: List[str] = []

        paper = link_paper(gse, series, derived, cfg)
        if paper.get("lookup_status") == "not_found":
            clinical_problems.append("Publication not resolved from GEO/PubMed search")

        row = to_series_row(series, derived, paper)
        snippets = []
        rule_hits = []
        llm_result = None

        if enable_papers:
            assets = get_paper_assets(gse, paper.get("pmid"), paper.get("doi"), paper.get("pmcid"), cfg)
            if assets.get("text"):
                snippets = find_snippets(gse, assets.get("text", ""), assets.get("source") or "pmc_xml", cfg)
                rule_hits = apply_rules(snippets)
                hit_fields = [hit.field for hit in rule_hits]
                missing_fields = _missing_llm_fields(hit_fields)
                if enable_llm and missing_fields:
                    llm_result = fill_fields(
                        gse,
                        cfg,
                        snippets,
                        missing_fields,
                        primary_client,
                        fallback_client,
                        cost_logger,
                    )
                row, field_data = merge_clinical(row, rule_hits, llm_result, snippets, clinical_problems)
                write_artifacts(out_dir, gse, snippets, field_data)
            else:
                clinical_problems.append("Paper assets unavailable")
                row, field_data = merge_clinical(row, [], None, [], clinical_problems)
                write_artifacts(out_dir, gse, [], field_data)
        else:
            row, field_data = merge_clinical(row, [], None, [], clinical_problems)
            write_artifacts(out_dir, gse, [], field_data)

        if clinical_problems:
            existing = row.get("Problems", "") or ""
            combined = "; ".join([p for p in [existing] + clinical_problems if p])
            row["Problems"] = combined
        for note in clinical_problems:
            problems.append((gse, note))

        if row.get("Data Type", "") == "High throughput sequencing":
            problems.append((gse, "Data type uncertain"))
        if "No placenta" in str(row.get("Problems", "")):
            problems.append((gse, "No placenta/decidua detected"))

        sample_rows = _sample_rows(series, samples, derived)
        return row, sample_rows, problems
    except Exception as exc:
        LOGGER.exception("Processing failed for %s", gse)
        return {"GEO Series ID (GSE___)": gse, "Problems": f"ERROR: {exc}"}, [], [(gse, f"ERROR: {exc}")]


def parse_ids(path: Path) -> List[str]:
    ids = []
    with path.open(newline="") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            acc = (row.get("accession") or "").strip().strip('"')
            if acc and acc.startswith("GSE"):
                ids.append(acc)
    return ids


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ids", required=True, help="CSV with header 'accession' and GSE IDs")
    ap.add_argument("--out", default="out", help="Output directory")
    ap.add_argument("--cache", default="cache", help="Cache directory")
    ap.add_argument("--threads", type=int, default=3)
    ap.add_argument("--enable-papers", action="store_true")
    ap.add_argument("--enable-llm", action="store_true")
    ap.add_argument("--papers-dir", default="papers")
    ap.add_argument("--primary-provider", default="openai")
    ap.add_argument("--primary-model", default="gpt-4.1-mini")
    ap.add_argument("--fallback-provider", default="anthropic")
    ap.add_argument("--fallback-model", default="claude-3.5-sonnet")
    args = ap.parse_args()

    cfg = PaperConfig(
        cache_dir=args.cache,
        out_dir=args.out,
        papers_dir=args.papers_dir,
        max_threads=args.threads,
        primary_provider=args.primary_provider,
        primary_model=args.primary_model,
        fallback_provider=args.fallback_provider,
        fallback_model=args.fallback_model,
    )
    cfg.ensure_dirs()

    ids_path = Path(args.ids)
    ids = parse_ids(ids_path)
    if not ids:
        LOGGER.error("No GSE IDs found in %s", ids_path)
        sys.exit(1)

    cache_dir = Path(args.cache)
    cache_dir.mkdir(parents=True, exist_ok=True)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    primary_client = LLMClient(cfg.primary_provider, cfg.primary_model) if args.enable_llm else None
    fallback_client = LLMClient(cfg.fallback_provider, cfg.fallback_model) if args.enable_llm else None
    cost_logger = CostLogger(cfg) if args.enable_llm else None

    series_path = out_dir / "series_master.csv"
    samples_path = out_dir / "samples.csv"
    problems_path = out_dir / "problems.csv"

    with series_path.open("w", newline="") as f_series:
        series_writer = csv.DictWriter(f_series, fieldnames=SERIES_COLUMNS)
        series_writer.writeheader()
    with samples_path.open("w", newline="") as f_samples:
        sample_writer = csv.DictWriter(f_samples, fieldnames=SAMPLE_COLUMNS)
        sample_writer.writeheader()
    with problems_path.open("w", newline="") as f_probs:
        prob_writer = csv.DictWriter(f_probs, fieldnames=PROBLEM_COLUMNS)
        prob_writer.writeheader()

    series_lock = threading.Lock()
    samples_lock = threading.Lock()
    problems_lock = threading.Lock()

    def append_series(row: Dict[str, object]) -> None:
        normalized = {col: row.get(col, "") for col in SERIES_COLUMNS}
        with series_lock:
            with series_path.open("a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=SERIES_COLUMNS)
                writer.writerow(normalized)

    def append_samples(rows: List[Dict[str, object]]) -> None:
        if not rows:
            return
        with samples_lock:
            with samples_path.open("a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=SAMPLE_COLUMNS)
                for row in rows:
                    normalized = {col: row.get(col, "") for col in SAMPLE_COLUMNS}
                    writer.writerow(normalized)

    def append_problems(items: List[Tuple[str, str]]) -> None:
        if not items:
            return
        with problems_lock:
            with problems_path.open("a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=PROBLEM_COLUMNS)
                for gse, message in items:
                    writer.writerow({"GSE": gse, "Problem": message})

    with ThreadPoolExecutor(max_workers=cfg.max_threads) as ex:
        futs = {
            ex.submit(
                process_one,
                gse,
                cache_dir,
                cfg,
                args.enable_papers,
                args.enable_llm,
                primary_client,
                fallback_client,
                cost_logger,
                out_dir,
            ): gse
            for gse in ids
        }
        for fut in tqdm(as_completed(futs), total=len(futs), desc="Processing"):
            row, sample_list, problems = fut.result()
            append_series(row)
            append_samples(sample_list)
            append_problems(problems)

    # Build Excel once at the end from the accumulated CSV
    try:
        import pandas as pd

        df = pd.read_csv(series_path)
        df.to_excel(out_dir / "series_master.xlsx", index=False)
    except Exception as exc:  # pragma: no cover - Excel is best effort
        LOGGER.warning("Failed to create Excel export: %s", exc)

    if cost_logger:
        cost_logger.write_report(out_dir)


if __name__ == "__main__":
    main()
