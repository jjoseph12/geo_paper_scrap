from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Dict, Iterable, Set

import requests

from .config import PaperConfig
EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"


def _cache_path(cfg: PaperConfig, gse: str, accession: str) -> Path:
    base = Path(cfg.cache_dir) / gse / "sra"
    base.mkdir(parents=True, exist_ok=True)
    return base / f"{accession}.xml"


def _fetch_sra_xml(accession: str, cfg: PaperConfig, cache_path: Path) -> str | None:
    if cache_path.exists():
        return cache_path.read_text(encoding="utf-8", errors="replace")
    params = {"db": "sra", "id": accession}
    try:
        resp = requests.get(EUTILS_BASE, params=params, timeout=60)
        if resp.status_code == 200 and resp.text.strip():
            cache_path.write_text(resp.text, encoding="utf-8")
            time.sleep(cfg.entrez_sleep_sec)
            return resp.text
    except Exception:
        return None
    return None


def _parse_study_accessions(xml_text: str) -> Set[str]:
    accessions = set()
    if not xml_text:
        return accessions
    for match in re.findall(r"STUDY\s+accession=\"(SRP\d+)\"", xml_text):
        accessions.add(match)
    return accessions


def resolve_sra_studies(gse: str, srx_ids: Iterable[str], cfg: PaperConfig) -> Dict[str, Set[str]]:
    """Return mapping of SRX to SRP accessions using NCBI SRA EFetch."""

    results: Dict[str, Set[str]] = {}
    for srx in srx_ids:
        accession = srx.strip()
        if not accession:
            continue
        cache_path = _cache_path(cfg, gse, accession)
        xml_text = _fetch_sra_xml(accession, cfg, cache_path)
        if not xml_text:
            continue
        studies = _parse_study_accessions(xml_text)
        if studies:
            results[accession] = studies
    return results
