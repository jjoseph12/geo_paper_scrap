from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Dict, Optional

import requests
from lxml import etree

from .config import PaperConfig
from .util_text import clean_text

LOGGER = logging.getLogger(__name__)
PMC_OAI_URL = "https://www.ncbi.nlm.nih.gov/pmc/oai/oai.cgi"


def _cache_path(cfg: PaperConfig, gse: str, filename: str) -> Path:
    base = Path(cfg.cache_dir) / gse
    base.mkdir(parents=True, exist_ok=True)
    return base / filename


def _fetch_url(url: str, dest: Path, headers: Optional[Dict[str, str]] = None) -> Optional[Path]:
    if dest.exists():
        return dest
    try:
        resp = requests.get(url, headers=headers, timeout=60)
        if resp.status_code == 200:
            dest.write_bytes(resp.content)
            return dest
        LOGGER.warning("Download failed %s status=%s", url, resp.status_code)
    except Exception as exc:
        LOGGER.warning("Download error %s: %s", url, exc)
    return None


def _pmc_xml_to_text(xml_bytes: bytes) -> str:
    try:
        parser = etree.XMLParser(recover=True)
        root = etree.fromstring(xml_bytes, parser=parser)
    except Exception:
        return xml_bytes.decode("utf-8", errors="replace")

    texts = []
    for elem in root.iter():
        if elem.text and elem.tag not in {"abstract", "front"}:
            txt = clean_text(elem.text)
            if txt:
                texts.append(txt)
    return "\n".join(texts)


def _extract_meta_from_pmc(xml_bytes: bytes) -> Dict[str, str]:
    meta = {}
    try:
        parser = etree.XMLParser(recover=True)
        root = etree.fromstring(xml_bytes, parser=parser)
        title_el = root.find('.//article-title')
        journal_el = root.find('.//journal-title')
        year_el = root.find('.//pub-date/year')
        if title_el is not None and title_el.text:
            meta['title'] = clean_text(title_el.text)
        if journal_el is not None and journal_el.text:
            meta['journal'] = clean_text(journal_el.text)
        if year_el is not None and year_el.text:
            meta['year'] = clean_text(year_el.text)
    except Exception:
        pass
    return meta


def _read_optional(path: Path) -> Optional[str]:
    if not path or not path.exists():
        return None
    return path.read_text(encoding="utf-8", errors="replace")


def _extract_pdf_text(pdf_path: Path) -> Optional[str]:
    try:
        from pdfminer.high_level import extract_text
    except ImportError:
        LOGGER.warning("pdfminer.six not installed; cannot parse %s", pdf_path)
        return None
    try:
        text = extract_text(str(pdf_path))
        return clean_text(text)
    except Exception as exc:
        LOGGER.warning("Failed to extract PDF text from %s: %s", pdf_path, exc)
        return None


def _find_user_supplied_asset(cfg: PaperConfig, gse: str) -> Dict[str, Optional[str]]:
    papers_dir = Path(cfg.papers_dir)
    if not papers_dir.exists():
        return {}
    matches = sorted(papers_dir.glob(f"{gse}.*"))
    if not matches:
        return {}
    # Prefer XML/HTML, then PDF, then TXT
    html = next((p for p in matches if p.suffix.lower() in {".html", ".htm"}), None)
    xml = next((p for p in matches if p.suffix.lower() == ".xml"), None)
    pdf = next((p for p in matches if p.suffix.lower() == ".pdf"), None)
    txt = next((p for p in matches if p.suffix.lower() in {".txt", ".text"}), None)
    if xml:
        xml_text = _read_optional(xml)
        return {"source": "html", "text": xml_text, "xml": xml_text, "html": xml_text, "pdf_path": None}
    if html:
        html_text = _read_optional(html)
        return {"source": "html", "text": html_text, "xml": None, "html": html_text, "pdf_path": None}
    if pdf:
        pdf_text = _extract_pdf_text(pdf)
        return {"source": "pdf_text", "text": pdf_text, "xml": None, "html": None, "pdf_path": str(pdf)}
    if txt:
        txt_text = _read_optional(txt)
        return {"source": "pdf_text", "text": txt_text, "xml": None, "html": None, "pdf_path": str(txt)}
    return {}


def get_paper_assets(
    gse: str,
    pmid: Optional[str],
    doi: Optional[str],
    pmcid: Optional[str],
    cfg: PaperConfig,
) -> Dict[str, Optional[str]]:
    """Fetch paper assets prioritising PMC XML, then user-provided HTML/PDF."""

    cfg.ensure_dirs()
    result: Dict[str, Optional[str]] = {
        "source": None,
        "text": None,
        "xml": None,
        "html": None,
        "pdf_path": None,
        "meta": {},
    }

    if pmcid:
        pmcid_clean = pmcid.replace("PMC", "").strip()
        params = {
            "verb": "GetRecord",
            "identifier": f"oai:pubmedcentral.nih.gov:{pmcid_clean}",
            "metadataPrefix": "pmc",
        }
        dest = _cache_path(cfg, gse, f"{pmcid_clean}_pmc.xml")
        xml_path = _fetch_url(PMC_OAI_URL, dest, headers=None)
        if xml_path:
            xml_bytes = xml_path.read_bytes()
            text = _pmc_xml_to_text(xml_bytes)
            result.update(
                {
                    "source": "pmc_xml",
                    "text": text,
                    "xml": xml_bytes.decode("utf-8", errors="replace"),
                    "meta": _extract_meta_from_pmc(xml_bytes),
                }
            )
            return result

    user_asset = _find_user_supplied_asset(cfg, gse)
    if user_asset:
        result.update(user_asset)
        result.setdefault("meta", {})
        return result

    # As a last resort, leave result with None text so upstream can decide to call LLM later.
    LOGGER.info("No paper assets for %s (pmid=%s doi=%s pmcid=%s)", gse, pmid, doi, pmcid)
    return result
