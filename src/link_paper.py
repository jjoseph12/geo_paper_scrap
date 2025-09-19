from __future__ import annotations

import os
import re
import time
from html import unescape
from typing import Dict, List, Optional

import requests
from lxml import etree

from .config import PaperConfig

EUTILS_ESEARCH = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
EUTILS_ELINK = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi"
EUTILS_EFETCH = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
IDCONV_URL = "https://pmc.ncbi.nlm.nih.gov/tools/idconv/api/v1/articles/"


def _sleep(cfg: PaperConfig) -> None:
    time.sleep(cfg.entrez_sleep_sec)


def _apply_ncbi_params(params: Dict[str, str]) -> Dict[str, str]:
    out = dict(params)
    tool = os.environ.get("PMC_TOOL", "geo_metadata_harvester")
    if tool:
        out.setdefault("tool", tool)
    email = os.environ.get("NCBI_EMAIL")
    if email:
        out.setdefault("email", email)
    api_key = os.environ.get("NCBI_API_KEY")
    if api_key:
        out.setdefault("api_key", api_key)
    return out


def _entrez_xml(url: str, params: Dict[str, str], cfg: PaperConfig) -> Optional[etree._Element]:
    try:
        resp = requests.get(url, params=_apply_ncbi_params(params), timeout=60)
    except Exception:
        return None
    if resp.status_code != 200 or not resp.text.strip():
        return None
    _sleep(cfg)
    try:
        return etree.fromstring(resp.content)
    except etree.XMLSyntaxError:
        return None


def _collect_pmids_from_series(series: Dict) -> List[str]:
    pmids: List[str] = []

    def _add(value: Optional[str]) -> None:
        if not value:
            return
        pid = value.strip()
        if pid and pid not in pmids:
            pmids.append(pid)

    for pid in series.get("pubmed_ids", []) or []:
        _add(pid)
    for ref in series.get("references", []) or []:
        _add(ref.get("pubmed_id"))
    for rel in series.get("relations", []) or []:
        if isinstance(rel, dict):
            combined = " ".join(filter(None, [rel.get("type"), rel.get("value"), rel.get("target")]))
        else:
            combined = " ".join(rel)
        for match in re.findall(r"PMID:(\d+)", combined):
            _add(match)
    return pmids


def _gds_to_pubmed_pmids(gse: str, cfg: PaperConfig) -> List[str]:
    """Resolve a GEO Series accession to PubMed IDs via Entrez."""

    root = _entrez_xml(
        EUTILS_ESEARCH,
        {"db": "gds", "term": gse, "retmode": "xml", "retmax": "1000"},
        cfg,
    )
    if root is None:
        return []
    uids = [el.text for el in root.findall(".//IdList/Id") if el is not None and el.text]
    if not uids:
        return []

    root_link = _entrez_xml(
        EUTILS_ELINK,
        {
            "dbfrom": "gds",
            "db": "pubmed",
            "retmode": "xml",
            "id": ",".join(uids),
        },
        cfg,
    )
    if root_link is None:
        return []

    pmids: List[str] = []
    for id_el in root_link.findall(".//LinkSetDb[DbTo='pubmed']/Link/Id"):
        if id_el is not None and id_el.text and id_el.text not in pmids:
            pmids.append(id_el.text)
    return pmids


def _text(elem: Optional[etree._Element]) -> str:
    if elem is None:
        return ""
    return unescape(etree.tostring(elem, method="text", encoding="unicode").strip())


def _parse_pubmed_article(root: etree._Element) -> Dict[str, str]:
    article = root.find(".//PubmedArticle")
    if article is None:
        return {}
    medline = article.find(".//MedlineCitation")
    art = medline.find("Article") if medline is not None else None
    if art is None:
        return {}

    meta: Dict[str, str] = {}
    meta["title"] = _text(art.find("ArticleTitle"))
    journal = art.find("Journal")
    meta["journal"] = _text(journal.find("Title")) if journal is not None else ""
    year = journal.find("JournalIssue/PubDate/Year") if journal is not None else None
    if year is None or not _text(year):
        year = medline.find("Article/Journal/JournalIssue/PubDate/MedlineDate") if medline is not None else None
    meta["year"] = _text(year)

    article_ids = article.findall(".//ArticleIdList/ArticleId")
    for aid in article_ids:
        id_type = (aid.get("IdType") or "").lower()
        value = (aid.text or "").strip()
        if not value:
            continue
        if id_type == "doi" and "doi" not in meta:
            meta["doi"] = value
        elif id_type == "pmc" and "pmcid" not in meta:
            meta["pmcid"] = value if value.startswith("PMC") else f"PMC{value}"
        elif id_type == "pubmed" and "pmid" not in meta:
            meta["pmid"] = value

    authors = []
    for author in art.findall("AuthorList/Author"):
        last = author.findtext("LastName") or author.findtext("CollectiveName")
        fore = author.findtext("ForeName") or author.findtext("Initials")
        name = " ".join(filter(None, [fore, last])) if last else (fore or "")
        if not name:
            continue
        affiliations = [aff.text or "" for aff in author.findall("AffiliationInfo/Affiliation")]
        emails = []
        for aff in affiliations:
            emails.extend(re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+", aff))
        authors.append({"name": name.strip(), "affiliations": affiliations, "emails": emails})

    if authors:
        meta["authors"] = authors
        last_author = authors[-1]
        meta["corresponding_author_name"] = last_author.get("name", "")
        if last_author.get("emails"):
            meta["corresponding_author_email"] = last_author["emails"][0]
        else:
            for info in reversed(authors):
                if info.get("emails"):
                    meta["corresponding_author_email"] = info["emails"][0]
                    break

    citation = _format_citation(meta)
    if citation:
        meta["citation"] = citation

    return meta


def _format_citation(meta: Dict[str, str]) -> str:
    authors = meta.get("authors") or []
    if authors:
        author_names = [a["name"] for a in authors if a.get("name")]
        if len(author_names) > 6:
            author_str = ", ".join(author_names[:6]) + ", et al."
        else:
            author_str = ", ".join(author_names)
    else:
        author_str = ""
    pieces = [piece for piece in [author_str, meta.get("title", ""), meta.get("journal", ""), meta.get("year", ""), meta.get("doi", "")] if piece]
    return ". ".join(pieces)


def _fetch_pubmed_meta(pmid: str, cfg: PaperConfig) -> Dict[str, str]:
    root = _entrez_xml(
        EUTILS_EFETCH,
        {"db": "pubmed", "id": pmid, "retmode": "xml"},
        cfg,
    )
    if root is None:
        return {}
    return _parse_pubmed_article(root)


def _idconv_lookup(pmid: str) -> Dict[str, str]:
    params = {
        "ids": pmid,
        "format": "json",
    }
    params = _apply_ncbi_params(params)
    try:
        resp = requests.get(IDCONV_URL, params=params, timeout=60)
    except Exception:
        return {}
    if resp.status_code != 200:
        return {}
    try:
        payload = resp.json()
    except ValueError:
        return {}
    records = payload.get("records") or []
    if not records:
        return {}
    record = records[0]
    result: Dict[str, str] = {}
    if record.get("pmcid"):
        result["pmcid"] = record["pmcid"]
    if record.get("doi"):
        result["doi"] = record["doi"]
    return result


def link_paper(gse: str, series: Dict, derived: Dict, cfg: PaperConfig) -> Dict[str, str]:
    pmids = _gds_to_pubmed_pmids(gse, cfg)
    source = "gds_elink" if pmids else ""

    if not pmids:
        # Fallback: GEO MINiML sometimes includes PubMed IDs before GDS links exist.
        fallback_pmids = _collect_pmids_from_series(series)
        if fallback_pmids:
            pmids = fallback_pmids
            source = "miniml_fallback"

    if not pmids:
        return {"lookup_status": "not_found"}

    pmid = pmids[0]
    paper = {"pmid": pmid}

    meta = _fetch_pubmed_meta(pmid, cfg)
    paper.update({k: v for k, v in meta.items() if k != "authors"})

    idconv_data = _idconv_lookup(pmid)
    for key, value in idconv_data.items():
        paper.setdefault(key, value)

    paper["lookup_status"] = source or "found"
    return paper
