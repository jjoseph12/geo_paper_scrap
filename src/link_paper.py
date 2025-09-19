from __future__ import annotations

import re
import time
from html import unescape
from typing import Dict, Iterable, List, Optional, Set

import requests
from lxml import etree

from .config import PaperConfig

EUTILS_EFETCH = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
EUTILS_ESEARCH = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"


def _collect_pmids_from_series(series: Dict) -> Set[str]:
    pmids: Set[str] = set()
    for pid in series.get("pubmed_ids", []) or []:
        if pid:
            pmids.add(pid.strip())
    for ref in series.get("references", []) or []:
        pid = ref.get("pubmed_id")
        if pid:
            pmids.add(pid.strip())
    for rel in series.get("relations", []) or []:
        if isinstance(rel, dict):
            combined = " ".join(filter(None, [rel.get("type"), rel.get("value"), rel.get("target")]))
        else:
            combined = " ".join(rel)
        for match in re.findall(r"PMID:(\d+)", combined):
            pmids.add(match)
    return pmids


def _collect_dois_from_series(series: Dict) -> Set[str]:
    dois: Set[str] = set()
    for ref in series.get("references", []) or []:
        doi = ref.get("doi") or ""
        if doi:
            dois.add(doi.strip())
    for rel in series.get("relations", []) or []:
        if isinstance(rel, dict):
            combined = " ".join(filter(None, [rel.get("type"), rel.get("value"), rel.get("target")]))
        else:
            combined = " ".join(rel)
        for match in re.findall(r"10\.\S+", combined):
            dois.add(match.rstrip('.'))
    return dois


def _sleep(cfg: PaperConfig) -> None:
    time.sleep(cfg.entrez_sleep_sec)


def _efetch_pubmed_xml(pmid: str, cfg: PaperConfig) -> Optional[etree._Element]:
    params = {"db": "pubmed", "id": pmid, "retmode": "xml"}
    try:
        resp = requests.get(EUTILS_EFETCH, params=params, timeout=60)
    except Exception:
        return None
    if resp.status_code != 200 or not resp.text.strip():
        return None
    _sleep(cfg)
    try:
        root = etree.fromstring(resp.content)
        return root
    except etree.XMLSyntaxError:
        return None


def _esearch_pubmed(term: str, cfg: PaperConfig) -> List[str]:
    params = {"db": "pubmed", "term": term, "retmax": 5}
    try:
        resp = requests.get(EUTILS_ESEARCH, params=params, timeout=60)
    except Exception:
        return []
    if resp.status_code != 200 or not resp.text.strip():
        return []
    _sleep(cfg)
    try:
        root = etree.fromstring(resp.content)
    except etree.XMLSyntaxError:
        return []
    return [el.text for el in root.findall(".//IdList/Id") if el is not None and el.text]


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
        id_type = aid.get("IdType") or ""
        value = (aid.text or "").strip()
        if id_type == "doi":
            meta["doi"] = value
        if id_type == "pmc":
            meta["pmcid"] = value if value.startswith("PMC") else f"PMC{value}"
        if id_type == "pubmed":
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
            # fallback: search earlier authors for email
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


def _choose_pmid(series: Dict, cfg: PaperConfig, pmids: Set[str], dois: Set[str]) -> Optional[str]:
    if pmids:
        return sorted(pmids)[0]
    title = series.get("title") or ""
    if title:
        hits = _esearch_pubmed(title, cfg)
        if hits:
            return hits[0]
    for doi in dois:
        hits = _esearch_pubmed(doi, cfg)
        if hits:
            return hits[0]
    summary = series.get("summary") or ""
    if summary:
        hits = _esearch_pubmed(summary[:200], cfg)
        if hits:
            return hits[0]
    return None


def link_paper(gse: str, series: Dict, derived: Dict, cfg: PaperConfig) -> Dict[str, str]:
    pmids = _collect_pmids_from_series(series)
    dois = _collect_dois_from_series(series)
    chosen_pmid = _choose_pmid(series, cfg, pmids, dois)

    paper: Dict[str, str] = {}
    if chosen_pmid:
        root = _efetch_pubmed_xml(chosen_pmid, cfg)
        if root is not None:
            meta = _parse_pubmed_article(root)
            paper.update(meta)
            paper.setdefault("pmid", chosen_pmid)
    # fallback to existing DOI/PMCID from MINiML references if metadata missing
    if not paper.get("doi") and dois:
        paper["doi"] = sorted(dois)[0]
    references = series.get("references", []) or []
    if references and not paper.get("citation"):
        for ref in references:
            citation = ref.get("citation")
            if citation:
                paper["citation"] = citation
                if not paper.get("pmid") and ref.get("pubmed_id"):
                    paper["pmid"] = ref.get("pubmed_id")
                if not paper.get("pmcid") and ref.get("pmcid"):
                    paper["pmcid"] = ref.get("pmcid")
                if not paper.get("doi") and ref.get("doi"):
                    paper["doi"] = ref.get("doi")
                break
    if paper:
        paper.setdefault("lookup_status", "found")
    else:
        paper = {"lookup_status": "not_found"}
    return paper
