from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, List

from .config import PaperConfig
from .util_text import clean_text, sliding_window, truncate


@dataclass
class Snippet:
    gse: str
    field_group: str
    source: str
    section_title: str
    text: str
    locator: str


FIELD_GROUP_KEYWORDS: Dict[str, List[str]] = {
    "ga_trimester": [
        "gestational age",
        "weeks",
        "trimester",
        "delivery",
        "collection",
        "sampling",
        "birth",
        "term",
        "preterm",
    ],
    "birthweight": ["birth weight", "birthweight", "bw", "grams", "kg"],
    "parity": ["parity", "nulliparous", "multiparous", "gravidity", "gravida", "g0p0"],
    "offspring": ["singleton", "twin", "multiple", "fetuses", "offspring"],
    "sex": ["sex", "male", "female", "fetus"],
    "race": ["race", "ethnicity", "self-reported", "hispanic", "white", "black", "asian"],
    "ancestry": ["ancestry", "strain", "c57", "european", "african", "admixed"],
    "maternal": [
        "maternal age",
        "maternal height",
        "maternal weight",
        "pre-pregnancy",
    ],
    "paternal": ["paternal age", "paternal height", "paternal weight"],
    "mode_delivery": ["cesarean", "caesarean", "c-section", "vaginal"],
    "pregnancy_complications": [
        "preeclampsia",
        "gestational diabetes",
        "hypertension",
        "preterm",
        "placenta previa",
        "placental abruption",
        "chorioamnionitis",
    ],
    "fetal_complications": [
        "fetal distress",
        "anomaly",
        "nicu",
        "iugr",
        "sga",
        "growth restriction",
    ],
    "site": ["hospital", "center", "university", "collected at", "recruited", "city", "country"],
}

_HEADING_RE = re.compile(r"(?im)^(?:\d+[.\)\-]\s+)?([A-Za-z][A-Za-z0-9 ,\-/()]{3,})\s*$")


def _build_heading_lookup(text: str) -> List[tuple[int, str]]:
    headings = []
    for match in _HEADING_RE.finditer(text):
        title = clean_text(match.group(1))
        if not title:
            continue
        headings.append((match.start(), title))
    return headings


def _nearest_heading(headings: List[tuple[int, str]], idx: int) -> str:
    last_title = ""
    for pos, title in headings:
        if pos <= idx:
            last_title = title
        else:
            break
    return last_title


def _score_window(window_text: str, keywords: List[str]) -> int:
    text_low = window_text.lower()
    score = 0
    for kw in keywords:
        score += text_low.count(kw)
    return score


def find_snippets(gse: str, paper_text: str, source: str, cfg: PaperConfig) -> List[Snippet]:
    if not paper_text:
        return []
    text = clean_text(paper_text)
    if not text:
        return []

    headings = _build_heading_lookup(text)
    snippets: List[Snippet] = []

    for field_group, keywords in FIELD_GROUP_KEYWORDS.items():
        scored: List[tuple[int, int, str]] = []  # (score, idx, text)
        for idx, window_text in sliding_window(text, cfg.window_chars, cfg.window_step):
            score = _score_window(window_text, keywords)
            if score <= 0:
                continue
            scored.append((score, idx, window_text))
        scored.sort(key=lambda x: x[0], reverse=True)
        for score, idx, window_text in scored[: cfg.max_snippets_per_field]:
            section_title = _nearest_heading(headings, idx)
            locator = f"offset:{idx}"
            snippets.append(
                Snippet(
                    gse=gse,
                    field_group=field_group,
                    source=source,
                    section_title=section_title,
                    text=truncate(window_text.strip(), cfg.window_chars),
                    locator=locator,
                )
            )
    return snippets
