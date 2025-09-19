from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from .paper_sections import Snippet
from .util_text import clean_text


@dataclass
class RuleHit:
    field: str
    provided: bool
    value: Optional[str]
    evidence: str
    confidence: float = 1.0
    source: Optional[str] = None
    locator: Optional[str] = None


TRIMESTER_PATTERNS = [
    (re.compile(r"\b(1st|first)\s+trimester\b", re.I), "1st"),
    (re.compile(r"\b(2nd|second)\s+trimester\b", re.I), "2nd"),
    (re.compile(r"\b(3rd|third)\s+trimester\b", re.I), "3rd"),
    (re.compile(r"\bterm\b", re.I), "term"),
    (re.compile(r"\b(preterm|premature)\b", re.I), "premature"),
]

GA_DELIVERY_RE = re.compile(
    r"gestational\s+age[^.\n]{0,60}(delivery|birth)[^0-9]{0,20}(\d{2,3})(?:\s*(weeks|wk))",
    re.I,
)

GA_COLLECTION_RE = re.compile(
    r"gestational\s+age[^.\n]{0,60}(collection|sampling)[^0-9]{0,20}(\d{2,3})(?:\s*(weeks|wk))",
    re.I,
)

BIRTHWEIGHT_RE = re.compile(
    r"birth[ -]?weight[^0-9]{0,40}((\d{3,4})\s*g|(\d\.\d)\s*kg)",
    re.I,
)

PRESENCE_PATTERNS = {
    "sex_of_offspring_provided": re.compile(r"\bsex\b", re.I),
    "parity_provided": re.compile(r"\bparity\b", re.I),
    "gravidity_provided": re.compile(r"\bgravidity|gravida\b", re.I),
    "num_offspring_per_pregnancy_provided": re.compile(r"\bsingleton|twin|triplet|multiple\b", re.I),
    "race_ethnicity_provided": re.compile(r"\brace|ethnicity|self-reported\b", re.I),
    "genetic_ancestry_or_strain_provided": re.compile(r"\bancestry|strain\b", re.I),
    "maternal_height_provided": re.compile(r"maternal\s+height", re.I),
    "maternal_prepreg_weight_provided": re.compile(r"pre-?pregnancy\s+weight|maternal\s+weight", re.I),
    "paternal_height_provided": re.compile(r"paternal\s+height", re.I),
    "paternal_weight_provided": re.compile(r"paternal\s+weight", re.I),
    "maternal_age_at_collection_provided": re.compile(r"maternal\s+age", re.I),
    "paternal_age_at_collection_provided": re.compile(r"paternal\s+age", re.I),
    "mode_of_delivery_provided": re.compile(r"cesarean|caesarean|c-section|vaginal", re.I),
}

OFFSPRING_KEYWORDS = ["singleton", "twin", "triplet", "multiple", "fetuses"]
RACE_KEYWORDS = ["race", "ethnicity", "self-reported", "hispanic", "white", "black", "asian"]
ANCESTRY_KEYWORDS = ["ancestry", "strain", "c57", "european", "african", "admixed"]

PREGNANCY_COMPLICATION_KEYWORDS = {
    "preeclampsia": re.compile(r"preeclampsia|pre-eclampsia", re.I),
    "gestational diabetes": re.compile(r"gestational\s+diabetes", re.I),
    "hypertension": re.compile(r"gestational\s+hypertension|pregnancy-induced\s+hypertension", re.I),
    "preterm birth": re.compile(r"preterm\s+birth|ptb", re.I),
    "placenta previa": re.compile(r"placenta\s+previa", re.I),
    "placental abruption": re.compile(r"placental\s+abruption", re.I),
    "chorioamnionitis": re.compile(r"chorioamnionitis", re.I),
}

FETAL_COMPLICATION_KEYWORDS = {
    "fetal distress": re.compile(r"fetal\s+distress", re.I),
    "congenital anomaly": re.compile(r"congenital\s+anomal(y|ies)", re.I),
    "nicu": re.compile(r"nicu", re.I),
    "iugr": re.compile(r"iugr|intrauterine\s+growth\s+restriction", re.I),
    "sga": re.compile(r"small\s+for\s+gestational\s+age|sga", re.I),
}

SITE_RE = re.compile(
    r"(collected at|recruited from|enrolled at|delivered at|performed at|obtained from)\s+([^.;\n]+)",
    re.I,
)

COUNTRY_LIST = {
    "united states",
    "usa",
    "china",
    "canada",
    "united kingdom",
    "australia",
    "germany",
    "france",
    "spain",
    "italy",
    "brazil",
    "india",
    "japan",
    "mexico",
    "sweden",
    "norway",
    "denmark",
    "finland",
    "netherlands",
    "russia",
    "korea",
    "hong kong",
    "taiwan",
    "singapore",
    "thailand",
    "argentina",
    "south africa",
}


def _add_hit(hits: Dict[str, RuleHit], key: str, value: Optional[str], evidence: str, source: str, locator: str, provided: bool = True) -> None:
    if key in hits:
        return
    hits[key] = RuleHit(field=key, provided=provided, value=value, evidence=clean_text(evidence), source=source, locator=locator)


def apply_rules(snippets: List[Snippet]) -> List[RuleHit]:
    hits: Dict[str, RuleHit] = {}
    pregnancy_complications: Set[str] = set()
    fetal_complications: Set[str] = set()
    for snip in snippets:
        text = snip.text
        lowered = text.lower()
        # Trimester
        if "pregnancy_trimester" not in hits:
            for pattern, value in TRIMESTER_PATTERNS:
                match = pattern.search(text)
                if match:
                    _add_hit(hits, "pregnancy_trimester", value, match.group(0), snip.source, snip.locator)
                    break
        # Gestational age delivery
        if "ga_at_delivery_weeks" not in hits:
            match = GA_DELIVERY_RE.search(text)
            if match:
                weeks = match.group(2)
                _add_hit(hits, "ga_at_delivery_weeks", weeks, match.group(0), snip.source, snip.locator)
                _add_hit(hits, "ga_at_delivery_provided", "yes", match.group(0), snip.source, snip.locator)
        if "ga_at_collection_weeks" not in hits:
            match = GA_COLLECTION_RE.search(text)
            if match:
                weeks = match.group(2)
                _add_hit(hits, "ga_at_collection_weeks", weeks, match.group(0), snip.source, snip.locator)
                _add_hit(hits, "ga_at_collection_provided", "yes", match.group(0), snip.source, snip.locator)
        # Birthweight
        if "birthweight_provided" not in hits:
            match = BIRTHWEIGHT_RE.search(text)
            if match:
                _add_hit(hits, "birthweight_provided", "yes", match.group(0), snip.source, snip.locator)
        # Provided flags
        for field, pattern in PRESENCE_PATTERNS.items():
            if field in hits:
                continue
            if pattern.search(text):
                _add_hit(hits, field, "yes", pattern.search(text).group(0), snip.source, snip.locator)
        # Offspring keywords for evidence
        if "num_offspring_per_pregnancy_provided" not in hits:
            if any(kw in lowered for kw in OFFSPRING_KEYWORDS):
                _add_hit(hits, "num_offspring_per_pregnancy_provided", "yes", text, snip.source, snip.locator)
        if "race_ethnicity_provided" not in hits:
            if any(kw in lowered for kw in RACE_KEYWORDS):
                _add_hit(hits, "race_ethnicity_provided", "yes", text, snip.source, snip.locator)
        if "genetic_ancestry_or_strain_provided" not in hits:
            if any(kw in lowered for kw in ANCESTRY_KEYWORDS):
                _add_hit(hits, "genetic_ancestry_or_strain_provided", "yes", text, snip.source, snip.locator)

        # Pregnancy complications list
        for name, pattern in PREGNANCY_COMPLICATION_KEYWORDS.items():
            if pattern.search(text):
                pregnancy_complications.add(name)
        for name, pattern in FETAL_COMPLICATION_KEYWORDS.items():
            if pattern.search(text):
                fetal_complications.add(name)

        # Hospital / Country
        if "hospital_center" not in hits:
            match = SITE_RE.search(text)
            if match:
                location = clean_text(match.group(2))
                _add_hit(hits, "hospital_center", location, match.group(0), snip.source, snip.locator)
                tokens = [tok.strip() for tok in location.split(",")]
                for token in reversed(tokens):
                    token_low = token.lower()
                    if token_low in COUNTRY_LIST:
                        _add_hit(hits, "country_of_collection", token, match.group(0), snip.source, snip.locator)
                        break

    if pregnancy_complications and "pregnancy_complications_list" not in hits:
        evidence = "; ".join(sorted(pregnancy_complications))
        _add_hit(hits, "pregnancy_complications_list", ", ".join(sorted(pregnancy_complications)), evidence, "rule", "aggregate")
        _add_hit(hits, "samples_from_pregnancy_complications_collected", "yes", evidence, "rule", "aggregate")
    if fetal_complications:
        evidence = "; ".join(sorted(fetal_complications))
        if "fetal_complications" not in hits:
            _add_hit(hits, "fetal_complications", ", ".join(sorted(fetal_complications)), evidence, "rule", "aggregate")
        if "fetal_complications_listed" not in hits:
            _add_hit(hits, "fetal_complications_listed", "yes", evidence, "rule", "aggregate")

    return list(hits.values())
