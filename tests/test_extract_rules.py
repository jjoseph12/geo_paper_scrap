from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.extract_rules import apply_rules, RuleHit
from src.paper_sections import Snippet


def make_snippet(text: str, field_group: str = "ga_trimester") -> Snippet:
    return Snippet(
        gse="GSETEST",
        field_group=field_group,
        source="pmc_xml",
        section_title="Methods",
        text=text,
        locator="offset:0",
    )


def test_trimester_and_gestational_age_rules():
    snippets = [
        make_snippet("Participants were in the 1st trimester with gestational age at delivery 39 weeks."),
        make_snippet(
            "Gestational age at collection was 12 weeks and birth weight 3500 g.",
            field_group="birthweight",
        ),
    ]
    hits = apply_rules(snippets)
    fields = {hit.field: hit for hit in hits}
    assert fields["pregnancy_trimester"].value == "1st"
    assert fields["ga_at_delivery_weeks"].value == "39"
    assert fields["ga_at_collection_weeks"].value == "12"
    assert fields["birthweight_provided"].value == "yes"


def test_complication_detection():
    text = "Participants with preeclampsia and IUGR were included."
    hits = apply_rules([make_snippet(text, field_group="pregnancy_complications")])
    fields = {hit.field: hit for hit in hits}
    assert fields["pregnancy_complications_list"].value == "preeclampsia"
    assert fields["samples_from_pregnancy_complications_collected"].value == "yes"
    assert fields["fetal_complications"].value == "iugr"
