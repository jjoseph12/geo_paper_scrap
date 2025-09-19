import re
from pathlib import Path

import pandas as pd

SERIES_COLUMNS = [
    "GEO Series ID (GSE___)",
    "Data Type",
    "Additional data types included in the entry (list, if any)",
    "SuperSeries (check if yes)",
    "If SuperSeries, list GEO Series that are part of the SuperSeries",
    "Total GEO sample size",
    "Sample size (placenta)",
    "Placental sampling",
    "Sample size (decidua)",
    "Title",
    "Organism",
    "Characteristics",
    "Experiment type",
    "Extracted molecule",
    "Extraction protocol",
    "Library Strategy",
    "Library source",
    "Library selection",
    "Instrument model",
    "Assay description",
    "Data processing",
    "Platform ID (list)",
    "SRA Study ID (raw data)",
    "BioSample/BioProject ID",
    "File types/resources provided (list)",
    "Submission date",
    "Last update date",
    "Organization name",
    "Contact name",
    "E-mail(s)",
    "Country",
    "Citation",
    "PMID",
    "PMCID",
    "doi (link)",
    "Supervisor/Contact/Corresponding author name",
    "Supervisor/Contact/Corresponding author email",
    "Main topic of the publication",
    "Pregnancy trimester (1st, 2nd, 3rd, term (for full-term delivery), premature (for early delivery due to complications)",
    "Birthweight of offspring provided (yes/no)",
    "Gestational Age at delivery provided (yes/no)",
    "GA at delivery (weeks)",
    "Gestational Age at sample collection provided (yes/no)",
    "GA at sample collection (weeks)",
    "Sex of Offspring Provided (yes/no)",
    "Parity provided (yes/no)",
    "Gravidity provided (yes/no)",
    "Number of offspring per pregnancy provided (yes/no)",
    "Self-reported race/ethnicity of mother provided (yes/no)",
    "Genetic ancestry or genetic strain provided (yes/no)",
    "Maternal Height provided (yes/no)",
    "Maternal Pre-pregnancy Weight provided (yes/no)",
    "Paternal Height provided (yes/no)",
    "Paternal Weight provided (yes/no)",
    "Maternal age at sample collection provided (yes/no)",
    "Paternal age at sample collection provided (yes/no)",
    "Samples from pregnancy complications collected",
    "Mode of delivery provided (yes/no)",
    "Pregnancy complications in data set (list)",
    "Fetal complications listed (yes/no)",
    "Fetal complications in data set (list)",
    "Other Phenotypes Provided (list)",
    "Hospital/Center where samples were collected",
    "Country where samples were collected",
    "Evidence (clinical)",
    "Source (clinical)",
    "Confidence (clinical)",
    "Problems",
]


def _pick_contact(series):
    contacts = series.get("contacts", []) or []
    if not contacts:
        return {}, {}
    primary = contacts[0]
    supervisor = None
    for contact in contacts:
        roles = " ".join(contact.get("roles", []))
        department = contact.get("department", "") or ""
        name = (contact.get("name") or "").lower()
        role_blob = f"{roles} {department}".lower()
        if any(keyword in role_blob for keyword in ["corresponding", "supervisor", "pi", "principal investigator"]):
            supervisor = contact
            break
        if "corresponding" in name:
            supervisor = contact
            break
    if supervisor is None:
        supervisor = primary
    return primary, supervisor


def _main_topic(series):
    summary = (series.get("summary") or "").strip()
    if not summary:
        return ""
    parts = re.split(r"(?<=[.!?])\s+", summary)
    return parts[0].strip() if parts else summary[:200]


def _combine_ids(*values):
    ids = []
    for value in values:
        if not value:
            continue
        ids.append(value)
    return "; ".join(ids)


def to_series_row(series, derived, paper):
    primary_contact, supervisor_contact = _pick_contact(series)

    contact_country = ""
    contact_org = ""
    contact_name = ""
    contact_email = ""
    if primary_contact:
        contact_country = primary_contact.get("country") or primary_contact.get("address", {}).get("country", "")
        contact_org = primary_contact.get("organization", "")
        contact_name = primary_contact.get("name", "")
        contact_email = primary_contact.get("email", "")

    supervisor_name = supervisor_contact.get("name", "") if supervisor_contact else ""
    supervisor_email = supervisor_contact.get("email", "") if supervisor_contact else ""

    hospital_parts = []
    countries = set()
    for contact in series.get("contacts", []) or []:
        dept = contact.get("department", "")
        org = contact.get("organization", "")
        if dept:
            hospital_parts.append(dept)
        elif org:
            hospital_parts.append(org)
        countries.add(contact.get("country") or contact.get("address", {}).get("country", ""))
    hospital_center = "; ".join(_ for _ in dict.fromkeys(hospital_parts) if _)
    country_samples = "; ".join(sorted({c for c in countries if c}))

    citation = paper.get("citation", "")

    if paper.get("corresponding_author_name"):
        supervisor_name = paper.get("corresponding_author_name", supervisor_name)
    if paper.get("corresponding_author_email"):
        supervisor_email = paper.get("corresponding_author_email", supervisor_email)

    biosample_bioproject = _combine_ids(derived.get("bioproject_ids"), derived.get("biosample_ids"))
    sra_studies = derived.get("sra_studies", "")

    flags = derived.get("flags", {})

    row = {
        "GEO Series ID (GSE___)": series.get("gse", ""),
        "Data Type": derived.get("primary_data_type", ""),
        "Additional data types included in the entry (list, if any)": derived.get("additional_data_types", ""),
        "SuperSeries (check if yes)": "Yes" if derived.get("is_superseries") else "No",
        "If SuperSeries, list GEO Series that are part of the SuperSeries": derived.get("subseries", ""),
        "Total GEO sample size": derived.get("total_samples", 0),
        "Sample size (placenta)": derived.get("placenta_count", 0),
        "Placental sampling": derived.get("placenta_sampling", ""),
        "Sample size (decidua)": derived.get("decidua_count", 0),
        "Title": series.get("title", ""),
        "Organism": derived.get("organism", ""),
        "Characteristics": derived.get("characteristics_summary", ""),
        "Experiment type": "; ".join(series.get("types", [])),
        "Extracted molecule": derived.get("extracted_molecules", ""),
        "Extraction protocol": derived.get("extraction_protocols", ""),
        "Library Strategy": derived.get("library_strategies", ""),
        "Library source": derived.get("library_sources", ""),
        "Library selection": derived.get("library_selections", ""),
        "Instrument model": derived.get("instrument_models", ""),
        "Assay description": derived.get("assay_description", ""),
        "Data processing": derived.get("data_processing", ""),
        "Platform ID (list)": derived.get("platform_ids", ""),
        "SRA Study ID (raw data)": sra_studies,
        "BioSample/BioProject ID": biosample_bioproject,
        "File types/resources provided (list)": derived.get("supplementary_files", ""),
        "Submission date": series.get("submission_date", ""),
        "Last update date": series.get("last_update_date", ""),
        "Organization name": contact_org,
        "Contact name": contact_name,
        "E-mail(s)": contact_email,
        "Country": contact_country,
        "Citation": citation,
        "PMID": paper.get("pmid", ""),
        "PMCID": paper.get("pmcid", ""),
        "doi (link)": paper.get("doi", ""),
        "Supervisor/Contact/Corresponding author name": supervisor_name,
        "Supervisor/Contact/Corresponding author email": supervisor_email,
        "Main topic of the publication": _main_topic(series),
        "Pregnancy trimester (1st, 2nd, 3rd, term (for full-term delivery), premature (for early delivery due to complications)": derived.get(
            "pregnancy_trimester", ""
        ),
        "Birthweight of offspring provided (yes/no)": flags.get("birthweight", "No"),
        "Gestational Age at delivery provided (yes/no)": flags.get("gestational_age_delivery", "No"),
        "Gestational Age at sample collection provided (yes/no)": flags.get("gestational_age_sample", "No"),
        "Sex of Offspring Provided (yes/no)": flags.get("sex_offspring", "No"),
        "Parity provided (yes/no)": flags.get("parity", "No"),
        "Gravidity provided (yes/no)": flags.get("gravidity", "No"),
        "Number of offspring per pregnancy provided (yes/no)": flags.get("offspring_number", "No"),
        "Self-reported race/ethnicity of mother provided (yes/no)": flags.get("race_ethnicity", "No"),
        "Genetic ancestry or genetic strain provided (yes/no)": flags.get("genetic_ancestry", "No"),
        "Maternal Height provided (yes/no)": flags.get("maternal_height", "No"),
        "Maternal Pre-pregnancy Weight provided (yes/no)": flags.get("maternal_weight", "No"),
        "Paternal Height provided (yes/no)": flags.get("paternal_height", "No"),
        "Paternal Weight provided (yes/no)": flags.get("paternal_weight", "No"),
        "Maternal age at sample collection provided (yes/no)": flags.get("maternal_age", "No"),
        "Paternal age at sample collection provided (yes/no)": flags.get("paternal_age", "No"),
        "Samples from pregnancy complications collected": flags.get("pregnancy_complications_samples", "No"),
        "Mode of delivery provided (yes/no)": flags.get("mode_of_delivery", "No"),
        "Pregnancy complications in data set (list)": derived.get("pregnancy_complications", ""),
        "Fetal complications listed (yes/no)": flags.get("fetal_complications_listed", "No"),
        "Fetal complications in data set (list)": derived.get("fetal_complications", ""),
        "Other Phenotypes Provided (list)": derived.get("other_phenotypes", ""),
        "Hospital/Center where samples were collected": hospital_center,
        "Country where samples were collected": country_samples,
        "Problems": derived.get("problems", ""),
    }
    return row


def write_outputs(series_rows, sample_rows, problems, outdir: Path):
    outdir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(series_rows, columns=SERIES_COLUMNS)
    csv_path = outdir / "series_master.csv"
    xlsx_path = outdir / "series_master.xlsx"
    df.to_csv(csv_path, index=False)
    df.to_excel(xlsx_path, index=False)
    sample_df = pd.DataFrame(sample_rows)
    sample_df.to_csv(outdir / "samples.csv", index=False)
    pd.DataFrame(problems, columns=["GSE", "Problem"]).to_csv(
        outdir / "problems.csv", index=False
    )
