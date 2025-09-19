from pathlib import Path
from copy import deepcopy
from lxml import etree


def _strip_ns(tree):
    for elem in tree.getiterator():
        if not hasattr(elem.tag, "find"):
            continue
        i = elem.tag.find("}")
        if i >= 0:
            elem.tag = elem.tag[i + 1 :]
    etree.cleanup_namespaces(tree)
    return tree


def _clean(text: str) -> str:
    return (text or "").strip()


def _join_name(person_elem) -> str:
    if person_elem is None:
        return ""
    parts = []
    for tag in ("First", "Middle", "Last", "Suffix"):
        val = person_elem.findtext(tag)
        if val:
            parts.append(_clean(val))
    return " ".join([p for p in parts if p])


def _contact_from_contributor(contrib) -> dict:
    contact = {
        "ref": contrib.get("iid") or "",
        "name": "",
        "email": _clean(contrib.findtext("Email")),
        "organization": _clean(contrib.findtext("Organization")),
        "department": _clean(contrib.findtext("Department")),
        "phone": _clean(contrib.findtext("Phone")),
        "roles": [],
        "country": "",
        "address": {
            "line": _clean(contrib.findtext("Address/Line")),
            "city": _clean(contrib.findtext("Address/City")),
            "state": _clean(contrib.findtext("Address/State")),
            "postal_code": _clean(contrib.findtext("Address/Postal-Code")),
            "country": _clean(contrib.findtext("Address/Country")),
        },
    }
    person = contrib.find("Person")
    if person is not None:
        contact["name"] = _join_name(person)
    if not contact["name"]:
        contact["name"] = _clean(contrib.findtext("Name"))
    if not contact["organization"]:
        contact["organization"] = _clean(contrib.findtext("Name"))
    if not contact["address"]["country"]:
        contact["address"]["country"] = _clean(contrib.findtext("Country"))
    contact["country"] = contact["address"].get("country", "")
    contact["roles"] = [
        _clean(role.text)
        for role in contrib.findall("Role")
        if _clean(role.text)
    ]
    return contact


def _contact_from_contact_elem(contact_elem) -> dict:
    contact = {
        "ref": _clean(contact_elem.get("iid")),
        "name": _clean(contact_elem.findtext("Name")),
        "email": _clean(contact_elem.findtext("Email")),
        "organization": _clean(contact_elem.findtext("Organization")),
        "department": _clean(contact_elem.findtext("Department")),
        "phone": _clean(contact_elem.findtext("Phone")),
        "roles": [],
        "country": _clean(contact_elem.findtext("Address/Country"))
        or _clean(contact_elem.findtext("Country")),
        "address": {
            "line": _clean(contact_elem.findtext("Address/Line")),
            "city": _clean(contact_elem.findtext("Address/City")),
            "state": _clean(contact_elem.findtext("Address/State")),
            "postal_code": _clean(contact_elem.findtext("Address/Postal-Code")),
            "country": _clean(contact_elem.findtext("Address/Country"))
            or _clean(contact_elem.findtext("Country")),
        },
    }
    return contact


def _collect_instrument_models(sample_elem):
    models = set()
    for inst in sample_elem.findall("Instrument-Model"):
        text = _clean(inst.text)
        if text:
            models.add(text)
        for child in inst:
            child_text = _clean(child.text)
            if child_text:
                models.add(child_text)
    return sorted(models)


def parse_miniml(xml_path: Path):
    parser = etree.XMLParser(recover=True)
    root = etree.parse(str(xml_path), parser)
    root = _strip_ns(root)

    contributors = {}
    for contrib in root.findall("Contributor"):
        cid = contrib.get("iid") or ""
        if not cid:
            continue
        contributors[cid] = _contact_from_contributor(contrib)

    ser = root.find("Series")
    if ser is None:
        raise RuntimeError("Series section missing")

    series = {}
    series["gse"] = _clean(ser.findtext("Accession"))
    series["title"] = _clean(ser.findtext("Title"))
    summary = _clean(ser.findtext("Summary"))
    overall_design = _clean(ser.findtext("Overall-Design"))
    series["summary"] = summary or overall_design
    series["overall_design"] = overall_design
    pubmed_id = _clean(ser.findtext("Pubmed-ID"))
    if pubmed_id:
        series.setdefault("pubmed_ids", []).append(pubmed_id)

    status_elem = ser.find("Status")
    if status_elem is not None:
        series["submission_date"] = _clean(status_elem.findtext("Submission-Date"))
        series["release_date"] = _clean(status_elem.findtext("Release-Date"))
        series["last_update_date"] = _clean(status_elem.findtext("Last-Update-Date"))
    else:
        series["submission_date"] = _clean(ser.findtext("Submission-Date"))
        series["release_date"] = _clean(ser.findtext("Release-Date"))
        series["last_update_date"] = _clean(ser.findtext("Last-Update-Date"))

    series["types"] = [
        _clean(t.text)
        for t in ser.findall("Type")
        if _clean(t.text)
    ]

    relations = []
    for rel in ser.findall("Relation"):
        relations.append(
            {
                "type": _clean(rel.get("type")),
                "value": _clean(rel.text),
                "target": _clean(rel.get("target")),
            }
        )
    for rel in ser.findall("Series-Relation"):
        relations.append(
            {
                "type": _clean(rel.get("type")),
                "value": _clean(rel.text),
                "target": _clean(rel.get("target")),
            }
        )
    series["relations"] = relations

    supp = []
    for sd in ser.findall("Supplementary-Data"):
        supp.append(
            {
                "type": _clean(sd.get("type")),
                "value": _clean(sd.text),
            }
        )
    series["supplementary_data"] = supp

    references = []
    for ref in ser.findall("Reference"):
        references.append(
            {
                "citation": _clean(ref.findtext("Citation")),
                "pubmed_id": _clean(ref.findtext("PubMed-ID"))
                or _clean(ref.findtext("Pubmed-ID")),
                "pmcid": _clean(ref.findtext("PMCID"))
                or _clean(ref.findtext("PMC-ID")),
                "doi": _clean(ref.findtext("DOI")),
                "title": _clean(ref.findtext("Title")),
            }
        )
    series["references"] = references

    contact_refs = []
    for cref in ser.findall("Contact-Ref"):
        ref = _clean(cref.get("ref"))
        if ref and ref in contributors:
            contact_refs.append(deepcopy(contributors[ref]))
        elif ref:
            contact_refs.append({"ref": ref})
    if not contact_refs:
        for contact_elem in root.findall("Contact"):
            contact_refs.append(_contact_from_contact_elem(contact_elem))
    series["contacts"] = contact_refs
    series["contributors"] = list(contributors.values())

    samples = []
    for sample_elem in root.findall("Sample"):
        sample = {}
        sample["gsm"] = _clean(sample_elem.findtext("Accession"))
        sample["title"] = _clean(sample_elem.findtext("Title"))
        sample["organism"] = _clean(sample_elem.findtext("Organism"))
        sample["type"] = _clean(sample_elem.findtext("Type"))
        sample["description"] = _clean(sample_elem.findtext("Description"))
        sample["data_processing"] = _clean(sample_elem.findtext("Data-Processing"))

        status = sample_elem.find("Status")
        if status is not None:
            sample["submission_date"] = _clean(status.findtext("Submission-Date"))
            sample["last_update_date"] = _clean(status.findtext("Last-Update-Date"))
        else:
            sample["submission_date"] = ""
            sample["last_update_date"] = ""

        platform_ref = sample_elem.find("Platform-Ref")
        platform_id = ""
        if platform_ref is not None:
            platform_id = _clean(platform_ref.get("ref")) or _clean(
                platform_ref.findtext("Accession")
            )
        sample["platform_id"] = platform_id

        sample["instrument_models"] = _collect_instrument_models(sample_elem)

        sample["library_strategy"] = _clean(sample_elem.findtext("Library-Strategy"))
        sample["library_source"] = _clean(sample_elem.findtext("Library-Source"))
        sample["library_selection"] = _clean(sample_elem.findtext("Library-Selection"))

        molecules = set()
        extraction_protocols = set()
        characteristics = []
        channel_organisms = set()
        for channel in sample_elem.findall("Channel"):
            mol = _clean(channel.findtext("Molecule"))
            if mol:
                molecules.add(mol)
            prot = _clean(channel.findtext("Extract-Protocol"))
            if prot:
                extraction_protocols.add(prot)
            src = _clean(channel.findtext("Source")) or _clean(
                channel.findtext("Source-Name")
            )
            if src:
                characteristics.append(("source_name", src))
            for cc in channel.findall("Characteristics"):
                tag = _clean(cc.get("tag"))
                val = _clean(cc.text)
                if not tag and not val:
                    continue
                characteristics.append((tag, val))
            org = _clean(channel.findtext("Organism"))
            if org:
                channel_organisms.add(org)
        if channel_organisms and not sample["organism"]:
            sample["organism"] = next(iter(channel_organisms))
        sample["channel_organisms"] = sorted(channel_organisms)
        sample["molecules"] = sorted(molecules)
        sample["extraction_protocols"] = sorted(extraction_protocols)
        sample["characteristics"] = characteristics

        sample_relations = []
        for rel in sample_elem.findall("Relation"):
            sample_relations.append(
                {
                    "type": _clean(rel.get("type")),
                    "value": _clean(rel.text),
                    "target": _clean(rel.get("target")),
                }
            )
        sample["relations"] = sample_relations

        supp_data = []
        for sd in sample_elem.findall("Supplementary-Data"):
            supp_data.append(
                {
                    "type": _clean(sd.get("type")),
                    "value": _clean(sd.text),
                }
            )
        sample["supplementary_data"] = supp_data

        samples.append(sample)

    return series, samples
