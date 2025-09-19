import re

CONTROLLED_TYPES = [
    "Expression microarray",
    "Bulk RNA sequencing",
    "Single cell RNA sequencing",
    "Spatial transcriptomics",
    "Proteomics",
    "Whole genome sequencing",
    "Whole exome sequencing",
    "Methylation array",
    "Methylation sequencing",
    "High throughput sequencing",
    "Expression profiling by array",
    "Single nucleus RNA sequence",
]


def normalize(s: str) -> str:
    return (s or "").strip().lower()


def map_datatype(series_title, series_types, library_strategies, platforms, instruments, text_blobs):
    """Return (primary_type, additional_types, problems) using provided hints."""
    s_title = normalize(series_title)
    hints = " ".join([normalize(x) for x in text_blobs if x])
    norm_types = [normalize(t) for t in (series_types or [])]

    types = set()
    preferred = None

    # Direct mapping from GEO "Type" values
    if "expression profiling by high throughput sequencing" in norm_types:
        types.add("High throughput sequencing")
        preferred = "High throughput sequencing"
    if "expression profiling by array" in norm_types or "expression profiling by microarray" in norm_types:
        types.add("Expression profiling by array")
        preferred = preferred or "Expression profiling by array"
    if "methylation profiling by array" in norm_types:
        types.add("Methylation array")
        preferred = preferred or "Methylation array"
    if "methylation profiling by high throughput sequencing" in norm_types:
        types.add("Methylation sequencing")
        preferred = preferred or "Methylation sequencing"
    if "proteomic profiling by mass spectrometry" in norm_types or "proteomic profiling" in norm_types:
        types.add("Proteomics")
        preferred = preferred or "Proteomics"

    # Arrays
    if any("expression profiling by array" in normalize(x) for x in library_strategies) or any(
        "humanht" in normalize(p) or "affymetrix" in normalize(p) for p in platforms
    ):
        types.add("Expression microarray")

    # Methylation array
    if any("methylation profiling by array" in normalize(x) for x in library_strategies) or any(
        "450k" in normalize(p) or "epic" in normalize(p) for p in platforms
    ):
        types.add("Methylation array")

    # RNA-seq and subcategories
    if any("rna-seq" in normalize(x) or "rna seq" in normalize(x) for x in library_strategies):
        if any(k in hints for k in ["single cell", "scrna", "10x", "chromium", "drop-seq", "smart-seq"]):
            types.add("Single cell RNA sequencing")
        elif any(k in hints for k in ["single nucleus", "snrna", "snr"]):
            types.add("Single nucleus RNA sequence")
        else:
            types.add("Bulk RNA sequencing")

    # Spatial
    if any(
        k in hints
        for k in [
            "spatial transcriptomics",
            "visium",
            "hd visium",
            "slide-seq",
            "merfish",
            "seqfish",
        ]
    ):
        types.add("Spatial transcriptomics")

    # WGS/WES
    if any(normalize(x) == "wgs" or "whole genome" in normalize(x) for x in library_strategies):
        types.add("Whole genome sequencing")
    if any(normalize(x) == "wxs" or "exome" in normalize(x) for x in library_strategies):
        types.add("Whole exome sequencing")

    # Methylation sequencing
    if any(k in hints for k in ["rrbs", "reduced representation bisulfite", "wgbs", "bisulfite sequencing"]):
        types.add("Methylation sequencing")

    # Proteomics
    if any(k in hints for k in ["proteomics", "mass spectrometry", "lc-ms", "ms/ms"]):
        types.add("Proteomics")

    problems = []
    if not types:
        types.add("High throughput sequencing")
        problems.append("Unclear data type; defaulted to 'High throughput sequencing'")

    order = [
        "Spatial transcriptomics",
        "Single cell RNA sequencing",
        "Single nucleus RNA sequence",
        "Bulk RNA sequencing",
        "Methylation sequencing",
        "Whole genome sequencing",
        "Whole exome sequencing",
        "Expression microarray",
        "Methylation array",
        "Proteomics",
        "Expression profiling by array",
        "High throughput sequencing",
    ]

    primary = None
    if preferred and preferred in types:
        primary = preferred
    else:
        for t in order:
            if t in types:
                primary = t
                break

    additional = [t for t in types if t != primary]
    return primary, additional, problems
