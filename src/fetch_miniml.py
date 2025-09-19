import tarfile
from pathlib import Path

from .utils import download, safe_mkdir


def _normalize_gse(gse: str) -> str:
    prefix = gse[:3]
    digits = gse[3:]
    try:
        num = int(digits)
    except ValueError:
        return gse
    return f"{prefix}{num}"


def _series_bucket(normalized_gse: str) -> str:
    digits = normalized_gse[3:]
    try:
        num = int(digits)
    except ValueError:
        return normalized_gse[:6] + "nnn"
    bucket_prefix = num // 1000
    if bucket_prefix == 0:
        return "GSE0nnn"
    return f"GSE{bucket_prefix}nnn"


def miniml_url(gse: str) -> str:
    normalized = _normalize_gse(gse)
    bucket = _series_bucket(normalized)
    return (
        f"https://ftp.ncbi.nlm.nih.gov/geo/series/{bucket}/{normalized}/miniml/"
        f"{normalized}_family.xml.tgz"
    )

def fetch_miniml(gse: str, cache_dir: Path) -> Path:
    series_dir = cache_dir / gse / "miniml"
    safe_mkdir(series_dir)
    tgz_path = series_dir / f"{gse}_family.xml.tgz"
    url = miniml_url(gse)
    download(url, tgz_path)
    return tgz_path

def extract_xml(tgz_path: Path) -> Path:
    out_xml = tgz_path.with_suffix("").with_suffix(".xml")
    if out_xml.exists():
        return out_xml
    with tarfile.open(tgz_path, "r:gz") as tf:
        members = [m for m in tf.getmembers() if m.name.endswith(".xml")]
        if not members:
            raise RuntimeError(f"No XML in {tgz_path}")
        tf.extract(members[0], path=tgz_path.parent)
        inner = tgz_path.parent / members[0].name
        inner.rename(out_xml)
    return out_xml
