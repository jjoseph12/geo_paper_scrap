import tarfile, re
from pathlib import Path
from .utils import download, safe_mkdir

def _series_bucket(gse: str) -> str:
    # GSE185119 -> GSE185nnn
    return gse[:6] + "nnn"

def miniml_url(gse: str) -> str:
    bucket = _series_bucket(gse)
    return f"https://ftp.ncbi.nlm.nih.gov/geo/series/{bucket}/{gse}/miniml/{gse}_family.xml.tgz"

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
