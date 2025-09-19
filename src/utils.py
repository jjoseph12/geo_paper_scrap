import os, time, hashlib, requests
from pathlib import Path

def safe_mkdir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def sha256_bytes(b: bytes):
    import hashlib
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()

def download(url: str, dest: Path, sleep=0.34, timeout=60):
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    r = requests.get(url, timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(f"Download failed {r.status_code} for {url}")
    dest.write_bytes(r.content)
    time.sleep(sleep)  # be polite
    return dest
