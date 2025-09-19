import os, time, hashlib, requests
from pathlib import Path

def safe_mkdir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def sha256_bytes(b: bytes):
    import hashlib
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()

def download(url: str, dest: Path, sleep=0.34, timeout=60, retries=3, backoff=2.0):
    if dest.exists() and dest.stat().st_size > 0:
        return dest

    last_error = None
    for attempt in range(retries):
        try:
            with requests.get(url, timeout=timeout, stream=True) as r:
                if r.status_code != 200:
                    raise RuntimeError(f"Download failed {r.status_code} for {url}")
                with dest.open("wb") as fh:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            fh.write(chunk)
            time.sleep(sleep)
            return dest
        except Exception as exc:
            last_error = exc
            if dest.exists():
                try:
                    dest.unlink()
                except OSError:
                    pass
            if attempt < retries - 1:
                wait = sleep * (backoff ** attempt)
                time.sleep(wait)
            else:
                break
    raise RuntimeError(f"Download failed after {retries} attempts for {url}: {last_error}")
