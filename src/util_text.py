from __future__ import annotations

import re
from typing import Iterable, List


_WHITESPACE_RE = re.compile(r"\s+")


def clean_text(text: str) -> str:
    if text is None:
        return ""
    text = text.replace("\xa0", " ")
    return _WHITESPACE_RE.sub(" ", text).strip()


def normalize_quotes(text: str) -> str:
    if text is None:
        return ""
    return text.replace("“", '"').replace("”", '"').replace("’", "'")


def sliding_window(text: str, window: int, step: int) -> Iterable[tuple[int, str]]:
    if window <= 0:
        yield 0, text
        return
    length = len(text)
    if length <= window:
        yield 0, text
        return
    idx = 0
    while idx < length:
        slice_text = text[idx : idx + window]
        yield idx, slice_text
        if idx + window >= length:
            break
        idx += step


def truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def unique_preserve_order(values: Iterable[str]) -> List[str]:
    seen = set()
    result = []
    for value in values:
        if not value:
            continue
        key = value.lower().strip()
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result
