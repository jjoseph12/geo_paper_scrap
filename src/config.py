from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class PaperConfig:
    cache_dir: str = "cache"
    out_dir: str = "out"
    papers_dir: str = "papers"
    max_threads: int = 4
    entrez_sleep_sec: float = 0.34
    max_snippets_per_field: int = 3
    window_chars: int = 1200
    window_step: int = 400
    accept_confidence: float = 0.70
    escalate_confidence: float = 0.60
    primary_provider: str = "openai"
    primary_model: str = "gpt-4.1-mini"
    fallback_provider: str = "anthropic"
    fallback_model: str = "claude-3.5-sonnet"
    price_in_per_mtok_primary: float = 0.5
    price_out_per_mtok_primary: float = 0.5
    price_in_per_mtok_fallback: float = 3.0
    price_out_per_mtok_fallback: float = 15.0

    def ensure_dirs(self) -> None:
        Path(self.cache_dir).mkdir(parents=True, exist_ok=True)
        Path(self.out_dir).mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_args(cls, args: Optional[dict] = None) -> "PaperConfig":
        if args is None:
            return cls()
        data = {}
        for field_name in cls.__dataclass_fields__:
            if args.get(field_name) is not None:
                data[field_name] = args[field_name]
        return cls(**data)


DEFAULT_CFG = PaperConfig()
