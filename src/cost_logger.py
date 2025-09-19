from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

from .config import PaperConfig


@dataclass
class CostEntry:
    gse: str
    provider: str
    model: str
    prompt_tokens: int
    completion_tokens: int


@dataclass
class CostLogger:
    cfg: PaperConfig
    entries: List[CostEntry] = field(default_factory=list)

    def log(self, gse: str, provider: str, model: str, usage: Dict[str, int]) -> None:
        self.entries.append(
            CostEntry(
                gse=gse,
                provider=provider,
                model=model,
                prompt_tokens=int(usage.get("prompt_tokens", 0)),
                completion_tokens=int(usage.get("completion_tokens", 0)),
            )
        )

    def totals(self) -> Dict[str, Dict[str, float]]:
        totals: Dict[str, Dict[str, float]] = {}
        for entry in self.entries:
            key = f"{entry.provider}:{entry.model}"
            provider_totals = totals.setdefault(
                key,
                {
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "cost": 0.0,
                },
            )
            provider_totals["prompt_tokens"] += entry.prompt_tokens
            provider_totals["completion_tokens"] += entry.completion_tokens
            provider_totals["cost"] += self._cost(entry)
        return totals

    def _cost(self, entry: CostEntry) -> float:
        if entry.provider == self.cfg.primary_provider and entry.model == self.cfg.primary_model:
            in_cost = self.cfg.price_in_per_mtok_primary * entry.prompt_tokens / 1_000_000
            out_cost = self.cfg.price_out_per_mtok_primary * entry.completion_tokens / 1_000_000
            return in_cost + out_cost
        if entry.provider == self.cfg.fallback_provider and entry.model == self.cfg.fallback_model:
            in_cost = self.cfg.price_in_per_mtok_fallback * entry.prompt_tokens / 1_000_000
            out_cost = self.cfg.price_out_per_mtok_fallback * entry.completion_tokens / 1_000_000
            return in_cost + out_cost
        return 0.0

    def write_report(self, out_dir: Path) -> None:
        out_dir.mkdir(parents=True, exist_ok=True)
        report = {
            "entries": [entry.__dict__ for entry in self.entries],
            "totals": self.totals(),
        }
        (out_dir / "cost_report.json").write_text(
            json.dumps(report, indent=2),
            encoding="utf-8",
        )
