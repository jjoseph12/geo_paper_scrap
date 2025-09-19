from __future__ import annotations

import json
import logging
from typing import Dict, Optional, Tuple

LOGGER = logging.getLogger(__name__)


class LLMClient:
    """Thin wrapper for JSON completion calls. Currently a placeholder requiring manual wiring."""

    def __init__(self, provider: str, model: str):
        self.provider = provider
        self.model = model
        self._available = False
        self._client = None
        self._initialise()

    def _initialise(self) -> None:
        provider = (self.provider or "").lower()
        if provider == "openai":
            try:
                from openai import OpenAI  # type: ignore

                self._client = OpenAI()
                self._available = True
            except Exception as exc:
                LOGGER.warning("OpenAI client not available: %s", exc)
        elif provider == "anthropic":
            try:
                import anthropic  # type: ignore

                self._client = anthropic.Anthropic()
                self._available = True
            except Exception as exc:
                LOGGER.warning("Anthropic client not available: %s", exc)
        else:
            LOGGER.warning("Unsupported LLM provider '%s'", provider)

    @property
    def available(self) -> bool:
        return bool(self._available and self._client)

    def complete_json(
        self,
        system_prompt: str,
        user_prompt: str,
        schema_hint: Dict,
    ) -> Tuple[Dict, Dict[str, int]]:
        if not self.available:
            raise RuntimeError(
                f"LLM provider '{self.provider}' not available. Install SDK and set API key."
            )

        provider = (self.provider or "").lower()
        if provider == "openai":
            return self._call_openai(system_prompt, user_prompt, schema_hint)
        if provider == "anthropic":
            return self._call_anthropic(system_prompt, user_prompt, schema_hint)
        raise RuntimeError(f"Unsupported provider {self.provider}")

    def _call_openai(
        self, system_prompt: str, user_prompt: str, schema_hint: Dict
    ) -> Tuple[Dict, Dict[str, int]]:
        from openai import APIError  # type: ignore

        try:
            response = self._client.responses.create(  # type: ignore[attr-defined]
                model=self.model,
                response_format={"type": "json_schema", "json_schema": schema_hint},
                input=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            )
        except APIError as exc:  # type: ignore
            raise RuntimeError(f"OpenAI API error: {exc}") from exc

        content = response.output[0].content[0].text  # type: ignore
        usage = {
            "prompt_tokens": getattr(response, "prompt_tokens", 0),
            "completion_tokens": getattr(response, "completion_tokens", 0),
        }
        return json.loads(content), usage

    def _call_anthropic(
        self, system_prompt: str, user_prompt: str, schema_hint: Dict
    ) -> Tuple[Dict, Dict[str, int]]:
        try:
            response = self._client.messages.create(  # type: ignore[attr-defined]
                model=self.model,
                system=system_prompt,
                max_tokens=800,
                temperature=0,
                messages=[{"role": "user", "content": user_prompt}],
            )
        except Exception as exc:
            raise RuntimeError(f"Anthropic API error: {exc}") from exc

        content = "".join(
            [
                block.text
                for block in getattr(response, "content", [])
                if getattr(block, "type", "") == "text"
            ]
        )
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Anthropic JSON decode error: {exc}") from exc
        usage = {
            "prompt_tokens": getattr(response, "usage", {}).get("input_tokens", 0),
            "completion_tokens": getattr(response, "usage", {}).get("output_tokens", 0),
        }
        return parsed, usage
