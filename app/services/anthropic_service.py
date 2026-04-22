from __future__ import annotations

import json
import requests

from app.config import Settings


class AnthropicService:
    BASE_URL = "https://api.anthropic.com/v1/messages"

    def __init__(self, settings: Settings):
        self.api_key = settings.anthropic_api_key
        self.model_main = settings.anthropic_model_main or "claude-sonnet-4-5"
        self.model_fast = settings.anthropic_model_fast or self.model_main

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    def generate(self, prompt: str, fast: bool = False, max_tokens: int = 2500) -> str:
        if not self.api_key:
            raise RuntimeError("ANTHROPIC_API_KEY is missing")
        model = self.model_fast if fast else self.model_main
        headers = {
            "x-api-key": self.api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        payload = {
            "model": model,
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        }
        response = requests.post(self.BASE_URL, headers=headers, json=payload, timeout=120)
        response.raise_for_status()
        data = response.json()
        parts = data.get("content", [])
        text_parts = [p.get("text", "") for p in parts if p.get("type") == "text"]
        return "\n".join(text_parts).strip()
