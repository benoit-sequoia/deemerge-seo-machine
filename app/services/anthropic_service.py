from __future__ import annotations
import json
import re
import requests
from ..config import Settings


class AnthropicService:
    base_url = "https://api.anthropic.com/v1/messages"

    def __init__(self, settings: Settings):
        self.settings = settings

    def generate_json(self, system_prompt: str, user_prompt: str, fast: bool = False) -> dict:
        if not self.settings.has_anthropic:
            return self._fallback_json(user_prompt)
        model = self.settings.anthropic_model_fast if fast else self.settings.anthropic_model_main
        payload = {
            "model": model,
            "max_tokens": 1600,
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_prompt}],
            "temperature": 0.3,
        }
        headers = {
            "x-api-key": self.settings.anthropic_api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        resp = requests.post(self.base_url, headers=headers, json=payload, timeout=90)
        resp.raise_for_status()
        data = resp.json()
        text = "".join(block.get("text", "") for block in data.get("content", []) if block.get("type") == "text")
        try:
            return json.loads(text)
        except Exception:
            return {"raw_text": text}

    def _fallback_json(self, user_prompt: str) -> dict:
        title = "DEEMERGE workflow article"
        m = re.search(r"primary keyword:\s*(.+)", user_prompt, re.I)
        if m:
            title = m.group(1).strip().splitlines()[0].title()
        body = (
            f"<p>{title} matters because teams lose context across email and chat.</p>"
            "<h2>Why it matters</h2><p>Related conversations get scattered and hard to act on.</p>"
            "<h2>How DEEMERGE helps</h2><p>DEEMERGE groups related threads, summarizes context, and shows what needs attention.</p>"
        )
        return {
            "title_tag": title,
            "meta_description": f"{title} for teams using DEEMERGE.",
            "h1": title,
            "excerpt": f"{title} guide.",
            "body_html": body,
            "faq_json": [],
            "image_prompt": f"Flat technical editorial illustration for {title}",
            "title_options_json": [title],
            "outline_json": ["Introduction", "Why it matters", "How DEEMERGE helps"],
            "brief_text": f"Write about {title} and connect it naturally to DEEMERGE.",
        }
