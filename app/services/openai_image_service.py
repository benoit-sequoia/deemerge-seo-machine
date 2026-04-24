from __future__ import annotations

import base64
from pathlib import Path
from typing import Optional

import requests

from app.config import Settings


class OpenAIImageService:
    BASE_URL = "https://api.openai.com/v1/images/generations"

    def __init__(self, settings: Settings):
        self.api_key = settings.openai_api_key
        self.model = settings.openai_image_model or "gpt-image-1"
        self.size = settings.openai_image_size or "1536x1024"
        self.quality = settings.openai_image_quality or "medium"

    def available(self) -> bool:
        return bool(self.api_key)

    def generate_image_file(self, *, prompt: str, output_path: str, background: str = "opaque") -> str:
        if not self.api_key:
            raise RuntimeError("OPENAI_API_KEY is missing")
        payload = {
            "model": self.model,
            "prompt": prompt,
            "size": self.size,
            "quality": self.quality,
            "background": background,
            "output_format": "png",
        }
        r = requests.post(
            self.BASE_URL,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=300,
        )
        r.raise_for_status()
        data = r.json()
        image_obj = (data.get("data") or [{}])[0]
        b64 = image_obj.get("b64_json")
        if not b64:
            raise RuntimeError(f"OpenAI image response missing b64_json: {str(data)[:1000]}")
        raw = base64.b64decode(b64)
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(raw)
        return str(out)
