from __future__ import annotations
import base64
import requests
from ..config import Settings


class DataForSEOService:
    base_url = "https://api.dataforseo.com/v3/dataforseo_labs/google/keyword_suggestions/live"

    def __init__(self, settings: Settings):
        self.settings = settings

    def keyword_suggestions(self, keyword: str, location_code: int = 2840, language_code: str = "en") -> list[dict]:
        if not self.settings.has_dataforseo:
            return self._fallback(keyword)
        auth = base64.b64encode(f"{self.settings.dataforseo_login}:{self.settings.dataforseo_password}".encode()).decode()
        headers = {"Authorization": f"Basic {auth}", "content-type": "application/json"}
        payload = [{
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "limit": 20,
        }]
        resp = requests.post(self.base_url, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        out = []
        for task in data.get("tasks", []):
            for result in task.get("result", []):
                for item in result.get("items", []):
                    out.append({
                        "keyword": item.get("keyword") or item.get("keyword_data", {}).get("keyword"),
                        "volume": (item.get("keyword_info") or {}).get("search_volume"),
                        "difficulty": (item.get("keyword_properties") or {}).get("keyword_difficulty"),
                    })
        return [x for x in out if x.get("keyword")]

    def _fallback(self, keyword: str) -> list[dict]:
        return [
            {"keyword": keyword, "volume": 200, "difficulty": 20},
            {"keyword": f"{keyword} software", "volume": 120, "difficulty": 24},
            {"keyword": f"best {keyword}", "volume": 90, "difficulty": 28},
        ]
