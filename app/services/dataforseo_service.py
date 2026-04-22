from __future__ import annotations

from typing import Any

import requests

from app.config import Settings


class DataForSEOService:
    BASE_URL = "https://api.dataforseo.com/v3"
    CLUSTER_SEEDS = {
        "shared_inbox": ["shared inbox", "team inbox", "gmail shared inbox"],
        "gmail_slack_coordination": ["integrate gmail and slack", "slack email workflow", "outlook slack workflow"],
        "email_triage": ["email triage", "email prioritization", "assign emails to team members"],
        "alternatives": ["front alternatives", "hiver alternative", "shared inbox vs help desk"],
    }

    def __init__(self, settings: Settings):
        self.settings = settings

    @property
    def enabled(self) -> bool:
        return bool(self.settings.dataforseo_login and self.settings.dataforseo_password)

    def _post(self, path: str, payload: list[dict[str, Any]]) -> dict[str, Any]:
        if not self.enabled:
            raise RuntimeError("DataForSEO credentials are missing")
        response = requests.post(
            f"{self.BASE_URL}{path}",
            auth=(self.settings.dataforseo_login, self.settings.dataforseo_password),
            json=payload,
            timeout=90,
        )
        response.raise_for_status()
        return response.json()

    def discover_keywords(self, limit_per_seed: int = 10) -> list[dict[str, Any]]:
        if not self.enabled:
            return self._fixture_keywords()
        ideas: list[dict[str, Any]] = []
        for cluster_key, seeds in self.CLUSTER_SEEDS.items():
            for seed in seeds[:2]:
                try:
                    data = self._post(
                        "/dataforseo_labs/google/keyword_suggestions/live",
                        [{"keyword": seed, "location_code": 2840, "language_code": "en", "limit": limit_per_seed}],
                    )
                    tasks = data.get("tasks", [])
                    for task in tasks:
                        for result in task.get("result", []):
                            for item in result.get("items", [])[:limit_per_seed]:
                                keyword = item.get("keyword")
                                if not keyword:
                                    continue
                                kinfo = item.get("keyword_info", {}) or item.get("keyword_info_normalized", {}) or {}
                                ideas.append(
                                    {
                                        "cluster_key": cluster_key,
                                        "primary_keyword": keyword,
                                        "secondary_keywords": [seed],
                                        "volume": float(kinfo.get("search_volume") or item.get("search_volume") or 0),
                                        "difficulty": float(item.get("keyword_properties", {}).get("keyword_difficulty") or 50),
                                        "intent_type": "traffic" if cluster_key == "email_triage" else "commercial",
                                    }
                                )
                except Exception:
                    continue
        deduped: dict[str, dict[str, Any]] = {}
        for item in ideas:
            if item["primary_keyword"] not in deduped:
                deduped[item["primary_keyword"]] = item
        return list(deduped.values()) or self._fixture_keywords()

    def _fixture_keywords(self) -> list[dict[str, Any]]:
        return [
            {
                "cluster_key": "shared_inbox",
                "primary_keyword": "gmail shared inbox",
                "secondary_keywords": ["shared inbox gmail", "shared inbox for teams"],
                "volume": 1200,
                "difficulty": 48,
                "intent_type": "commercial",
            },
            {
                "cluster_key": "email_triage",
                "primary_keyword": "email triage",
                "secondary_keywords": ["inbox triage", "email prioritization"],
                "volume": 900,
                "difficulty": 36,
                "intent_type": "traffic",
            },
            {
                "cluster_key": "alternatives",
                "primary_keyword": "hiver alternative",
                "secondary_keywords": ["hiver alternatives", "gmail shared inbox alternative"],
                "volume": 500,
                "difficulty": 41,
                "intent_type": "commercial",
            },
        ]
