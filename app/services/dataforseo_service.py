from __future__ import annotations

from typing import Any

from app.config import Settings


class DataForSEOService:
    def __init__(self, settings: Settings):
        self.settings = settings

    def discover_keywords(self) -> list[dict[str, Any]]:
        # Placeholder until live API wiring is added.
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
        ]
