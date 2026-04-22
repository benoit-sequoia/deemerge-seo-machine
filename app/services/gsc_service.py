from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from app.config import Settings


class GSCService:
    def __init__(self, settings: Settings):
        self.settings = settings

    def query_pages(self, days: int = 28) -> list[dict[str, Any]]:
        # Fixture style data until live API wiring is added.
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=days - 1)
        return [
            {
                "date_start": start.isoformat(),
                "date_end": end.isoformat(),
                "page_url": f"{self.settings.blog_base_url.rstrip('/')}/best-unified-inbox-apps-in-2025",
                "clicks": 6,
                "impressions": 1805,
                "ctr": 0.003324,
                "position": 33.4,
            },
            {
                "date_start": start.isoformat(),
                "date_end": end.isoformat(),
                "page_url": f"{self.settings.blog_base_url.rstrip('/')}/integrate-gmail-and-slack",
                "clicks": 4,
                "impressions": 1107,
                "ctr": 0.0036,
                "position": 29.1,
            },
        ]

    def query_queries(self, days: int = 28) -> list[dict[str, Any]]:
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=days - 1)
        return [
            {
                "date_start": start.isoformat(),
                "date_end": end.isoformat(),
                "query": "unified inbox",
                "page_url": f"{self.settings.blog_base_url.rstrip('/')}/best-unified-inbox-apps-in-2025",
                "clicks": 0,
                "impressions": 142,
                "ctr": 0.0,
                "position": 33.4,
            },
            {
                "date_start": start.isoformat(),
                "date_end": end.isoformat(),
                "query": "integrate gmail and slack",
                "page_url": f"{self.settings.blog_base_url.rstrip('/')}/integrate-gmail-and-slack",
                "clicks": 1,
                "impressions": 81,
                "ctr": 0.0123,
                "position": 29.1,
            },
        ]
