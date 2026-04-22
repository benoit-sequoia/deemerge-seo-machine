from __future__ import annotations

from datetime import date, timedelta
from typing import Any
from urllib.parse import quote

import requests

from app.config import Settings


class GSCService:
    SEARCH_BASE = "https://searchconsole.googleapis.com"

    def __init__(self, settings: Settings):
        self.settings = settings

    def _access_token(self) -> str | None:
        creds_json = self.settings.decode_google_service_account()
        if not creds_json:
            return None
        try:
            from google.auth.transport.requests import Request
            from google.oauth2 import service_account
        except Exception as exc:
            raise RuntimeError("google-auth dependencies are missing") from exc
        creds = service_account.Credentials.from_service_account_info(
            creds_json,
            scopes=[
                "https://www.googleapis.com/auth/webmasters.readonly",
                "https://www.googleapis.com/auth/webmasters",
            ],
        )
        creds.refresh(Request())
        return creds.token

    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        token = self._access_token()
        if not token:
            raise RuntimeError("Search Console credentials are missing")
        response = requests.post(
            f"{self.SEARCH_BASE}{path}",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=payload,
            timeout=90,
        )
        response.raise_for_status()
        return response.json() if response.text else {}

    def query_pages(self, days: int = 28, data_state: str = "final") -> list[dict[str, Any]]:
        if not self.settings.gsc_site_url or not self.settings.google_service_account_json_b64:
            return self._fixture_pages(days)
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=days - 1)
        property_url = quote(self.settings.gsc_site_url, safe="")
        data = self._post(
            f"/webmasters/v3/sites/{property_url}/searchAnalytics/query",
            {
                "startDate": start.isoformat(),
                "endDate": end.isoformat(),
                "dimensions": ["page"],
                "rowLimit": 25000,
                "dataState": data_state,
            },
        )
        rows = []
        for row in data.get("rows", []):
            keys = row.get("keys", [])
            if not keys:
                continue
            rows.append(
                {
                    "date_start": start.isoformat(),
                    "date_end": end.isoformat(),
                    "page_url": keys[0],
                    "clicks": row.get("clicks", 0.0),
                    "impressions": row.get("impressions", 0.0),
                    "ctr": row.get("ctr", 0.0),
                    "position": row.get("position", 0.0),
                }
            )
        return rows or self._fixture_pages(days)

    def query_queries(self, days: int = 28, data_state: str = "final") -> list[dict[str, Any]]:
        if not self.settings.gsc_site_url or not self.settings.google_service_account_json_b64:
            return self._fixture_queries(days)
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=days - 1)
        property_url = quote(self.settings.gsc_site_url, safe="")
        data = self._post(
            f"/webmasters/v3/sites/{property_url}/searchAnalytics/query",
            {
                "startDate": start.isoformat(),
                "endDate": end.isoformat(),
                "dimensions": ["page", "query"],
                "rowLimit": 25000,
                "dataState": data_state,
            },
        )
        rows = []
        for row in data.get("rows", []):
            keys = row.get("keys", [])
            if len(keys) < 2:
                continue
            rows.append(
                {
                    "date_start": start.isoformat(),
                    "date_end": end.isoformat(),
                    "page_url": keys[0],
                    "query": keys[1],
                    "clicks": row.get("clicks", 0.0),
                    "impressions": row.get("impressions", 0.0),
                    "ctr": row.get("ctr", 0.0),
                    "position": row.get("position", 0.0),
                }
            )
        return rows or self._fixture_queries(days)

    def inspect_url(self, inspection_url: str) -> dict[str, Any]:
        if not self.settings.gsc_site_url or not self.settings.google_service_account_json_b64:
            return {
                "inspectionResult": {
                    "indexStatusResult": {
                        "coverageState": "Submitted and indexed",
                        "indexingState": "INDEXING_ALLOWED",
                        "lastCrawlTime": date.today().isoformat(),
                        "googleCanonical": inspection_url,
                    }
                }
            }
        return self._post(
            "/v1/urlInspection/index:inspect",
            {"inspectionUrl": inspection_url, "siteUrl": self.settings.gsc_site_url},
        )

    def _fixture_pages(self, days: int) -> list[dict[str, Any]]:
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=days - 1)
        return [
            {
                "date_start": start.isoformat(),
                "date_end": end.isoformat(),
                "page_url": f"{self.settings.blog_base_url}/best-unified-inbox-apps-in-2025",
                "clicks": 6,
                "impressions": 1805,
                "ctr": 0.003324,
                "position": 33.4,
            },
            {
                "date_start": start.isoformat(),
                "date_end": end.isoformat(),
                "page_url": f"{self.settings.blog_base_url}/integrate-gmail-and-slack",
                "clicks": 4,
                "impressions": 1107,
                "ctr": 0.00361,
                "position": 29.6,
            },
        ]

    def _fixture_queries(self, days: int) -> list[dict[str, Any]]:
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=days - 1)
        return [
            {
                "date_start": start.isoformat(),
                "date_end": end.isoformat(),
                "query": "unified inbox",
                "page_url": f"{self.settings.blog_base_url}/best-unified-inbox-apps-in-2025",
                "clicks": 0,
                "impressions": 142,
                "ctr": 0.0,
                "position": 33.4,
            },
            {
                "date_start": start.isoformat(),
                "date_end": end.isoformat(),
                "query": "gmail slack integration",
                "page_url": f"{self.settings.blog_base_url}/integrate-gmail-and-slack",
                "clicks": 1,
                "impressions": 71,
                "ctr": 0.014,
                "position": 29.6,
            },
        ]
