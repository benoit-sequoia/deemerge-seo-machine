from __future__ import annotations
import time
from datetime import date, timedelta
import jwt
import requests
from ..config import Settings


class GSCService:
    token_url = "https://oauth2.googleapis.com/token"
    search_analytics_url = "https://searchconsole.googleapis.com/webmasters/v3/sites/{site}/searchAnalytics/query"
    inspection_url = "https://searchconsole.googleapis.com/v1/urlInspection/index:inspect"
    sitemap_url = "https://www.googleapis.com/webmasters/v3/sites/{site}/sitemaps/{feedpath}"

    def __init__(self, settings: Settings):
        self.settings = settings

    def _access_token(self) -> str | None:
        info = self.settings.google_service_account_info()
        if not info:
            return None
        now = int(time.time())
        payload = {
            "iss": info["client_email"],
            "scope": "https://www.googleapis.com/auth/webmasters",
            "aud": self.token_url,
            "iat": now,
            "exp": now + 3600,
        }
        assertion = jwt.encode(payload, info["private_key"], algorithm="RS256")
        resp = requests.post(self.token_url, data={"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "assertion": assertion}, timeout=30)
        resp.raise_for_status()
        return resp.json()["access_token"]

    def query_page_data(self, days: int = 30) -> list[dict]:
        if not self.settings.has_gsc:
            return self._fallback_page_data()
        token = self._access_token()
        site = requests.utils.quote(self.settings.gsc_site_url, safe="")
        url = self.search_analytics_url.format(site=site)
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=days - 1)
        payload = {"startDate": start.isoformat(), "endDate": end.isoformat(), "dimensions": ["page"], "rowLimit": 25000, "dataState": "all"}
        resp = requests.post(url, headers={"Authorization": f"Bearer {token}"}, json=payload, timeout=60)
        resp.raise_for_status()
        return [{"page_url": row["keys"][0], "clicks": row.get("clicks", 0), "impressions": row.get("impressions", 0), "ctr": row.get("ctr", 0), "position": row.get("position", 0)} for row in resp.json().get("rows", [])]

    def query_query_data(self, days: int = 30) -> list[dict]:
        if not self.settings.has_gsc:
            return self._fallback_query_data()
        token = self._access_token()
        site = requests.utils.quote(self.settings.gsc_site_url, safe="")
        url = self.search_analytics_url.format(site=site)
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=days - 1)
        payload = {"startDate": start.isoformat(), "endDate": end.isoformat(), "dimensions": ["query", "page"], "rowLimit": 25000, "dataState": "all"}
        resp = requests.post(url, headers={"Authorization": f"Bearer {token}"}, json=payload, timeout=60)
        resp.raise_for_status()
        return [{"query": row["keys"][0], "page_url": row["keys"][1], "clicks": row.get("clicks", 0), "impressions": row.get("impressions", 0), "ctr": row.get("ctr", 0), "position": row.get("position", 0)} for row in resp.json().get("rows", [])]

    def inspect_url(self, page_url: str) -> dict:
        if not self.settings.has_gsc:
            return {"coverage_state": "Submitted and indexed", "indexing_state": "INDEXING_ALLOWED", "last_crawl_time": None}
        token = self._access_token()
        resp = requests.post(self.inspection_url, headers={"Authorization": f"Bearer {token}"}, json={"inspectionUrl": page_url, "siteUrl": self.settings.gsc_site_url}, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def submit_sitemap(self, sitemap_url: str) -> dict:
        if not self.settings.has_gsc:
            return {"ok": True, "mock": True}
        token = self._access_token()
        site = requests.utils.quote(self.settings.gsc_site_url, safe="")
        feedpath = requests.utils.quote(sitemap_url, safe="")
        url = self.sitemap_url.format(site=site, feedpath=feedpath)
        resp = requests.put(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
        return {"ok": resp.ok, "status_code": resp.status_code}

    def _fallback_page_data(self) -> list[dict]:
        return [
            {"page_url": f"{self.settings.blog_base_url}/best-unified-inbox-apps-2025", "clicks": 6, "impressions": 1805, "ctr": 0.0033, "position": 29.1},
            {"page_url": f"{self.settings.blog_base_url}/integrate-gmail-and-slack", "clicks": 4, "impressions": 1107, "ctr": 0.0036, "position": 24.0},
            {"page_url": f"{self.settings.blog_base_url}/best-shared-inbox-solution-for-collaboration", "clicks": 0, "impressions": 939, "ctr": 0.0, "position": 31.2},
            {"page_url": f"{self.settings.blog_base_url}/front-app-alternatives", "clicks": 0, "impressions": 739, "ctr": 0.0, "position": 33.7},
        ]

    def _fallback_query_data(self) -> list[dict]:
        return [
            {"query": "best unified inbox apps", "page_url": f"{self.settings.blog_base_url}/best-unified-inbox-apps-2025", "clicks": 6, "impressions": 1805, "ctr": 0.0033, "position": 29.1},
            {"query": "integrate gmail and slack", "page_url": f"{self.settings.blog_base_url}/integrate-gmail-and-slack", "clicks": 4, "impressions": 1107, "ctr": 0.0036, "position": 24.0},
            {"query": "shared inbox solution", "page_url": f"{self.settings.blog_base_url}/best-shared-inbox-solution-for-collaboration", "clicks": 0, "impressions": 939, "ctr": 0.0, "position": 31.2},
            {"query": "front alternatives", "page_url": f"{self.settings.blog_base_url}/front-app-alternatives", "clicks": 0, "impressions": 739, "ctr": 0.0, "position": 33.7},
            {"query": "deemerge", "page_url": f"{self.settings.deemerge_base_url}/", "clicks": 40, "impressions": 92, "ctr": 0.43, "position": 1.2},
        ]
