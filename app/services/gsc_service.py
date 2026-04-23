from __future__ import annotations

from datetime import date, timedelta
from typing import Any
from urllib.parse import quote

from app.config import Settings

try:
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2 import service_account
except Exception:  # pragma: no cover
    AuthorizedSession = None
    service_account = None


SCOPES = [
    "https://www.googleapis.com/auth/webmasters.readonly",
    "https://www.googleapis.com/auth/webmasters",
]


class GSCService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._session = None
        info = settings.decode_google_service_account()
        if info and settings.gsc_site_url and AuthorizedSession and service_account:
            creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
            self._session = AuthorizedSession(creds)

    @property
    def is_live(self) -> bool:
        return self._session is not None and bool(self.settings.gsc_site_url)

    def _date_range(self, days: int) -> tuple[str, str]:
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=days - 1)
        return start.isoformat(), end.isoformat()

    def _query(self, dimensions: list[str], days: int = 28, row_limit: int = 25000) -> dict[str, Any]:
        if not self.is_live:
            raise RuntimeError('GSC live credentials are not configured')
        start, end = self._date_range(days)
        site_url = quote(self.settings.gsc_site_url, safe='')
        url = f'https://searchconsole.googleapis.com/webmasters/v3/sites/{site_url}/searchAnalytics/query'
        payload = {
            'startDate': start,
            'endDate': end,
            'dimensions': dimensions,
            'rowLimit': row_limit,
            'type': 'web',
            'dataState': 'final',
        }
        resp = self._session.post(url, json=payload, timeout=120)
        resp.raise_for_status()
        return resp.json()

    def query_pages(self, days: int = 28) -> list[dict[str, Any]]:
        data = self._query(['page'], days=days)
        rows = []
        for row in data.get('rows', []):
            keys = row.get('keys', [])
            if not keys:
                continue
            rows.append({
                'page_url': keys[0].rstrip('/'),
                'clicks': float(row.get('clicks', 0)),
                'impressions': float(row.get('impressions', 0)),
                'ctr': float(row.get('ctr', 0)),
                'position': float(row.get('position', 0)),
            })
        return rows

    def query_queries(self, days: int = 28) -> list[dict[str, Any]]:
        data = self._query(['page', 'query'], days=days)
        rows = []
        for row in data.get('rows', []):
            keys = row.get('keys', [])
            if len(keys) < 2:
                continue
            rows.append({
                'page_url': keys[0].rstrip('/'),
                'query': keys[1],
                'clicks': float(row.get('clicks', 0)),
                'impressions': float(row.get('impressions', 0)),
                'ctr': float(row.get('ctr', 0)),
                'position': float(row.get('position', 0)),
            })
        return rows
