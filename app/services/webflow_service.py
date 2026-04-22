from __future__ import annotations

from typing import Any

import requests

from app.config import Settings


class WebflowService:
    BASE_URL = "https://api.webflow.com/v2"

    def __init__(self, settings: Settings):
        self.token = settings.webflow_token
        self.site_id = settings.webflow_site_id
        self.collection_id = settings.webflow_collection_id

    @property
    def enabled(self) -> bool:
        return bool(self.token and self.collection_id)

    @property
    def headers(self) -> dict[str, str]:
        if not self.token:
            raise RuntimeError("WEBFLOW_TOKEN is missing")
        return {
            "Authorization": f"Bearer {self.token}",
            "accept": "application/json",
            "content-type": "application/json",
        }

    def _request(self, method: str, path: str, *, json_payload: dict[str, Any] | None = None, params: dict[str, Any] | None = None) -> dict[str, Any]:
        response = requests.request(method, f"{self.BASE_URL}{path}", headers=self.headers, json=json_payload, params=params, timeout=60)
        response.raise_for_status()
        if response.text:
            return response.json()
        return {}

    def list_items(self, limit: int = 100, offset: int = 0) -> dict:
        return self._request("GET", f"/collections/{self.collection_id}/items", params={"limit": limit, "offset": offset})

    def list_all_items(self, limit: int = 100) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        offset = 0
        while True:
            data = self.list_items(limit=limit, offset=offset)
            batch = data.get("items", [])
            if not batch:
                break
            items.extend(batch)
            pagination = data.get("pagination", {})
            total = pagination.get("total")
            if total is None or len(items) >= total or len(batch) < limit:
                break
            offset += limit
        return items

    def create_staged_item(self, field_data: dict[str, Any], is_draft: bool = True, is_archived: bool = False) -> dict[str, Any]:
        payload = {"fieldData": field_data, "isArchived": is_archived, "isDraft": is_draft}
        return self._request("POST", f"/collections/{self.collection_id}/items/bulk", json_payload=payload, params={"skipInvalidFiles": "true"})

    def update_staged_item(self, item_id: str, field_data: dict[str, Any], is_draft: bool = True, is_archived: bool = False) -> dict[str, Any]:
        payload = {"items": [{"id": item_id, "isArchived": is_archived, "isDraft": is_draft, "fieldData": field_data}]}
        return self._request("PATCH", f"/collections/{self.collection_id}/items", json_payload=payload, params={"skipInvalidFiles": "true"})

    def publish_items(self, item_ids: list[str]) -> dict[str, Any]:
        return self._request("POST", f"/collections/{self.collection_id}/items/publish", json_payload={"itemIds": item_ids})
