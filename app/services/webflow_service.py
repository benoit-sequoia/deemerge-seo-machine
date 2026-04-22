from __future__ import annotations

from typing import Any, Optional

import requests

from app.config import Settings


class WebflowService:
    BASE_URL = "https://api.webflow.com/v2"

    def __init__(self, settings: Settings):
        self.token = settings.webflow_token
        self.site_id = settings.webflow_site_id
        self.collection_id = settings.webflow_collection_id

    @property
    def headers(self) -> dict[str, str]:
        if not self.token:
            raise RuntimeError("WEBFLOW_TOKEN is missing")
        return {
            "Authorization": f"Bearer {self.token}",
            "accept": "application/json",
            "content-type": "application/json",
        }

    def _request(self, method: str, path: str, *, params: Optional[dict[str, Any]] = None, json_data: Any = None) -> dict[str, Any]:
        response = requests.request(
            method,
            f"{self.BASE_URL}{path}",
            headers=self.headers,
            params=params,
            json=json_data,
            timeout=60,
        )
        response.raise_for_status()
        if not response.content:
            return {}
        return response.json()

    def collection_details(self) -> dict[str, Any]:
        if not self.collection_id:
            raise RuntimeError("WEBFLOW_COLLECTION_ID is missing")
        return self._request("GET", f"/collections/{self.collection_id}")

    def list_items(self, limit: int = 100) -> dict[str, Any]:
        if not self.collection_id:
            raise RuntimeError("WEBFLOW_COLLECTION_ID is missing")
        return self._request("GET", f"/collections/{self.collection_id}/items", params={"limit": limit})

    def find_item_by_slug(self, slug: str, limit: int = 100) -> Optional[dict[str, Any]]:
        data = self.list_items(limit=limit)
        for item in data.get("items", []):
            field_data = item.get("fieldData", {})
            if field_data.get("slug") == slug:
                return item
        return None

    def create_item(self, field_data: dict[str, Any], *, is_draft: bool = True) -> dict[str, Any]:
        payload = {"isDraft": is_draft, "isArchived": False, "fieldData": field_data}
        return self._request("POST", f"/collections/{self.collection_id}/items", json_data=payload)

    def update_item(self, item_id: str, field_data: dict[str, Any], *, is_draft: bool = True) -> dict[str, Any]:
        payload = {"isDraft": is_draft, "isArchived": False, "fieldData": field_data}
        return self._request("PATCH", f"/collections/{self.collection_id}/items/{item_id}", json_data=payload)

    def publish_items(self, item_ids: list[str]) -> dict[str, Any]:
        return self._request("POST", f"/collections/{self.collection_id}/items/publish", json_data={"itemIds": item_ids})
