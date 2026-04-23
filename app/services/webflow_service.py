from __future__ import annotations

from typing import Any, Optional

import hashlib
import mimetypes
from pathlib import Path

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

    def get_item(self, item_id: str) -> dict[str, Any]:
        if not self.collection_id:
            raise RuntimeError("WEBFLOW_COLLECTION_ID is missing")
        return self._request("GET", f"/collections/{self.collection_id}/items/{item_id}")

    def find_item_by_slug(self, slug: str, limit: int = 100) -> Optional[dict[str, Any]]:
        data = self.list_items(limit=limit)
        for item in data.get("items", []):
            field_data = item.get("fieldData", {})
            if field_data.get("slug") == slug:
                return item
        return None

    def find_fallback_image_field_value(self, *field_slugs: str, limit: int = 100) -> Optional[dict[str, Any]]:
        data = self.list_items(limit=limit)
        for item in data.get("items", []):
            field_data = item.get("fieldData", {})
            for slug in field_slugs:
                value = field_data.get(slug)
                if isinstance(value, dict) and value.get("url"):
                    out = {"url": value.get("url")}
                    if value.get("alt") is not None:
                        out["alt"] = value.get("alt")
                    if value.get("fileId"):
                        out["fileId"] = value.get("fileId")
                    return out
        return None

    def upload_asset_file(self, file_path: str, *, alt: Optional[str] = None, parent_folder: Optional[str] = None) -> dict[str, Any]:
        if not self.site_id:
            raise RuntimeError("WEBFLOW_SITE_ID is missing")
        path = Path(file_path)
        raw = path.read_bytes()
        file_hash = hashlib.md5(raw).hexdigest()
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        payload: dict[str, Any] = {"fileName": path.name, "fileHash": file_hash}
        if parent_folder:
            payload["parentFolder"] = parent_folder
        meta = self._request("POST", f"/sites/{self.site_id}/assets", json_data=payload)
        upload_url = meta.get("uploadUrl")
        upload_details = meta.get("uploadDetails") or {}
        if not upload_url or not upload_details:
            raise RuntimeError("Webflow asset metadata response missing uploadUrl or uploadDetails")
        files = {"file": (path.name, raw, content_type)}
        resp = requests.post(upload_url, data=upload_details, files=files, timeout=120)
        if resp.status_code not in (200, 201, 204):
            raise RuntimeError(f"Webflow asset binary upload failed: {resp.status_code} {resp.text[:500]}")
        return {
            "fileId": meta.get("id"),
            "url": meta.get("hostedUrl") or meta.get("assetUrl") or upload_url,
            "alt": alt or path.stem.replace("-", " ").title(),
        }

    def create_item(self, field_data: dict[str, Any], *, is_draft: bool = True) -> dict[str, Any]:
        payload = {"isDraft": is_draft, "isArchived": False, "fieldData": field_data}
        return self._request("POST", f"/collections/{self.collection_id}/items", json_data=payload)

    def update_item(self, item_id: str, field_data: dict[str, Any], *, is_draft: bool = True) -> dict[str, Any]:
        payload = {"isDraft": is_draft, "isArchived": False, "fieldData": field_data}
        return self._request("PATCH", f"/collections/{self.collection_id}/items/{item_id}", json_data=payload)

    def publish_items(self, item_ids: list[str]) -> dict[str, Any]:
        return self._request("POST", f"/collections/{self.collection_id}/items/publish", json_data={"itemIds": item_ids})
