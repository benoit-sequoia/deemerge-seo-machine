from __future__ import annotations
import hashlib
import json
import requests
from typing import Dict
from ..config import Settings


class WebflowService:
    base_url = "https://api.webflow.com/v2"

    def __init__(self, settings: Settings):
        self.settings = settings

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.settings.webflow_token}",
            "Content-Type": "application/json",
            "accept": "application/json",
        }

    def collection_details(self) -> dict:
        if not self.settings.has_webflow:
            return {"id": self.settings.webflow_collection_id or "mock_collection", "displayName": "Mock Blog Collection", "fields": []}
        url = f"{self.base_url}/collections/{self.settings.webflow_collection_id}"
        resp = requests.get(url, headers=self._headers(), timeout=30)
        resp.raise_for_status()
        return resp.json()

    def list_items(self, limit: int = 100) -> list[dict]:
        if not self.settings.has_webflow:
            return [
                {"id": "mock1", "fieldData": {"name": "Best Unified Inbox Apps", "slug": "best-unified-inbox-apps-2025"}},
                {"id": "mock2", "fieldData": {"name": "Integrate Gmail and Slack", "slug": "integrate-gmail-and-slack"}},
                {"id": "mock3", "fieldData": {"name": "Front Alternatives", "slug": "front-app-alternatives"}},
                {"id": "mock4", "fieldData": {"name": "Shared Inbox Solution", "slug": "best-shared-inbox-solution-for-collaboration"}},
            ]
        url = f"{self.base_url}/collections/{self.settings.webflow_collection_id}/items"
        resp = requests.get(url, headers=self._headers(), params={"limit": limit}, timeout=60)
        resp.raise_for_status()
        return resp.json().get("items", [])

    def find_by_slug(self, slug: str) -> dict | None:
        for item in self.list_items(limit=100):
            if (item.get("fieldData") or {}).get("slug") == slug:
                return item
        return None

    def build_item_payload(self, content: dict) -> dict:
        fmap = self.settings.webflow_field_map()
        field_data = {
            fmap["name"]: content["name"],
            fmap["slug"]: content["slug"],
        }
        if fmap.get("summary"):
            field_data[fmap["summary"]] = content.get("summary", "")
        if fmap.get("body"):
            field_data[fmap["body"]] = content.get("body_html", "")
        if fmap.get("meta_title"):
            field_data[fmap["meta_title"]] = content.get("meta_title", "")
        if fmap.get("meta_description"):
            field_data[fmap["meta_description"]] = content.get("meta_description", "")
        if fmap.get("featured_image") and content.get("featured_image_url"):
            field_data[fmap["featured_image"]] = {"url": content["featured_image_url"], "alt": content.get("featured_image_alt", content["name"])}
        return {"isDraft": True, "isArchived": False, "fieldData": field_data}

    def upsert_staged_item(self, slug: str, content: dict) -> dict:
        payload = self.build_item_payload(content)
        payload_hash = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
        if not self.settings.has_webflow:
            existing = self.find_by_slug(slug)
            return {"item_id": existing["id"] if existing else f"mock_{slug}", "payload_hash": payload_hash, "mock": True}
        existing = self.find_by_slug(slug)
        if existing:
            item_id = existing["id"]
            url = f"{self.base_url}/collections/{self.settings.webflow_collection_id}/items/{item_id}"
            resp = requests.patch(url, headers=self._headers(), json=payload, timeout=60)
            resp.raise_for_status()
            return {"item_id": item_id, "payload_hash": payload_hash, "mock": False}
        url = f"{self.base_url}/collections/{self.settings.webflow_collection_id}/items"
        resp = requests.post(url, headers=self._headers(), json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        item = data.get("items", [{}])[0] if isinstance(data.get("items"), list) else data
        return {"item_id": item.get("id"), "payload_hash": payload_hash, "mock": False}

    def publish_items(self, item_ids: list[str]) -> dict:
        if not item_ids:
            return {"published": 0}
        if not self.settings.has_webflow:
            return {"published": len(item_ids), "mock": True}
        url = f"{self.base_url}/collections/{self.settings.webflow_collection_id}/items/publish"
        resp = requests.post(url, headers=self._headers(), json={"itemIds": item_ids}, timeout=60)
        resp.raise_for_status()
        return {"published": len(item_ids), "mock": False, "response": resp.json()}
