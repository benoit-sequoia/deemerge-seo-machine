from __future__ import annotations
import requests
from ..config import Settings


class SlackService:
    def __init__(self, settings: Settings):
        self.settings = settings

    def send(self, text: str) -> dict:
        if not self.settings.slack_webhook_url:
            return {"ok": False, "skipped": True, "reason": "missing webhook"}
        resp = requests.post(self.settings.slack_webhook_url, json={"text": text}, timeout=20)
        return {"ok": resp.ok, "status_code": resp.status_code, "text": resp.text[:500]}
