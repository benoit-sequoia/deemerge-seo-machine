from __future__ import annotations

import requests

from app.config import Settings


class SlackService:
    def __init__(self, settings: Settings):
        self.webhook_url = settings.slack_webhook_url

    @property
    def enabled(self) -> bool:
        return bool(self.webhook_url)

    def send(self, text: str) -> None:
        if not self.webhook_url:
            return
        response = requests.post(self.webhook_url, json={"text": text}, timeout=20)
        response.raise_for_status()

    def send_message(self, text: str) -> None:
        self.send(text)
