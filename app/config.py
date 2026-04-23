from __future__ import annotations

import base64
import json
import os
from dataclasses import dataclass
from typing import Any, Optional


DEFAULT_WEBFLOW_FIELD_MAP = {
    "name": "name",
    "slug": "slug",
    "summary": "post-short-details-2",
    "excerpt": "post-short-details-2",
    "body": "post-details",
    "seo_title": "seo-title",
    "seo_description": "meta-description",
    "published_date": "post-date-time-2",
    "featured_image": "post-image",
    "og_image": "og-image",
    "category": "post-category",
    "read_time": "post-read-time-3",
}


@dataclass(frozen=True)
class Settings:
    app_env: str
    app_timezone: str
    sqlite_path: str
    deemerge_base_url: str
    blog_base_url: str
    max_new_articles_per_week: int
    max_rewrites_per_week: int
    anthropic_api_key: Optional[str]
    anthropic_model_main: Optional[str]
    anthropic_model_fast: Optional[str]
    webflow_token: Optional[str]
    webflow_site_id: Optional[str]
    webflow_collection_id: Optional[str]
    webflow_field_map_json: Optional[str]
    gsc_site_url: Optional[str]
    google_service_account_json_b64: Optional[str]
    dataforseo_login: Optional[str]
    dataforseo_password: Optional[str]
    slack_webhook_url: Optional[str]
    smtp_host: Optional[str]
    smtp_port: Optional[int]
    smtp_user: Optional[str]
    smtp_pass: Optional[str]
    alert_email_to: Optional[str]

    def decode_google_service_account(self) -> Optional[dict[str, Any]]:
        if not self.google_service_account_json_b64:
            return None
        decoded = base64.b64decode(self.google_service_account_json_b64).decode("utf-8")
        return json.loads(decoded)

    def webflow_field_map(self) -> dict[str, str]:
        if not self.webflow_field_map_json:
            return dict(DEFAULT_WEBFLOW_FIELD_MAP)
        try:
            parsed = json.loads(self.webflow_field_map_json)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"WEBFLOW_FIELD_MAP_JSON is not valid JSON: {exc}") from exc
        if not isinstance(parsed, dict):
            raise RuntimeError("WEBFLOW_FIELD_MAP_JSON must decode to an object")
        merged = dict(DEFAULT_WEBFLOW_FIELD_MAP)
        merged.update({str(k): str(v) for k, v in parsed.items() if v not in (None, "")})
        return merged


def _optional(name: str) -> Optional[str]:
    value = os.environ.get(name)
    return value if value not in (None, "") else None


def load_settings() -> Settings:
    sqlite_path = os.environ.get("SQLITE_PATH", "/data/deemerge_seo_machine.db")
    return Settings(
        app_env=os.environ.get("APP_ENV", "development"),
        app_timezone=os.environ.get("APP_TIMEZONE", "Asia/Singapore"),
        sqlite_path=sqlite_path,
        deemerge_base_url=os.environ.get("DEEMERGE_BASE_URL", "https://www.deemerge.ai"),
        blog_base_url=os.environ.get("BLOG_BASE_URL", "https://www.deemerge.ai/blog"),
        max_new_articles_per_week=int(os.environ.get("MAX_NEW_ARTICLES_PER_WEEK", "10")),
        max_rewrites_per_week=int(os.environ.get("MAX_REWRITES_PER_WEEK", "5")),
        anthropic_api_key=_optional("ANTHROPIC_API_KEY"),
        anthropic_model_main=_optional("ANTHROPIC_MODEL_MAIN"),
        anthropic_model_fast=_optional("ANTHROPIC_MODEL_FAST"),
        webflow_token=_optional("WEBFLOW_TOKEN"),
        webflow_site_id=_optional("WEBFLOW_SITE_ID"),
        webflow_collection_id=_optional("WEBFLOW_COLLECTION_ID"),
        webflow_field_map_json=_optional("WEBFLOW_FIELD_MAP_JSON"),
        gsc_site_url=_optional("GSC_SITE_URL"),
        google_service_account_json_b64=_optional("GOOGLE_SERVICE_ACCOUNT_JSON_B64"),
        dataforseo_login=_optional("DATAFORSEO_LOGIN"),
        dataforseo_password=_optional("DATAFORSEO_PASSWORD"),
        slack_webhook_url=_optional("SLACK_WEBHOOK_URL"),
        smtp_host=_optional("SMTP_HOST"),
        smtp_port=int(os.environ["SMTP_PORT"]) if os.environ.get("SMTP_PORT") else None,
        smtp_user=_optional("SMTP_USER"),
        smtp_pass=_optional("SMTP_PASS"),
        alert_email_to=_optional("ALERT_EMAIL_TO"),
    )
