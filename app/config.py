from __future__ import annotations
import base64
import json
import os
from dataclasses import dataclass
from typing import Any, Dict


def _get(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


@dataclass(frozen=True)
class Settings:
    app_env: str
    app_timezone: str
    sqlite_path: str
    site_key: str
    site_name: str
    domain: str
    deemerge_base_url: str
    blog_base_url: str
    webflow_token: str
    webflow_site_id: str
    webflow_collection_id: str
    webflow_field_map_json: str
    gsc_site_url: str
    google_service_account_json_b64: str
    dataforseo_login: str
    dataforseo_password: str
    anthropic_api_key: str
    anthropic_model_main: str
    anthropic_model_fast: str
    slack_webhook_url: str
    smtp_host: str
    smtp_port: int
    smtp_user: str
    smtp_pass: str
    alert_email_to: str
    max_new_articles_per_week: int
    max_rewrites_per_week: int

    @property
    def is_dev(self) -> bool:
        return self.app_env.lower() != "production"

    @property
    def has_webflow(self) -> bool:
        return bool(self.webflow_token and self.webflow_site_id and self.webflow_collection_id)

    @property
    def has_gsc(self) -> bool:
        return bool(self.google_service_account_json_b64 and self.gsc_site_url)

    @property
    def has_dataforseo(self) -> bool:
        return bool(self.dataforseo_login and self.dataforseo_password)

    @property
    def has_anthropic(self) -> bool:
        return bool(self.anthropic_api_key)

    def webflow_field_map(self) -> Dict[str, Any]:
        raw = self.webflow_field_map_json.strip()
        if not raw:
            return {
                "name": "name",
                "slug": "slug",
                "summary": "summary",
                "body": "post-body",
                "meta_title": "seo-title",
                "meta_description": "seo-description",
                "featured_image": "featured-image",
            }
        return json.loads(raw)

    def google_service_account_info(self) -> Dict[str, Any]:
        if not self.google_service_account_json_b64:
            return {}
        decoded = base64.b64decode(self.google_service_account_json_b64).decode("utf-8")
        return json.loads(decoded)


def load_settings() -> Settings:
    return Settings(
        app_env=_get("APP_ENV", "production"),
        app_timezone=_get("APP_TIMEZONE", "Asia/Singapore"),
        sqlite_path=_get("SQLITE_PATH", "/data/deemerge_seo_machine.db"),
        site_key=_get("SITE_KEY", "deemerge"),
        site_name=_get("SITE_NAME", "DEEMERGE"),
        domain=_get("DOMAIN", "deemerge.ai"),
        deemerge_base_url=_get("DEEMERGE_BASE_URL", "https://www.deemerge.ai"),
        blog_base_url=_get("BLOG_BASE_URL", "https://www.deemerge.ai/blog"),
        webflow_token=_get("WEBFLOW_TOKEN"),
        webflow_site_id=_get("WEBFLOW_SITE_ID"),
        webflow_collection_id=_get("WEBFLOW_COLLECTION_ID"),
        webflow_field_map_json=_get("WEBFLOW_FIELD_MAP_JSON"),
        gsc_site_url=_get("GSC_SITE_URL", "https://www.deemerge.ai/"),
        google_service_account_json_b64=_get("GOOGLE_SERVICE_ACCOUNT_JSON_B64"),
        dataforseo_login=_get("DATAFORSEO_LOGIN"),
        dataforseo_password=_get("DATAFORSEO_PASSWORD"),
        anthropic_api_key=_get("ANTHROPIC_API_KEY"),
        anthropic_model_main=_get("ANTHROPIC_MODEL_MAIN", "claude-3-5-sonnet-20241022"),
        anthropic_model_fast=_get("ANTHROPIC_MODEL_FAST", "claude-3-5-haiku-20241022"),
        slack_webhook_url=_get("SLACK_WEBHOOK_URL"),
        smtp_host=_get("SMTP_HOST"),
        smtp_port=int(_get("SMTP_PORT", "587")),
        smtp_user=_get("SMTP_USER"),
        smtp_pass=_get("SMTP_PASS"),
        alert_email_to=_get("ALERT_EMAIL_TO"),
        max_new_articles_per_week=int(_get("MAX_NEW_ARTICLES_PER_WEEK", "10")),
        max_rewrites_per_week=int(_get("MAX_REWRITES_PER_WEEK", "5")),
    )
