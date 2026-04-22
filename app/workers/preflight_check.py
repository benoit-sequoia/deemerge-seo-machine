from __future__ import annotations

from app.services.webflow_service import WebflowService
from app.workers._common import ensure_run_log, finish_run_log, write_json_log


REQUIRED_KEYS = [
    "WEBFLOW_TOKEN",
    "WEBFLOW_SITE_ID",
    "WEBFLOW_COLLECTION_ID",
    "GSC_SITE_URL",
    "GOOGLE_SERVICE_ACCOUNT_JSON_B64",
    "ANTHROPIC_API_KEY",
    "DATAFORSEO_LOGIN",
    "DATAFORSEO_PASSWORD",
    "SLACK_WEBHOOK_URL",
]


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "preflight_check")
    data = {
        "required_keys": {key: bool(getattr(settings, key.lower(), None)) for key in []},
        "env_presence": {
            "WEBFLOW_TOKEN": bool(settings.webflow_token),
            "WEBFLOW_SITE_ID": bool(settings.webflow_site_id),
            "WEBFLOW_COLLECTION_ID": bool(settings.webflow_collection_id),
            "GSC_SITE_URL": bool(settings.gsc_site_url),
            "GOOGLE_SERVICE_ACCOUNT_JSON_B64": bool(settings.google_service_account_json_b64),
            "ANTHROPIC_API_KEY": bool(settings.anthropic_api_key),
            "DATAFORSEO_LOGIN": bool(settings.dataforseo_login),
            "DATAFORSEO_PASSWORD": bool(settings.dataforseo_password),
            "SLACK_WEBHOOK_URL": bool(settings.slack_webhook_url),
        },
        "webflow": {},
    }
    errors = 0
    if settings.webflow_token and settings.webflow_collection_id:
        try:
            service = WebflowService(settings)
            details = service.collection_details()
            data["webflow"] = {
                "ok": True,
                "collection_id": details.get("id"),
                "display_name": details.get("displayName"),
                "field_count": len(details.get("fields", [])),
            }
        except Exception as exc:
            data["webflow"] = {"ok": False, "error": str(exc)}
            errors += 1
    else:
        data["webflow"] = {"ok": False, "error": "Missing token or collection id"}
        errors += 1
    write_json_log("preflight_check.json", data)
    finish_run_log(db, run_id, "success" if errors == 0 else "warning", items_processed=1, error_count=errors)
    logger.info("Preflight check complete")
    return 0
