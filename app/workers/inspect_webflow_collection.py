from __future__ import annotations

from app.config import DEFAULT_WEBFLOW_FIELD_MAP
from app.services.webflow_service import WebflowService
from app.workers._common import ensure_run_log, finish_run_log, write_json_log


def _build_summary(details: dict) -> dict:
    fields = details.get("fields", [])
    summary = {
        "collection_id": details.get("id"),
        "display_name": details.get("displayName"),
        "slug": details.get("slug"),
        "fields": [
            {
                "slug": field.get("slug"),
                "display_name": field.get("displayName"),
                "type": field.get("type"),
                "required": field.get("isRequired"),
            }
            for field in fields
        ],
    }
    return summary


def _suggest_map(details: dict) -> dict:
    field_slugs = {field.get("slug") for field in details.get("fields", [])}
    mapping = {}
    for logical_key, preferred_slug in DEFAULT_WEBFLOW_FIELD_MAP.items():
        mapping[logical_key] = preferred_slug if preferred_slug in field_slugs else None
    return mapping


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "inspect_webflow_collection")
    service = WebflowService(settings)
    details = service.collection_details()
    summary = _build_summary(details)
    suggestion = _suggest_map(details)
    write_json_log("webflow_collection_details.json", details)
    write_json_log("webflow_collection_fields_summary.json", summary)
    write_json_log("webflow_field_map_suggestion.json", suggestion)
    finish_run_log(db, run_id, "success", items_processed=1)
    logger.info("Webflow collection inspection complete")
    return 0
