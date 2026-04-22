from __future__ import annotations

import json
from datetime import datetime, timezone

from app.services.webflow_service import WebflowService
from app.workers._common import ensure_run_log, finish_run_log


def _field_data_from_draft(draft, field_map: dict[str, str]) -> dict:
    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    field_data = {
        field_map["name"]: draft["h1"],
        field_map["slug"]: draft["slug"],
        field_map["summary"]: draft["excerpt"] or draft["meta_description"] or draft["h1"],
        field_map["body"]: draft["body_html"],
        field_map["seo_title"]: draft["title_tag"],
        field_map["seo_description"]: draft["meta_description"] or draft["excerpt"] or draft["h1"],
        field_map["published_date"]: now_iso,
    }
    return field_data


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "webflow_sync_articles")
    # New article sync is intentionally guarded because your real Webflow collection
    # requires og-image and the asset upload path is not implemented in this package yet.
    logger.warning("webflow_sync_articles skipped: real collection requires og-image and image upload is not implemented yet")
    finish_run_log(db, run_id, "skipped", items_processed=0)
    return 0
