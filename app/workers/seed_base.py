from __future__ import annotations

import json

from app.workers._common import ensure_run_log, finish_run_log, load_sql


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "seed_base")
    db.executescript(load_sql("migrations/003_seed_clusters.sql"))
    db.execute(
        """
        INSERT INTO sites(site_key, site_name, domain, timezone, webflow_site_id, webflow_collection_id, gsc_site_url)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(site_key) DO UPDATE SET
          site_name=excluded.site_name,
          domain=excluded.domain,
          timezone=excluded.timezone,
          webflow_site_id=excluded.webflow_site_id,
          webflow_collection_id=excluded.webflow_collection_id,
          gsc_site_url=excluded.gsc_site_url,
          updated_at=CURRENT_TIMESTAMP
        """,
        [
            "deemerge",
            "DEEMERGE",
            "www.deemerge.ai",
            settings.app_timezone,
            settings.webflow_site_id,
            settings.webflow_collection_id,
            settings.gsc_site_url,
        ],
    )
    defaults = {
        "weekly_targets": {"new_articles": settings.max_new_articles_per_week, "rewrites": settings.max_rewrites_per_week},
        "core_clusters": ["shared_inbox", "gmail_slack_coordination", "email_triage", "alternatives"],
    }
    for key, value in defaults.items():
        db.execute(
            "INSERT INTO settings(key, value_json) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=CURRENT_TIMESTAMP",
            [key, json.dumps(value)],
        )
    finish_run_log(db, run_id, "success", items_processed=1)
    logger.info("Base data seeded")
    return 0
