from __future__ import annotations

import json

from app.services.dataforseo_service import DataForSEOService
from app.workers._common import ensure_run_log, finish_run_log


CLUSTER_FIT = {
    "shared_inbox": 100,
    "gmail_slack_coordination": 95,
    "email_triage": 90,
    "alternatives": 95,
}

COMMERCIAL_MAP = {
    "traffic": 40,
    "commercial": 85,
    "bofu": 100,
}


def _score(volume: float, difficulty: float, fit: float, cluster: float, commercial: float) -> float:
    volume_score = min(100.0, volume / 20.0)
    rankability_score = max(0.0, 100.0 - difficulty)
    serp_realism_score = max(30.0, rankability_score)
    return round(
        0.25 * volume_score + 0.20 * rankability_score + 0.20 * fit + 0.15 * cluster + 0.10 * commercial + 0.10 * serp_realism_score,
        2,
    )


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "keyword_intake")
    site = db.fetchone("SELECT id FROM sites WHERE site_key='deemerge'")
    if not site:
        raise RuntimeError("Site row missing. Run seed_base first.")
    site_id = int(site["id"])

    service = DataForSEOService(settings)
    ideas = service.discover_keywords()

    processed = 0
    for item in ideas[:limit]:
        cluster = db.fetchone("SELECT id, cluster_key, priority FROM clusters WHERE cluster_key=?", [item["cluster_key"]])
        if not cluster:
            continue
        fit = float(CLUSTER_FIT.get(item["cluster_key"], 70))
        cluster_score = max(50.0, 110.0 - float(cluster["priority"]))
        commercial = float(COMMERCIAL_MAP.get(item["intent_type"], 50))
        total_score = _score(float(item["volume"]), float(item["difficulty"]), fit, cluster_score, commercial)
        db.execute(
            """
            INSERT INTO keyword_candidates(site_id, cluster_id, primary_keyword, secondary_keywords_json, source, volume, difficulty, intent_type, fit_score, cluster_score, total_score, status)
            VALUES (?, ?, ?, ?, 'dataforseo', ?, ?, ?, ?, ?, ?, 'new')
            ON CONFLICT(site_id, primary_keyword) DO UPDATE SET
              secondary_keywords_json=excluded.secondary_keywords_json,
              volume=excluded.volume,
              difficulty=excluded.difficulty,
              intent_type=excluded.intent_type,
              fit_score=excluded.fit_score,
              cluster_score=excluded.cluster_score,
              total_score=excluded.total_score,
              updated_at=CURRENT_TIMESTAMP
            """,
            [
                site_id,
                cluster["id"],
                item["primary_keyword"],
                json.dumps(item["secondary_keywords"]),
                item["volume"],
                item["difficulty"],
                item["intent_type"],
                fit,
                cluster_score,
                total_score,
            ],
        )
        keyword_row = db.fetchone("SELECT id FROM keyword_candidates WHERE site_id=? AND primary_keyword=?", [site_id, item["primary_keyword"]])
        db.execute(
            "INSERT OR IGNORE INTO article_generation_queue(keyword_candidate_id, priority, status) VALUES (?, ?, 'queued')",
            [keyword_row["id"], int(total_score)],
        )
        processed += 1
    finish_run_log(db, run_id, "success", items_processed=processed)
    logger.info("Queued %s keyword candidates", processed)
    return 0
