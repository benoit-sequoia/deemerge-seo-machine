from __future__ import annotations

import json
from collections import defaultdict

from app.workers._common import ensure_run_log, finish_run_log


def _norm(value: float, max_value: float) -> float:
    if max_value <= 0:
        return 0.0
    return min(100.0, (value / max_value) * 100.0)


def _business_fit(slug: str) -> float:
    value = slug.lower()
    if any(x in value for x in ["shared-inbox", "gmail", "slack", "outlook", "alternative", "triage", "mailbox"]):
        return 100.0
    if any(x in value for x in ["email", "notification", "time-management"]):
        return 70.0
    return 30.0


def _cluster_value(cluster_id: int | None) -> float:
    return 100.0 if cluster_id else 40.0


def _position_score(position: float) -> float:
    if position < 8:
        return 25.0
    if position <= 40:
        return 100.0 - abs(24.0 - position) * 2.0
    if position <= 70:
        return 35.0
    return 10.0


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "recovery_score")
    db.execute("DELETE FROM recovery_queue")
    db.execute("DELETE FROM recovery_candidates")

    rows = db.fetchall(
        """
        SELECT cp.id AS page_id, cp.page_url, cp.slug, cp.cluster_id,
               COALESCE(SUM(g.impressions), 0) AS impressions_28d,
               COALESCE(SUM(g.clicks), 0) AS clicks_28d,
               COALESCE(AVG(g.ctr), 0) AS ctr_28d,
               COALESCE(AVG(g.position), 0) AS position_28d
        FROM content_pages cp
        LEFT JOIN gsc_page_daily g ON g.page_url = cp.page_url
        GROUP BY cp.id, cp.page_url, cp.slug, cp.cluster_id
        HAVING impressions_28d > 0
        ORDER BY impressions_28d DESC
        """
    )
    if not rows:
        logger.info("No pages with impressions yet")
        finish_run_log(db, run_id, "success")
        return 0

    max_impressions = max(float(r["impressions_28d"]) for r in rows)
    processed = 0
    for row in rows[: max(limit, len(rows))]:
        queries = db.fetchall(
            "SELECT query, SUM(impressions) AS impressions FROM gsc_query_daily WHERE page_url=? GROUP BY query ORDER BY impressions DESC LIMIT 10",
            [row["page_url"]],
        )
        top_queries = [{"query": q["query"], "impressions": q["impressions"]} for q in queries]
        impression_score = _norm(float(row["impressions_28d"]), max_impressions)
        expected_ctr = 0.03 if row["position_28d"] <= 20 else 0.015 if row["position_28d"] <= 40 else 0.005
        ctr_gap_score = max(0.0, min(100.0, (expected_ctr - float(row["ctr_28d"])) * 4000.0))
        position_score = max(0.0, min(100.0, _position_score(float(row["position_28d"]))))
        business_fit = _business_fit(str(row["slug"]))
        cluster_value = _cluster_value(row["cluster_id"])
        opportunity_score = round(
            0.35 * impression_score + 0.25 * ctr_gap_score + 0.20 * position_score + 0.10 * business_fit + 0.10 * cluster_value,
            2,
        )
        candidate_id = db.insert(
            """
            INSERT INTO recovery_candidates(page_id, window_start, window_end, impressions_28d, clicks_28d, ctr_28d, position_28d, top_queries_json, opportunity_score, reason_json, status)
            VALUES (?, date('now','-28 day'), date('now','-1 day'), ?, ?, ?, ?, ?, ?, ?, 'new')
            """,
            [
                row["page_id"],
                row["impressions_28d"],
                row["clicks_28d"],
                row["ctr_28d"],
                row["position_28d"],
                json.dumps(top_queries),
                opportunity_score,
                json.dumps(
                    {
                        "impression_score": impression_score,
                        "ctr_gap_score": ctr_gap_score,
                        "position_score": position_score,
                        "business_fit": business_fit,
                        "cluster_value": cluster_value,
                    }
                ),
            ],
        )
        db.execute(
            "INSERT OR IGNORE INTO recovery_queue(page_id, candidate_id, priority, status) VALUES (?, ?, ?, 'queued')",
            [row["page_id"], candidate_id, int(opportunity_score)],
        )
        processed += 1

    finish_run_log(db, run_id, "success", items_processed=processed)
    logger.info("Scored %s recovery candidates", processed)
    return 0
