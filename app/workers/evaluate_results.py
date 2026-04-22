from __future__ import annotations

from datetime import UTC, datetime

from app.workers._common import ensure_run_log, finish_run_log


def _parse_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = str(value).strip()
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        try:
            dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _score(before_clicks: float, after_clicks: float, before_ctr: float, after_ctr: float, before_pos: float, after_pos: float) -> tuple[float, str]:
    click_lift = max(-100.0, min(100.0, (after_clicks - before_clicks) * 10.0))
    ctr_lift = max(-100.0, min(100.0, (after_ctr - before_ctr) * 4000.0))
    pos_gain = max(-100.0, min(100.0, (before_pos - after_pos) * 3.0))
    result = 50.0 + 0.35 * click_lift + 0.25 * ctr_lift + 0.20 * pos_gain
    if result >= 80:
        return result, "keep"
    if result >= 60:
        return result, "watch"
    if result >= 40:
        return result, "rewrite_again"
    return result, "stop"


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "evaluate_results")
    rows = db.fetchall(
        """
        SELECT pv.id AS version_id, pv.page_id, pv.published_at, cp.page_url
        FROM page_versions pv
        JOIN content_pages cp ON cp.id = pv.page_id
        WHERE pv.published_at IS NOT NULL
        ORDER BY pv.published_at DESC
        LIMIT ?
        """,
        [limit],
    )
    processed = 0
    now = datetime.now(UTC)
    for row in rows:
        published = _parse_utc(row["published_at"])
        if not published:
            continue
        age_days = (now - published).days
        for eval_day in (14, 28, 42):
            if age_days < eval_day:
                continue
            existing = db.fetchone("SELECT id FROM recovery_results WHERE page_id=? AND version_id=? AND eval_day=?", [row["page_id"], row["version_id"], eval_day])
            if existing:
                continue
            before = db.fetchone(
                "SELECT COALESCE(SUM(clicks),0) AS clicks, COALESCE(AVG(ctr),0) AS ctr, COALESCE(AVG(position),0) AS position FROM gsc_page_daily WHERE page_url=? AND date BETWEEN date('now', ?) AND date('now', ?)",
                [row["page_url"], f"-{eval_day * 2} day", f"-{eval_day + 1} day"],
            )
            after = db.fetchone(
                "SELECT COALESCE(SUM(clicks),0) AS clicks, COALESCE(AVG(ctr),0) AS ctr, COALESCE(AVG(position),0) AS position FROM gsc_page_daily WHERE page_url=? AND date BETWEEN date('now', ?) AND date('now', '-1 day')",
                [row["page_url"], f"-{eval_day} day"],
            )
            score, decision = _score(float(before["clicks"]), float(after["clicks"]), float(before["ctr"]), float(after["ctr"]), float(before["position"]), float(after["position"]))
            db.execute(
                """
                INSERT INTO recovery_results(page_id, version_id, eval_day, clicks_before, clicks_after, impressions_before, impressions_after, ctr_before, ctr_after, position_before, position_after, query_expansion_score, assisted_internal_clicks, result_score, decision)
                VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?, 0, 0, ?, ?)
                """,
                [row["page_id"], row["version_id"], eval_day, before["clicks"], after["clicks"], before["ctr"], after["ctr"], before["position"], after["position"], score, decision],
            )
            processed += 1
    finish_run_log(db, run_id, "success", items_processed=processed)
    logger.info("Evaluated %s rewrite checkpoints", processed)
    return 0
