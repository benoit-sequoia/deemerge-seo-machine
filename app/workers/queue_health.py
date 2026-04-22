from __future__ import annotations

from app.services.slack_service import SlackService
from app.workers._common import ensure_run_log, finish_run_log


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "queue_health")
    stats = db.fetchone(
        """
        SELECT
          (SELECT COUNT(*) FROM article_generation_queue WHERE status='queued') AS new_articles_queued,
          (SELECT COUNT(*) FROM recovery_queue WHERE status='queued') AS rewrites_queued,
          (SELECT COUNT(*) FROM error_log WHERE created_at >= datetime('now', '-24 hour')) AS errors_24h
        """
    )
    text = (
        "DEEMERGE SEO queue health\n"
        f"New article queue: {stats['new_articles_queued']}\n"
        f"Rewrite queue: {stats['rewrites_queued']}\n"
        f"Errors last 24h: {stats['errors_24h']}"
    )
    logger.info(text.replace("\n", " | "))
    slack = SlackService(settings)
    try:
        if settings.slack_webhook_url:
            slack.send(text)
    except Exception as exc:
        logger.warning("Slack alert failed: %s", exc)
    finish_run_log(db, run_id, "success", items_processed=1)
    return 0
