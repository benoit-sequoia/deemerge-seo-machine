from __future__ import annotations

import argparse
import sys
from pathlib import Path

from app.config import load_settings
from app.db import Database
from app.logger import setup_logger
from app.workers.article_brief import run as article_brief_run
from app.workers.article_write import run as article_write_run
from app.workers.backup_db import run as backup_db_run
from app.workers.evaluate_results import run as evaluate_results_run
from app.workers.gsc_collect import run as gsc_collect_run
from app.workers.gsc_inspect_recent import run as gsc_inspect_recent_run
from app.workers.image_generate import run as image_generate_run
from app.workers.import_existing_blog import run as import_existing_blog_run
from app.workers.init_db import run as init_db_run
from app.workers.keyword_intake import run as keyword_intake_run
from app.workers.link_resolve import run as link_resolve_run
from app.workers.plan_publish import run as plan_publish_run
from app.workers.publish_due import run as publish_due_run
from app.workers.queue_health import run as queue_health_run
from app.workers.recovery_brief import run as recovery_brief_run
from app.workers.recovery_rewrite import run as recovery_rewrite_run
from app.workers.recovery_score import run as recovery_score_run
from app.workers.seed_base import run as seed_base_run
from app.workers.send_weekly_summary import run as send_weekly_summary_run
from app.workers.validate_articles import run as validate_articles_run
from app.workers.validate_rewrites import run as validate_rewrites_run
from app.workers.webflow_sync_articles import run as webflow_sync_articles_run
from app.workers.webflow_sync_rewrites import run as webflow_sync_rewrites_run

logger = setup_logger()

COMMANDS = {
    "init_db": init_db_run,
    "seed_base": seed_base_run,
    "import_existing_blog": import_existing_blog_run,
    "gsc_collect": gsc_collect_run,
    "gsc_inspect_recent": gsc_inspect_recent_run,
    "recovery_score": recovery_score_run,
    "recovery_brief": recovery_brief_run,
    "recovery_rewrite": recovery_rewrite_run,
    "validate_rewrites": validate_rewrites_run,
    "keyword_intake": keyword_intake_run,
    "article_brief": article_brief_run,
    "article_write": article_write_run,
    "validate_articles": validate_articles_run,
    "link_resolve": link_resolve_run,
    "image_generate": image_generate_run,
    "webflow_sync_rewrites": webflow_sync_rewrites_run,
    "webflow_sync_articles": webflow_sync_articles_run,
    "plan_publish": plan_publish_run,
    "publish_due": publish_due_run,
    "evaluate_results": evaluate_results_run,
    "send_weekly_summary": send_weekly_summary_run,
    "queue_health": queue_health_run,
    "backup_db": backup_db_run,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="DEEMERGE SEO Machine CLI")
    parser.add_argument("command", choices=COMMANDS.keys())
    parser.add_argument("--limit", type=int, default=10)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    settings = load_settings()
    db = Database(settings)

    try:
        return COMMANDS[args.command](db=db, settings=settings, logger=logger, limit=args.limit)
    except Exception as exc:
        logger.exception("Command failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
