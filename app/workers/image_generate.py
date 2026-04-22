from __future__ import annotations

from app.workers._common import ensure_run_log, finish_run_log


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, 'image_generate')
    logger.info("image_generate scaffold ready")
    finish_run_log(db, run_id, "success")
    return 0
