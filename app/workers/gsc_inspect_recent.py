from __future__ import annotations

from app.workers._common import ensure_run_log, finish_run_log


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "gsc_inspect_recent")
    logger.info("gsc_inspect_recent scaffold ready. Live URL Inspection wiring not yet added.")
    finish_run_log(db, run_id, "success")
    return 0
