from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path

from app.workers._common import ensure_run_log, finish_run_log


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "backup_db")
    src = Path(settings.sqlite_path)
    if not src.exists():
        raise RuntimeError(f"Database file does not exist: {src}")
    backup_dir = Path("/backups")
    backup_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    dst = backup_dir / f"deemerge_seo_machine_{ts}.db"
    shutil.copy2(src, dst)
    finish_run_log(db, run_id, "success", items_processed=1)
    logger.info("Backup created: %s", dst)
    return 0
