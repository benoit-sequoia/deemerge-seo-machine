from __future__ import annotations

from pathlib import Path

from app.workers._common import ensure_run_log, finish_run_log


PNG_BYTES = bytes.fromhex(
    "89504E470D0A1A0A0000000D4948445200000001000000010802000000907724"
    "0000000A49444154789C636000000200015D0B2A0000000049454E44AE426082"
)


def _ensure_image(queue_id: int, label: str, root: Path) -> str:
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{label}_{queue_id}.png"
    if not path.exists():
        path.write_bytes(PNG_BYTES)
    return str(path)


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "image_generate")
    image_dir = Path(settings.sqlite_path).resolve().parent / "images"
    processed = 0

    article_rows = db.fetchall(
        "SELECT ad.id AS draft_id, ad.image_prompt FROM article_drafts ad LEFT JOIN image_queue iq ON iq.source_type='article' AND iq.source_id=ad.id WHERE iq.id IS NULL LIMIT ?",
        [limit],
    )
    for row in article_rows:
        qid = db.insert("INSERT INTO image_queue(source_type, source_id, prompt, status) VALUES ('article', ?, ?, 'queued')", [row["draft_id"], row["image_prompt"] or "DEEMERGE editorial image"])
        path = _ensure_image(qid, "article", image_dir)
        db.execute("UPDATE image_queue SET status='generated' WHERE id=?", [qid])
        db.execute("INSERT INTO generated_images(queue_id, local_path, alt_text, status) VALUES (?, ?, ?, 'generated')", [qid, path, "DEEMERGE blog illustration"])
        processed += 1

    rewrite_rows = db.fetchall(
        "SELECT pv.id AS version_id, pv.title_tag FROM page_versions pv LEFT JOIN image_queue iq ON iq.source_type='rewrite' AND iq.source_id=pv.id WHERE iq.id IS NULL LIMIT ?",
        [limit],
    )
    for row in rewrite_rows:
        qid = db.insert("INSERT INTO image_queue(source_type, source_id, prompt, status) VALUES ('rewrite', ?, ?, 'queued')", [row["version_id"], row["title_tag"] or "DEEMERGE rewrite image"])
        path = _ensure_image(qid, "rewrite", image_dir)
        db.execute("UPDATE image_queue SET status='generated' WHERE id=?", [qid])
        db.execute("INSERT INTO generated_images(queue_id, local_path, alt_text, status) VALUES (?, ?, ?, 'generated')", [qid, path, "DEEMERGE article illustration"])
        processed += 1

    finish_run_log(db, run_id, "success", items_processed=processed)
    logger.info("Generated %s placeholder images", processed)
    return 0
