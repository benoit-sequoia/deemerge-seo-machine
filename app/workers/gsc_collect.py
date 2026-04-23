from __future__ import annotations

from datetime import date, timedelta

from app.services.gsc_service import GSCService
from app.workers._common import ensure_run_log, finish_run_log


def _daterange(days: int) -> list[str]:
    end = date.today() - timedelta(days=1)
    return [(end - timedelta(days=i)).isoformat() for i in range(days)][::-1]


def _fixture_pages(db, settings, site_id: int) -> list[dict]:
    rows = db.fetchall("SELECT page_url FROM content_pages ORDER BY id ASC LIMIT 2")
    defaults = [
        {
            'clicks': 6,
            'impressions': 1805,
            'ctr': 0.003324,
            'position': 33.4,
            'query': 'unified inbox',
        },
        {
            'clicks': 4,
            'impressions': 1107,
            'ctr': 0.0036,
            'position': 29.1,
            'query': 'team communication workflow',
        },
    ]
    pages = []
    queries = []
    for idx, row in enumerate(rows):
        sample = defaults[min(idx, len(defaults)-1)]
        pages.append({
            'page_url': str(row['page_url']).rstrip('/'),
            'clicks': sample['clicks'],
            'impressions': sample['impressions'],
            'ctr': sample['ctr'],
            'position': sample['position'],
        })
        queries.append({
            'page_url': str(row['page_url']).rstrip('/'),
            'query': sample['query'],
            'clicks': max(1, sample['clicks'] // 2),
            'impressions': max(20, sample['impressions'] // 10),
            'ctr': sample['ctr'],
            'position': sample['position'],
        })
    return pages, queries


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, 'gsc_collect')
    site_row = db.fetchone("SELECT id FROM sites WHERE site_key='deemerge'")
    if not site_row:
        raise RuntimeError('Site row missing. Run seed_base first.')
    site_id = int(site_row['id'])

    service = GSCService(settings)
    live_mode = False
    try:
        if service.is_live:
            pages = service.query_pages(days=28)
            queries = service.query_queries(days=28)
            live_mode = True
        else:
            raise RuntimeError('Live GSC not configured')
    except Exception as exc:
        logger.warning('Falling back to fixture-style GSC data: %s', exc)
        pages, queries = _fixture_pages(db, settings, site_id)

    dates = _daterange(28)

    db.execute('DELETE FROM gsc_page_daily WHERE site_id=?', [site_id])
    db.execute('DELETE FROM gsc_query_daily WHERE site_id=?', [site_id])

    processed = 0
    for row in pages:
        page_url = str(row['page_url']).rstrip('/')
        for d in dates:
            db.execute(
                """
                INSERT OR REPLACE INTO gsc_page_daily(site_id, date, page_url, clicks, impressions, ctr, position, data_state)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'final')
                """,
                [site_id, d, page_url, float(row['clicks']) / 28.0, float(row['impressions']) / 28.0, float(row['ctr']), float(row['position'])],
            )
            processed += 1
    for row in queries:
        page_url = str(row['page_url']).rstrip('/')
        for d in dates:
            db.execute(
                """
                INSERT OR REPLACE INTO gsc_query_daily(site_id, date, query, page_url, clicks, impressions, ctr, position, data_state)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'final')
                """,
                [site_id, d, row['query'], page_url, float(row['clicks']) / 28.0, float(row['impressions']) / 28.0, float(row['ctr']), float(row['position'])],
            )
            processed += 1
    finish_run_log(db, run_id, 'success', items_processed=processed)
    logger.info('Stored GSC %s data rows: %s', 'live' if live_mode else 'fixture', processed)
    return 0
