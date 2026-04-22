from __future__ import annotations
import json
import sys
from pathlib import Path

from .config import load_settings
from .db import apply_migrations, connect, query_all, query_one
from .helpers import ensure_dir, expected_ctr_for_position, next_weekday_slots, normalize, score_position, slugify, utcnow, utcnow_iso
from .services.anthropic_service import AnthropicService
from .services.dataforseo_service import DataForSEOService
from .services.email_service import EmailService
from .services.gsc_service import GSCService
from .services.slack_service import SlackService
from .services.webflow_service import WebflowService
from .workers.common import dump_log, finish_run, start_run

ROOT = Path(__file__).resolve().parent.parent
MIGRATIONS_DIR = ROOT / "migrations"
PROMPTS_DIR = ROOT / "prompts"


def _load_prompt(name: str) -> str:
    path = PROMPTS_DIR / name
    return path.read_text(encoding="utf-8") if path.exists() else ""


def init_db() -> None:
    conn = connect()
    apply_migrations(conn, MIGRATIONS_DIR)
    print("init_db ok")


def seed_base() -> None:
    settings = load_settings()
    conn = connect()
    conn.execute(
        "INSERT OR IGNORE INTO sites(site_key, site_name, domain, timezone, webflow_site_id, webflow_collection_id, gsc_site_url) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            settings.site_key,
            settings.site_name,
            settings.domain,
            settings.app_timezone,
            settings.webflow_site_id or "unset",
            settings.webflow_collection_id or "unset",
            settings.gsc_site_url,
        ),
    )
    conn.executescript((MIGRATIONS_DIR / "003_seed_clusters.sql").read_text(encoding="utf-8"))
    conn.commit()
    print("seed_base ok")


def preflight_check() -> None:
    settings = load_settings()
    webflow = WebflowService(settings)
    data = {
        "has_webflow": settings.has_webflow,
        "has_gsc": settings.has_gsc,
        "has_anthropic": settings.has_anthropic,
        "has_dataforseo": settings.has_dataforseo,
        "sqlite_path": settings.sqlite_path,
    }
    try:
        details = webflow.collection_details()
        data["webflow_collection_check"] = {"ok": True, "id": details.get("id"), "name": details.get("displayName")}
    except Exception as exc:
        data["webflow_collection_check"] = {"ok": False, "error": str(exc)}
    dump_log("preflight_check.json", data)
    print(json.dumps(data, indent=2))


def inspect_webflow_collection() -> None:
    settings = load_settings()
    webflow = WebflowService(settings)
    details = webflow.collection_details()
    fields = details.get("fields") or details.get("fieldConfigs") or []
    summary = [{"slug": f.get("slug"), "displayName": f.get("displayName") or f.get("name"), "type": f.get("type")} for f in fields]
    dump_log("webflow_collection_details.json", details)
    dump_log("webflow_collection_fields_summary.json", summary)
    dump_log("webflow_field_map_suggestion.json", settings.webflow_field_map())
    print("inspect_webflow_collection ok")


def import_existing_blog() -> None:
    settings = load_settings()
    conn = connect()
    run_id = start_run(conn, "import_existing_blog")
    site = query_one(conn, "SELECT id FROM sites WHERE site_key=?", (settings.site_key,))
    webflow = WebflowService(settings)
    items = webflow.list_items(limit=100)
    inserted = 0
    for item in items:
        fd = item.get("fieldData") or {}
        slug = fd.get("slug")
        if not slug:
            continue
        url = f"{settings.blog_base_url}/{slug}"
        title = fd.get("name") or slug.replace("-", " ").title()
        conn.execute(
            "INSERT OR IGNORE INTO content_pages(site_id, page_url, slug, title_current, h1_current, page_type, status, webflow_item_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (site["id"], url, slug, title, title, "blog", "active", item.get("id")),
        )
        inserted += 1
    conn.commit()
    finish_run(conn, run_id, items_processed=inserted)
    print(f"import_existing_blog ok {inserted}")


def gsc_collect() -> None:
    settings = load_settings()
    conn = connect()
    run_id = start_run(conn, "gsc_collect")
    site = query_one(conn, "SELECT id FROM sites WHERE site_key=?", (settings.site_key,))
    gsc = GSCService(settings)
    page_rows = gsc.query_page_data()
    query_rows = gsc.query_query_data()
    today = utcnow().date().isoformat()
    for row in page_rows:
        conn.execute(
            "INSERT OR REPLACE INTO gsc_page_daily(site_id, date, page_url, clicks, impressions, ctr, position, data_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (site["id"], today, row["page_url"], row["clicks"], row["impressions"], row["ctr"], row["position"], "final"),
        )
    for row in query_rows:
        conn.execute(
            "INSERT OR REPLACE INTO gsc_query_daily(site_id, date, query, page_url, clicks, impressions, ctr, position, data_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (site["id"], today, row["query"], row["page_url"], row["clicks"], row["impressions"], row["ctr"], row["position"], "final"),
        )
    conn.commit()
    finish_run(conn, run_id, items_processed=len(page_rows) + len(query_rows))
    print(f"gsc_collect ok pages={len(page_rows)} queries={len(query_rows)}")


def recovery_score() -> None:
    conn = connect()
    run_id = start_run(conn, "recovery_score")
    rows = query_all(
        conn,
        """
        SELECT cp.id AS page_id, cp.page_url, cp.slug, cp.title_current, g.impressions, g.clicks, g.ctr, g.position
        FROM content_pages cp
        JOIN gsc_page_daily g ON g.page_url = cp.page_url
        WHERE cp.page_type='blog' AND g.date = (SELECT MAX(date) FROM gsc_page_daily)
        """,
    )
    max_impressions = max([r["impressions"] for r in rows], default=0)
    count = 0
    for row in rows:
        imp_score = normalize(row["impressions"], max_impressions)
        ctr_gap = max(0.0, expected_ctr_for_position(row["position"]) - float(row["ctr"])) * 1000
        ctr_gap_score = min(100.0, ctr_gap)
        pos_score = score_position(float(row["position"]))
        text = row["slug"] or ""
        business_fit = 100.0 if any(k in text for k in ["inbox", "gmail", "slack", "front", "email"]) else 70.0
        cluster_value = 100.0 if any(k in text for k in ["inbox", "gmail", "email"]) else 60.0
        opportunity = round(0.35 * imp_score + 0.25 * ctr_gap_score + 0.20 * pos_score + 0.10 * business_fit + 0.10 * cluster_value, 2)
        top_queries = query_all(conn, "SELECT query, clicks, impressions, ctr, position FROM gsc_query_daily WHERE page_url=? ORDER BY impressions DESC LIMIT 5", (row["page_url"],))
        conn.execute(
            "INSERT INTO recovery_candidates(page_id, window_start, window_end, impressions_28d, clicks_28d, ctr_28d, position_28d, top_queries_json, opportunity_score, reason_json, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                row["page_id"],
                utcnow().date().isoformat(),
                utcnow().date().isoformat(),
                row["impressions"],
                row["clicks"],
                row["ctr"],
                row["position"],
                json.dumps([dict(x) for x in top_queries]),
                opportunity,
                json.dumps({"business_fit": business_fit, "cluster_value": cluster_value}),
                "new",
            ),
        )
        candidate_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute("INSERT OR IGNORE INTO recovery_queue(page_id, candidate_id, priority, status) VALUES (?, ?, ?, ?)", (row["page_id"], candidate_id, int(1000 - opportunity * 10), "queued"))
        count += 1
    conn.commit()
    finish_run(conn, run_id, items_processed=count)
    print(f"recovery_score ok {count}")


def recovery_brief() -> None:
    conn = connect()
    run_id = start_run(conn, "recovery_brief")
    rows = query_all(
        conn,
        """
        SELECT rq.id AS queue_id, rq.page_id, cp.title_current, cp.slug, rc.top_queries_json, rc.opportunity_score
        FROM recovery_queue rq
        JOIN content_pages cp ON cp.id=rq.page_id
        JOIN recovery_candidates rc ON rc.id=rq.candidate_id
        WHERE rq.status='queued'
        ORDER BY rc.opportunity_score DESC
        LIMIT 5
        """,
    )
    n = 0
    for row in rows:
        top_queries = json.loads(row["top_queries_json"] or "[]")
        primary = top_queries[0]["query"] if top_queries else row["slug"].replace("-", " ")
        brief = {
            "page_id": row["page_id"],
            "primary_query": primary,
            "queries": top_queries,
            "brief_text": f"Improve this article so it matches the query '{primary}' more directly, keeps DEEMERGE relevant, adds stronger internal links, and improves CTR.",
        }
        conn.execute("UPDATE recovery_queue SET status=? WHERE id=?", ("briefed", row["queue_id"]))
        conn.execute("INSERT OR REPLACE INTO settings(key, value_json, updated_at) VALUES (?, ?, ?)", (f"recovery_brief:{row['page_id']}", json.dumps(brief), utcnow_iso()))
        n += 1
    conn.commit()
    finish_run(conn, run_id, items_processed=n)
    print(f"recovery_brief ok {n}")


def recovery_rewrite() -> None:
    conn = connect()
    settings = load_settings()
    run_id = start_run(conn, "recovery_rewrite")
    anthropic = AnthropicService(settings)
    rows = query_all(
        conn,
        """
        SELECT rq.id AS queue_id, cp.id AS page_id, cp.slug, cp.title_current, s.value_json AS brief_json
        FROM recovery_queue rq
        JOIN content_pages cp ON cp.id=rq.page_id
        LEFT JOIN settings s ON s.key = 'recovery_brief:' || cp.id
        WHERE rq.status='briefed'
        LIMIT 5
        """,
    )
    n = 0
    for row in rows:
        brief = json.loads(row["brief_json"] or "{}")
        user_prompt = f"Primary keyword: {brief.get('primary_query', row['slug'].replace('-', ' '))}\nCurrent title: {row['title_current']}\nTask: Rewrite the article for stronger search intent match and a natural DEEMERGE section."
        data = anthropic.generate_json(_load_prompt("rewrite_article_system.txt"), user_prompt)
        max_version = query_one(conn, "SELECT COALESCE(MAX(version_no), 0) AS v FROM page_versions WHERE page_id=?", (row["page_id"],))["v"]
        conn.execute(
            "INSERT INTO page_versions(page_id, version_no, source_type, title_tag, meta_description, h1, intro_html, body_html, headings_json, internal_links_json, cta_variant, notes_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                row["page_id"],
                max_version + 1,
                "rewrite",
                data.get("title_tag") or data.get("h1") or row["title_current"],
                data.get("meta_description", ""),
                data.get("h1") or row["title_current"],
                data.get("excerpt", ""),
                data.get("body_html", ""),
                json.dumps(data.get("outline_json", [])),
                json.dumps([]),
                "demo",
                json.dumps({"brief": brief}),
            ),
        )
        conn.execute("UPDATE recovery_queue SET status=? WHERE id=?", ("rewritten", row["queue_id"]))
        n += 1
    conn.commit()
    finish_run(conn, run_id, items_processed=n)
    print(f"recovery_rewrite ok {n}")


def validate_rewrites() -> None:
    conn = connect()
    run_id = start_run(conn, "validate_rewrites")
    rows = query_all(conn, "SELECT pv.id, pv.title_tag, pv.meta_description, pv.body_html FROM page_versions pv LEFT JOIN webflow_items wi ON wi.page_type='rewrite' AND wi.source_id=pv.id WHERE pv.source_type='rewrite' AND wi.id IS NULL LIMIT 20")
    n = 0
    for row in rows:
        score = 0
        if row["title_tag"]:
            score += 30
        if row["meta_description"] and len(row["meta_description"]) <= 180:
            score += 20
        if row["body_html"] and "DEEMERGE" in row["body_html"]:
            score += 30
        if row["body_html"] and "<h2>" in row["body_html"]:
            score += 20
        if score >= 75:
            n += 1
    finish_run(conn, run_id, items_processed=n)
    print(f"validate_rewrites ok {n}")


def keyword_intake() -> None:
    conn = connect()
    settings = load_settings()
    run_id = start_run(conn, "keyword_intake")
    dfs = DataForSEOService(settings)
    clusters = query_all(conn, "SELECT id, cluster_key, cluster_name FROM clusters WHERE status='active' ORDER BY priority ASC")
    seeds = {
        "shared_inbox": "shared inbox",
        "gmail_slack_coordination": "gmail slack workflow",
        "email_triage": "email triage",
        "alternatives": "front alternatives",
    }
    created = 0
    for cluster in clusters:
        suggestions = dfs.keyword_suggestions(seeds.get(cluster["cluster_key"], cluster["cluster_name"]))[:5]
        for suggestion in suggestions:
            kw = suggestion["keyword"]
            fit = 100 if cluster["cluster_key"] != "alternatives" or "alternative" in kw else 80
            commercial = 90 if any(x in kw for x in ["software", "best", "alternative"]) else 60
            volume_score = min(100, float(suggestion.get("volume") or 0) / 10)
            rankability = max(0, 100 - float(suggestion.get("difficulty") or 30) * 2)
            total = round(0.25 * volume_score + 0.20 * rankability + 0.20 * fit + 0.15 * 100 + 0.10 * commercial + 0.10 * 80, 2)
            conn.execute(
                "INSERT OR IGNORE INTO keyword_candidates(site_id, cluster_id, primary_keyword, secondary_keywords_json, source, volume, difficulty, intent_type, fit_score, cluster_score, total_score, status) VALUES ((SELECT id FROM sites WHERE site_key=?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    settings.site_key,
                    cluster["id"],
                    kw,
                    json.dumps([kw, f"best {kw}", f"{kw} software"]),
                    "dataforseo" if settings.has_dataforseo else "fallback",
                    suggestion.get("volume"),
                    suggestion.get("difficulty"),
                    "traffic" if cluster["cluster_key"] != "alternatives" else "commercial",
                    fit,
                    100,
                    total,
                    "new",
                ),
            )
            conn.execute("INSERT OR IGNORE INTO article_generation_queue(keyword_candidate_id, priority, status) VALUES ((SELECT id FROM keyword_candidates WHERE primary_keyword=?), ?, ?)", (kw, int(1000 - total * 10), "queued"))
            created += 1
    conn.commit()
    finish_run(conn, run_id, items_processed=created)
    print(f"keyword_intake ok {created}")


def article_brief() -> None:
    conn = connect()
    run_id = start_run(conn, "article_brief")
    rows = query_all(conn, "SELECT q.id AS queue_id, kc.cluster_id, kc.primary_keyword, kc.secondary_keywords_json, c.cluster_name FROM article_generation_queue q JOIN keyword_candidates kc ON kc.id=q.keyword_candidate_id JOIN clusters c ON c.id=kc.cluster_id WHERE q.status='queued' ORDER BY kc.total_score DESC LIMIT 10")
    n = 0
    for row in rows:
        brief_text = f"Write a search intent matched article for {row['primary_keyword']} inside the cluster {row['cluster_name']}. Explain naturally where DEEMERGE fits."
        conn.execute(
            "INSERT OR REPLACE INTO article_briefs(queue_id, cluster_id, primary_keyword, secondary_keywords_json, search_intent, article_angle, title_options_json, outline_json, internal_links_json, cta_angle, brief_text) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                row["queue_id"],
                row["cluster_id"],
                row["primary_keyword"],
                row["secondary_keywords_json"],
                "informational",
                "traffic capture with product relevance",
                json.dumps([row["primary_keyword"].title()]),
                json.dumps(["Introduction", "Why it matters", "How DEEMERGE helps"]),
                json.dumps([]),
                "demo",
                brief_text,
            ),
        )
        conn.execute("UPDATE article_generation_queue SET status=? WHERE id=?", ("briefed", row["queue_id"]))
        n += 1
    conn.commit()
    finish_run(conn, run_id, items_processed=n)
    print(f"article_brief ok {n}")


def article_write() -> None:
    conn = connect()
    settings = load_settings()
    anthropic = AnthropicService(settings)
    run_id = start_run(conn, "article_write")
    rows = query_all(conn, "SELECT ab.*, q.id AS queue_id FROM article_briefs ab JOIN article_generation_queue q ON q.id=ab.queue_id WHERE q.status='briefed' LIMIT 10")
    n = 0
    for row in rows:
        data = anthropic.generate_json(_load_prompt("article_write_system.txt"), f"Primary keyword: {row['primary_keyword']}\nSecondary keywords: {row['secondary_keywords_json']}\nTask: Write a blog article and explain DEEMERGE naturally.")
        slug = slugify(row["primary_keyword"])
        conn.execute(
            "INSERT OR REPLACE INTO article_drafts(queue_id, title_tag, meta_description, slug, h1, excerpt, body_html, faq_json, schema_json, image_prompt, quality_score, validation_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                row["queue_id"],
                data.get("title_tag") or data.get("h1") or row["primary_keyword"].title(),
                data.get("meta_description", ""),
                slug,
                data.get("h1") or row["primary_keyword"].title(),
                data.get("excerpt", ""),
                data.get("body_html", ""),
                json.dumps(data.get("faq_json", [])),
                json.dumps({}),
                data.get("image_prompt", f"Illustration for {row['primary_keyword']}"),
                0,
                json.dumps({}),
            ),
        )
        conn.execute("UPDATE article_generation_queue SET status=? WHERE id=?", ("written", row["queue_id"]))
        n += 1
    conn.commit()
    finish_run(conn, run_id, items_processed=n)
    print(f"article_write ok {n}")


def validate_articles() -> None:
    conn = connect()
    run_id = start_run(conn, "validate_articles")
    rows = query_all(conn, "SELECT ad.id, ad.title_tag, ad.meta_description, ad.body_html FROM article_drafts ad JOIN article_generation_queue q ON q.id=ad.queue_id WHERE q.status='written' LIMIT 20")
    n = 0
    for row in rows:
        score = 0
        if row["title_tag"]:
            score += 25
        if row["meta_description"] and len(row["meta_description"]) <= 180:
            score += 15
        if row["body_html"] and "<h2>" in row["body_html"]:
            score += 20
        if row["body_html"] and "DEEMERGE" in row["body_html"]:
            score += 20
        if row["body_html"] and len(row["body_html"]) > 200:
            score += 20
        conn.execute("UPDATE article_drafts SET quality_score=?, validation_json=? WHERE id=?", (score, json.dumps({"score": score}), row["id"]))
        n += 1
    conn.commit()
    finish_run(conn, run_id, items_processed=n)
    print(f"validate_articles ok {n}")


def link_resolve() -> None:
    conn = connect()
    run_id = start_run(conn, "link_resolve")
    pages = query_all(conn, "SELECT id, title_current, page_url FROM content_pages WHERE page_type='blog' ORDER BY id LIMIT 10")
    drafted = query_all(conn, "SELECT ad.id, ad.body_html FROM article_drafts ad LIMIT 20")
    n = 0
    for draft in drafted:
        body = draft["body_html"]
        for page in pages[:3]:
            anchor = page["title_current"] or page["page_url"]
            body += f'<p>Related reading: <a href="{page["page_url"]}">{anchor}</a>.</p>'
        conn.execute("UPDATE article_drafts SET body_html=? WHERE id=?", (body, draft["id"]))
        n += 1
    conn.commit()
    finish_run(conn, run_id, items_processed=n)
    print(f"link_resolve ok {n}")


def image_generate() -> None:
    conn = connect()
    ensure_dir("/data/images")
    run_id = start_run(conn, "image_generate")
    articles = query_all(conn, "SELECT id, slug, h1 FROM article_drafts WHERE id NOT IN (SELECT source_id FROM image_queue WHERE source_type='article') LIMIT 20")
    n = 0
    for article in articles:
        prompt = f"Editorial flat illustration for {article['h1']}"
        conn.execute("INSERT INTO image_queue(source_type, source_id, prompt, status) VALUES (?, ?, ?, ?)", ("article", article["id"], prompt, "generated"))
        qid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        img_path = Path("/data/images") / f"article_{article['slug']}.svg"
        svg = f'<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="630"><rect width="100%" height="100%" fill="white"/><text x="60" y="140" font-size="42" font-family="Arial">{article["h1"]}</text><text x="60" y="220" font-size="28" font-family="Arial">DEEMERGE</text></svg>'
        img_path.write_text(svg, encoding="utf-8")
        conn.execute("INSERT INTO generated_images(queue_id, local_path, alt_text, status) VALUES (?, ?, ?, ?)", (qid, str(img_path), article["h1"], "generated"))
        n += 1
    conn.commit()
    finish_run(conn, run_id, items_processed=n)
    print(f"image_generate ok {n}")


def _local_file_url(path: str) -> str:
    return f"file://{path}"


def webflow_sync_rewrites() -> None:
    settings = load_settings()
    conn = connect()
    run_id = start_run(conn, "webflow_sync_rewrites")
    webflow = WebflowService(settings)
    rows = query_all(conn, "SELECT pv.id, cp.id AS page_id, cp.slug, pv.title_tag, pv.meta_description, pv.body_html FROM page_versions pv JOIN content_pages cp ON cp.id=pv.page_id LEFT JOIN webflow_items wi ON wi.page_type='rewrite' AND wi.source_id=pv.id WHERE pv.source_type='rewrite' AND wi.id IS NULL LIMIT 10")
    n = 0
    for row in rows:
        res = webflow.upsert_staged_item(row["slug"], {"name": row["title_tag"], "slug": row["slug"], "summary": row["meta_description"], "body_html": row["body_html"], "meta_title": row["title_tag"], "meta_description": row["meta_description"]})
        conn.execute("INSERT INTO webflow_items(page_type, source_id, collection_id, item_id, slug, is_draft, sync_status, last_sync_at, payload_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", ("rewrite", row["id"], settings.webflow_collection_id or "mock_collection", res["item_id"], row["slug"], 1, "synced", utcnow_iso(), res["payload_hash"]))
        n += 1
    conn.commit()
    finish_run(conn, run_id, items_processed=n)
    print(f"webflow_sync_rewrites ok {n}")


def webflow_sync_articles() -> None:
    settings = load_settings()
    conn = connect()
    run_id = start_run(conn, "webflow_sync_articles")
    webflow = WebflowService(settings)
    rows = query_all(conn, "SELECT ad.id, ad.slug, ad.title_tag, ad.meta_description, ad.body_html, ad.h1, gi.local_path FROM article_drafts ad LEFT JOIN image_queue iq ON iq.source_type='article' AND iq.source_id=ad.id LEFT JOIN generated_images gi ON gi.queue_id=iq.id LEFT JOIN webflow_items wi ON wi.page_type='new_article' AND wi.source_id=ad.id WHERE wi.id IS NULL LIMIT 20")
    n = 0
    for row in rows:
        res = webflow.upsert_staged_item(row["slug"], {"name": row["h1"], "slug": row["slug"], "summary": row["meta_description"], "body_html": row["body_html"], "meta_title": row["title_tag"], "meta_description": row["meta_description"], "featured_image_url": _local_file_url(row["local_path"]) if row["local_path"] else "", "featured_image_alt": row["h1"]})
        conn.execute("INSERT INTO webflow_items(page_type, source_id, collection_id, item_id, slug, is_draft, sync_status, last_sync_at, payload_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", ("new_article", row["id"], settings.webflow_collection_id or "mock_collection", res["item_id"], row["slug"], 1, "synced", utcnow_iso(), res["payload_hash"]))
        n += 1
    conn.commit()
    finish_run(conn, run_id, items_processed=n)
    print(f"webflow_sync_articles ok {n}")


def plan_publish() -> None:
    conn = connect()
    run_id = start_run(conn, "plan_publish")
    unscheduled = query_all(conn, "SELECT page_type, source_id FROM webflow_items wi WHERE wi.sync_status='synced' AND NOT EXISTS (SELECT 1 FROM publish_plan pp WHERE pp.source_type=wi.page_type AND pp.source_id=wi.source_id) ORDER BY wi.created_at LIMIT 20")
    slots = next_weekday_slots(utcnow(), len(unscheduled))
    n = 0
    for row, slot in zip(unscheduled, slots):
        conn.execute("INSERT INTO publish_plan(source_type, source_id, planned_publish_ts_utc, status) VALUES (?, ?, ?, ?)", (row["page_type"], row["source_id"], slot.isoformat(), "planned"))
        n += 1
    conn.commit()
    finish_run(conn, run_id, items_processed=n)
    print(f"plan_publish ok {n}")


def publish_due() -> None:
    settings = load_settings()
    conn = connect()
    run_id = start_run(conn, "publish_due")
    webflow = WebflowService(settings)
    slack = SlackService(settings)
    due = query_all(conn, "SELECT pp.id, pp.source_type, pp.source_id, wi.item_id, wi.id AS wi_id, wi.slug FROM publish_plan pp JOIN webflow_items wi ON wi.page_type=pp.source_type AND wi.source_id=pp.source_id WHERE pp.status='planned' AND pp.planned_publish_ts_utc <= ?", (utcnow_iso(),))
    item_ids = [row["item_id"] for row in due if row["item_id"]]
    result = webflow.publish_items(item_ids) if due else {"published": 0}
    n = 0
    for row in due:
        conn.execute("UPDATE publish_plan SET status=?, actual_publish_ts_utc=? WHERE id=?", ("published", utcnow_iso(), row["id"]))
        conn.execute("UPDATE webflow_items SET is_draft=0, sync_status=?, last_published=?, updated_at=? WHERE id=?", ("published", utcnow_iso(), utcnow_iso(), row["wi_id"]))
        if row["source_type"] == "new_article":
            article = query_one(conn, "SELECT h1 FROM article_drafts WHERE id=?", (row["source_id"],))
            url = f"{settings.blog_base_url}/{row['slug']}"
            conn.execute(
                "INSERT OR IGNORE INTO content_pages(site_id, page_url, slug, title_current, h1_current, page_type, status, webflow_item_id, last_published_at) VALUES ((SELECT id FROM sites WHERE site_key=?), ?, ?, ?, ?, ?, ?, ?, ?)",
                (settings.site_key, url, row["slug"], article["h1"] if article else row["slug"], article["h1"] if article else row["slug"], "blog", "active", row["item_id"], utcnow_iso()),
            )
        n += 1
    conn.commit()
    if n:
        GSCService(settings).submit_sitemap(f"{settings.deemerge_base_url}/sitemap.xml")
        slack.send(f"DEEMERGE SEO published {n} item(s).")
    finish_run(conn, run_id, items_processed=n, cost_json=json.dumps(result))
    print(f"publish_due ok {n}")


def gsc_inspect_recent() -> None:
    settings = load_settings()
    conn = connect()
    run_id = start_run(conn, "gsc_inspect_recent")
    gsc = GSCService(settings)
    pages = query_all(conn, "SELECT page_url FROM content_pages ORDER BY last_published_at DESC, id DESC LIMIT 5")
    n = 0
    for page in pages:
        data = gsc.inspect_url(page["page_url"])
        conn.execute(
            "INSERT INTO gsc_url_inspections(site_id, page_url, inspection_ts, coverage_state, indexing_state, last_crawl_time, canonical_url, raw_json) VALUES ((SELECT id FROM sites WHERE site_key=?), ?, ?, ?, ?, ?, ?, ?)",
            (
                settings.site_key,
                page["page_url"],
                utcnow_iso(),
                data.get("coverage_state") or data.get("inspectionResult", {}).get("indexStatusResult", {}).get("coverageState"),
                data.get("indexing_state") or data.get("inspectionResult", {}).get("indexStatusResult", {}).get("robotsTxtState"),
                data.get("last_crawl_time") or data.get("inspectionResult", {}).get("indexStatusResult", {}).get("lastCrawlTime"),
                data.get("canonical_url") or data.get("inspectionResult", {}).get("indexStatusResult", {}).get("googleCanonical"),
                json.dumps(data),
            ),
        )
        n += 1
    conn.commit()
    finish_run(conn, run_id, items_processed=n)
    print(f"gsc_inspect_recent ok {n}")


def evaluate_results() -> None:
    conn = connect()
    run_id = start_run(conn, "evaluate_results")
    versions = query_all(conn, "SELECT pv.id AS version_id, pv.page_id, cp.page_url FROM page_versions pv JOIN content_pages cp ON cp.id=pv.page_id WHERE pv.source_type='rewrite' LIMIT 20")
    n = 0
    for version in versions:
        before = query_one(conn, "SELECT impressions_28d, clicks_28d, ctr_28d, position_28d FROM recovery_candidates WHERE page_id=? ORDER BY id DESC LIMIT 1", (version["page_id"],))
        after = query_one(conn, "SELECT SUM(impressions) AS impressions, SUM(clicks) AS clicks, AVG(ctr) AS ctr, AVG(position) AS position FROM gsc_page_daily WHERE page_url=?", (version["page_url"],))
        if before and after and after["impressions"] is not None:
            click_lift_score = min(100, max(0, (after["clicks"] or 0) - (before["clicks_28d"] or 0)) * 10)
            ctr_lift_score = min(100, max(0, ((after["ctr"] or 0) - (before["ctr_28d"] or 0)) * 10000))
            pos_gain_score = min(100, max(0, ((before["position_28d"] or 0) - (after["position"] or 0)) * 4))
            result = round(0.35 * click_lift_score + 0.25 * ctr_lift_score + 0.20 * pos_gain_score + 0.10 * 50 + 0.10 * 50, 2)
            decision = "keep" if result >= 60 else "watch"
            conn.execute(
                "INSERT OR REPLACE INTO recovery_results(page_id, version_id, eval_day, clicks_before, clicks_after, impressions_before, impressions_after, ctr_before, ctr_after, position_before, position_after, query_expansion_score, assisted_internal_clicks, result_score, decision) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    version["page_id"],
                    version["version_id"],
                    28,
                    before["clicks_28d"],
                    after["clicks"],
                    before["impressions_28d"],
                    after["impressions"],
                    before["ctr_28d"],
                    after["ctr"],
                    before["position_28d"],
                    after["position"],
                    0,
                    0,
                    result,
                    decision,
                ),
            )
            n += 1
    conn.commit()
    finish_run(conn, run_id, items_processed=n)
    print(f"evaluate_results ok {n}")


def queue_health() -> None:
    settings = load_settings()
    conn = connect()
    slack = SlackService(settings)
    article_q = query_one(conn, "SELECT COUNT(*) AS c FROM article_generation_queue WHERE status IN ('queued','briefed','written')")["c"]
    recovery_q = query_one(conn, "SELECT COUNT(*) AS c FROM recovery_queue WHERE status IN ('queued','briefed','rewritten')")["c"]
    payload = {"article_queue": article_q, "recovery_queue": recovery_q}
    dump_log("queue_health.json", payload)
    if article_q < 5 or recovery_q < 3:
        slack.send(f"DEEMERGE SEO queue low. articles={article_q}, recovery={recovery_q}")
    print(json.dumps(payload, indent=2))


def send_weekly_summary() -> None:
    settings = load_settings()
    conn = connect()
    email = EmailService(settings)
    summary = {
        "pages": query_one(conn, "SELECT COUNT(*) AS c FROM content_pages")["c"],
        "keywords": query_one(conn, "SELECT COUNT(*) AS c FROM keyword_candidates")["c"],
        "drafts": query_one(conn, "SELECT COUNT(*) AS c FROM article_drafts")["c"],
        "published": query_one(conn, "SELECT COUNT(*) AS c FROM publish_plan WHERE status='published'")["c"],
        "recovery_versions": query_one(conn, "SELECT COUNT(*) AS c FROM page_versions WHERE source_type='rewrite'")["c"],
    }
    dump_log("weekly_summary.json", summary)
    email.send("DEEMERGE SEO weekly summary", "\n".join(f"{k}: {v}" for k, v in summary.items()))
    print(json.dumps(summary, indent=2))


def backup_db() -> None:
    settings = load_settings()
    src = Path(settings.sqlite_path)
    dst_dir = ensure_dir("/data/backups")
    dst = dst_dir / f"deemerge_seo_machine_{utcnow().strftime('%Y%m%d_%H%M%S')}.db"
    dst.write_bytes(src.read_bytes())
    print(f"backup_db ok {dst}")


COMMANDS = {
    name: obj
    for name, obj in globals().items()
    if callable(obj)
    and not name.startswith("_")
    and name not in {"Path", "connect", "query_all", "query_one", "load_settings", "apply_migrations", "ensure_dir", "expected_ctr_for_position", "next_weekday_slots", "normalize", "score_position", "slugify", "utcnow", "utcnow_iso", "AnthropicService", "DataForSEOService", "EmailService", "GSCService", "SlackService", "WebflowService", "dump_log", "finish_run", "start_run"}
}


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print("Available commands:")
        for key in sorted(COMMANDS):
            print(" -", key)
        return 1
    COMMANDS[sys.argv[1]]()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
