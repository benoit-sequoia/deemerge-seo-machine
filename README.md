# DEEMERGE SEO Machine

Isolated SEO automation stack for DEEMERGE only. It is separated from Legend, X, and LinkedIn.

## What is included

- SQLite database and migrations
- recovery engine for existing articles
- new article engine
- Webflow collection inspection and staged sync
- Google Search Console collector
- Anthropic writing integration
- DataForSEO keyword intake integration
- Slack and email notifications
- publish planner and publish runner

## Config source

Use Hostinger Docker Manager environment variables only. Do not store secrets in files or in code.

## Main commands

```bash
python -m app.main init_db
python -m app.main seed_base
python -m app.main preflight_check
python -m app.main inspect_webflow_collection
python -m app.main import_existing_blog
python -m app.main gsc_collect
python -m app.main recovery_score
python -m app.main recovery_brief
python -m app.main recovery_rewrite
python -m app.main validate_rewrites
python -m app.main keyword_intake
python -m app.main article_brief
python -m app.main article_write
python -m app.main validate_articles
python -m app.main link_resolve
python -m app.main image_generate
python -m app.main webflow_sync_rewrites
python -m app.main webflow_sync_articles
python -m app.main plan_publish
python -m app.main publish_due
python -m app.main gsc_inspect_recent
python -m app.main evaluate_results
python -m app.main queue_health
python -m app.main send_weekly_summary
python -m app.main backup_db
```

## First live sequence

1. upload project to VPS
2. create Hostinger Docker project
3. set environment variables in Hostinger Docker Manager
4. run first boot script
5. inspect `/logs/webflow_collection_fields_summary.json` and `/logs/webflow_field_map_suggestion.json`
6. set final `WEBFLOW_FIELD_MAP_JSON` in Hostinger
7. rerun `inspect_webflow_collection` and first sync commands

## Notes

- Real API code is implemented for Anthropic, Webflow, DataForSEO, and Search Console.
- If credentials are missing, the machine falls back to fixture data or mock behavior so the pipeline can still be tested.
- Webflow field mapping must be confirmed against your actual collection before the first live content sync.
