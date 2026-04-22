# DEEMERGE SEO Machine

Isolated DEEMERGE only SEO automation stack.

## What is in this package

This repository contains a runnable v1 codebase for:

1. importing existing blog items from Webflow or fixtures
2. pulling Search Console data from Google or fixtures
3. scoring recovery opportunities
4. generating recovery briefs and rewrite drafts
5. discovering new keywords from DataForSEO or fixtures
6. generating article briefs and article drafts
7. validating rewrites and new articles
8. resolving internal links
9. generating placeholder featured images
10. syncing drafts to Webflow or local fallback mode
11. planning publish slots
12. publishing articles and rewrites
13. evaluating results and sending notifications

## Isolation

This stack is siloed from Legend, X, and LinkedIn.
Use separate:

1. Docker project
2. SQLite database
3. Hostinger environment variables
4. logs
5. prompts
6. Slack webhook
7. Webflow collection

## Production deployment shape

This repo is prepared for Hostinger Docker Manager:

- code is baked into the image during build
- only persistent Docker volumes are mounted for `/data`, `/logs`, and `/backups`
- production secrets must be stored only in Hostinger Docker Manager environment variables

## Main commands

```bash
python -m app.main init_db
python -m app.main seed_base
python -m app.main import_existing_blog
python -m app.main gsc_collect
python -m app.main gsc_inspect_recent
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
python -m app.main evaluate_results
python -m app.main send_weekly_summary
python -m app.main queue_health
python -m app.main backup_db
```

## First boot on Hostinger

After the container starts, run:

```bash
bash /app/scripts/hostinger_first_boot.sh
```

See `docs/HOSTINGER_DEPLOY.md` for the deploy sequence.
