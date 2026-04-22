# Hostinger deployment

Use Hostinger Docker Manager with this repository.

## Important
All secrets must be entered in Hostinger Docker Manager as environment variables.
Do not store production secrets in files inside this repo.

## Deploy shape
This repo is now production shaped for Docker Manager:
- code is copied into the image at build time
- only persistent Docker volumes are mounted for `/data`, `/logs`, and `/backups`
- no bind mounts of local source folders are used

## Required environment variables
Set these in Hostinger Docker Manager before starting the container:

- APP_ENV=production
- APP_TIMEZONE=Asia/Singapore
- SQLITE_PATH=/data/deemerge_seo_machine.db
- DEEMERGE_BASE_URL=https://www.deemerge.ai
- BLOG_BASE_URL=https://www.deemerge.ai/blog
- MAX_NEW_ARTICLES_PER_WEEK=10
- MAX_REWRITES_PER_WEEK=5
- ANTHROPIC_API_KEY
- ANTHROPIC_MODEL_MAIN
- ANTHROPIC_MODEL_FAST
- WEBFLOW_TOKEN
- WEBFLOW_SITE_ID
- WEBFLOW_COLLECTION_ID
- GSC_SITE_URL=https://www.deemerge.ai/
- GOOGLE_SERVICE_ACCOUNT_JSON_B64
- DATAFORSEO_LOGIN
- DATAFORSEO_PASSWORD
- SLACK_WEBHOOK_URL

Optional:
- SMTP_HOST
- SMTP_PORT
- SMTP_USER
- SMTP_PASS
- ALERT_EMAIL_TO

## First commands after container start
Use the Hostinger integrated terminal and run:

```bash
bash /app/scripts/hostinger_first_boot.sh
```

Then, if needed, run workers one by one:

```bash
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
```
