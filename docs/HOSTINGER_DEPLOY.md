# Hostinger deploy

## Upload

Upload the whole `deemerge_seo_machine` folder or the zip archive to the VPS and extract it.

## Environment variables

Store all variables in Hostinger Docker Manager. Do not use `.env` files.

Core variables:
- APP_ENV
- SQLITE_PATH
- WEBFLOW_TOKEN
- WEBFLOW_SITE_ID
- WEBFLOW_COLLECTION_ID
- WEBFLOW_FIELD_MAP_JSON
- GSC_SITE_URL
- GOOGLE_SERVICE_ACCOUNT_JSON_B64
- ANTHROPIC_API_KEY
- DATAFORSEO_LOGIN
- DATAFORSEO_PASSWORD
- SLACK_WEBHOOK_URL

## First commands inside the running container

```bash
python -m app.main init_db
python -m app.main seed_base
python -m app.main preflight_check
python -m app.main inspect_webflow_collection
python -m app.main import_existing_blog
python -m app.main gsc_collect
python -m app.main recovery_score
python -m app.main keyword_intake
```

Then inspect the logs folder and set the real `WEBFLOW_FIELD_MAP_JSON`.

## Next commands

```bash
python -m app.main recovery_brief
python -m app.main recovery_rewrite
python -m app.main validate_rewrites
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

## Important

Do not let live publish run before the Webflow field map is confirmed.
