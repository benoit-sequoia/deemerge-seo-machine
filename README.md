# DEEMERGE SEO machine

This package is isolated from Legend, LinkedIn, and X.

Current state of this build:
- safe first boot commands are implemented
- Webflow collection inspection is implemented
- existing blog import is implemented
- recovery candidate scoring and keyword intake are implemented
- basic article and rewrite draft generation is implemented with fallback templates
- rewrite sync to existing Webflow items is implemented as draft updates
- new article sync is intentionally blocked for now because your real Webflow collection requires `og-image` and the image upload pipeline is not implemented yet

## First boot inside the running container

```bash
bash /app/scripts/hostinger_first_boot.sh
```

That will create these files in `/logs`:
- `preflight_check.json`
- `webflow_collection_details.json`
- `webflow_collection_fields_summary.json`
- `webflow_field_map_suggestion.json`

## Important

Do not run `webflow_sync_articles` yet.

You can run `webflow_sync_rewrites` later for existing articles after first boot if the imported pages have matching Webflow item ids.


V3 fixes: live Search Console support when Google credentials are valid, URL-matched fixture fallback, HTML fragment cleanup for Anthropic output, stricter draft validation.


V5 quality patch adds stricter meta generation, no unsupported claims, no h1 in body, mandatory DEEMERGE section, and mandatory CTA section.
