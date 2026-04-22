# Hostinger deploy

After the container is deployed and environment variables are saved, open the project terminal and run:

```bash
bash /app/scripts/hostinger_first_boot.sh
```

Then check `/logs` for:
- `preflight_check.json`
- `webflow_collection_fields_summary.json`
- `webflow_field_map_suggestion.json`

Current limitation:
- do not run `webflow_sync_articles` yet because the real DEEMERGE Webflow collection requires `og-image` and that asset upload path is not implemented in this package
