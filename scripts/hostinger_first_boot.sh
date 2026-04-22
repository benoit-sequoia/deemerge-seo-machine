#!/usr/bin/env bash
set -euo pipefail
python -m app.main init_db
python -m app.main seed_base
python -m app.main preflight_check
python -m app.main inspect_webflow_collection
python -m app.main import_existing_blog
python -m app.main gsc_collect
python -m app.main recovery_score
python -m app.main keyword_intake
