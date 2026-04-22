#!/usr/bin/env bash
set -euo pipefail

python -m app.main init_db
python -m app.main seed_base
python -m app.main import_existing_blog || true
python -m app.main gsc_collect || true
python -m app.main recovery_score || true
python -m app.main keyword_intake || true

echo "First boot sequence completed."
