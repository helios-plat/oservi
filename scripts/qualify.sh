#!/usr/bin/env bash
set -u
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
echo "SOURCE_SHA=$(git rev-parse HEAD)"
python -c "import oservi; print('BASE_IMPORT_GATE=PASS')"
python -m pytest --collect-only -q
python -m pytest -q
python -m ruff check .
python -m mypy .
