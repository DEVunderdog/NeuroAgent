#!/bin/bash

set -euo pipefail

echo "--- running database migration ---"
alembic upgrade head

echo "--- starting initial operation ---"
python -m app.init_ops

echo "--- launching main application ---"
uvicorn app.main:app --host 127.0.0.1 --port 8000 --workers 1 --log-level debug