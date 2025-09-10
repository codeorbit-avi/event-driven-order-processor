#!/usr/bin/env bash
set -euo pipefail

# Create/activate venv (Debian/Ubuntu-friendly)
if [ ! -d "venv" ]; then
  python3 -m venv venv
fi
source venv/bin/activate

pip install -r requirements.txt

# Run dev server
uvicorn app:app --reload --port 8000
