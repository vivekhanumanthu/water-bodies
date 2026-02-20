#!/usr/bin/env bash
set -euo pipefail

python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt

if ! command -v java >/dev/null 2>&1; then
  echo "Warning: Java runtime not found. PySpark requires Java (JRE 11+ recommended)."
fi

echo "Environment ready. Activate with: source .venv/bin/activate"
