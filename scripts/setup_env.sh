#!/usr/bin/env sh
set -eu

if command -v python3 >/dev/null 2>&1; then
  PYTHON_CMD="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_CMD="python"
else
  echo "Python was not found on PATH." >&2
  exit 1
fi

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)

cd "$REPO_ROOT"
echo "Using Python command: $PYTHON_CMD"

# Check if the .venv already exists and has packages matching the requirements.txt
if [ -d ".venv" ]; then
  if .venv/bin/python -m pip freeze | grep -F -x -f
    <(grep -F -v "^-r" requirements.txt | sort) >/dev/null; then
    echo "Virtual environment already exists and is up to date."
    echo "Activate with:"
    echo "  . .venv/bin/activate"
    exit 0
  else
    echo "Virtual environment exists but does not match requirements.txt. Recreating..."
    rm -rf .venv
  fi
fi

"$PYTHON_CMD" -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt

echo
echo "Environment ready."
echo "Activate with:"
echo "  . .venv/bin/activate"
