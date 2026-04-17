#!/usr/bin/env bash
# PostToolUse hook: run ruff check on any .py file written/edited by Claude.
# Receives hook JSON on stdin.

set -euo pipefail

# Resolve repo root relative to this script's location (.claude/hooks/ → repo root)
REPO=$(cd "$(dirname "$0")/../.." && pwd)

f=$(jq -r '.tool_input.file_path // empty')
[[ "$f" == *.py ]] || exit 0

if [[ "$f" == */collector/* ]]; then
    cd "$REPO/collector"
elif [[ "$f" == */bot/* ]]; then
    cd "$REPO/bot"
else
    exit 0
fi

nix develop "$REPO#default" --command ruff check "$f"
