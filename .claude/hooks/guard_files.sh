#!/usr/bin/env bash
# guard_files.sh — PreToolUse hook for Edit|Write|MultiEdit.
# Blocks modification of secrets, the guard layer itself, and the approval token.

set -u

INPUT="$(cat)"
FILE="$(printf '%s' "$INPUT" | python3 -c '
import json, sys
try:
    print(json.load(sys.stdin).get("tool_input", {}).get("file_path", ""))
except Exception:
    print("")
' 2>/dev/null)"

[ -z "$FILE" ] && exit 0

case "$FILE" in
    *.env|*/.env)
        echo "GUARD BLOCKED: .env is human-managed. Ask the human to change credentials." >&2
        exit 2 ;;
    *dbt/profiles.yml)
        echo "GUARD BLOCKED: dbt/profiles.yml contains Snowflake credentials and is human-managed." >&2
        exit 2 ;;
    */.claude/settings.json|*/.claude/hooks/*)
        echo "GUARD BLOCKED: the guard layer cannot be edited by the agent. Ask the human." >&2
        exit 2 ;;
    */.claude/APPROVED)
        echo "GUARD BLOCKED: only the human creates approval tokens (touch .claude/APPROVED)." >&2
        exit 2 ;;
esac

exit 0
