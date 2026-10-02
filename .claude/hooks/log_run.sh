#!/usr/bin/env bash
# log_run.sh — PostToolUse hook for Bash. Appends an audit line per command.
set -u
INPUT="$(cat)"
PROJ="${CLAUDE_PROJECT_DIR:-$(pwd)}"
mkdir -p "$PROJ/runs"
printf '%s' "$INPUT" | python3 -c '
import json, sys, datetime
try:
    d = json.load(sys.stdin)
    cmd = d.get("tool_input", {}).get("command", "")
    entry = {"ts": datetime.datetime.now().isoformat(timespec="seconds"),
             "command": cmd[:800]}
    print(json.dumps(entry))
except Exception:
    pass
' >> "$PROJ/runs/bash_ledger.jsonl" 2>/dev/null
exit 0
