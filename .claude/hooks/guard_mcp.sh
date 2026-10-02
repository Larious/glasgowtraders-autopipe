#!/usr/bin/env bash
# guard_mcp.sh — PreToolUse hook for MCP tool calls (matcher: mcp__.*).
# The Bash guard cannot see MCP traffic: a connected WordPress MCP server is
# its own authenticated write path to the live site. This hook closes that
# side door: any MCP tool whose name contains a write verb requires the same
# fresh, single-use .claude/APPROVED token as a live script run.
# Read tools (get/list/search/count/query/run_report/inspect) pass untouched.
#
# Exit 0 = allow (normal permission flow still applies). Exit 2 = block.

set -u

INPUT="$(cat)"
TOOL="$(printf '%s' "$INPUT" | python3 -c '
import json, sys
try:
    print(json.load(sys.stdin).get("tool_name", ""))
except Exception:
    print("")
' 2>/dev/null)"

[ -z "$TOOL" ] && exit 0
case "$TOOL" in mcp__*) ;; *) exit 0 ;; esac

PROJ="${CLAUDE_PROJECT_DIR:-$(pwd)}"
TOKEN="$PROJ/.claude/APPROVED"
TOKEN_TTL_SECS=3600

token_fresh() {
    [ -f "$TOKEN" ] || return 1
    local mtime now
    mtime=$(stat -c %Y "$TOKEN" 2>/dev/null || stat -f %m "$TOKEN" 2>/dev/null || echo 0)
    case "$mtime" in ''|*[!0-9]*) mtime=0 ;; esac
    now=$(date +%s)
    [ $(( now - mtime )) -lt "$TOKEN_TTL_SECS" ]
}

# Write verbs across the WordPress MCP tool set (wp_create_*, wp_update_*,
# wp_delete_*, wp_upload_*, wp_add_post_terms, wp_replace_in_post,
# wp_restore_revision). Deliberately broad: ANY MCP tool with a write verb is
# gated, because this project has no legitimate ungated MCP write.
if printf '%s' "$TOOL" | grep -Eq '_(add|create|update|delete|upload|restore|replace)(_|$)'; then
    if token_fresh; then
        rm -f "$TOKEN"   # single-use: one token authorizes one write call
        exit 0
    fi
    echo "GUARD BLOCKED: MCP write tool '$TOOL' needs a fresh approval token. MCP writes bypass the scripts and the ledger — prefer the script path. If this write is genuinely needed, show the human the plan; they authorize with: touch .claude/APPROVED (60 min, single use, one call)." >&2
    exit 2
fi

exit 0
