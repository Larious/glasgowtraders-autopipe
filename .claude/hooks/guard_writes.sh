#!/usr/bin/env bash
# guard_writes.sh — PreToolUse hook for Bash tool calls.
# Exit 0 = no objection (normal permission flow still applies).
# Exit 2 = BLOCK; stderr is shown to Claude as the reason.
# NOTE: exit 1 does NOT block in Claude Code — policy denials must be exit 2.
#
# This is a tripwire, not a sandbox. It enforces the CLAUDE.md write contract:
# live WordPress writes require a fresh, single-use .claude/APPROVED token.

set -u

INPUT="$(cat)"
CMD="$(printf '%s' "$INPUT" | python3 -c '
import json, sys
try:
    print(json.load(sys.stdin).get("tool_input", {}).get("command", ""))
except Exception:
    print("")
' 2>/dev/null)"

# Fail open on unparseable input rather than bricking the session.
[ -z "$CMD" ] && exit 0

PROJ="${CLAUDE_PROJECT_DIR:-$(pwd)}"
TOKEN="$PROJ/.claude/APPROVED"
TOKEN_TTL_SECS=3600

deny() {
    echo "GUARD BLOCKED: $1" >&2
    exit 2
}

token_fresh() {
    [ -f "$TOKEN" ] || return 1
    local mtime now
    # GNU stat first (-c), BSD/macOS fallback (-f). On Linux, `stat -f %m`
    # "succeeds" but prints the mount point, so order and validation matter.
    mtime=$(stat -c %Y "$TOKEN" 2>/dev/null || stat -f %m "$TOKEN" 2>/dev/null || echo 0)
    case "$mtime" in ''|*[!0-9]*) mtime=0 ;; esac
    now=$(date +%s)
    [ $(( now - mtime )) -lt "$TOKEN_TTL_SECS" ]
}

consume_token() {
    rm -f "$TOKEN"
}

require_token() {
    if token_fresh; then
        consume_token
        exit 0   # authorized: token was fresh and is now consumed (single-use)
    fi
    deny "$1 Live writes need a fresh approval token. Show the human the dry-run summary; they authorize with: touch .claude/APPROVED (valid 60 min, single use)."
}

# ---------- 1. Secret hygiene ----------------------------------------------
if printf '%s' "$CMD" | grep -Eq '(^|[;&| ])(cat|less|more|head|tail|bat|strings|xxd|vi|vim|nano|open|source)[[:space:]]' \
   && printf '%s' "$CMD" | grep -Eq '\.env([[:space:]";&|]|$)'; then
    deny "Reading .env is not allowed. Use: python3 scripts/check_env.py (reports SET/MISSING without values)."
fi
if printf '%s' "$CMD" | grep -Eq 'dbt/profiles\.yml' \
   && printf '%s' "$CMD" | grep -Eq '(cat|less|more|head|tail|bat|strings|grep|open|vi|vim|nano)'; then
    deny "Reading dbt/profiles.yml is not allowed (contains Snowflake credentials)."
fi
if printf '%s' "$CMD" | grep -Eq '(echo|printf|printenv)[^|;&]*\$\{?(WP_APP_PASSWORD|GOOGLE_API_KEY|GOOGLE_PLACES_API_KEY|SNOWFLAKE_PASSWORD)'; then
    deny "Printing credential environment variables to the transcript is not allowed."
fi

# ---------- 2. Always forbidden --------------------------------------------
printf '%s' "$CMD" | grep -Eq 'rm[[:space:]]+(-[a-zA-Z]*r[a-zA-Z]*[[:space:]]+)+("?/([^t]|$)|~|"?\$HOME)' \
    && deny "Recursive delete outside the project or temp areas."
printf '%s' "$CMD" | grep -Eq 'git push[^|;&]*(--force|-f )' \
    && deny "Force-push is not allowed."
printf '%s' "$CMD" | grep -Eq 'git add[[:space:]]+(-A|--all|\.)([[:space:]]|$)' \
    && deny "Blanket staging is not allowed (dbt/profiles.yml is still tracked). Stage files explicitly by name."
printf '%s' "$CMD" | grep -Eqi 'force=true' \
    && deny "force=true permanently deletes in WordPress. Trash-only policy (CLAUDE.md rule 4)."
printf '%s' "$CMD" | grep -Eqi 'drop table|truncate table' \
    && deny "Destructive SQL is not allowed from the agent."

# ---------- 3. Gated WordPress writes ---------------------------------------
WRITE_SCRIPTS='bulk_publish_plumbers\.py|enhanced_publisher\.py|batch_snipe_location\.py|snipe_one_plumber\.py|populate_all_locations\.py|fix_nested_options\.py|add_plumber_category\.py|backfill_place_ids\.py|fix_bad_hours\.py'
DESTRUCTIVE_SCRIPTS='bulk_delete_listings\.py'

if printf '%s' "$CMD" | grep -Eq "$DESTRUCTIVE_SCRIPTS"; then
    require_token "bulk_delete_listings.py trashes live listings."
fi

if printf '%s' "$CMD" | grep -Eq "$WRITE_SCRIPTS"; then
    if printf '%s' "$CMD" | grep -q -- '--dry-run'; then
        exit 0
    fi
    # backfill/fix scripts are read-only unless --live / --fix is present
    if printf '%s' "$CMD" | grep -Eq 'backfill_place_ids\.py|fix_bad_hours\.py' \
       && ! printf '%s' "$CMD" | grep -Eq -- '--live|--fix'; then
        exit 0
    fi
    require_token "This command writes to the live site without --dry-run."
fi

# Raw HTTP writes to the site (curl/httpie/wget)
if printf '%s' "$CMD" | grep -Eq '(curl|http|wget)[^|;&]*' \
   && printf '%s' "$CMD" | grep -Eqi 'glasgowtrader|\$WP_BASE_URL' \
   && printf '%s' "$CMD" | grep -Eq -- '-X[[:space:]]*(POST|PUT|PATCH|DELETE)|--method[[:space:]]*(POST|PUT|PATCH|DELETE)|--data|-d[[:space:]]|--json'; then
    require_token "Raw HTTP write to the live WordPress site."
fi

exit 0
