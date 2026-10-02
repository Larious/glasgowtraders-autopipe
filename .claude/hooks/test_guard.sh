#!/usr/bin/env bash
# test_guard.sh — verifies the PreToolUse guards behave as specified.
# Run from the repo root:  bash .claude/hooks/test_guard.sh
# Exit 0 = all scenarios pass.

set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
export CLAUDE_PROJECT_DIR="$(cd "$HERE/../.." && pwd)"
GW="$HERE/guard_writes.sh"
GF="$HERE/guard_files.sh"
TOKEN="$CLAUDE_PROJECT_DIR/.claude/APPROVED"
PASS=0; FAIL=0

bash_case() {  # bash_case <expected_exit> <description> <command string>
    local expected="$1" desc="$2" cmd="$3" rc
    printf '{"tool_input": {"command": %s}}' \
        "$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$cmd")" \
        | bash "$GW" >/dev/null 2>&1
    rc=$?
    verdict "$expected" "$rc" "$desc"
}

file_case() {  # file_case <expected_exit> <description> <file_path>
    local expected="$1" desc="$2" path="$3" rc
    printf '{"tool_input": {"file_path": "%s"}}' "$path" \
        | bash "$GF" >/dev/null 2>&1
    rc=$?
    verdict "$expected" "$rc" "$desc"
}

verdict() {
    local expected="$1" rc="$2" desc="$3"
    if [ "$rc" -eq "$expected" ]; then
        echo "  PASS  [$rc] $desc"; PASS=$((PASS+1))
    else
        echo "  FAIL  [got $rc, want $expected] $desc"; FAIL=$((FAIL+1))
    fi
}

rm -f "$TOKEN"
echo "guard_writes.sh:"
bash_case 2 "bulk delete without token is blocked"            'python3 bulk_delete_listings.py'
bash_case 0 "dry-run publish is allowed"                      'python3 batch_snipe_location.py --location "Airdrie" --dry-run'
bash_case 2 "live publish without token is blocked"           'python3 batch_snipe_location.py --location "Airdrie"'
bash_case 2 "snipe (no dry-run flag exists) is blocked"       'python3 snipe_one_plumber.py'
bash_case 0 "backfill without --live is allowed (read-only)"  'python3 scripts/backfill_place_ids.py --limit 50'
bash_case 2 "backfill --live without token is blocked"        'python3 scripts/backfill_place_ids.py --live'
bash_case 0 "fix_bad_hours scan is allowed"                   'python3 fix_bad_hours.py'
bash_case 2 "fix_bad_hours --fix without token is blocked"    'python3 fix_bad_hours.py --fix'
bash_case 2 "cat .env is blocked"                             'cat .env'
bash_case 0 "cat .env.example is allowed"                     'cat .env.example'
bash_case 2 "reading dbt/profiles.yml is blocked"             'cat dbt/profiles.yml'
bash_case 2 "echoing WP_APP_PASSWORD is blocked"              'echo $WP_APP_PASSWORD'
bash_case 2 "git add -A is blocked"                           'git add -A'
bash_case 2 "git add . is blocked"                            'git add .'
bash_case 0 "explicit git add is allowed"                     'git add data/place_ledger.jsonl'
bash_case 2 "force-push is blocked"                           'git push --force origin main'
bash_case 2 "curl DELETE to the site without token is blocked" 'curl -u "$WP_USERNAME:$WP_APP_PASSWORD" -X DELETE "$WP_BASE_URL/wp-json/wp/v2/listing/27139"'
bash_case 2 "WP force=true hard delete is blocked"            'curl -X DELETE "$WP_BASE_URL/wp-json/wp/v2/listing/27139?force=true"'
bash_case 0 "plain GET curl to the site is allowed"           'curl -u "$WP_USERNAME:$WP_APP_PASSWORD" "$WP_BASE_URL/wp-json/wp/v2/listing?per_page=5"'
bash_case 2 "rm -rf on home is blocked"                       'rm -rf $HOME/glasgowtraders-autopipe'
bash_case 0 "rm -rf on /tmp is allowed"                       'rm -rf /tmp/gt-scratch'
bash_case 0 "harmless command is allowed"                     'ls -la runs/'

echo "single-use token:"
touch "$TOKEN"
bash_case 0 "live publish WITH fresh token is allowed"        'python3 batch_snipe_location.py --location "Airdrie"'
if [ ! -f "$TOKEN" ]; then
    echo "  PASS  token was consumed (single-use)"; PASS=$((PASS+1))
else
    echo "  FAIL  token still present after authorized run"; FAIL=$((FAIL+1)); rm -f "$TOKEN"
fi
bash_case 2 "second live run after consumption is blocked"    'python3 batch_snipe_location.py --location "Airdrie"'

echo "stale token:"
touch "$TOKEN"
OLD=$(( $(date +%s) - 7200 ))
touch -d "@$OLD" "$TOKEN" 2>/dev/null || touch -t "$(date -r $OLD +%Y%m%d%H%M 2>/dev/null || echo 202601010000)" "$TOKEN" 2>/dev/null
bash_case 2 "2-hour-old token is rejected as stale"           'python3 batch_snipe_location.py --location "Airdrie"'
rm -f "$TOKEN"

echo "guard_files.sh:"
file_case 2 "editing .env is blocked"                 "/repo/.env"
file_case 2 "editing dbt/profiles.yml is blocked"     "/repo/dbt/profiles.yml"
file_case 2 "editing the hooks is blocked"            "/repo/.claude/hooks/guard_writes.sh"
file_case 2 "editing settings.json is blocked"        "/repo/.claude/settings.json"
file_case 2 "creating the APPROVED token is blocked"  "/repo/.claude/APPROVED"
file_case 0 "editing a pipeline script is allowed"    "/repo/scripts/audit_listings.py"
file_case 0 "editing CLAUDE.md is allowed"            "/repo/CLAUDE.md"

GM="$HERE/guard_mcp.sh"
mcp_case() {  # mcp_case <expected_exit> <description> <tool_name>
    local expected="$1" desc="$2" tool="$3" rc
    printf '{"tool_name": "%s", "tool_input": {}}' "$tool" \
        | bash "$GM" >/dev/null 2>&1
    rc=$?
    verdict "$expected" "$rc" "$desc"
}

echo "guard_mcp.sh:"
rm -f "$TOKEN"
mcp_case 0 "MCP read (wp_get_post) is allowed"                "mcp__WordPress__wp_get_post"
mcp_case 0 "MCP read (wp_list_posts) is allowed"              "mcp__WordPress__wp_list_posts"
mcp_case 0 "MCP read (wp_count_posts) is allowed"             "mcp__WordPress__wp_count_posts"
mcp_case 0 "MCP read (wp_search) is allowed"                  "mcp__WordPress__wp_search"
mcp_case 0 "non-WordPress MCP read is allowed"                "mcp__Semrush__execute_report"
mcp_case 2 "MCP write (wp_create_post) without token blocked" "mcp__WordPress__wp_create_post"
mcp_case 2 "MCP write (wp_update_post) without token blocked" "mcp__WordPress__wp_update_post"
mcp_case 2 "MCP write (wp_delete_term) without token blocked" "mcp__WordPress__wp_delete_term"
mcp_case 2 "MCP write (wp_upload_media) without token blocked" "mcp__WordPress__wp_upload_media"
mcp_case 2 "MCP write (wp_add_post_terms) without token blocked" "mcp__WordPress__wp_add_post_terms"
mcp_case 2 "MCP write (wp_replace_in_post) without token blocked" "mcp__WordPress__wp_replace_in_post"
touch "$TOKEN"
mcp_case 0 "MCP write WITH fresh token is allowed"            "mcp__WordPress__wp_create_post"
if [ ! -f "$TOKEN" ]; then
    echo "  PASS  MCP token was consumed (single-use)"; PASS=$((PASS+1))
else
    echo "  FAIL  MCP token still present after authorized call"; FAIL=$((FAIL+1)); rm -f "$TOKEN"
fi
mcp_case 2 "second MCP write after consumption is blocked"    "mcp__WordPress__wp_update_post"

echo
echo "RESULT: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
