# CLAUDE.md — glasgowtraders-autopipe

Pipeline that discovers Glasgow-area tradespeople, enriches them, and publishes
listings to glasgowtrader.co.uk (WordPress + ListingPro theme). You are working
on a **live commercial site with 1,000+ published listings**. Reads are cheap.
Writes are gated. When in doubt, dry-run and show the human the plan.

## Golden rules (non-negotiable)

1. **Credentials come from `.env` only.** Never hardcode keys or passwords in
   any file, command, commit, or chat output. Never print `.env`,
   `dbt/profiles.yml`, or a `$WP_APP_PASSWORD`-style variable to the
   transcript. Scripts load secrets with `python-dotenv`.
2. **Dry-run is the default for every WordPress write.** A live run requires
   BOTH the live invocation AND a fresh approval token (see "Approval gate").
   This covers ALL write paths equally: pipeline scripts, raw curl, and any
   connected WordPress MCP tool (`wp_create_*`, `wp_update_*`, `wp_delete_*`,
   `wp_upload_*`, `wp_add_*`, `wp_replace_*`, `wp_restore_*`) — MCP writes
   also bypass the ledger, so prefer the script path even with a token. The
   PreToolUse hooks enforce all of this. Do not attempt to work around the
   guards; if one blocks you, stop and tell the human why.
3. **Every listing is keyed on `google_place_id`.** Dedup on place_id via
   `data/place_ledger.jsonl`, never on title strings — title matching produced
   the April 2026 duplicate mess. Any code that creates a listing must record
   `{place_id → wp_post_id}` in the ledger in the same run.
4. **Never permanently delete.** WordPress deletes go to trash only
   (`force=true` is blocked by the guard). Never empty trash.
5. **Google ToS boundary.** Do not write or run code that downloads Google
   Places photos, or that expands what we store from Places (business names,
   addresses, reviews are restricted content; directory use is a named
   prohibited use). The one safe datum is `place_id` — storable indefinitely.
   Direction of travel: replace Google-sourced content with the ListingPro
   claim flow, Companies House API, and Gas Safe verification.
6. **One post per business.** A business serving several towns gets one post
   with multiple `location` taxonomy terms — never one post per location.
7. **The domain is `glasgowtrader.co.uk` — no "s".** API base:
   `https://www.glasgowtrader.co.uk` (the www matters; bare and `glasgowtraders`
   variants fail DNS/SSL). Always use `$WP_BASE_URL`, never type the domain.
8. **Small batches.** ≤25 listing creates or trashes per run, ≥1 s sleep
   between creates, ≥0.3 s between media/meta calls, timeout on every request,
   and verify a sample of 3 via the bridge after each batch. Bulk meta
   backfills (e.g. place_id) may exceed 25 per approved run because they
   checkpoint to the ledger and rate-limit internally. The host drops SSL
   connections under sustained load.
9. **Edit files with the Edit tool.** Never paste code via heredocs or nano —
   that corrupted indentation repeatedly in this repo's history.
10. **Never modify** `.env`, `dbt/profiles.yml`, `.claude/settings.json`, or
    anything in `.claude/hooks/`. The file guard blocks these; that is intended.

## Environment

- Host: macOS (zsh). System `python3` is **3.14** — it breaks dbt (mashumaro
  bug). dbt lives in a Python 3.12 venv: `source dbt-env/bin/activate`, then
  `cd dbt && dbt run` / `dbt test`. Deactivate before running pipeline scripts.
- Docker (should be up): `glasgowtraders-autopipe-airflow-1` (:8080),
  `...-postgres-1` (:5433, user `pipeline`, db `glasgow_traders`),
  `...-redis-1` (:6379). Check with `docker ps`.
- dbt targets **Snowflake** (db `GLASGOW_TRADERS`, schema `INTERMEDIATE`).
  Models: `stg_raw_listings` (view) and `int_listings_enriched` (table with
  `health_score`, `meta_description`). Nothing consumes the mart yet — that is
  backlog item 5, not an accident to "fix" silently.

### Environment variables (canonical names; some older scripts read the alias)

| Canonical               | Legacy alias     | Used for                              |
|-------------------------|------------------|---------------------------------------|
| `WP_BASE_URL`           | —                | `https://www.glasgowtrader.co.uk`      |
| `WP_USERNAME`           | `WP_USER`        | WP application-password auth (user `Dev`) |
| `WP_APP_PASSWORD`       | —                | WP application password                |
| `GOOGLE_PLACES_API_KEY` | `GOOGLE_API_KEY` | Places API                             |

New code reads the canonical name and falls back to the alias. To check config
without exposing values: `python3 scripts/check_env.py` (prints SET/MISSING only).

## Architecture and script inventory

Flow today: Google Places → filters (UK-only, ≤15 km, trade-keyword) →
WP REST (`/wp/v2/listing`, `/wp/v2/media`) → custom bridge writes ListingPro
meta. The directory started with plumbers but now spans multiple trades
(roofers, builders, joiners, electricians...) — treat the trade keyword list
and `listing-category` ID as per-run parameters, never assume "plumber".
The Snowflake/dbt layer runs in parallel and is not yet in the write path.

| Script | Class | Notes |
|---|---|---|
| `scripts/check_env.py` | READ | Config sanity, no values printed |
| `scripts/audit_listings.py` | READ | Full-site health report → `runs/` |
| `scripts/backfill_place_ids.py` | READ; WRITE with `--live` | Builds the place_id ledger |
| `batch_snipe_location.py` | WRITE | Per-location discover+publish; has `--dry-run` |
| `bulk_publish_plumbers.py` | WRITE | Multi-location publisher; has `--dry-run` |
| `enhanced_publisher.py` | WRITE | CSV publisher w/ photos+hours (photo download now ToS-frozen) |
| `snipe_one_plumber.py` | WRITE | Single-listing test publisher; no dry-run flag → always needs a token |
| `bulk_delete_listings.py` | DESTRUCTIVE | Trash-only; always needs a token; only from a human-reviewed audit CSV, never an ID range |
| `fix_bad_hours.py` | READ; WRITE with `--fix` | Hours repair |

## WordPress / ListingPro specifics

- Custom bridge plugin (`gt-listingpro-rest-bridge-v2.php`):
  - `GET  /wp-json/glasgow-traders/v1/listing-meta/{post_id}` → all post meta,
    unserialized (see `meta.lp_listingpro_options.value`).
  - `POST /wp-json/glasgow-traders/v1/listing-options/{post_id}` → merges
    arbitrary keys into the `lp_listingpro_options` serialized array (it strips
    `save_post` hooks first so ListingPro can't corrupt the write) and echoes
    written fields back under `verified`.
- Sidebar data lives at the TOP LEVEL of `lp_listingpro_options` (`phone`,
  `gAddress`, `latitude`, `longitude`, `website`, `tagline_text`,
  `claimed_section`, `business_hours`, `gallery`). Never nest under an
  `options` sub-key — that was the April bug.
- Gallery: write image IDs to post meta `gallery_image_ids` (comma-separated)
  and `gallery` inside lp options. Featured image via `featured_media`.
- Hours format: `{"Monday": {"open": "08:00am", "close": "05:00pm"}, ...}`.
  Google encodes 24/7 as a single period with day 0 and **no close key** —
  expand it to all seven days `12:00am–11:59pm`; do not save one Sunday row.
- Taxonomies: `listing-category` (Plumber = 22) and `location` (107 terms;
  Glasgow = 250, Airdrie = 173 — fetch the rest from
  `/wp/v2/location?per_page=100`, don't hardcode the map).
- WP media upload headers are latin-1: ASCII-sanitize filenames
  (`name.encode('ascii', 'ignore')`) and strip invisible Unicode tag
  characters from business names before building slugs (one listing shipped
  with a percent-encoded flag-emoji slug).

## Approval gate (how live writes happen)

1. You run the dry-run and post a summary: what will be created or changed,
   counts, sample rows, estimated Google API cost.
2. The human reviews and, if satisfied, runs in their own terminal:
   `touch .claude/APPROVED`
3. The token is valid for **60 minutes** and is **single-use** — the guard
   consumes it when it authorizes one live command. One token, one command.
4. You run the live command, then update `data/place_ledger.jsonl`, append a
   run summary to `runs/`, and report results with sample verification.

Never create `.claude/APPROVED` yourself, and never suggest pre-creating
tokens for future runs.

## The ledger

`data/place_ledger.jsonl` — one JSON object per line:
`{"place_id": "...", "wp_post_id": 123, "name": "...", "phone": "+44...",
"source": "csv|findplace|manual", "ts": "ISO-8601"}`.
It is the dedup source of truth and **must be committed to git**. (It's
`.jsonl` deliberately: the repo `.gitignore` excludes `*.csv`.)

## Subagents (delegate; don't do their work in the main thread)

| Agent | Class | Use for |
|---|---|---|
| `listing-auditor` | READ-ONLY | Site health, data quality, pre-publish checks |
| `dedup-analyst` | PROPOSE-ONLY | Turning audit output into a reviewable trash plan |
| `listing-publisher` | WRITE (gated) | Discovery, publishing, place_id backfill, trash runs |
| `warehouse-engineer` | WAREHOUSE | dbt models, Snowflake, Airflow DAGs |

`listing-publisher` is the only agent permitted to make live WordPress writes,
and only through the approval gate below. If any other agent is asked to write,
it refuses and names the publisher.

## Slash commands

- `/audit-listings` — read-only site health report (dupes, missing place_ids,
  non-UK addresses, junk phones, merchants miscategorized, photo-less posts).
- `/backfill-place-ids` — resolve and record place_ids for existing listings.
- `/dedup-report` — group suspected duplicates from the latest audit and
  propose (never execute) a trash list.
- `/publish-location <Location>` — dry-run discovery + publish for one location.

## Backlog (work top-down; ask before reordering)

1. Run `/audit-listings`; present findings.
2. Run `/backfill-place-ids` (dry-run → human token → live).
3. Patch the three publishers to (a) consult the ledger before creating,
   (b) write `google_place_id` into lp options at creation, and (c) attach an
   extra location term to an existing post instead of creating a clone.
4. Trash approved duplicates/junk from the dedup report (token-gated, from a
   reviewed CSV).
5. Wire publishing to read from Snowflake `int_listings_enriched`
   (health_score ≥ 60 gate; use its `meta_description`) via the Airflow DAG.
6. Location-page content workflow — unique copy per `/location/` page; these
   are the only URLs currently ranking. Site copy must follow the anti-AI
   writing guide (`SKILL.md` in `glasgowtrader-agents/`) when available.

## Known traps (each one cost real hours)

- Deleted post IDs 404 on re-delete — re-fetch current IDs every run; never
  reuse a hardcoded range.
- `zsh` chokes on pasted `#` comments and multi-line snippets — write scripts
  to files and execute them; don't paste blocks into the shell.
- Google Find Place with a placeholder `place_id` returns 400
  INVALID_ARGUMENT — validate inputs before spending API calls.
- Title-based dedup misses punctuation/spacing/branch variants — that is why
  the ledger exists.
- Snowflake auth failures mean the pipeline role lacks grants on
  warehouse/db/schema. Fix the grants; never switch to ACCOUNTADMIN.
