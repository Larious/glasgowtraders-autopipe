# Live place_id backfill — 2026-07-21 — COMPLETE (two passes)

## Final outcome
- **Ledger: 0 → 984 entries** (`data/place_ledger.jsonl`, staged to git).
- Pass 1 (09:55, token 09:54): 16 writes, then the process — wrongly launched
  as a subagent's background task — was reaped when the subagent's turn ended.
- Pass 2 (10:20 token, main session): completed all 1,023 listings, exit 0.

## Pass 2 stats (`runs/backfill_live_20260721_resume.log`)
| bucket | count |
|---|---|
| ledger (skipped, pass-1) | 16 |
| csv | 102 |
| findplace | 866 |
| written | 968 |
| DUPLICATE_OF (skipped) | 13 |
| review (human decides) | 20 |
| unresolved | 6 |

Review rows → `data/place_backfill_review.csv`; duplicates align with the 10
groups in `data/dedup_trash_plan.csv`.

## The false alarm, corrected
Between the passes, reads suggested pass-1's writes had been wiped server-side;
the working theory was that ListingPro re-serializes `lp_listingpro_options`
and drops unknown keys. **That theory was wrong.** Root cause:
**LiteSpeed caches authenticated `/wp-json/` GET responses**
(`x-litespeed-cache: hit`) — reads were serving pre-write snapshots. Verified
by cache-busted reads: all pass-1 writes were durable all along (5/5 sampled
PRESENT+match in lp options), and pass-2 writes verified 2/2 in the standalone
meta.

## Changes that came out of it (all tested; 15 unit tests green)
- `scripts/backfill_place_ids.py`: retry/backoff on transient SSL resets;
  incremental CSV flush; HTML-entity title cleaning; dry-run DUPLICATE_OF
  preview; standalone-meta verification; `nocache` cache-buster on all reads.
- `scripts/audit_listings.py`: reads standalone `gt_google_place_id`;
  `nocache` cache-buster.
- Bridge v2.2.0 (deployed): mirrors `google_place_id` to standalone post meta
  `gt_google_place_id` (belt-and-braces vs. option-array rewrites) and echoes
  it in `verified`. Old bridge v2.0.0 left inactive on the site — recommend
  deleting via wp-admin.

## Data notes
- The 16 pass-1 posts have their place_id in lp options only (old bridge —
  no standalone copy). `extract_place_id` falls back to lp options, so audits
  see them. A future one-off pass could mirror them to standalone meta.
- Site-config recommendation (human): exclude `/wp-json/` from LiteSpeed
  caching (wp-admin → LiteSpeed Cache → Cache → Do Not Cache URIs) —
  stale authenticated REST reads will keep biting anything unbusted.

## Next backlog steps
- Review `data/place_backfill_review.csv` (20 rows) + `runs/requery_proposals.csv`.
- Dedup: review `data/dedup_trash_plan.csv` (10 trash candidates, 3 NEEDS_HUMAN),
  add missing location terms to keepers, then token-gated trash run.
- Patch the three publishers to consult the ledger (backlog item 3).
