---
description: Resolve google_place_id for existing listings and build the dedup ledger (dry-run first, token-gated live)
---

Delegate to the **listing-publisher** subagent. Sequence:

1. `python3 scripts/backfill_place_ids.py` (dry-run is the default). If this
   is the first ever run, do `--limit 50` first to sanity-check resolution
   quality, then the full dry-run.
2. Present the plan: counts by source (wp / csv / findplace), review-needed
   count, DUPLICATE_OF count, unresolved count, estimated Find Place calls,
   plus 5 sample rows from `data/place_backfill_plan.csv`. DUPLICATE_OF rows
   are duplicate listings surfacing — list them explicitly.
3. STOP for the human. Live approval: `touch .claude/APPROVED`.
4. `python3 scripts/backfill_place_ids.py --live` (the script checkpoints to
   the ledger per write, so an interrupted run resumes safely).
5. Report ledger size before/after, hand `data/place_backfill_review.csv`
   to the human for the low-confidence matches, and stage the ledger:
   `git add data/place_ledger.jsonl` (never `git add -A`).
