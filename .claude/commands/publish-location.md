---
description: Discover and publish tradespeople for one location (dry-run first, token-gated live)
argument-hint: <Location name, e.g. Airdrie>
---

Target location: $ARGUMENTS

Delegate to the **listing-publisher** subagent. Sequence:

1. Confirm the location term exists:
   `GET $WP_BASE_URL/wp-json/wp/v2/location?search=$ARGUMENTS` — if it
   doesn't, stop and ask rather than creating taxonomy terms.
2. If `data/place_ledger.jsonl` is missing or empty, stop: the backfill
   (backlog item 2) must land first, otherwise this run will recreate
   duplicates.
3. Dry-run: `python3 batch_snipe_location.py --location "$ARGUMENTS" --dry-run`
4. Cross-check every candidate against the ledger by place_id (and by
   normalized phone as a fallback). Until backlog item 3 patches the
   publisher to do this itself, YOU do it here, and mark ledger hits as
   SKIP-duplicate in the summary.
5. Present the decision summary (create/skip counts, 5 samples, API cost,
   exact live command) and STOP for the human token.
6. Live run, ≤25 creates. Then verify 3 via the bridge, append every create
   to the ledger, write a run summary to `runs/`, and stage the ledger by
   name.

Reminder: a business already on the site that also serves $ARGUMENTS gets
the $ARGUMENTS location term ADDED to its existing post — not a new post.
