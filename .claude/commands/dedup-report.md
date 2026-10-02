---
description: Group suspected duplicate listings from the newest audit and draft a human-review trash plan (proposal only)
---

Delegate to the **dedup-analyst** subagent. It reads the newest
`runs/audit_flags_*.csv`, groups duplicates (phone first, then title),
selects a KEEP per group (place_id > photos > oldest post), and writes
`runs/dedup_plan_<ts>.csv` with KEEP / TRASH_CANDIDATE / NEEDS_HUMAN rows.

Nothing is deleted by this command under any circumstances. Present the
summary and tell the human: review the CSV, remove any rows you disagree
with, and the trash run itself happens later as a separate token-gated step
via listing-publisher.

If no audit CSV exists yet, run `/audit-listings` first.
