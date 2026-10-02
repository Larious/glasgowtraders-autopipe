---
description: Read-only health audit of every published listing (dupes, junk data, missing place_ids)
---

Delegate this to the **listing-auditor** subagent.

The auditor runs `scripts/check_env.py`, then `scripts/audit_listings.py`
(a `--limit 25` smoke pass first if this is the session's first audit), and
reports from the generated `runs/audit_*.md`:

- totals (audited / flagged / clean),
- flag counts sorted descending,
- the 10 largest duplicate-phone groups with post IDs,
- ranked, non-executed recommendations.

No WRITE-class script may run during an audit. When the report is back,
surface it to the human verbatim plus one line on what you'd do next.
