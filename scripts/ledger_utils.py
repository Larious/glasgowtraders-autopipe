#!/usr/bin/env python3
"""ledger_utils.py — shared helpers for data/place_ledger.jsonl, the dedup
source of truth (CLAUDE.md rule 3: every listing is keyed on google_place_id;
dedup on place_id, never on title strings).

Every publisher must:
  1. load_ledger() before a run,
  2. existing_post_for(by_place, place_id) before creating — on a hit, attach
     the location term to the existing post instead of creating a clone,
  3. record() immediately after a successful create, in the same run.
"""
import json
import os
import time
from datetime import datetime

DEFAULT_LEDGER = "data/place_ledger.jsonl"


def load_ledger(path=DEFAULT_LEDGER):
    """Return (by_post, by_place) dicts; tolerant of missing/corrupt lines."""
    by_post, by_place = {}, {}
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                by_post[int(rec.get("wp_post_id", 0))] = rec
                by_place[rec.get("place_id", "")] = rec
    return by_post, by_place


def existing_post_for(by_place, place_id):
    """wp_post_id already holding this place_id, or None."""
    rec = by_place.get(place_id)
    return int(rec["wp_post_id"]) if rec else None


def record(path, place_id, wp_post_id, name, phone, source):
    """Append one ledger entry (call in the same run as the create)."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    rec = {"place_id": place_id, "wp_post_id": wp_post_id, "name": name,
           "phone": phone, "source": source,
           "ts": datetime.now().isoformat(timespec="seconds")}
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return rec


def attach_location(session, wp_base, post_id, term_id):
    """Add a location term to an existing post (idempotent). Returns True if
    the term is present afterwards. Cache-busts the read: LiteSpeed caches
    authenticated /wp-json/ GETs on this host."""
    r = session.get(f"{wp_base}/wp-json/wp/v2/listing/{post_id}",
                    params={"_fields": "id,location",
                            "nocache": f"{time.time():.6f}"},
                    timeout=30)
    if r.status_code != 200:
        return False
    current = r.json().get("location", []) or []
    if term_id in current:
        return True
    r = session.post(f"{wp_base}/wp-json/wp/v2/listing/{post_id}",
                     json={"location": current + [term_id]}, timeout=30)
    if r.status_code != 200:
        return False
    return term_id in (r.json().get("location", []) or [])
