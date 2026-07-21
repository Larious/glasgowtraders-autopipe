#!/usr/bin/env python3
"""fix_missing_categories.py — assign listing-category to posts that were
created without one (batch_snipe underscore-vs-hyphen bug). Reads the target
posts from the ledger by timestamp prefix, recomputes the category from the
business name via the batch sniper's own rules, and only writes posts that
currently have an empty listing-category. Dry-run by default; --live gated.

Usage:
  python3 scripts/fix_missing_categories.py --since 2026-07-21T13:0 [--live]
"""
import argparse
import json
import os
import sys
import time

import requests
from requests.auth import HTTPBasicAuth

try:
    from dotenv import load_dotenv
    load_dotenv(override=True)
except ImportError:
    pass

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# reuse the trade registry + categorize() from the batch sniper
_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)
import importlib.util
_spec = importlib.util.spec_from_file_location(
    "batch_snipe", os.path.join(_root, "batch_snipe_location.py"))
bs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bs)

WP = (os.getenv("WP_BASE_URL") or "https://www.glasgowtrader.co.uk").rstrip("/")
AUTH = HTTPBasicAuth(os.getenv("WP_USERNAME") or os.getenv("WP_USER", ""),
                     os.getenv("WP_APP_PASSWORD", ""))
LEDGER = "data/place_ledger.jsonl"
CAT_LABELS = bs.CAT_LABELS

session = requests.Session()
session.auth = AUTH


def current_category(post_id):
    r = session.get(f"{WP}/wp-json/wp/v2/listing/{post_id}",
                    params={"_fields": "id,listing-category",
                            "nocache": f"{time.time():.6f}"}, timeout=30)
    if r.status_code != 200:
        return None
    return r.json().get("listing-category", []) or []


def set_category(post_id, cat_id):
    r = session.post(f"{WP}/wp-json/wp/v2/listing/{post_id}",
                     json={"listing-category": [cat_id]}, timeout=30)
    if r.status_code not in (200, 201):
        return False
    return cat_id in (r.json().get("listing-category", []) or [])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", required=True,
                    help="ledger ts prefix, e.g. 2026-07-21T13:0")
    ap.add_argument("--trade", default="gardener", choices=sorted(bs.TRADES))
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--sleep", type=float, default=0.4)
    args = ap.parse_args()

    if not AUTH.password:
        sys.exit("WP credentials missing — run: python3 scripts/check_env.py")

    trade_cfg = bs.TRADES[args.trade]
    targets = [json.loads(l) for l in open(LEDGER, encoding="utf-8")
               if json.loads(l)["ts"].startswith(args.since)]
    mode = "LIVE" if args.live else "DRY-RUN"
    print(f"[{mode}] {len(targets)} ledger posts since {args.since}")

    plan, fixed, skipped, failed = [], 0, 0, 0
    for rec in targets:
        pid = rec["wp_post_id"]
        cat = bs.categorize(rec["name"], trade_cfg)
        label = CAT_LABELS.get(cat, str(cat))
        cur = current_category(pid)
        time.sleep(0.3)
        if cur is None:
            print(f"  {pid} MISSING (404?) — skip")
            skipped += 1
            continue
        if cur:
            print(f"  {pid} already categorized {cur} — skip")
            skipped += 1
            continue
        plan.append((pid, cat, label, rec["name"]))
        print(f"  {pid} -> {label}({cat})  {rec['name'][:40]}")

    if not args.live:
        print(f"\n{len(plan)} would be fixed. Re-run with --live after "
              "approval (touch .claude/APPROVED).")
        return

    for pid, cat, label, name in plan:
        if set_category(pid, cat):
            fixed += 1
        else:
            failed += 1
            print(f"  SET FAILED for {pid}")
        time.sleep(args.sleep)
    print(f"\n[LIVE] fixed {fixed}, failed {failed}, skipped {skipped}")


if __name__ == "__main__":
    main()
