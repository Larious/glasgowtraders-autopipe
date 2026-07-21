#!/usr/bin/env python3
"""bulk_delete_listings.py — trash listings from a human-reviewed CSV.

Rules (CLAUDE.md): trash-only (never force), CSV input only (never an ID
range), ≤25 per run, re-verify each post exists before trashing, dry-run by
default; --live is guard-gated (approval token).

CSV format: header row with a `post_id` column and an `action` column;
only rows with action == TRASH are used.

Usage:
  python3 bulk_delete_listings.py --csv data/dedup_trash_final.csv [--live]
"""
import argparse
import csv
import os
import sys
import time

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

WP = (os.getenv("WP_BASE_URL") or "https://www.glasgowtrader.co.uk").rstrip("/")
MAX_PER_RUN = 25

session = requests.Session()
session.auth = (
    os.getenv("WP_USERNAME") or os.getenv("WP_USER", ""),
    os.getenv("WP_APP_PASSWORD", ""),
)


def load_trash_ids(csv_path):
    """Post IDs from TRASH rows only; refuses over-limit batches."""
    ids = []
    with open(csv_path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if (row.get("action") or "").strip().upper() == "TRASH":
                ids.append(int(row["post_id"]))
    if len(ids) > MAX_PER_RUN:
        sys.exit(f"{len(ids)} TRASH rows exceeds the {MAX_PER_RUN}-per-run "
                 "batch rule — split the CSV.")
    return ids


def fetch_status(post_id):
    """Current status, or None if the post doesn't exist. Cache-busted:
    LiteSpeed caches authenticated /wp-json/ reads on this host."""
    r = session.get(f"{WP}/wp-json/wp/v2/listing/{post_id}",
                    params={"_fields": "id,status,title",
                            "nocache": f"{time.time():.6f}"},
                    timeout=30)
    if r.status_code != 200:
        return None
    return r.json()


def trash(post_id):
    # force is intentionally never sent true: trash-only, restorable.
    r = session.delete(f"{WP}/wp-json/wp/v2/listing/{post_id}",
                       params={"force": "false"}, timeout=30)
    return r.status_code == 200


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True,
                    help="human-reviewed trash plan CSV (post_id, action)")
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--sleep", type=float, default=1.0)
    args = ap.parse_args()

    if not session.auth[1]:
        sys.exit("WP credentials missing — run: python3 scripts/check_env.py")

    ids = load_trash_ids(args.csv)
    mode = "LIVE" if args.live else "DRY-RUN"
    print(f"[{mode}] {len(ids)} posts to trash from {args.csv}")

    # Re-verify every target first (deleted IDs 404 on re-delete; never
    # trust a stale list).
    targets = []
    for pid in ids:
        info = fetch_status(pid)
        time.sleep(0.3)
        if info is None:
            print(f"  SKIP {pid}: not found (already deleted?)")
        elif info.get("status") != "publish":
            print(f"  SKIP {pid}: status={info.get('status')!r}")
        else:
            title = (info.get("title", {}) or {}).get("rendered", "")
            targets.append(pid)
            print(f"  TRASH {pid}: {title[:55]}")

    if not args.live:
        print(f"\nNo writes made. {len(targets)} would be trashed. "
              "Re-run with --live after approval (touch .claude/APPROVED).")
        return

    done = fail = 0
    for pid in targets:
        if trash(pid):
            done += 1
        else:
            fail += 1
            print(f"  TRASH FAILED for {pid}")
        time.sleep(args.sleep)

    print(f"\n[LIVE] trashed {done}, failed {fail} "
          "(restorable: WP Admin -> Listings -> Trash)")


if __name__ == "__main__":
    main()
