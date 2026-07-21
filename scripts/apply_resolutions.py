#!/usr/bin/env python3
"""apply_resolutions.py — one-shot applier for the 2026-07-21 review/dedup
resolutions. Dry-run by default; --live is guard-gated (approval token).

Does three kinds of write, all ≤25 total per the batch rule:
  1. write_place_id  — human-ACCEPTed review rows from
     data/place_backfill_review_resolved.csv → bridge write + ledger append.
  2. write_place_id + move_ledger — dedup keeper fixes from
     data/dedup_execution_plan.json: the live backfill recorded the place_id
     against the post that will be trashed; write the place_id to the keeper
     and repoint the ledger entry at it.
  3. add_location    — extra location terms for keepers whose duplicate
     carried a different town (one post per business, many location terms).

Usage:
  python3 scripts/apply_resolutions.py [--live] [--sleep 0.4]
"""
import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime

import requests
from requests.auth import HTTPBasicAuth

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

WP = (os.getenv("WP_BASE_URL") or "https://www.glasgowtrader.co.uk").rstrip("/")
AUTH = HTTPBasicAuth(
    os.getenv("WP_USERNAME") or os.getenv("WP_USER", ""),
    os.getenv("WP_APP_PASSWORD", ""),
)
LEDGER = "data/place_ledger.jsonl"
PLAN = "data/dedup_execution_plan.json"

session = requests.Session()
session.auth = AUTH


def build_ops(accept_rows, keeper_moves, location_adds, ledger_by_post):
    """Turn the reviewed inputs into an ordered list of write operations."""
    ops = []
    for row in accept_rows:
        if row.get("decision") != "ACCEPT":
            continue
        pid = int(row["post_id"])
        if pid in ledger_by_post:
            continue
        ops.append({"op": "write_place_id", "post_id": pid,
                    "place_id": row["place_id"], "name": row["title"],
                    "ledger_action": "append"})
    for place_id, mv in keeper_moves.items():
        ops.append({"op": "write_place_id", "post_id": int(mv["to_post"]),
                    "place_id": place_id, "name": mv["name"],
                    "ledger_action": "none"})
        ops.append({"op": "move_ledger", "post_id": int(mv["to_post"]),
                    "place_id": place_id, "name": mv["name"],
                    "from_post": int(mv["from_post"])})
    for post_id, loc in location_adds.items():
        ops.append({"op": "add_location", "post_id": int(post_id),
                    "term_id": int(loc["term_id"]),
                    "term_name": loc["term_name"]})
    return ops


def rewrite_ledger_entry(path, place_id, new_post_id, name):
    """Repoint the ledger line for place_id at new_post_id (atomic rewrite)."""
    lines = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            if rec.get("place_id") == place_id:
                rec["wp_post_id"] = new_post_id
                rec["name"] = name
                rec["ts"] = datetime.now().isoformat(timespec="seconds")
            lines.append(json.dumps(rec, ensure_ascii=False))
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    os.replace(tmp, path)


def load_ledger_by_post():
    out = {}
    if os.path.exists(LEDGER):
        with open(LEDGER, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    rec = json.loads(line)
                    out[int(rec.get("wp_post_id", 0))] = rec
    return out


def write_place_id(post_id, place_id):
    r = session.post(
        f"{WP}/wp-json/glasgow-traders/v1/listing-options/{post_id}",
        json={"google_place_id": place_id}, timeout=30,
    )
    if r.status_code != 200:
        return False
    v = r.json().get("verified", {})
    return (v.get("google_place_id") == place_id
            and v.get("gt_google_place_id") == place_id)


def add_location_term(post_id, term_id):
    # LiteSpeed caches authenticated /wp-json/ reads — cache-bust the GET.
    r = session.get(f"{WP}/wp-json/wp/v2/listing/{post_id}",
                    params={"_fields": "id,location",
                            "nocache": f"{time.time():.6f}"},
                    timeout=30)
    if r.status_code != 200:
        return False
    current = r.json().get("location", []) or []
    if term_id in current:
        return True
    r = session.post(f"{WP}/wp-json/wp/v2/listing/{post_id}",
                     json={"location": current + [term_id]}, timeout=30)
    if r.status_code != 200:
        return False
    return term_id in (r.json().get("location", []) or [])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--sleep", type=float, default=0.4)
    args = ap.parse_args()

    if not AUTH.password:
        sys.exit("WP credentials missing — run: python3 scripts/check_env.py")

    plan = json.load(open(PLAN, encoding="utf-8"))
    accept_rows = list(csv.DictReader(
        open(plan["review_accepts_csv"], encoding="utf-8")))
    ledger_by_post = load_ledger_by_post()
    ops = build_ops(accept_rows, plan["keeper_moves"],
                    plan["location_adds"], ledger_by_post)

    writes = [o for o in ops if o["op"] != "move_ledger"]
    if len(writes) > 25:
        sys.exit(f"{len(writes)} writes exceeds the 25-per-run batch rule.")

    mode = "LIVE" if args.live else "DRY-RUN"
    print(f"[{mode}] {len(ops)} operations ({len(writes)} WP writes):")
    for o in ops:
        if o["op"] == "write_place_id":
            print(f'  write_place_id  post {o["post_id"]:>6} <- {o["place_id"]}'
                  f'  ({o["name"][:38]})')
        elif o["op"] == "move_ledger":
            print(f'  move_ledger     post {o["from_post"]:>6} -> {o["post_id"]}'
                  f'  ({o["name"][:38]})')
        else:
            print(f'  add_location    post {o["post_id"]:>6} += '
                  f'{o["term_name"]}({o["term_id"]})')

    if not args.live:
        print("\nNo writes made. Re-run with --live after approval "
              "(touch .claude/APPROVED).")
        return

    ok = fail = 0
    for o in ops:
        if o["op"] == "write_place_id":
            if write_place_id(o["post_id"], o["place_id"]):
                ok += 1
                if o.get("ledger_action") == "append":
                    with open(LEDGER, "a", encoding="utf-8") as f:
                        f.write(json.dumps(
                            {"place_id": o["place_id"],
                             "wp_post_id": o["post_id"], "name": o["name"],
                             "phone": "", "source": "manual",
                             "ts": datetime.now().isoformat(timespec="seconds")},
                            ensure_ascii=False) + "\n")
            else:
                fail += 1
                print(f'  WRITE FAILED: place_id for post {o["post_id"]}')
            time.sleep(args.sleep)
        elif o["op"] == "move_ledger":
            rewrite_ledger_entry(LEDGER, o["place_id"], o["post_id"], o["name"])
            ok += 1
        elif o["op"] == "add_location":
            if add_location_term(o["post_id"], o["term_id"]):
                ok += 1
            else:
                fail += 1
                print(f'  WRITE FAILED: location for post {o["post_id"]}')
            time.sleep(args.sleep)

    print(f"\n[LIVE] done: {ok} ok, {fail} failed")


if __name__ == "__main__":
    main()
