#!/usr/bin/env python3
"""backfill_place_ids.py — resolve google_place_id for every published listing
and build data/place_ledger.jsonl, the dedup source of truth.

Resolution order per listing:
  1. Already in the ledger -> skip (resume-safe).
  2. google_place_id already in lp_listingpro_options -> record (source=wp), no API cost.
  3. Match local plumbers_by_location_*.csv by phone, then by name (source=csv), no API cost.
  4. Google Find Place from text "{title}, {gAddress}" (source=findplace).
     Name similarity < 0.62 goes to data/place_backfill_review.csv instead of
     the ledger — a human decides those.
  If a resolved place_id already maps to a DIFFERENT post, the row is marked
  DUPLICATE_OF and nothing is written: that is a duplicate listing surfacing.

Dry-run by default: prints a summary and writes data/place_backfill_plan.csv.
--live: POSTs {"google_place_id": ...} to the bridge per post and appends the
ledger as it goes. --live is gated by the PreToolUse guard (approval token).

Usage:
  python3 scripts/backfill_place_ids.py [--limit N] [--sleep 0.35] [--live]
"""
import argparse
import csv
import difflib
import glob
import html
import json
import os
import re
import ssl
import sys
import time
from datetime import datetime

import requests
from requests.auth import HTTPBasicAuth

# Transient errors worth retrying: the host drops SSL connections under
# sustained load (documented trap), which surfaces as a raw ConnectionResetError
# or, when requests wraps it, a ConnectionError / Timeout / SSLError.
TRANSIENT_ERRORS = (
    ConnectionResetError,
    ssl.SSLError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
)


def request_with_retry(fn, *, retries=5, base_delay=1.0, sleep=time.sleep):
    """Call fn(); retry transient network errors with exponential backoff.

    Retries up to `retries` total attempts, sleeping base_delay * 2**(k-1)
    seconds before the k-th retry. Non-transient exceptions propagate
    immediately. The last transient exception is re-raised after exhaustion.
    """
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except TRANSIENT_ERRORS:
            if attempt == retries:
                raise
            sleep(base_delay * (2 ** (attempt - 1)))

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
GKEY = os.getenv("GOOGLE_PLACES_API_KEY") or os.getenv("GOOGLE_API_KEY", "")
LEDGER = "data/place_ledger.jsonl"
SIM_THRESHOLD = 0.62

session = requests.Session()
session.auth = AUTH


def clean_title(rendered):
    """WP rendered titles carry HTML entities (&#038;, &#8211;, ...); decode
    them before querying Google or scoring name similarity."""
    return html.unescape(rendered or "")


def norm_name(t):
    return re.sub(r"[^a-z0-9]", "", (t or "").lower())


def norm_phone(p):
    digits = re.sub(r"\D", "", p or "")
    return digits[-10:] if len(digits) >= 10 else digits


def load_ledger():
    by_post, by_place = {}, {}
    if os.path.exists(LEDGER):
        with open(LEDGER, encoding="utf-8") as f:
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


def append_ledger(rec):
    os.makedirs("data", exist_ok=True)
    with open(LEDGER, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def load_csv_index():
    """Index every discovery CSV in the repo by phone and by normalized name."""
    by_phone, by_name = {}, {}
    for path in glob.glob("plumbers_by_location_*.csv") + glob.glob("data/*_by_location_*.csv"):
        try:
            with open(path, encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    pid = (row.get("google_place_id") or "").strip()
                    if not pid:
                        continue
                    name = row.get("name", "")
                    np, nn = norm_phone(row.get("phone", "")), norm_name(name)
                    if np:
                        by_phone.setdefault(np, (pid, name))
                    if nn:
                        # name collisions across CSVs: keep only unambiguous names
                        if nn in by_name and by_name[nn][0] != pid:
                            by_name[nn] = None
                        else:
                            by_name.setdefault(nn, (pid, name))
        except (OSError, csv.Error):
            continue
    by_name = {k: v for k, v in by_name.items() if v}
    return by_phone, by_name


def fetch_all_listings():
    out, page = [], 1
    while True:
        r = request_with_retry(lambda p=page: session.get(
            f"{WP}/wp-json/wp/v2/listing",
            params={"per_page": 100, "page": p, "status": "publish",
                    "_fields": "id,title,link"},
            timeout=30,
        ))
        if r.status_code != 200:
            break
        batch = r.json()
        if not batch:
            break
        out.extend(batch)
        page += 1
    return out


def fetch_listing_meta(post_id):
    # LiteSpeed caches authenticated /wp-json/ GETs on this host; a unique
    # query param forces a cache miss so reads reflect the current DB state.
    r = request_with_retry(lambda: session.get(
        f"{WP}/wp-json/glasgow-traders/v1/listing-meta/{post_id}",
        params={"nocache": f"{time.time():.6f}"}, timeout=30
    ))
    if r.status_code != 200:
        return {}
    return r.json().get("meta", {}) or {}


def lp_options(meta):
    return (meta.get("lp_listingpro_options", {}) or {}).get("value", {}) or {}


def extract_place_id(meta):
    """Standalone gt_google_place_id first (durable — bridge v2.1), then the
    legacy lp_listingpro_options key (ListingPro wipes unknown keys there)."""
    standalone = (meta.get("gt_google_place_id", {}) or {}).get("value", "")
    if standalone:
        return standalone
    return lp_options(meta).get("google_place_id", "") or ""


def find_place(query):
    if not GKEY:
        return None
    r = request_with_retry(lambda: requests.get(
        "https://maps.googleapis.com/maps/api/place/findplacefromtext/json",
        params={"input": query, "inputtype": "textquery",
                "fields": "place_id,name,formatted_address", "key": GKEY},
        timeout=30,
    ))
    data = r.json()
    cands = data.get("candidates") or []
    return cands[0] if cands else None


def write_place_id(post_id, place_id):
    r = request_with_retry(lambda: session.post(
        f"{WP}/wp-json/glasgow-traders/v1/listing-options/{post_id}",
        json={"google_place_id": place_id},
        timeout=30,
    ))
    if r.status_code != 200:
        return False
    verified = r.json().get("verified", {})
    # Require the standalone-meta echo (bridge v2.1): lp_listingpro_options is
    # wiped server-side, so an old bridge that only echoes the lp key must
    # fail loudly rather than record writes that won't survive.
    return (verified.get("google_place_id") == place_id
            and verified.get("gt_google_place_id") == place_id)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--sleep", type=float, default=0.35)
    ap.add_argument("--live", action="store_true",
                    help="write to WordPress + ledger (guard-gated)")
    args = ap.parse_args()

    if not AUTH.password:
        sys.exit("WP credentials missing — run: python3 scripts/check_env.py")

    by_post, by_place = load_ledger()
    csv_phone, csv_name = load_csv_index()
    listings = fetch_all_listings()
    if args.limit:
        listings = listings[: args.limit]

    mode = "LIVE" if args.live else "DRY-RUN"
    print(f"[{mode}] {len(listings)} listings | ledger has {len(by_post)} | "
          f"CSV index: {len(csv_phone)} phones / {len(csv_name)} names")

    stats = {"ledger": 0, "wp": 0, "csv": 0, "findplace": 0,
             "review": 0, "duplicate": 0, "unresolved": 0, "written": 0}

    # Write plan/review rows incrementally and flush each one, so a hard failure
    # (e.g. SSL resets exhausting the retries) still leaves partial results on
    # disk instead of losing the whole run.
    os.makedirs("data", exist_ok=True)
    plan_f = open("data/place_backfill_plan.csv", "w", newline="", encoding="utf-8")
    plan_w = csv.writer(plan_f)
    plan_w.writerow(["post_id", "title", "place_id", "source", "detail"])
    plan_f.flush()
    review_state = {"f": None, "w": None}

    def emit_plan(row):
        plan_w.writerow(row)
        plan_f.flush()

    def emit_review(row):
        if review_state["f"] is None:
            rf = open("data/place_backfill_review.csv", "w", newline="",
                      encoding="utf-8")
            rw = csv.writer(rf)
            rw.writerow(["post_id", "title", "address", "candidate_place_id",
                         "candidate_name", "candidate_address", "similarity"])
            review_state["f"], review_state["w"] = rf, rw
        review_state["w"].writerow(row)
        review_state["f"].flush()

    try:
        for i, post in enumerate(listings, 1):
            pid = post["id"]
            title = clean_title(post.get("title", {}).get("rendered", ""))
            if pid in by_post:
                stats["ledger"] += 1
                continue

            meta = fetch_listing_meta(pid)
            lp = lp_options(meta)
            phone = lp.get("phone", "")
            addr = lp.get("gAddress", "")
            place_id, source, matched_name = None, None, ""

            existing = extract_place_id(meta)
            if existing:
                place_id, source, matched_name = existing, "wp", title
            if not place_id:
                hit = csv_phone.get(norm_phone(phone)) if phone else None
                if hit:
                    place_id, source, matched_name = hit[0], "csv", hit[1]
            if not place_id:
                hit = csv_name.get(norm_name(title))
                if hit:
                    place_id, source, matched_name = hit[0], "csv", hit[1]
            if not place_id:
                cand = find_place(f"{title}, {addr or 'Glasgow, Scotland, UK'}")
                time.sleep(args.sleep)
                if cand:
                    sim = difflib.SequenceMatcher(
                        None, norm_name(title), norm_name(cand.get("name", ""))
                    ).ratio()
                    if sim >= SIM_THRESHOLD:
                        place_id, source = cand["place_id"], "findplace"
                        matched_name = cand.get("name", "")
                    else:
                        stats["review"] += 1
                        emit_review([pid, title, addr, cand.get("place_id", ""),
                                     cand.get("name", ""),
                                     cand.get("formatted_address", ""),
                                     f"{sim:.2f}"])
                        continue

            if not place_id:
                stats["unresolved"] += 1
                emit_plan([pid, title, "", "UNRESOLVED", ""])
                continue

            clash = by_place.get(place_id)
            if clash and int(clash.get("wp_post_id", 0)) != pid:
                stats["duplicate"] += 1
                emit_plan([pid, title, place_id, "DUPLICATE_OF",
                           clash.get("wp_post_id", "")])
                continue

            stats[source] += 1
            emit_plan([pid, title, place_id, source, matched_name])

            if not args.live:
                # Track resolutions in-run so a later post resolving to the
                # same place_id is flagged DUPLICATE_OF in the plan preview
                # (live mode gets this via the ledger append below).
                by_place[place_id] = {"wp_post_id": pid}

            if args.live:
                if write_place_id(pid, place_id):
                    rec = {"place_id": place_id, "wp_post_id": pid,
                           "name": title, "phone": phone, "source": source,
                           "ts": datetime.now().isoformat(timespec="seconds")}
                    append_ledger(rec)
                    by_place[place_id] = rec
                    by_post[pid] = rec
                    stats["written"] += 1
                else:
                    print(f"  WRITE FAILED for post {pid} — stopping.")
                    break
                time.sleep(args.sleep)

            if i % 50 == 0:
                print(f"  ...{i}/{len(listings)}")
    finally:
        plan_f.close()
        if review_state["f"]:
            review_state["f"].close()

    print(f"\n[{mode}] summary")
    for k, v in stats.items():
        print(f"  {k:<11} {v}")
    print("  plan   -> data/place_backfill_plan.csv")
    if stats["review"]:
        print("  review -> data/place_backfill_review.csv (human decides these)")
    if not args.live:
        print("\nNo writes made. Re-run with --live after approval "
              "(touch .claude/APPROVED).")


if __name__ == "__main__":
    main()
