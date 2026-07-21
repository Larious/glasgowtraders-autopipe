#!/usr/bin/env python3
"""audit_listings.py — READ-ONLY health audit of every published listing.

Pulls all listings plus their ListingPro meta via the bridge plugin and flags:
  NO_PLACE_ID   not in data/place_ledger.jsonl and no google_place_id in meta
  DUP_PHONE     same normalized phone on more than one post
  DUP_TITLE     same normalized title on more than one post
  NON_UK        gAddress present but not a UK address
  BAD_PHONE     junk phone (too short, 000000-style)
  NO_PHOTOS     no featured image and no gallery
  NO_HOURS      business_hours empty (informational)
  MERCHANT      title looks like a supplies merchant, not a tradesperson
  NO_ADDRESS    empty gAddress (informational)

Writes runs/audit_<ts>.md and runs/audit_flags_<ts>.csv. Makes zero writes
to WordPress.

Usage: python3 scripts/audit_listings.py [--limit N] [--sleep 0.15]
"""
import argparse
import csv
import json
import os
import re
import sys
import time
from collections import defaultdict
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
LEDGER_PATH = "data/place_ledger.jsonl"
MERCHANT_RE = re.compile(
    r"suppli|merchant|wholesal|plumb centre|plumbase|plumb center|"
    r"trade counter|building supplies|bathroom showroom",
    re.I,
)

session = requests.Session()
session.auth = AUTH


def norm_phone(p):
    digits = re.sub(r"\D", "", p or "")
    return digits[-10:] if len(digits) >= 10 else digits


def norm_title(t):
    return re.sub(r"[^a-z0-9]", "", (t or "").lower())


def ledger_post_ids():
    ids = set()
    if os.path.exists(LEDGER_PATH):
        with open(LEDGER_PATH, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    ids.add(int(json.loads(line).get("wp_post_id", 0)))
                except (ValueError, json.JSONDecodeError):
                    continue
    return ids


def fetch_all_listings():
    out, page = [], 1
    while True:
        r = session.get(
            f"{WP}/wp-json/wp/v2/listing",
            params={"per_page": 100, "page": page, "status": "publish",
                    "_fields": "id,title,link"},
            timeout=30,
        )
        if r.status_code != 200:
            break
        batch = r.json()
        if not batch:
            break
        out.extend(batch)
        page += 1
    return out


def fetch_meta(post_id):
    # LiteSpeed caches authenticated /wp-json/ GETs on this host; a unique
    # query param forces a cache miss so audits see current DB state.
    r = session.get(
        f"{WP}/wp-json/glasgow-traders/v1/listing-meta/{post_id}",
        params={"nocache": f"{time.time():.6f}"}, timeout=30
    )
    if r.status_code != 200:
        return None
    return r.json().get("meta", {})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="audit only first N (smoke test)")
    ap.add_argument("--sleep", type=float, default=0.15)
    args = ap.parse_args()

    if not AUTH.password:
        sys.exit("WP credentials missing — run: python3 scripts/check_env.py")

    listings = fetch_all_listings()
    if args.limit:
        listings = listings[: args.limit]
    print(f"Auditing {len(listings)} published listings on {WP}")

    known_ids = ledger_post_ids()
    rows, phones, titles = [], defaultdict(list), defaultdict(list)

    for i, post in enumerate(listings, 1):
        pid = post["id"]
        title = post.get("title", {}).get("rendered", "") or ""
        meta = fetch_meta(pid) or {}
        lp = (meta.get("lp_listingpro_options", {}) or {}).get("value", {}) or {}

        phone = lp.get("phone", "") or ""
        addr = lp.get("gAddress", "") or ""
        hours = lp.get("business_hours") or {}
        # Durable copy is standalone meta (bridge v2.2); lp options is legacy.
        place_id = ((meta.get("gt_google_place_id", {}) or {}).get("value", "")
                    or lp.get("google_place_id", "") or "")
        has_thumb = bool((meta.get("_thumbnail_id", {}) or {}).get("value"))
        has_gallery = bool((meta.get("gallery_image_ids", {}) or {}).get("value"))

        flags = []
        if not place_id and pid not in known_ids:
            flags.append("NO_PLACE_ID")
        if addr and not re.search(r"\b(UK|United Kingdom)\s*$", addr.strip(), re.I):
            flags.append("NON_UK")
        np = norm_phone(phone)
        if phone and (len(np) < 9 or re.search(r"(0{6}|123456)", np)):
            flags.append("BAD_PHONE")
        if not phone:
            flags.append("NO_PHONE")
        if not addr:
            flags.append("NO_ADDRESS")
        if not (has_thumb or has_gallery):
            flags.append("NO_PHOTOS")
        if not hours:
            flags.append("NO_HOURS")
        if MERCHANT_RE.search(title):
            flags.append("MERCHANT")

        if np:
            phones[np].append(pid)
        titles[norm_title(title)].append(pid)

        rows.append({
            "post_id": pid, "title": title, "url": post.get("link", ""),
            "phone": phone, "address": addr, "place_id": place_id,
            "flags": flags,
        })
        if i % 50 == 0:
            print(f"  ...{i}/{len(listings)}")
        time.sleep(args.sleep)

    for np, ids in phones.items():
        if len(ids) > 1:
            for r in rows:
                if r["post_id"] in ids:
                    r["flags"].append("DUP_PHONE")
    for nt, ids in titles.items():
        if nt and len(ids) > 1:
            for r in rows:
                if r["post_id"] in ids:
                    r["flags"].append("DUP_TITLE")

    ts = datetime.now().strftime("%Y%m%d_%H%M")
    os.makedirs("runs", exist_ok=True)
    csv_path = f"runs/audit_flags_{ts}.csv"
    md_path = f"runs/audit_{ts}.md"

    flagged = [r for r in rows if r["flags"]]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["post_id", "title", "url", "flags", "phone", "address", "place_id"])
        for r in flagged:
            w.writerow([r["post_id"], r["title"], r["url"],
                        "|".join(sorted(set(r["flags"]))), r["phone"],
                        r["address"], r["place_id"]])

    counts = defaultdict(int)
    for r in rows:
        for fl in set(r["flags"]):
            counts[fl] += 1

    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"# Listing audit — {ts}\n\n")
        f.write(f"Total published listings audited: **{len(rows)}**\n")
        f.write(f"Listings with at least one flag: **{len(flagged)}**\n\n")
        f.write("| Flag | Count |\n|---|---|\n")
        for fl in sorted(counts, key=counts.get, reverse=True):
            f.write(f"| {fl} | {counts[fl]} |\n")
        f.write(f"\nDetail: `{csv_path}`\n")
        dup_groups = [ids for ids in phones.values() if len(ids) > 1]
        if dup_groups:
            f.write(f"\n## Duplicate phone groups ({len(dup_groups)})\n\n")
            for ids in sorted(dup_groups, key=len, reverse=True)[:40]:
                names = [r["title"] for r in rows if r["post_id"] in ids]
                f.write(f"- posts {ids}: {names[0]}\n")

    print(f"\nDone. {len(flagged)}/{len(rows)} flagged.")
    print(f"Report:  {md_path}")
    print(f"Detail:  {csv_path}")
    for fl in sorted(counts, key=counts.get, reverse=True):
        print(f"  {fl:<12} {counts[fl]}")


if __name__ == "__main__":
    main()
