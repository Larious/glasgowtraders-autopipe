#!/usr/bin/env python3
"""fix_missing_featured.py — repair listings that published without a featured
image (the WP media endpoint returns intermittent 500/400 under sustained
upload load; a failed upload used to leave the listing image-less).

Scans published listings for featured_media == 0, regenerates the branded tile
from the listing's own category/name/town, uploads it with retry, and sets it.
Idempotent: listings that already have an image are skipped.

Dry-run by default; --live is guard-gated.

The 2026-07-22 incident: the host exhausted its image-processing resources
mid-sweep (WP derivative counts decayed 28 -> 13 -> 2 -> hard 500) and stayed
that way for hours, so a first repair attempt burned 29 listings x 4 retries
against a server that could not have succeeded. Hence --ids-file (skip the
12-minute rescan), body capture on failure (never fly blind again), and
--max-consecutive-failures (stop early instead of grinding).

Usage:
  python3 scripts/fix_missing_featured.py [--live] [--limit N]
  python3 scripts/fix_missing_featured.py --ids-file data/missing_featured_ids.txt --live
"""
import argparse
import importlib.util
import os
import re
import ssl
import sys
import time

import requests
from requests.auth import HTTPBasicAuth

try:
    from dotenv import load_dotenv
    load_dotenv(override=True)
except ImportError:
    pass

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "scripts"))
import seo_fields
import tile_gen
_bs = importlib.util.spec_from_file_location(
    "batch_snipe", os.path.join(_ROOT, "batch_snipe_location.py"))
bs = importlib.util.module_from_spec(_bs); _bs.loader.exec_module(bs)

WP = (os.getenv("WP_BASE_URL") or "https://www.glasgowtrader.co.uk").rstrip("/")
AUTH = HTTPBasicAuth(os.getenv("WP_USERNAME") or os.getenv("WP_USER", ""),
                     os.getenv("WP_APP_PASSWORD", ""))
TILE_DIR = "assets/featured/generated"
TRANSIENT = (ConnectionResetError, ssl.SSLError,
             requests.exceptions.ConnectionError, requests.exceptions.Timeout)

session = requests.Session()
session.auth = AUTH


def find_missing(limit=0):
    """Published listings with no featured image."""
    out, page = [], 1
    while True:
        r = session.get(f"{WP}/wp-json/wp/v2/listing",
                        params={"per_page": 100, "page": page, "status": "publish",
                                "_fields": "id,title,featured_media,listing-category",
                                "nocache": f"{time.time():.6f}"}, timeout=30)
        if r.status_code != 200:
            break
        batch = r.json()
        if not batch:
            break
        for p in batch:
            if not p.get("featured_media"):
                out.append(p)
                if limit and len(out) >= limit:
                    return out
        page += 1
        time.sleep(0.15)
    return out


def load_ids(path):
    """Read known-bad post ids, then re-check each against the site — the file
    is a snapshot and some may have been fixed since it was written."""
    with open(path) as f:
        ids = [int(x) for x in f.read().split() if x.strip().isdigit()]
    out = []
    for pid in ids:
        r = session.get(f"{WP}/wp-json/wp/v2/listing/{pid}",
                        params={"_fields": "id,title,featured_media,status,listing-category",
                                "nocache": f"{time.time():.6f}"}, timeout=30)
        if r.status_code != 200:
            print(f"  skip {pid}: HTTP {r.status_code}")
            continue
        d = r.json()
        if d.get("featured_media"):
            print(f"  skip {pid}: already has image {d['featured_media']}")
            continue
        if d.get("status") != "publish":
            print(f"  skip {pid}: status={d.get('status')}")
            continue
        out.append(d)
        time.sleep(0.1)
    return out


def preflight():
    """One tiny upload to prove the media endpoint can accept writes at all.
    After the 07-22 incident, never start a 46-item run on faith."""
    png = bytes.fromhex(
        "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c4"
        "890000000d4944415478da63f8ffff3f0005fe02fea6c2c8e00000000049454e44ae426082")
    r = session.post(f"{WP}/wp-json/wp/v2/media",
                     headers={"Content-Disposition": 'attachment; filename="gt-preflight.png"',
                              "Content-Type": "image/png"}, data=png, timeout=60)
    if r.status_code != 201:
        print(f"PREFLIGHT FAILED: HTTP {r.status_code}\n  {r.text[:400]}")
        return False
    mid = r.json()["id"]
    print(f"preflight OK (media {mid}); leaving it in place for the human to remove")
    return True


def town_for(pid, fallback="Glasgow"):
    try:
        r = session.get(f"{WP}/wp-json/glasgow-traders/v1/listing-meta/{pid}",
                        params={"nocache": f"{time.time():.6f}"}, timeout=30)
        lp = (r.json().get("meta", {}).get("lp_listingpro_options", {})
              or {}).get("value", {}) or {}
        return seo_fields.town_from_address(lp.get("gAddress", ""), fallback)
    except Exception:
        return fallback


def upload_tile(path, name, tries=4):
    slug = (re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")[:48] or "listing")
    fname = (slug + ".jpg").encode("ascii", "ignore").decode("ascii")
    with open(path, "rb") as f:
        data = f.read()
    for attempt in range(1, tries + 1):
        try:
            # See batch_snipe_location.build_and_upload_tile — ?gt_tile=1 opts
            # into the trimmed derivative set for generated tiles.
            r = session.post(f"{WP}/wp-json/wp/v2/media?gt_tile=1",
                             headers={"Content-Disposition": f'attachment; filename="{fname}"',
                                      "Content-Type": "image/jpeg"},
                             data=data, timeout=60)
        except TRANSIENT:
            if attempt == tries:
                return None
            time.sleep(2 ** (attempt - 1)); continue
        if r.status_code == 201:
            mid = r.json()["id"]
            try:
                session.post(f"{WP}/wp-json/wp/v2/media/{mid}",
                             json={"alt_text": f"{name} — Glasgow Trader"}, timeout=30)
            except TRANSIENT:
                pass
            return mid
        if attempt == tries:
            # Capture the body: a WP 500 names the underlying PHP/WP error, and
            # not logging it cost a full misdiagnosis on 2026-07-22.
            print(f"      upload failed after {tries} tries: {r.status_code}\n"
                  f"      body: {r.text[:300]}")
            return None
        time.sleep(2 ** (attempt - 1))
    return None


def set_featured(pid, mid):
    r = session.post(f"{WP}/wp-json/wp/v2/listing/{pid}",
                     json={"featured_media": mid}, timeout=30)
    return r.status_code in (200, 201) and r.json().get("featured_media") == mid


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--sleep", type=float, default=0.6)
    ap.add_argument("--ids-file", help="known-bad post ids, skips the full rescan")
    ap.add_argument("--max-consecutive-failures", type=int, default=3,
                    help="abort if this many uploads fail in a row (host is down)")
    args = ap.parse_args()
    if not AUTH.password:
        sys.exit("WP credentials missing — run: python3 scripts/check_env.py")

    mode = "LIVE" if args.live else "DRY-RUN"
    targets = load_ids(args.ids_file) if args.ids_file else find_missing(args.limit)
    if args.limit:
        targets = targets[:args.limit]
    print(f"[{mode}] {len(targets)} listings missing a featured image", flush=True)
    os.makedirs(TILE_DIR, exist_ok=True)

    if args.live and targets and not preflight():
        sys.exit("Media endpoint is not accepting uploads — aborting before the "
                 "batch. Check host disk/memory, then re-run.")

    fixed = failed = streak = 0
    for p in targets:
        pid = p["id"]
        name = seo_fields.focus_keyphrase(p.get("title", {}).get("rendered", ""))
        cat = (p.get("listing-category") or [0])[0]
        label = bs.CAT_LABELS.get(cat, "Tradesperson")
        print(f"  {pid} [{label}] {name[:44]}", flush=True)
        if not args.live:
            continue
        town = town_for(pid)
        path = os.path.join(TILE_DIR, f"{pid}.jpg")
        tile_gen.make_business_tile(name, cat, town, path)
        mid = upload_tile(path, name)
        if mid and set_featured(pid, mid):
            fixed += 1
            streak = 0
            print(f"      ok -> media {mid}", flush=True)
        else:
            failed += 1
            streak += 1
            print(f"      FAILED for {pid}", flush=True)
            if streak >= args.max_consecutive_failures:
                print(f"\nABORTED: {streak} consecutive failures — the host is "
                      f"refusing uploads. {fixed} fixed, {len(targets) - fixed - failed} "
                      f"untouched. Re-run when it recovers.", flush=True)
                break
        time.sleep(args.sleep)

    if not args.live:
        print("\nNo writes made. Re-run with --live after approval "
              "(touch .claude/APPROVED).")
    else:
        print(f"\n[LIVE] fixed {fixed}, failed {failed}")


if __name__ == "__main__":
    main()
