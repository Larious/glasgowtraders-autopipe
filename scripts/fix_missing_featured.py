#!/usr/bin/env python3
"""fix_missing_featured.py — repair listings that published without a featured
image (the WP media endpoint returns intermittent 500/400 under sustained
upload load; a failed upload used to leave the listing image-less).

Scans published listings for featured_media == 0, regenerates the branded tile
from the listing's own category/name/town, uploads it with retry, and sets it.
Idempotent: listings that already have an image are skipped.

Dry-run by default; --live is guard-gated.

Usage:
  python3 scripts/fix_missing_featured.py [--live] [--limit N]
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
            r = session.post(f"{WP}/wp-json/wp/v2/media",
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
            print(f"      upload failed after {tries} tries: {r.status_code}")
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
    args = ap.parse_args()
    if not AUTH.password:
        sys.exit("WP credentials missing — run: python3 scripts/check_env.py")

    mode = "LIVE" if args.live else "DRY-RUN"
    targets = find_missing(args.limit)
    print(f"[{mode}] {len(targets)} listings missing a featured image")
    os.makedirs(TILE_DIR, exist_ok=True)

    fixed = failed = 0
    for p in targets:
        pid = p["id"]
        name = seo_fields.focus_keyphrase(p.get("title", {}).get("rendered", ""))
        cat = (p.get("listing-category") or [0])[0]
        label = bs.CAT_LABELS.get(cat, "Tradesperson")
        print(f"  {pid} [{label}] {name[:44]}")
        if not args.live:
            continue
        town = town_for(pid)
        path = os.path.join(TILE_DIR, f"{pid}.jpg")
        tile_gen.make_business_tile(name, cat, town, path)
        mid = upload_tile(path, name)
        if mid and set_featured(pid, mid):
            fixed += 1
        else:
            failed += 1
            print(f"      FAILED for {pid}")
        time.sleep(args.sleep)

    if not args.live:
        print("\nNo writes made. Re-run with --live after approval "
              "(touch .claude/APPROVED).")
    else:
        print(f"\n[LIVE] fixed {fixed}, failed {failed}")


if __name__ == "__main__":
    main()
