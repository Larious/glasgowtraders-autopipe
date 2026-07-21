#!/usr/bin/env python3
"""apply_seo_all.py — site-wide Yoast SEO backfill for existing listings.

For every published listing:
  - focus keyphrase = business name, meta description built from stored WP data
    (business name + its real listing-category + town from gAddress). Written
    only when the listing has no meta description yet (idempotent / resumable).
  - featured image: only when the listing has NONE (featured_media == 0) — a
    branded per-business tile is generated and set. Listings that already have
    a photo are never touched.

No Google API calls by default (meta descriptions are rating-less to keep a
1,000+ run cheap and ToS-light). Retry/backoff on transient SSL resets; each
completed post id is checkpointed to runs/seo_backfill_done.txt so an
interrupted run resumes.

Dry-run by default; --live is guard-gated.

Usage:
  python3 scripts/apply_seo_all.py [--live] [--limit N]
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

WP = (os.getenv("WP_BASE_URL") or "https://www.glasgowtrader.co.uk").rstrip("/")
AUTH = HTTPBasicAuth(os.getenv("WP_USERNAME") or os.getenv("WP_USER", ""),
                     os.getenv("WP_APP_PASSWORD", ""))
DONE_FILE = "runs/seo_backfill_done.txt"
TILE_DIR = "assets/featured/generated"

TRANSIENT = (ConnectionResetError, ssl.SSLError,
             requests.exceptions.ConnectionError, requests.exceptions.Timeout,
             requests.exceptions.ChunkedEncodingError)

session = requests.Session()
session.auth = AUTH


def retry(fn, tries=5, base=1.0):
    for i in range(1, tries + 1):
        try:
            return fn()
        except TRANSIENT:
            if i == tries:
                raise
            time.sleep(base * 2 ** (i - 1))


def load_done():
    if os.path.exists(DONE_FILE):
        return {int(x) for x in open(DONE_FILE).read().split() if x.strip()}
    return set()


def mark_done(pid):
    os.makedirs("runs", exist_ok=True)
    with open(DONE_FILE, "a") as f:
        f.write(f"{pid}\n")


def category_names():
    r = retry(lambda: session.get(f"{WP}/wp-json/wp/v2/listing-category",
              params={"per_page": 100, "nocache": time.time()}, timeout=30))
    return {c["id"]: c["name"] for c in r.json()}


def iter_listings():
    page = 1
    while True:
        r = retry(lambda p=page: session.get(f"{WP}/wp-json/wp/v2/listing",
                  params={"per_page": 100, "page": p, "status": "publish",
                          "_fields": "id,title,featured_media,listing-category",
                          "nocache": time.time()}, timeout=30))
        if r.status_code != 200:
            break
        batch = r.json()
        if not batch:
            break
        for post in batch:
            yield post
        page += 1
        time.sleep(0.2)


def fetch_meta(pid):
    r = retry(lambda: session.get(
        f"{WP}/wp-json/glasgow-traders/v1/listing-meta/{pid}",
        params={"nocache": time.time()}, timeout=30))
    return r.json().get("meta", {}) if r.status_code == 200 else {}


def write_yoast(pid, kw, md):
    r = retry(lambda: session.post(
        f"{WP}/wp-json/glasgow-traders/v1/listing-options/{pid}",
        json={"yoast_focuskw": kw, "yoast_metadesc": md}, timeout=30))
    if r.status_code != 200:
        return False
    v = r.json().get("verified", {})
    return v.get("yoast_focuskw") == kw and v.get("yoast_metadesc") == md


def upload_and_set_featured(pid, name, cat_id, town):
    os.makedirs(TILE_DIR, exist_ok=True)
    slug = (re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")[:48] or "listing")
    path = os.path.join(TILE_DIR, f"{pid}.jpg")
    tile_gen.make_business_tile(name, cat_id, town, path)
    with open(path, "rb") as f:
        data = f.read()
    fname = (slug + ".jpg").encode("ascii", "ignore").decode("ascii")
    r = retry(lambda: session.post(f"{WP}/wp-json/wp/v2/media",
              headers={"Content-Disposition": f'attachment; filename="{fname}"',
                       "Content-Type": "image/jpeg"}, data=data, timeout=60))
    if r.status_code != 201:
        return False
    mid = r.json()["id"]
    retry(lambda: session.post(f"{WP}/wp-json/wp/v2/media/{mid}",
          json={"alt_text": f"{name} — Glasgow Trader"}, timeout=30))
    r = retry(lambda: session.post(f"{WP}/wp-json/wp/v2/listing/{pid}",
              json={"featured_media": mid}, timeout=30))
    return r.status_code in (200, 201) and r.json().get("featured_media") == mid


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--sleep", type=float, default=0.5)
    args = ap.parse_args()
    if not AUTH.password:
        sys.exit("WP credentials missing — run: python3 scripts/check_env.py")

    cat_names = category_names()
    done = load_done()
    mode = "LIVE" if args.live else "DRY-RUN"
    print(f"[{mode}] scanning published listings | {len(done)} already done")

    stats = {"scanned": 0, "yoast_written": 0, "yoast_skip_existing": 0,
             "featured_added": 0, "featured_present": 0, "failed": 0}
    processed = 0
    for post in iter_listings():
        pid = post["id"]
        stats["scanned"] += 1
        if pid in done:
            continue
        title = seo_fields.focus_keyphrase(post.get("title", {}).get("rendered", ""))
        if not title:
            continue
        cat_id = (post.get("listing-category") or [0])[0]
        label = cat_names.get(cat_id, "Tradesperson")

        meta = fetch_meta(pid)
        time.sleep(0.2)
        existing_md = (meta.get("_yoast_wpseo_metadesc", {}) or {}).get("value", "")
        lp = (meta.get("lp_listingpro_options", {}) or {}).get("value", {}) or {}
        town = seo_fields.town_from_address(lp.get("gAddress", ""))
        need_featured = not post.get("featured_media")
        md = seo_fields.meta_description(title, label, town)

        if existing_md:
            stats["yoast_skip_existing"] += 1
            if not need_featured:
                if args.live:
                    mark_done(pid)
                continue

        if not args.live:
            if not existing_md:
                stats["yoast_written"] += 1
            if need_featured:
                stats["featured_added"] += 1
            else:
                stats["featured_present"] += 1
            if stats["scanned"] <= 8:
                print(f"  {pid} [{label}] featured={'ADD' if need_featured else 'keep'} "
                      f"kw={title!r}\n      meta: {md}")
        else:
            ok = True
            if not existing_md:
                if write_yoast(pid, title, md):
                    stats["yoast_written"] += 1
                else:
                    ok = False
            if need_featured:
                if upload_and_set_featured(pid, title, cat_id, town):
                    stats["featured_added"] += 1
                else:
                    ok = False
            else:
                stats["featured_present"] += 1
            if ok:
                mark_done(pid)
            else:
                stats["failed"] += 1
                print(f"  FAILED post {pid}")
            time.sleep(args.sleep)

        processed += 1
        if args.live and processed % 50 == 0:
            print(f"  ...{processed} processed "
                  f"(yoast {stats['yoast_written']}, tiles {stats['featured_added']})")
        if args.limit and processed >= args.limit:
            break

    print(f"\n[{mode}] summary")
    for k, v in stats.items():
        print(f"  {k:<20} {v}")
    if not args.live:
        print("\nNo writes made. Re-run with --live after approval "
              "(touch .claude/APPROVED). Resumable via runs/seo_backfill_done.txt.")


if __name__ == "__main__":
    main()
