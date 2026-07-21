#!/usr/bin/env python3
"""apply_seo_batch.py — backfill a unique featured image + Yoast SEO onto
listings published before the SEO wiring existed (Glasgow gardener batch 1).

Per ledger post since --since:
  1. generate a unique branded tile (business name hero) and upload it,
  2. set it as the featured image,
  3. write Yoast focus keyphrase (business name) + meta description.

Rating/town for the meta description are re-fetched from Google Place Details
(one Basic-data call per post) since they weren't stored at publish time.

Dry-run by default; --live is guard-gated. Generated tiles are written to
assets/featured/generated/ for inspection.

Usage:
  python3 scripts/apply_seo_batch.py --since 2026-07-21T13:0 --trade gardener [--live]
"""
import argparse
import importlib.util
import json
import os
import re
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
GKEY = os.getenv("GOOGLE_PLACES_API_KEY") or os.getenv("GOOGLE_API_KEY", "")
LEDGER = "data/place_ledger.jsonl"
TILE_DIR = "assets/featured/generated"
CAT_LABELS = bs.CAT_LABELS

session = requests.Session()
session.auth = AUTH


def slugify(name):
    s = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return (s[:48] or "listing")


def upload_tile(path, name):
    fname = (slugify(name) + ".jpg").encode("ascii", "ignore").decode("ascii")
    with open(path, "rb") as f:
        data = f.read()
    r = session.post(f"{WP}/wp-json/wp/v2/media",
                     headers={"Content-Disposition": f'attachment; filename="{fname}"',
                              "Content-Type": "image/jpeg"},
                     data=data, timeout=60)
    if r.status_code == 201:
        j = r.json()
        # set alt text for accessibility + minor SEO
        session.post(f"{WP}/wp-json/wp/v2/media/{j['id']}",
                     json={"alt_text": f"{name} — Glasgow Trader"}, timeout=30)
        return j["id"]
    print(f"        media upload failed: {r.status_code} {r.text[:120]}")
    return None


def google_rating_town(place_id):
    if not GKEY:
        return 0, 0, ""
    d = requests.get("https://maps.googleapis.com/maps/api/place/details/json",
                     params={"place_id": place_id,
                             "fields": "rating,user_ratings_total,formatted_address",
                             "key": GKEY}, timeout=15).json().get("result", {})
    return (d.get("rating", 0), d.get("user_ratings_total", 0),
            seo_fields.town_from_address(d.get("formatted_address", "")))


def set_featured(post_id, media_id):
    r = session.post(f"{WP}/wp-json/wp/v2/listing/{post_id}",
                     json={"featured_media": media_id}, timeout=30)
    return r.status_code in (200, 201) and r.json().get("featured_media") == media_id


def write_yoast(post_id, focuskw, metadesc):
    r = session.post(f"{WP}/wp-json/glasgow-traders/v1/listing-options/{post_id}",
                     json={"yoast_focuskw": focuskw, "yoast_metadesc": metadesc},
                     timeout=30)
    if r.status_code != 200:
        return False
    v = r.json().get("verified", {})
    return v.get("yoast_focuskw") == focuskw and v.get("yoast_metadesc") == metadesc


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", required=True, help="ledger ts prefix")
    ap.add_argument("--trade", default="gardener", choices=sorted(bs.TRADES))
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--sleep", type=float, default=0.5)
    args = ap.parse_args()

    if not AUTH.password:
        sys.exit("WP credentials missing — run: python3 scripts/check_env.py")

    trade_cfg = bs.TRADES[args.trade]
    targets = [json.loads(l) for l in open(LEDGER, encoding="utf-8")
               if json.loads(l)["ts"].startswith(args.since)]
    mode = "LIVE" if args.live else "DRY-RUN"
    print(f"[{mode}] {len(targets)} posts since {args.since}\n")
    os.makedirs(TILE_DIR, exist_ok=True)

    ok = fail = 0
    for rec in targets:
        pid = rec["wp_post_id"]
        cat = bs.categorize(rec["name"], trade_cfg)
        label = CAT_LABELS.get(cat, trade_cfg["label"])
        rating, reviews, town = google_rating_town(rec["place_id"])
        time.sleep(0.25)
        town = town or "Glasgow"
        kw = seo_fields.focus_keyphrase(rec["name"])
        md = seo_fields.meta_description(rec["name"], label, town,
                                         rating=rating, reviews=reviews)
        tile = tile_gen.make_business_tile(
            rec["name"], cat, town, os.path.join(TILE_DIR, f"{pid}.jpg"))
        print(f"  {pid} [{label}] tile={os.path.basename(tile)} kw={kw!r}")
        print(f"        meta ({len(md)}c): {md}")

        if not args.live:
            continue

        media_id = upload_tile(tile, rec["name"])
        if media_id and set_featured(pid, media_id):
            pass
        else:
            fail += 1
            print("        FEATURED FAILED")
        if write_yoast(pid, kw, md):
            ok += 1
        else:
            fail += 1
            print("        YOAST FAILED")
        time.sleep(args.sleep)

    if not args.live:
        print(f"\nTiles written to {TILE_DIR}/ for inspection. No writes made. "
              "Re-run with --live after approval (touch .claude/APPROVED).")
    else:
        print(f"\n[LIVE] {ok} posts SEO-enriched, {fail} failures")


if __name__ == "__main__":
    main()
