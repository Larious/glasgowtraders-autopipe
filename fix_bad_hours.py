#!/usr/bin/env python3
"""
Glasgow Traders AutoPipe — Bulk Hours Fix
==========================================
Scans all listings on the WordPress site, detects ones with
the bad 12:00 AM - 11:59 PM default hours, re-fetches correct
hours from Google Places API, and re-injects them via the
ListingPro bridge plugin.

Usage:
    python3 fix_bad_hours.py          # dry run (report only)
    python3 fix_bad_hours.py --fix    # actually fix the bad ones
"""

import sys
import os
import time
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

load_dotenv()

# ── Config ──────────────────────────────────────────────────
WP_BASE_URL = os.getenv("WP_BASE_URL", "https://www.glasgowtrader.co.uk")
WP_USER = os.getenv("WP_USERNAME") or os.getenv("WP_USER", "")
WP_APP_PASSWORD = os.getenv("WP_APP_PASSWORD", "")
GOOGLE_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "")
AUTH = HTTPBasicAuth(WP_USER, WP_APP_PASSWORD)

if not WP_APP_PASSWORD:
    sys.exit("WP credentials missing — run: python3 scripts/check_env.py")

DRY_RUN = "--fix" not in sys.argv


# ── Step 1: Fetch all listings from WordPress ──────────────
def fetch_all_listings():
    """Page through WP REST API to get all listing posts."""
    listings = []
    page = 1
    while True:
        resp = requests.get(
            f"{WP_BASE_URL}/wp-json/wp/v2/listing",
            params={"per_page": 50, "page": page, "status": "publish"},
            auth=AUTH, timeout=30,
        )
        if resp.status_code == 400:
            break  # no more pages
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        listings.extend(batch)
        page += 1
        time.sleep(0.3)
    return listings


# ── Step 2: Check each listing's current hours ─────────────
def get_listing_hours(post_id):
    """Fetch lp_listingpro_options and return business_hours dict."""
    resp = requests.get(
        f"{WP_BASE_URL}/wp-json/glasgow-traders/v1/listing-meta/{post_id}",
        auth=AUTH, timeout=15,
    )
    if resp.status_code != 200:
        return None
    meta = resp.json().get("meta", {})
    lp_opts = meta.get("lp_listingpro_options", {}).get("value", {})
    if not isinstance(lp_opts, dict):
        return None
    return lp_opts.get("business_hours", {})


def is_bad_hours(hours):
    """Detect the 12:00 AM - 11:59 PM default pattern."""
    if not hours or not isinstance(hours, dict):
        return False
    for day, times in hours.items():
        if isinstance(times, dict):
            o = times.get("open", "").lower().replace(" ", "")
            c = times.get("close", "").lower().replace(" ", "")
            # Match variations: 12:00am/00:00am and 11:59pm/23:59pm
            if o in ("12:00am", "00:00am") and c in ("11:59pm", "23:59pm"):
                return True
    return False


# ── Step 3: Find place_id for a listing ─────────────────────
def find_place_id(listing_name, lp_opts=None):
    """
    Try to get the place_id. First check if it's stored in
    lp_listingpro_options, otherwise search Google.
    """
    # Check if stored in meta
    if lp_opts and isinstance(lp_opts, dict):
        pid = lp_opts.get("place_id", "")
        if pid:
            return pid

    # Search Google Places
    resp = requests.get(
        "https://maps.googleapis.com/maps/api/place/findplacefromtext/json",
        params={
            "input": listing_name,
            "inputtype": "textquery",
            "fields": "place_id,name",
            "key": GOOGLE_API_KEY,
        },
        timeout=15,
    )
    data = resp.json()
    candidates = data.get("candidates", [])
    if candidates:
        return candidates[0]["place_id"]
    return None


# ── Step 4: Fetch correct hours from Google ─────────────────
def fetch_google_hours(place_id):
    """Get opening_hours from Google Place Details."""
    resp = requests.get(
        "https://maps.googleapis.com/maps/api/place/details/json",
        params={
            "place_id": place_id,
            "fields": "name,opening_hours",
            "key": GOOGLE_API_KEY,
        },
        timeout=15,
    )
    result = resp.json().get("result", {})
    return result.get("opening_hours", {})


def convert_to_lp_hours(opening_hours):
    """Convert Google periods to ListingPro format."""
    periods = opening_hours.get("periods", [])

    # 24/7 business: single period with no close key
    if len(periods) == 1 and "close" not in periods[0]:
        return {}

    lp_hours = {}
    day_names = ["Sunday", "Monday", "Tuesday", "Wednesday",
                 "Thursday", "Friday", "Saturday"]

    for period in periods:
        day_idx = period.get("open", {}).get("day", 0)
        day_name = day_names[day_idx]
        open_time = period.get("open", {}).get("time", "0900")
        close_time = period.get("close", {}).get("time", "1700")

        oh = int(open_time[:2])
        om = open_time[2:]
        ch = int(close_time[:2])
        cm = close_time[2:]

        open_fmt = f"{oh if oh <= 12 else oh-12:02d}:{om}{'am' if oh < 12 else 'pm'}"
        close_fmt = f"{ch if ch <= 12 else ch-12:02d}:{cm}{'am' if ch < 12 else 'pm'}"
        lp_hours[day_name] = {"open": open_fmt, "close": close_fmt}

    return lp_hours


# ── Step 5: Inject fixed hours ──────────────────────────────
def inject_fixed_hours(post_id, lp_hours):
    """Write corrected business_hours into lp_listingpro_options."""
    resp = requests.get(
        f"{WP_BASE_URL}/wp-json/glasgow-traders/v1/listing-meta/{post_id}",
        auth=AUTH, timeout=15,
    )
    if resp.status_code != 200:
        return False

    meta = resp.json().get("meta", {})
    lp_opts = meta.get("lp_listingpro_options", {}).get("value", {})
    if not isinstance(lp_opts, dict):
        lp_opts = {}

    # Update only business_hours, keep everything else
    lp_opts["business_hours"] = lp_hours

    resp = requests.post(
        f"{WP_BASE_URL}/wp-json/glasgow-traders/v1/listing-options/{post_id}",
        json=lp_opts,
        auth=AUTH, timeout=15,
    )
    return resp.status_code == 200


# ── Main ────────────────────────────────────────────────────
def main():
    mode = "DRY RUN" if DRY_RUN else "FIX MODE"
    print("=" * 60)
    print(f"  Glasgow Traders — Bulk Hours Fix ({mode})")
    print("=" * 60)

    if not GOOGLE_API_KEY:
        sys.exit("ERROR: GOOGLE_PLACES_API_KEY not set in .env")

    # Fetch all listings
    print("\n[1/4] Fetching all listings from WordPress...")
    listings = fetch_all_listings()
    print(f"  Found {len(listings)} published listings")

    # Check each listing
    print("\n[2/4] Scanning for bad hours...")
    bad_listings = []
    for listing in listings:
        post_id = listing["id"]
        title = listing["title"]["rendered"]
        hours = get_listing_hours(post_id)

        if is_bad_hours(hours):
            bad_listings.append({"id": post_id, "title": title})
            print(f"  BAD  #{post_id} — {title}")
        else:
            print(f"  OK   #{post_id} — {title}")
        time.sleep(0.2)

    print(f"\n  {len(bad_listings)} listings with bad hours out of {len(listings)} total")

    if not bad_listings:
        print("\n  No bad hours found. All good!")
        return

    if DRY_RUN:
        print("\n  Run with --fix to repair these listings:")
        print(f"    python3 fix_bad_hours.py --fix")
        return

    # Fix bad listings
    print(f"\n[3/4] Fixing {len(bad_listings)} listings...")
    fixed = 0
    failed = 0

    for item in bad_listings:
        post_id = item["id"]
        title = item["title"]
        print(f"\n  Fixing #{post_id} — {title}")

        # Find place_id
        place_id = find_place_id(title)
        if not place_id:
            print(f"    SKIP: Could not find on Google")
            failed += 1
            continue

        # Get correct hours
        google_hours = fetch_google_hours(place_id)
        if not google_hours.get("periods"):
            print(f"    SKIP: No hours on Google")
            failed += 1
            continue

        lp_hours = convert_to_lp_hours(google_hours)
        if not lp_hours:
            print(f"    SKIP: 24/7 business, clearing bad hours")
            # Still inject empty hours to remove the bad default
            inject_fixed_hours(post_id, {})
            fixed += 1
            continue

        # Inject
        ok = inject_fixed_hours(post_id, lp_hours)
        if ok:
            days_str = ", ".join(lp_hours.keys())
            print(f"    FIXED: {days_str}")
            fixed += 1
        else:
            print(f"    FAILED: Could not write to WordPress")
            failed += 1

        time.sleep(0.5)  # rate limit Google API

    # Summary
    print("\n" + "=" * 60)
    print(f"  DONE: {fixed} fixed, {failed} failed")
    print("=" * 60)


if __name__ == "__main__":
    main()
