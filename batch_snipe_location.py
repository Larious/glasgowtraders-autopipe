#!/usr/bin/env python3
"""
Glasgow Traders AutoPipe — Location Batch Sniper
=================================================
Finds all plumbers in a given location via Google Places,
enriches each with full details (4 photos, hours, address,
phone, website), and publishes to WordPress with the correct
location category.

Usage:
    python3 batch_snipe_location.py --location "Airdrie"
    python3 batch_snipe_location.py --location "Alexandria"
    python3 batch_snipe_location.py --location "Airdrie" --dry-run
"""

import argparse
import sys
import os
import time
import json
import requests
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

load_dotenv()

# ── Config ──────────────────────────────────────────────────
WP_BASE_URL = os.getenv("WP_BASE_URL", "https://www.glasgowtrader.co.uk")
WP_USER = os.getenv("WP_USERNAME") or os.getenv("WP_USER", "")
WP_APP_PASSWORD = os.getenv("WP_APP_PASSWORD", "")
GOOGLE_API_KEY = (os.getenv("GOOGLE_PLACES_API_KEY")
                  or os.getenv("GOOGLE_API_KEY", ""))
PLUMBER_CAT_ID = int(os.getenv("PLUMBER_CAT_ID", "22"))

if not WP_APP_PASSWORD:
    sys.exit("WP credentials missing — run: python3 scripts/check_env.py")

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "scripts"))
import ledger_utils

AUTH = HTTPBasicAuth(WP_USER, WP_APP_PASSWORD)

# Resilient session with retries
session = requests.Session()
retry = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retry))
session.auth = AUTH


# ── Helper: Find or create WP location taxonomy ────────────
def get_or_create_location(location_name):
    """Find the location term ID in WordPress, or create it."""
    print(f"\n[LOCATION] Looking for '{location_name}'...")

    # Search existing locations
    resp = session.get(
        f"{WP_BASE_URL}/wp-json/wp/v2/location",
        params={"search": location_name, "per_page": 50},
        timeout=15,
    )
    if resp.status_code == 200:
        for loc in resp.json():
            if loc["name"].lower().strip() == location_name.lower().strip():
                print(f"  Found: ID={loc['id']} name={loc['name']}")
                return loc["id"]

    # Create new location if not found
    print(f"  Not found. Creating '{location_name}'...")
    resp = session.post(
        f"{WP_BASE_URL}/wp-json/wp/v2/location",
        json={"name": location_name},
        timeout=15,
    )
    if resp.status_code == 201:
        loc_id = resp.json()["id"]
        print(f"  Created: ID={loc_id}")
        return loc_id

    print(f"  ERROR creating location: {resp.status_code} {resp.text[:200]}")
    return None


# ── Helper: Get existing listings to avoid duplicates ───────
def get_existing_listings():
    """Fetch all published listing titles to skip duplicates."""
    existing = set()
    page = 1
    while True:
        resp = session.get(
            f"{WP_BASE_URL}/wp-json/wp/v2/listing",
            params={"per_page": 100, "page": page, "status": "publish"},
            timeout=30,
        )
        if resp.status_code == 400 or not resp.json():
            break
        for post in resp.json():
            existing.add(post["title"]["rendered"].lower().strip())
        page += 1
        time.sleep(0.5)
    return existing


# ── Step 1: Search Google Places for plumbers ───────────────
def search_plumbers(location_name):
    """
    Use Google Places Text Search to find plumbers in the location.
    Returns a list of place_ids (up to 60 via pagination).
    """
    print(f"\n[SEARCH] Finding plumbers in {location_name}...")
    all_results = []
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {
        "query": f"plumbers in {location_name}",
        "key": GOOGLE_API_KEY,
    }

    while True:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        for r in data.get("results", []):
            all_results.append({
                "place_id": r["place_id"],
                "name": r.get("name", "Unknown"),
            })

        # Check for next page
        next_token = data.get("next_page_token")
        if not next_token:
            break
        # Google requires a short delay before using next_page_token
        time.sleep(2)
        params = {"pagetoken": next_token, "key": GOOGLE_API_KEY}

    print(f"  Found {len(all_results)} plumbers")
    return all_results


# ── Step 2: Enrich a single place ───────────────────────────
def enrich_place(place_id):
    """Fetch full details from Google Place Details API."""
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields": ",".join([
            "name",
            "formatted_address",
            "formatted_phone_number",
            "international_phone_number",
            "website",
            "geometry",
            "photo",
            "rating",
            "user_ratings_total",
            "opening_hours",
            "business_status",
            "editorial_summary",
        ]),
        "key": GOOGLE_API_KEY,
    }

    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    r = resp.json().get("result", {})

    if not r:
        return None

    # Collect up to 4 photo references
    photo_refs = []
    for photo in r.get("photos", [])[:4]:
        photo_refs.append(photo["photo_reference"])

    # Convert opening hours to ListingPro format
    lp_hours = {}
    periods = r.get("opening_hours", {}).get("periods", [])

    # 24/7 business: single period with no close key
    if len(periods) == 1 and "close" not in periods[0]:
        pass  # skip lp_hours
    elif periods:
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

    hours_text = ""
    if r.get("opening_hours", {}).get("weekday_text"):
        hours_text = "\n".join(r["opening_hours"]["weekday_text"])

    enriched = {
        "name":              r.get("name", "Unknown"),
        "formatted_address": r.get("formatted_address", ""),
        "phone":             r.get("formatted_phone_number", ""),
        "phone_intl":        r.get("international_phone_number", ""),
        "website":           r.get("website", ""),
        "lat":               r.get("geometry", {}).get("location", {}).get("lat"),
        "lng":               r.get("geometry", {}).get("location", {}).get("lng"),
        "rating":            r.get("rating"),
        "review_count":      r.get("user_ratings_total"),
        "hours_text":        hours_text,
        "lp_hours":          lp_hours,
        "about":             r.get("editorial_summary", {}).get("overview", ""),
        "photo_refs":        photo_refs,
        "place_id":          place_id,
    }

    return enriched


# ── Step 3: images — FROZEN ─────────────────────────────────
def upload_all_images(enriched):
    """Google ToS: downloading Places photos is a prohibited use for this
    directory (CLAUDE.md rule 5). Listings go up without Google photos;
    images come later via the ListingPro claim flow."""
    return None, []


# ── Step 4: Build listing description ───────────────────────
def build_description(enriched, location_name):
    """Generate a professional business description."""
    name = enriched["name"]
    address = enriched["formatted_address"]
    rating = enriched.get("rating")
    reviews = enriched.get("review_count")
    website = enriched.get("website", "")
    about = enriched.get("about", "")

    # Start with Google's editorial summary if available
    if about:
        desc = f"{about}\n\n"
    else:
        desc = f"{name} is a professional plumbing service serving {location_name} and the surrounding Glasgow area. "

    # Add rating info
    if rating and reviews:
        desc += f"They have an excellent {rating}-star rating from {reviews} customer reviews on Google. "

    # Add website mention
    if website:
        desc += f"Visit their website for more information about their plumbing services."

    # Add opening hours section
    if enriched.get("hours_text"):
        desc += f"\n\n<strong>Opening Hours</strong>\n{enriched['hours_text']}"

    return desc


# ── Step 5: Create WordPress listing ────────────────────────
def create_listing(enriched, location_id, featured_media_id, location_name):
    """Create the listing post in WordPress."""
    description = build_description(enriched, location_name)

    payload = {
        "title": enriched["name"],
        "content": description,
        "status": "publish",
        "listing_category": [PLUMBER_CAT_ID],
        "location": [location_id],
    }

    if featured_media_id:
        payload["featured_media"] = featured_media_id

    resp = session.post(
        f"{WP_BASE_URL}/wp-json/wp/v2/listing",
        json=payload,
        timeout=30,
    )

    if resp.status_code == 201:
        data = resp.json()
        return data["id"], data.get("link", "")
    print(f"    FAILED to create: {resp.status_code} {resp.text[:200]}")
    return None, None


# ── Step 6: Inject ListingPro options ───────────────────────
def inject_listingpro_options(post_id, enriched, gallery_ids, location_name):
    """Write all data into lp_listingpro_options serialized array."""
    options_data = {
        "phone":            enriched.get("phone", ""),
        "website":          enriched.get("website", ""),
        "gAddress":         enriched.get("formatted_address", ""),
        "latitude":         str(enriched.get("lat", "")),
        "longitude":        str(enriched.get("lng", "")),
        "tagline_text":     f"Plumber in {location_name}",
        "email":            "",
        "claimed_section":  "not_claimed",
        "price_status":     "notsay",
        # Dedup key (rule 3); bridge v2.2 mirrors this to standalone meta.
        "google_place_id":  enriched.get("place_id", ""),
    }

    # Add business hours
    if enriched.get("lp_hours"):
        options_data["business_hours"] = enriched["lp_hours"]

    # Add gallery images
    if gallery_ids:
        options_data["gallery"] = ",".join(map(str, gallery_ids))

    resp = session.post(
        f"{WP_BASE_URL}/wp-json/glasgow-traders/v1/listing-options/{post_id}",
        json=options_data,
        timeout=15,
    )

    return resp.status_code == 200


# ── Main ────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Batch snipe plumbers by location")
    parser.add_argument("--location", required=True, help="Location name (e.g. Airdrie)")
    parser.add_argument("--dry-run", action="store_true", help="Search only, don't publish")
    args = parser.parse_args()

    location_name = args.location

    print("=" * 60)
    print(f"  Glasgow Traders — Batch Sniper")
    print(f"  Location: {location_name}")
    print(f"  Mode: {'DRY RUN' if args.dry_run else 'PUBLISH'}")
    print("=" * 60)

    if not GOOGLE_API_KEY:
        sys.exit("ERROR: GOOGLE_PLACES_API_KEY not set in .env")

    # Get or create the WP location category
    location_id = get_or_create_location(location_name)
    if not location_id:
        sys.exit("ERROR: Could not find or create location in WordPress")

    # Get existing listings to skip duplicates
    print("\n[DEDUP] Fetching existing listings...")
    existing = get_existing_listings()
    print(f"  {len(existing)} existing listings on site")

    # Ledger is the real dedup authority (place_id, not titles)
    by_post, by_place = ledger_utils.load_ledger()
    print(f"  Ledger: {len(by_place)} place_ids")

    # Search for plumbers in this location
    results = search_plumbers(location_name)

    if not results:
        sys.exit(f"No plumbers found in {location_name}")

    if args.dry_run:
        print(f"\n[DRY RUN] Would process {len(results)} plumbers:")
        for i, r in enumerate(results, 1):
            held_by = ledger_utils.existing_post_for(by_place, r["place_id"])
            if held_by:
                note = f"(SKIP - ledger post {held_by}; would attach location)"
            elif r["name"].lower().strip() in existing:
                note = "(SKIP - title already exists)"
            else:
                note = ""
            print(f"  {i}. {r['name']} {note}")
        print(f"\nRun without --dry-run to publish these listings.")
        return

    # Process each plumber
    created = 0
    skipped = 0
    failed = 0

    for i, result in enumerate(results, 1):
        name = result["name"]
        place_id = result["place_id"]

        print(f"\n{'─' * 60}")
        print(f"  [{i}/{len(results)}] {name}")
        print(f"{'─' * 60}")

        # Ledger check first: same business already published anywhere on the
        # site gets this location term attached — never a clone post.
        held_by = ledger_utils.existing_post_for(by_place, place_id)
        if held_by:
            if ledger_utils.attach_location(session, WP_BASE_URL, held_by,
                                            location_id):
                print(f"  SKIP: in ledger as post {held_by} — "
                      f"attached location '{location_name}' instead")
            else:
                print(f"  SKIP: in ledger as post {held_by} — "
                      f"location attach FAILED, review manually")
            skipped += 1
            time.sleep(0.5)
            continue

        # Legacy title check (weak; ledger above is authoritative)
        if name.lower().strip() in existing:
            print(f"  SKIP: Already exists on site")
            skipped += 1
            continue

        # Enrich
        print(f"  Enriching from Google...")
        enriched = enrich_place(place_id)
        if not enriched:
            print(f"  FAILED: Could not enrich")
            failed += 1
            continue

        print(f"    Address: {enriched['formatted_address']}")
        print(f"    Phone:   {enriched['phone'] or 'N/A'}")
        print(f"    Website: {enriched['website'] or 'N/A'}")
        print(f"    Rating:  {enriched['rating']} ({enriched['review_count']} reviews)")
        print(f"    Hours:   {len(enriched.get('lp_hours', {}))} days")
        print(f"    Photos:  {len(enriched.get('photo_refs', []))}")

        # Upload images
        print(f"  Uploading images...")
        featured_id, gallery_ids = upload_all_images(enriched)
        print(f"    Featured: {featured_id}, Gallery: {gallery_ids}")

        # Create listing
        print(f"  Creating listing...")
        post_id, link = create_listing(enriched, location_id, featured_id, location_name)
        if not post_id:
            failed += 1
            continue
        print(f"    Post ID: {post_id}")
        print(f"    URL: {link}")

        # Inject ListingPro options
        print(f"  Injecting ListingPro sidebar data...")
        ok = inject_listingpro_options(post_id, enriched, gallery_ids, location_name)
        print(f"    Result: {'OK' if ok else 'FAILED'}")

        if ok:
            created += 1
            # Record in the ledger in the same run (rule 3)
            rec = ledger_utils.record(ledger_utils.DEFAULT_LEDGER, place_id,
                                      post_id, enriched["name"],
                                      enriched.get("phone", ""), "findplace")
            by_place[place_id] = rec
            # Add to existing set so we don't duplicate within this run
            existing.add(name.lower().strip())
        else:
            failed += 1

        # Rate limit: be gentle on Google API and your server
        time.sleep(1.5)

    # Summary
    print(f"\n{'=' * 60}")
    print(f"  BATCH COMPLETE — {location_name}")
    print(f"{'=' * 60}")
    print(f"  Found:   {len(results)}")
    print(f"  Created: {created}")
    print(f"  Skipped: {skipped} (already existed)")
    print(f"  Failed:  {failed}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
