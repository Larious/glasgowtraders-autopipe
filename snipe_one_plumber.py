"""
Glasgow Traders - Single Listing Sniper v5
Full gallery support + lp_listingpro_options serialized array
Reads credentials from .env file - NEVER hardcode secrets.
"""

import requests
import sys
import os
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

load_dotenv()

GOOGLE_API_KEY  = os.getenv("GOOGLE_PLACES_API_KEY")
WP_BASE_URL     = os.getenv("WP_BASE_URL")
WP_USER         = os.getenv("WP_USERNAME")
WP_APP_PASSWORD = os.getenv("WP_APP_PASSWORD")

if not all([GOOGLE_API_KEY, WP_BASE_URL, WP_USER, WP_APP_PASSWORD]):
    sys.exit("ERROR: Missing credentials in .env file.")

AUTH = HTTPBasicAuth(WP_USER, WP_APP_PASSWORD)
CAT_ID_PLUMBER = 22
GLASGOW_LOC_ID = None


def find_glasgow_location_id():
    print("\n=== STEP 0: Finding Glasgow location ID ===")
    resp = requests.get(
        f"{WP_BASE_URL}/wp-json/wp/v2/location",
        params={"per_page": 100, "search": "glasgow"},
        auth=AUTH, timeout=15,
    )
    if resp.status_code == 200:
        for term in resp.json():
            if "glasgow" in term["name"].lower():
                print(f"  Found: ID={term['id']} name={term['name']}")
                return term["id"]
    print("  Glasgow not found.")
    return None


def discover_plumber():
    print("\n=== STEP 1: Searching Google Places ===")
    resp = requests.get(
        "https://maps.googleapis.com/maps/api/place/textsearch/json",
        params={"query": "electrician in Glasgow Scotland", "key": GOOGLE_API_KEY},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") != "OK" or not data.get("results"):
        print(f"  Error: {data.get('status')} {data.get('error_message', '')}")
        return None
    first = data["results"][0]
    print(f"  Found: {first['name']}")
    return first["place_id"]


def enrich_place(place_id):
    print("\n=== STEP 2: Enriching from Google ===")
    resp = requests.get(
        "https://maps.googleapis.com/maps/api/place/details/json",
        params={
            "place_id": place_id,
            "fields": "name,formatted_address,formatted_phone_number,"
                      "international_phone_number,website,geometry,"
                      "photo,rating,user_ratings_total,opening_hours,"
                      "business_status,editorial_summary",
            "key": GOOGLE_API_KEY,
        },
        timeout=15,
    )
    resp.raise_for_status()
    r = resp.json().get("result", {})
    if not r:
        return None

    photo_refs = []
    if r.get("photos"):
        for p in r["photos"][:4]:
            photo_refs.append(p["photo_reference"])

    lp_hours = {}
    if r.get("opening_hours", {}).get("periods"):
        day_names = ["Sunday", "Monday", "Tuesday", "Wednesday",
                     "Thursday", "Friday", "Saturday"]
        for period in r["opening_hours"]["periods"]:
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

    print(f"  Name:    {enriched['name']}")
    print(f"  Address: {enriched['formatted_address']}")
    print(f"  Phone:   {enriched['phone']}")
    print(f"  Website: {enriched['website']}")
    print(f"  Coords:  {enriched['lat']}, {enriched['lng']}")
    print(f"  Rating:  {enriched['rating']} ({enriched['review_count']} reviews)")
    print(f"  Hours:   {len(lp_hours)} days")
    print(f"  Photos:  {len(photo_refs)}")
    return enriched


def upload_images(photo_refs, name):
    if not photo_refs:
        print("\n=== STEP 3: No photos, skipping ===")
        return []
    print(f"\n=== STEP 3: Uploading {len(photo_refs)} images ===")
    media_ids = []
    for i, ref in enumerate(photo_refs):
        photo_url = (
            f"https://maps.googleapis.com/maps/api/place/photo"
            f"?maxwidth=1200&photoreference={ref}&key={GOOGLE_API_KEY}"
        )
        print(f"  Downloading photo {i+1}/{len(photo_refs)}...")
        img = requests.get(photo_url, timeout=30)
        if img.status_code != 200:
            print(f"  Download failed: {img.status_code}")
            continue
        safe = name.lower().replace(" ", "-").replace("'", "")[:40]
        wp = requests.post(
            f"{WP_BASE_URL}/wp-json/wp/v2/media",
            data=img.content,
            headers={
                "Content-Disposition": f'attachment; filename="{safe}-{i+1}.jpg"',
                "Content-Type": img.headers.get("Content-Type", "image/jpeg"),
            },
            auth=AUTH, timeout=30,
        )
        if wp.status_code == 201:
            mid = wp.json()["id"]
            media_ids.append(mid)
            print(f"  Uploaded! Media ID: {mid}")
        else:
            print(f"  Failed: {wp.status_code}")
    return media_ids


def create_listing(enriched, first_media_id, loc_id):
    print("\n=== STEP 4: Creating listing ===")
    about = enriched.get("about") or (
        f"{enriched['name']} is a professional plumber serving "
        f"Glasgow and the surrounding areas."
    )
    content = f"<p>{about}</p>"
    if enriched.get("hours_text"):
        content += "\n<h3>Opening Hours</h3>\n<p>"
        content += enriched["hours_text"].replace("\n", "<br>")
        content += "</p>"

    payload = {
        "title":            enriched["name"],
        "content":          content,
        "status":           "publish",
        "featured_media":   first_media_id or 0,
        "listing-category": [CAT_ID_PLUMBER],
    }
    if loc_id:
        payload["location"] = [loc_id]

    resp = requests.post(
        f"{WP_BASE_URL}/wp-json/wp/v2/listing",
        json=payload, auth=AUTH, timeout=30,
    )
    if resp.status_code in [200, 201]:
        d = resp.json()
        if isinstance(d, list):
            d = d[0]
        pid = d.get("id")
        link = d.get("link", "")
        print(f"  Created! Post ID: {pid}")
        print(f"  Link: {link}")
        return pid, link
    print(f"  FAILED ({resp.status_code}): {resp.text[:300]}")
    return None, None


def inject_listingpro_options(post_id, enriched):
    print("\n=== STEP 5: Writing to lp_listingpro_options ===")
    options_data = {
        "phone":            enriched.get("phone", ""),
        "website":          enriched.get("website", ""),
        "gAddress":         enriched.get("formatted_address", ""),
        "latitude":         str(enriched.get("lat", "")),
        "longitude":        str(enriched.get("lng", "")),
        "tagline_text":     "Plumber in Glasgow",
        "email":            "",
        "claimed_section":  "not_claimed",
        "price_status":     "notsay",
    }
    if enriched.get("lp_hours"):
        options_data["business_hours"] = enriched["lp_hours"]

    resp = requests.post(
        f"{WP_BASE_URL}/wp-json/glasgow-traders/v1/listing-options/{post_id}",
        json=options_data, auth=AUTH, timeout=15,
    )
    if resp.status_code == 200:
        data = resp.json()
        print(f"  {data.get('message', '')}")
        verified = data.get("verified", {})
        for k, v in verified.items():
            preview = str(v)[:50] if v else "(empty)"
            print(f"    {k:<20} {preview}")
        return data.get("success", False)
    print(f"  Failed: {resp.status_code}")
    return False


def set_gallery(post_id, media_ids):
    if not media_ids:
        return
    print(f"\n=== STEP 5b: Setting gallery ({len(media_ids)} images) ===")
    gallery_str = ",".join(str(m) for m in media_ids)
    resp = requests.post(
        f"{WP_BASE_URL}/wp-json/wp/v2/listing/{post_id}",
        json={"meta": {"gallery_image_ids": gallery_str}},
        auth=AUTH, timeout=15,
    )
    print(f"  gallery_image_ids = {gallery_str}")
    print(f"  Status: {resp.status_code}")


def verify_listing(post_id):
    print("\n=== STEP 6: Verifying ===")
    resp = requests.get(
        f"{WP_BASE_URL}/wp-json/glasgow-traders/v1/listing-meta/{post_id}",
        auth=AUTH, timeout=15,
    )
    if resp.status_code != 200:
        print(f"  Could not verify: {resp.status_code}")
        return False
    meta = resp.json().get("meta", {})
    lp = meta.get("lp_listingpro_options", {}).get("value", {})
    gal = meta.get("gallery_image_ids", {}).get("value", "")
    if not isinstance(lp, dict):
        print("  lp_listingpro_options not a dict.")
        return False
    fields = ["phone", "website", "gAddress", "latitude", "longitude"]
    all_ok = True
    for f in fields:
        val = lp.get(f, "")
        status = "OK" if val else "MISSING"
        if not val:
            all_ok = False
        print(f"  {f:<20} {status:<8} {str(val)[:50]}")
    img_count = len(gal.split(",")) if gal else 0
    print(f"  gallery_images     {'OK' if img_count > 0 else 'MISSING':<8} {img_count} images")
    if not gal:
        all_ok = False
    return all_ok


def main():
    print("=" * 60)
    print("  GLASGOW TRADERS - Sniper v5")
    print("  Full gallery + lp_listingpro_options")
    print("=" * 60)

    global GLASGOW_LOC_ID
    GLASGOW_LOC_ID = find_glasgow_location_id()

    place_id = discover_plumber()
    if not place_id:
        sys.exit("ABORTED: No plumber found.")

    enriched = enrich_place(place_id)
    if not enriched:
        sys.exit("ABORTED: Could not enrich.")

    media_ids = upload_images(enriched.get("photo_refs", []), enriched["name"])
    first_media = media_ids[0] if media_ids else None

    post_id, link = create_listing(enriched, first_media, GLASGOW_LOC_ID)
    if not post_id:
        sys.exit("FAILED: Could not create listing.")

    inject_listingpro_options(post_id, enriched)

    set_gallery(post_id, media_ids)

    verified = verify_listing(post_id)

    print("\n" + "=" * 60)
    print("  PIPELINE COMPLETE")
    print("=" * 60)
    print(f"  Business:  {enriched['name']}")
    print(f"  Address:   {enriched['formatted_address']}")
    print(f"  Phone:     {enriched['phone']}")
    print(f"  Website:   {enriched['website']}")
    print(f"  Coords:    {enriched['lat']}, {enriched['lng']}")
    print(f"  Rating:    {enriched['rating']} ({enriched['review_count']} reviews)")
    print(f"  Hours:     {len(enriched.get('lp_hours', {}))} days")
    print(f"  Photos:    {len(media_ids)} uploaded")
    print(f"  WP Post:   {post_id}")
    print(f"  Live URL:  {link}")
    print(f"  Verified:  {'YES' if verified else 'NO'}")
    print("=" * 60)


if __name__ == "__main__":
    main()
