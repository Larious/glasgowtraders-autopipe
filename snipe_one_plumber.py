import os
"""
Glasgow Traders - Single Listing Sniper v7
- Proper business description (no duplicate sidebar data)
- Fixed hours parsing (24/7 + normal + closed days)
- Fixed 12-hour time format
- Gallery writes to gallery_image_ids
"""

import requests
import sys
from requests.auth import HTTPBasicAuth

GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "")
WP_BASE_URL = "https://www.glasgowtrader.co.uk"
WP_USER = "Dev"
WP_APP_PASSWORD = os.environ.get("WP_APP_PASSWORD", "")

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
    print("  Glasgow location not found in taxonomy.")
    return None


def discover_plumber(query="plumber in Glasgow Scotland"):
    print(f"\n=== STEP 1: Searching Google Places ===")
    print(f"  Query: {query}")
    resp = requests.get(
        "https://maps.googleapis.com/maps/api/place/textsearch/json",
        params={"query": query, "key": GOOGLE_API_KEY},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") != "OK" or not data.get("results"):
        print(f"  Error: {data.get('status')} {data.get('error_message', '')}")
        return None
    first = data["results"][0]
    print(f"  Found: {first['name']}")
    print(f"  Place ID: {first['place_id']}")
    return first["place_id"]


def to_12h(t):
    h = int(t[:2])
    m = t[2:]
    if h == 0:
        return f"12:{m}am"
    elif h < 12:
        return f"{h}:{m}am"
    elif h == 12:
        return f"12:{m}pm"
    else:
        return f"{h - 12}:{m}pm"


def enrich_place(place_id):
    print("\n=== STEP 2: Enriching from Google Places ===")
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
        print("  No result returned.")
        return None

    photo_refs = []
    if r.get("photos"):
        for p in r["photos"][:4]:
            photo_refs.append(p["photo_reference"])

    lp_hours = {}
    all_days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    periods = r.get("opening_hours", {}).get("periods", [])
    if periods:
        day_names = [
            "Sunday", "Monday", "Tuesday", "Wednesday",
            "Thursday", "Friday", "Saturday",
        ]
        is_24_7 = (
            len(periods) == 1
            and periods[0].get("open", {}).get("time") == "0000"
            and "close" not in periods[0]
        )
        if is_24_7:
            for day in all_days:
                lp_hours[day] = {"open": "12:00am", "close": "11:59pm"}
        else:
            for period in periods:
                day_idx = period.get("open", {}).get("day", 0)
                day_name = day_names[day_idx]
                open_time = period.get("open", {}).get("time", "0900")
                close_time = period.get("close", {}).get("time", "1700")
                lp_hours[day_name] = {"open": to_12h(open_time), "close": to_12h(close_time)}
            for day in all_days:
                if day not in lp_hours:
                    lp_hours[day] = {"open": "closed", "close": "closed"}

    enriched = {
        "name": r.get("name", "Unknown"),
        "formatted_address": r.get("formatted_address", ""),
        "phone": r.get("formatted_phone_number", ""),
        "intl_phone": r.get("international_phone_number", ""),
        "website": r.get("website", ""),
        "lat": r.get("geometry", {}).get("location", {}).get("lat", 0),
        "lng": r.get("geometry", {}).get("location", {}).get("lng", 0),
        "rating": r.get("rating", 0),
        "review_count": r.get("user_ratings_total", 0),
        "photo_refs": photo_refs,
        "lp_hours": lp_hours,
        "place_id": place_id,
        "summary": r.get("editorial_summary", {}).get("overview", ""),
    }

    print(f"  Name:    {enriched['name']}")
    print(f"  Address: {enriched['formatted_address']}")
    print(f"  Phone:   {enriched['phone']}")
    print(f"  Website: {enriched['website']}")
    print(f"  Rating:  {enriched['rating']} ({enriched['review_count']} reviews)")
    print(f"  Photos:  {len(photo_refs)} available")
    print(f"  Hours:   {len(lp_hours)} days")
    return enriched


def upload_images(photo_refs, name):
    if not photo_refs:
        print("\n=== STEP 3: No photos available, skipping ===")
        return []
    print(f"\n=== STEP 3: Uploading {len(photo_refs)} images ===")
    media_ids = []
    for i, ref in enumerate(photo_refs):
        photo_url = (
            f"https://maps.googleapis.com/maps/api/place/photo"
            f"?maxwidth=1200&photoreference={ref}&key={GOOGLE_API_KEY}"
        )
        print(f"  Downloading photo {i + 1}/{len(photo_refs)}...")
        img = requests.get(photo_url, timeout=30)
        if img.status_code != 200:
            print(f"  Download failed: {img.status_code}")
            continue
        safe = name.lower().replace(" ", "-").replace("'", "")[:40]
        wp = requests.post(
            f"{WP_BASE_URL}/wp-json/wp/v2/media",
            data=img.content,
            headers={
                "Content-Disposition": f'attachment; filename="{safe}-{i + 1}.jpg"',
                "Content-Type": img.headers.get("Content-Type", "image/jpeg"),
            },
            auth=AUTH,
            timeout=30,
        )
        if wp.status_code == 201:
            mid = wp.json()["id"]
            media_ids.append(mid)
            print(f"  Uploaded! Media ID: {mid}")
        else:
            print(f"  Upload failed: {wp.status_code} {wp.text[:200]}")
    print(f"  Total uploaded: {len(media_ids)}")
    return media_ids


def create_listing(enriched, first_media_id, location_id):
    print("\n=== STEP 4: Creating WordPress listing ===")

    name = enriched["name"]
    address = enriched["formatted_address"]
    rating = enriched["rating"]
    reviews = enriched["review_count"]
    summary = enriched.get("summary", "")

    parts = []
    if summary:
        parts.append(f"<p>{summary}</p>")

    parts.append(
        f"<p>{name} is a trusted plumbing service located in Glasgow. "
        f"Based at {address}, they provide professional plumbing solutions "
        f"for both residential and commercial customers in the Glasgow area.</p>"
    )

    if rating and reviews:
        parts.append(
            f"<p>With a {rating}/5 star rating from {reviews} customer reviews, "
            f"{name} is one of Glasgow's highest-rated plumbing services.</p>"
        )

    parts.append(
        f"<p>Contact {name} today for expert plumbing assistance. "
        f"You can find their phone number, opening hours, and location "
        f"on this page.</p>"
    )

    content = "\n".join(parts)

    payload = {
        "title": enriched["name"],
        "content": content,
        "status": "publish",
        "listing-category": [CAT_ID_PLUMBER],
    }
    if location_id:
        payload["location"] = [location_id]
    if first_media_id:
        payload["featured_media"] = first_media_id

    resp = requests.post(
        f"{WP_BASE_URL}/wp-json/wp/v2/listing",
        json=payload,
        auth=AUTH,
        timeout=30,
    )
    if resp.status_code == 201:
        data = resp.json()
        post_id = data["id"]
        link = data.get("link", "")
        print(f"  Created! Post ID: {post_id}")
        print(f"  URL: {link}")
        return post_id, link
    else:
        print(f"  FAILED: {resp.status_code}")
        print(f"  {resp.text[:300]}")
        return None, None


def inject_listingpro_options(post_id, enriched):
    print("\n=== STEP 5: Injecting lp_listingpro_options ===")
    options = {
        "tagline_text": "Plumber in Glasgow",
        "gAddress": enriched["formatted_address"],
        "latitude": str(enriched["lat"]),
        "longitude": str(enriched["lng"]),
        "phone": enriched["phone"],
        "website": enriched["website"],
        "email": "",
        "twitter": "",
        "facebook": "",
        "linkedin": "",
        "youtube": "",
        "instagram": "",
        "video": "",
        "gallery": "",
        "price_status": "notsay",
        "list_price": "",
        "list_price_to": "",
        "Plan_id": "0",
        "lp_purchase_days": "",
        "reviews_ids": "",
        "claimed_section": "not_claimed",
        "business_hours": enriched.get("lp_hours", {}),
    }
    resp = requests.post(
        f"{WP_BASE_URL}/wp-json/glasgow-traders/v1/listing-options/{post_id}",
        json=options,
        auth=AUTH,
        timeout=15,
    )
    if resp.status_code == 200:
        print("  lp_listingpro_options saved successfully.")
        result = resp.json()
        for field in ["phone", "gAddress", "latitude", "longitude", "website"]:
            val = result.get("verified", {}).get(field, "?")
            print(f"    {field}: {val}")
    else:
        print(f"  FAILED: {resp.status_code} {resp.text[:200]}")


def set_gallery(post_id, media_ids):
    if not media_ids:
        print("\n=== STEP 5b: No images to set as gallery ===")
        return
    print(f"\n=== STEP 5b: Setting gallery ({len(media_ids)} images) ===")
    gallery_str = ",".join(str(m) for m in media_ids)

    resp = requests.post(
        f"{WP_BASE_URL}/wp-json/wp/v2/listing/{post_id}",
        json={"meta": {"gallery_image_ids": gallery_str}},
        auth=AUTH,
        timeout=15,
    )
    print(f"  gallery_image_ids = {gallery_str}")
    print(f"  REST API status: {resp.status_code}")

    resp2 = requests.post(
        f"{WP_BASE_URL}/wp-json/glasgow-traders/v1/listing-options/{post_id}",
        json={"gallery": gallery_str},
        auth=AUTH,
        timeout=15,
    )
    print(f"  lp_options.gallery status: {resp2.status_code}")

    check = requests.get(
        f"{WP_BASE_URL}/wp-json/glasgow-traders/v1/listing-meta/{post_id}",
        auth=AUTH,
        timeout=15,
    )
    if check.status_code == 200:
        meta = check.json().get("meta", {})
        gid = meta.get("gallery_image_ids", {}).get("value", "")
        if gid:
            count = len(gid.split(","))
            print(f"  Verified: {count} images in gallery_image_ids")
        else:
            print("  WARNING: gallery_image_ids is empty after save!")


def verify_listing(post_id):
    print(f"\n=== STEP 6: Verifying listing {post_id} ===")
    resp = requests.get(
        f"{WP_BASE_URL}/wp-json/glasgow-traders/v1/listing-meta/{post_id}",
        auth=AUTH,
        timeout=15,
    )
    if resp.status_code != 200:
        print("  Could not fetch listing meta for verification.")
        return False

    meta = resp.json().get("meta", {})
    lp = meta.get("lp_listingpro_options", {}).get("value", {})
    gal = meta.get("gallery_image_ids", {}).get("value", "")

    if not isinstance(lp, dict):
        print("  lp_listingpro_options is not a dict.")
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
    print(f"  {'gallery_images':<20} {'OK' if img_count > 0 else 'MISSING':<8} {img_count} images")
    if not gal:
        all_ok = False
    return all_ok


def main():
    print("=" * 60)
    print("  GLASGOW TRADERS - Sniper v7")
    print("  Description + hours fix + gallery")
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
    print(f"  Verified:  {'YES' if verified else 'NO - check warnings above'}")
    print("=" * 60)


if __name__ == "__main__":
    main()
