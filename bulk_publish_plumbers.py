import os
"""
Glasgow Traders - Bulk Plumber Publisher v3
All locations with geographic filtering + unicode safety + duplicate detection.
"""

import requests
import sys
import time
import math
import re
from requests.auth import HTTPBasicAuth

GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "")
WP_BASE_URL = "https://www.glasgowtrader.co.uk"
WP_USER = "Dev"
WP_APP_PASSWORD = os.environ.get("WP_APP_PASSWORD", "")

AUTH = HTTPBasicAuth(WP_USER, WP_APP_PASSWORD)
CAT_ID_PLUMBER = 22

LOCATIONS = {
    "Anniesland": {"id": 175, "lat": 55.8893, "lng": -4.3392},
    "Arden": {"id": 176, "lat": 55.8103, "lng": -4.3194},
    "Arrochar": {"id": 177, "lat": 56.2369, "lng": -4.7447},
    "Auchinairn": {"id": 178, "lat": 55.8933, "lng": -4.2028},
    "Ayr": {"id": 179, "lat": 55.4588, "lng": -4.6299},
    "Baillieston": {"id": 180, "lat": 55.8457, "lng": -4.1147},
    "Balloch": {"id": 181, "lat": 56.0007, "lng": -4.5839},
    "Balmaha": {"id": 182, "lat": 56.0842, "lng": -4.5247},
    "Bankhead": {"id": 183, "lat": 55.8140, "lng": -4.3400},
    "Bardowie": {"id": 184, "lat": 55.9280, "lng": -4.2860},
    "Barlanark": {"id": 185, "lat": 55.8556, "lng": -4.1322},
    "Barmulloch": {"id": 186, "lat": 55.8843, "lng": -4.1920},
    "Bathgate": {"id": 187, "lat": 55.9020, "lng": -3.6440},
    "Bearsden": {"id": 188, "lat": 55.9174, "lng": -4.3339},
    "Bellahouston": {"id": 189, "lat": 55.8430, "lng": -4.3166},
    "Bellshill": {"id": 190, "lat": 55.8177, "lng": -4.0256},
    "Bishopbriggs": {"id": 191, "lat": 55.9038, "lng": -4.2271},
    "Bishopton": {"id": 192, "lat": 55.9034, "lng": -4.5015},
    "Bridge of Weir": {"id": 193, "lat": 55.8536, "lng": -4.5732},
    "Bridgeton": {"id": 194, "lat": 55.8497, "lng": -4.2234},
    "Calton": {"id": 195, "lat": 55.8512, "lng": -4.2290},
    "Cardonald": {"id": 196, "lat": 55.8450, "lng": -4.3530},
    "Cardross": {"id": 197, "lat": 55.9620, "lng": -4.6510},
    "Carluke": {"id": 198, "lat": 55.7340, "lng": -3.8370},
    "Carmyle": {"id": 199, "lat": 55.8360, "lng": -4.1460},
    "Castlemilk": {"id": 200, "lat": 55.8105, "lng": -4.2372},
    "Cathcart": {"id": 201, "lat": 55.8170, "lng": -4.2750},
    "Cessnock": {"id": 202, "lat": 55.8490, "lng": -4.2780},
    "Clydebank": {"id": 203, "lat": 55.8998, "lng": -4.4043},
    "Coatbridge": {"id": 204, "lat": 55.8627, "lng": -4.0247},
    "Cumbernauld": {"id": 205, "lat": 55.9465, "lng": -3.9933},
    "Cumnock": {"id": 206, "lat": 55.4530, "lng": -4.2670},
    "Dalmarnock": {"id": 207, "lat": 55.8396, "lng": -4.2050},
    "Dennistoun": {"id": 208, "lat": 55.8618, "lng": -4.2103},
    "Drumchapel": {"id": 209, "lat": 55.9030, "lng": -4.3830},
    "Dumbarton": {"id": 210, "lat": 55.9451, "lng": -4.5690},
    "Dumbreck": {"id": 211, "lat": 55.8380, "lng": -4.3040},
    "Eaglesham": {"id": 212, "lat": 55.7420, "lng": -4.2850},
    "East Kilbride": {"id": 214, "lat": 55.7644, "lng": -4.1764},
    "Easterhouse": {"id": 213, "lat": 55.8620, "lng": -4.0816},
    "Falkirk": {"id": 215, "lat": 56.0019, "lng": -3.7839},
    "Finnieston": {"id": 216, "lat": 55.8640, "lng": -4.2870},
    "Galston": {"id": 217, "lat": 55.5990, "lng": -4.3800},
    "Garrowhill": {"id": 218, "lat": 55.8530, "lng": -4.1070},
    "Garthamlock": {"id": 219, "lat": 55.8730, "lng": -4.1330},
    "Giffnock": {"id": 220, "lat": 55.8042, "lng": -4.2947},
    "Glasgow": {"id": 250, "lat": 55.8642, "lng": -4.2518},
    "Govan": {"id": 221, "lat": 55.8560, "lng": -4.3100},
    "Govanhill": {"id": 222, "lat": 55.8390, "lng": -4.2580},
    "Greenock": {"id": 223, "lat": 55.9496, "lng": -4.7649},
    "Hamilton": {"id": 224, "lat": 55.7735, "lng": -4.0389},
    "Helensburgh": {"id": 225, "lat": 56.0130, "lng": -4.7330},
    "Hillhead": {"id": 226, "lat": 55.8750, "lng": -4.2880},
    "Hillington": {"id": 227, "lat": 55.8540, "lng": -4.3640},
    "Houston": {"id": 228, "lat": 55.8680, "lng": -4.5500},
    "Hyndland": {"id": 229, "lat": 55.8760, "lng": -4.3110},
    "Ibrox": {"id": 231, "lat": 55.8530, "lng": -4.3000},
    "Irvine": {"id": 230, "lat": 55.6120, "lng": -4.6680},
    "Jordanhill": {"id": 232, "lat": 55.8860, "lng": -4.3440},
    "Kelvindale": {"id": 233, "lat": 55.8880, "lng": -4.3190},
    "Kelvingrove": {"id": 234, "lat": 55.8680, "lng": -4.2830},
    "Kennishead": {"id": 235, "lat": 55.8130, "lng": -4.3220},
    "Kilmarnock": {"id": 238, "lat": 55.6111, "lng": -4.4960},
    "Kilmaurs": {"id": 236, "lat": 55.6370, "lng": -4.5270},
    "Kilwinning": {"id": 237, "lat": 55.6550, "lng": -4.7060},
    "Kinning Park": {"id": 239, "lat": 55.8490, "lng": -4.2900},
    "Kirkintilloch": {"id": 240, "lat": 55.9392, "lng": -4.1520},
    "Knightswood": {"id": 241, "lat": 55.8920, "lng": -4.3660},
    "Lanark": {"id": 242, "lat": 55.6730, "lng": -3.7770},
    "Langbank": {"id": 243, "lat": 55.9250, "lng": -4.5800},
    "Langside": {"id": 244, "lat": 55.8260, "lng": -4.2680},
    "Larkhall": {"id": 245, "lat": 55.7370, "lng": -3.9710},
    "Livingston": {"id": 246, "lat": 55.8832, "lng": -3.5157},
    "Lochwinnoch": {"id": 247, "lat": 55.7930, "lng": -4.6310},
    "Luss": {"id": 248, "lat": 56.0980, "lng": -4.6360},
    "Maryhill": {"id": 249, "lat": 55.8949, "lng": -4.2804},
    "Milngavie": {"id": 251, "lat": 55.9416, "lng": -4.3142},
    "Milton": {"id": 252, "lat": 55.9010, "lng": -4.2190},
    "Motherwell": {"id": 253, "lat": 55.7892, "lng": -3.9915},
    "Mount Florida": {"id": 254, "lat": 55.8270, "lng": -4.2600},
    "Muirend": {"id": 255, "lat": 55.8170, "lng": -4.2700},
    "Neilston": {"id": 256, "lat": 55.7820, "lng": -4.4270},
    "Newton Mearns": {"id": 257, "lat": 55.7700, "lng": -4.3360},
    "Paisley": {"id": 258, "lat": 55.8456, "lng": -4.4239},
    "Parkhead": {"id": 259, "lat": 55.8520, "lng": -4.1890},
    "Partick": {"id": 260, "lat": 55.8700, "lng": -4.3100},
    "Pollok": {"id": 261, "lat": 55.8280, "lng": -4.3370},
    "Pollokshaws": {"id": 262, "lat": 55.8270, "lng": -4.2990},
    "Pollokshields": {"id": 263, "lat": 55.8370, "lng": -4.2770},
    "Port Glasgow": {"id": 264, "lat": 55.9340, "lng": -4.6890},
    "Possilpark": {"id": 265, "lat": 55.8870, "lng": -4.2440},
    "Prestwick": {"id": 266, "lat": 55.4940, "lng": -4.6150},
    "Rutherglen": {"id": 267, "lat": 55.8291, "lng": -4.2141},
    "Shawlands": {"id": 268, "lat": 55.8280, "lng": -4.2810},
    "Shettleston": {"id": 269, "lat": 55.8530, "lng": -4.1600},
    "Springburn": {"id": 270, "lat": 55.8850, "lng": -4.2280},
    "Stewarton": {"id": 271, "lat": 55.6810, "lng": -4.5130},
    "Stirling": {"id": 272, "lat": 56.1165, "lng": -3.9369},
    "Strathbungo": {"id": 273, "lat": 55.8340, "lng": -4.2720},
    "Tarbet": {"id": 274, "lat": 56.2130, "lng": -4.7210},
    "Tollcross": {"id": 275, "lat": 55.8480, "lng": -4.1780},
    "Townhead": {"id": 276, "lat": 55.8660, "lng": -4.2430},
    "Troon": {"id": 277, "lat": 55.5430, "lng": -4.6600},
    "Uddingston": {"id": 278, "lat": 55.8190, "lng": -4.0870},
    "Yoker": {"id": 279, "lat": 55.8890, "lng": -4.3870},
}

MAX_DISTANCE_KM = 15
PLUMBER_KEYWORDS = ["plumb", "heating", "gas", "pipe", "drain", "boiler"]


def safe_filename(name):
    ascii_only = re.sub(r'[^\x00-\x7F]', '', name)
    safe = ascii_only.lower().replace(" ", "-").replace("'", "")
    safe = re.sub(r'[^a-z0-9\-]', '', safe)
    safe = re.sub(r'-+', '-', safe).strip('-')
    return safe[:40] if safe else "listing"


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


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def fetch_existing_titles():
    print("\n  Loading existing listings for duplicate detection...")
    existing = set()
    page = 1
    while True:
        resp = requests.get(
            f"{WP_BASE_URL}/wp-json/wp/v2/listing",
            params={"per_page": 100, "page": page, "status": "publish,draft,pending"},
            auth=AUTH, timeout=30,
        )
        if resp.status_code != 200:
            break
        listings = resp.json()
        if not listings:
            break
        for listing in listings:
            title = listing.get("title", {}).get("rendered", "").strip()
            existing.add(title.lower())
        page += 1
    print(f"  Found {len(existing)} existing listings")
    return existing


def is_valid_plumber(place, center_lat, center_lng):
    name = place.get("name", "").lower()
    address = place.get("formatted_address", "")

    if "UK" not in address and "United Kingdom" not in address:
        return False, "Not in UK"

    loc = place.get("geometry", {}).get("location", {})
    plat = loc.get("lat", 0)
    plng = loc.get("lng", 0)
    dist = haversine_km(center_lat, center_lng, plat, plng)
    if dist > MAX_DISTANCE_KM:
        return False, f"Too far ({dist:.1f}km)"

    has_keyword = any(kw in name for kw in PLUMBER_KEYWORDS)
    types = place.get("types", [])
    is_plumber_type = "plumber" in types
    if not has_keyword and not is_plumber_type:
        return False, f"Not a plumber: {name}"

    return True, "OK"


def discover_plumbers(location_name, center_lat, center_lng):
    all_results = []
    query = f"plumber in {location_name} Scotland"
    next_page_token = None

    for page in range(3):
        params = {"query": query, "key": GOOGLE_API_KEY}
        if next_page_token:
            params = {"pagetoken": next_page_token, "key": GOOGLE_API_KEY}
            time.sleep(2)

        resp = requests.get(
            "https://maps.googleapis.com/maps/api/place/textsearch/json",
            params=params, timeout=15,
        )
        data = resp.json()
        if data.get("status") not in ("OK", "ZERO_RESULTS"):
            break

        results = data.get("results", [])
        all_results.extend(results)

        next_page_token = data.get("next_page_token")
        if not next_page_token:
            break

    valid = []
    filtered_count = 0
    for place in all_results:
        ok, reason = is_valid_plumber(place, center_lat, center_lng)
        if ok:
            valid.append(place)
        else:
            filtered_count += 1

    return valid, len(all_results), filtered_count


def enrich_place(place_id):
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
    r = resp.json().get("result", {})
    if not r:
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

    return {
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


def upload_images(photo_refs, name):
    media_ids = []
    safe = safe_filename(name)
    for i, ref in enumerate(photo_refs):
        photo_url = (
            f"https://maps.googleapis.com/maps/api/place/photo"
            f"?maxwidth=1200&photoreference={ref}&key={GOOGLE_API_KEY}"
        )
        try:
            img = requests.get(photo_url, timeout=30)
            if img.status_code != 200:
                continue
            wp = requests.post(
                f"{WP_BASE_URL}/wp-json/wp/v2/media",
                data=img.content,
                headers={
                    "Content-Disposition": f'attachment; filename="{safe}-{i + 1}.jpg"',
                    "Content-Type": img.headers.get("Content-Type", "image/jpeg"),
                },
                auth=AUTH, timeout=30,
            )
            if wp.status_code == 201:
                media_ids.append(wp.json()["id"])
        except Exception as e:
            print(f"      Photo {i + 1} error: {e}")
    return media_ids


def create_listing(enriched, first_media_id, location_id, location_name):
    name = enriched["name"]
    address = enriched["formatted_address"]
    rating = enriched["rating"]
    reviews = enriched["review_count"]
    summary = enriched.get("summary", "")

    parts = []
    if summary:
        parts.append(f"<p>{summary}</p>")
    parts.append(
        f"<p>{name} is a trusted plumbing service located in {location_name}. "
        f"Based at {address}, they provide professional plumbing solutions "
        f"for both residential and commercial customers in the {location_name} area.</p>"
    )
    if rating and reviews:
        parts.append(
            f"<p>With a {rating}/5 star rating from {reviews} customer reviews, "
            f"{name} is one of {location_name}'s highest-rated plumbing services.</p>"
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
        "location": [location_id],
    }
    if first_media_id:
        payload["featured_media"] = first_media_id

    resp = requests.post(
        f"{WP_BASE_URL}/wp-json/wp/v2/listing",
        json=payload, auth=AUTH, timeout=30,
    )
    if resp.status_code == 201:
        data = resp.json()
        return data["id"], data.get("link", "")
    return None, None


def inject_listingpro_options(post_id, enriched, location_name):
    options = {
        "tagline_text": f"Plumber in {location_name}",
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
    requests.post(
        f"{WP_BASE_URL}/wp-json/glasgow-traders/v1/listing-options/{post_id}",
        json=options, auth=AUTH, timeout=15,
    )


def set_gallery(post_id, media_ids):
    if not media_ids:
        return
    gallery_str = ",".join(str(m) for m in media_ids)
    requests.post(
        f"{WP_BASE_URL}/wp-json/wp/v2/listing/{post_id}",
        json={"meta": {"gallery_image_ids": gallery_str}},
        auth=AUTH, timeout=15,
    )
    requests.post(
        f"{WP_BASE_URL}/wp-json/glasgow-traders/v1/listing-options/{post_id}",
        json={"gallery": gallery_str},
        auth=AUTH, timeout=15,
    )


def publish_one(place, location_name, location_id, existing_titles):
    place_id = place["place_id"]
    name = place.get("name", "Unknown")

    if name.lower() in existing_titles:
        return "skipped"

    try:
        enriched = enrich_place(place_id)
        if not enriched:
            return "failed"

        media_ids = upload_images(enriched.get("photo_refs", []), enriched["name"])
        first_media = media_ids[0] if media_ids else None

        post_id, link = create_listing(enriched, first_media, location_id, location_name)
        if not post_id:
            return "failed"

        inject_listingpro_options(post_id, enriched, location_name)
        set_gallery(post_id, media_ids)

        existing_titles.add(name.lower())

        print(f"    + {name} | {len(media_ids)} photos | {enriched['phone']} | Post {post_id}")
        return "published"

    except Exception as e:
        print(f"    X {name} | ERROR: {e}")
        return "failed"


def main():
    dry_run = "--dry-run" in sys.argv
    start_time = time.time()

    print("=" * 60)
    print("  GLASGOW TRADERS - Bulk Plumber Publisher v3")
    print(f"  Locations: {len(LOCATIONS)}")
    print(f"  Mode: {'DRY RUN' if dry_run else 'LIVE PUBLISH'}")
    print("=" * 60)

    existing_titles = fetch_existing_titles()

    stats = {"discovered": 0, "published": 0, "skipped": 0, "failed": 0}
    location_summary = []

    for location_name, loc_data in LOCATIONS.items():
        location_id = loc_data["id"]
        center_lat = loc_data["lat"]
        center_lng = loc_data["lng"]

        valid, total, filtered = discover_plumbers(location_name, center_lat, center_lng)
        stats["discovered"] += len(valid)

        # Count how many are new vs duplicate
        new_count = sum(1 for p in valid if p.get("name", "").lower() not in existing_titles)

        print(f"  {location_name:<20} {total:>3} found | {filtered:>2} filtered | {len(valid):>2} valid | {new_count:>2} new")

        if dry_run:
            location_summary.append((location_name, len(valid), new_count))
            continue

        loc_published = 0
        for place in valid:
            result = publish_one(place, location_name, location_id, existing_titles)
            stats[result] += 1
            if result == "published":
                loc_published += 1
                time.sleep(3)

        location_summary.append((location_name, len(valid), loc_published))

    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)

    print(f"\n{'=' * 60}")
    print("  BULK PUBLISH COMPLETE")
    print(f"{'=' * 60}")
    print(f"  Locations:   {len(LOCATIONS)}")
    print(f"  Discovered:  {stats['discovered']}")
    print(f"  Published:   {stats['published']}")
    print(f"  Skipped:     {stats['skipped']}")
    print(f"  Failed:      {stats['failed']}")
    print(f"  Time:        {minutes}m {seconds}s")
    print(f"{'=' * 60}")

    if dry_run:
        total_new = sum(n for _, _, n in location_summary)
        print(f"\n  DRY RUN - {total_new} new listings would be published")
        print(f"\n  Top locations by new listings:")
        for name, valid, new in sorted(location_summary, key=lambda x: -x[2])[:20]:
            if new > 0:
                print(f"    {name:<20} {new:>2} new / {valid} valid")


if __name__ == "__main__":
    main()
