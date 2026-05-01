# Glasgow Traders - Bulk Builder Publisher
# Category: Builder (ID: 82) | All 107 locations | Image gate enforced

import requests
import os

# Load .env
_envpath = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_envpath):
    with open(_envpath) as _f:
        for _line in _f:
            if "=" in _line and not _line.startswith("#"):
                _k, _v = _line.strip().split("=", 1)
                os.environ[_k] = _v
import sys
import time
import math
import re
from requests.auth import HTTPBasicAuth

GOOGLE_API_KEY  = os.environ.get("GOOGLE_API_KEY", "")
WP_BASE_URL     = "https://www.glasgowtrader.co.uk"
WP_USER         = "Dev"
WP_APP_PASSWORD = "vMKQMNmn7OQ0 MzkVqalj 6aZv"
AUTH            = HTTPBasicAuth(WP_USER, WP_APP_PASSWORD)
CAT_ID_BUILDER  = 82
MAX_DISTANCE_KM = 15

BUILDER_KEYWORDS = [
    "builder", "building", "construction", "contractor", "renovation",
    "joinery", "joiner", "carpentry", "carpenter", "brickwork", "bricklayer",
    "extension", "refurbishment", "conversion", "property maintenance",
    "roofing", "plastering", "groundwork"
]

LOCATIONS = {
    "Anniesland":     {"id": 175, "lat": 55.8893, "lng": -4.3392},
    "Arden":          {"id": 176, "lat": 55.8103, "lng": -4.3194},
    "Arrochar":       {"id": 177, "lat": 56.2369, "lng": -4.7447},
    "Auchinairn":     {"id": 178, "lat": 55.8933, "lng": -4.2028},
    "Ayr":            {"id": 179, "lat": 55.4588, "lng": -4.6299},
    "Baillieston":    {"id": 180, "lat": 55.8457, "lng": -4.1147},
    "Balloch":        {"id": 181, "lat": 56.0007, "lng": -4.5839},
    "Balmaha":        {"id": 182, "lat": 56.0842, "lng": -4.5247},
    "Bankhead":       {"id": 183, "lat": 55.8140, "lng": -4.3400},
    "Bardowie":       {"id": 184, "lat": 55.9280, "lng": -4.2860},
    "Barlanark":      {"id": 185, "lat": 55.8556, "lng": -4.1322},
    "Barmulloch":     {"id": 186, "lat": 55.8843, "lng": -4.1920},
    "Bathgate":       {"id": 187, "lat": 55.9020, "lng": -3.6440},
    "Bearsden":       {"id": 188, "lat": 55.9174, "lng": -4.3339},
    "Bellahouston":   {"id": 189, "lat": 55.8430, "lng": -4.3166},
    "Bellshill":      {"id": 190, "lat": 55.8177, "lng": -4.0256},
    "Bishopbriggs":   {"id": 191, "lat": 55.9038, "lng": -4.2271},
    "Bishopton":      {"id": 192, "lat": 55.9034, "lng": -4.5015},
    "Bridge of Weir": {"id": 193, "lat": 55.8536, "lng": -4.5732},
    "Bridgeton":      {"id": 194, "lat": 55.8497, "lng": -4.2234},
    "Calton":         {"id": 195, "lat": 55.8512, "lng": -4.2290},
    "Cardonald":      {"id": 196, "lat": 55.8450, "lng": -4.3530},
    "Cardross":       {"id": 197, "lat": 55.9620, "lng": -4.6510},
    "Carluke":        {"id": 198, "lat": 55.7340, "lng": -3.8370},
    "Carmyle":        {"id": 199, "lat": 55.8360, "lng": -4.1460},
    "Castlemilk":     {"id": 200, "lat": 55.8105, "lng": -4.2372},
    "Cathcart":       {"id": 201, "lat": 55.8170, "lng": -4.2750},
    "Cessnock":       {"id": 202, "lat": 55.8490, "lng": -4.2780},
    "Clydebank":      {"id": 203, "lat": 55.8998, "lng": -4.4043},
    "Coatbridge":     {"id": 204, "lat": 55.8627, "lng": -4.0247},
    "Cumbernauld":    {"id": 205, "lat": 55.9465, "lng": -3.9933},
    "Cumnock":        {"id": 206, "lat": 55.4530, "lng": -4.2670},
    "Dalmarnock":     {"id": 207, "lat": 55.8396, "lng": -4.2050},
    "Dennistoun":     {"id": 208, "lat": 55.8618, "lng": -4.2103},
    "Drumchapel":     {"id": 209, "lat": 55.9030, "lng": -4.3830},
    "Dumbarton":      {"id": 210, "lat": 55.9451, "lng": -4.5690},
    "Dumbreck":       {"id": 211, "lat": 55.8380, "lng": -4.3040},
    "Eaglesham":      {"id": 212, "lat": 55.7420, "lng": -4.2850},
    "East Kilbride":  {"id": 214, "lat": 55.7644, "lng": -4.1764},
    "Easterhouse":    {"id": 213, "lat": 55.8620, "lng": -4.0816},
    "Falkirk":        {"id": 215, "lat": 56.0019, "lng": -3.7839},
    "Finnieston":     {"id": 216, "lat": 55.8640, "lng": -4.2870},
    "Galston":        {"id": 217, "lat": 55.5990, "lng": -4.3800},
    "Garrowhill":     {"id": 218, "lat": 55.8530, "lng": -4.1070},
    "Garthamlock":    {"id": 219, "lat": 55.8730, "lng": -4.1330},
    "Giffnock":       {"id": 220, "lat": 55.8042, "lng": -4.2947},
    "Glasgow":        {"id": 250, "lat": 55.8642, "lng": -4.2518},
    "Govan":          {"id": 221, "lat": 55.8560, "lng": -4.3100},
    "Govanhill":      {"id": 222, "lat": 55.8390, "lng": -4.2580},
    "Greenock":       {"id": 223, "lat": 55.9496, "lng": -4.7649},
    "Hamilton":       {"id": 224, "lat": 55.7735, "lng": -4.0389},
    "Helensburgh":    {"id": 225, "lat": 56.0130, "lng": -4.7330},
    "Hillhead":       {"id": 226, "lat": 55.8750, "lng": -4.2880},
    "Hillington":     {"id": 227, "lat": 55.8540, "lng": -4.3640},
    "Houston":        {"id": 228, "lat": 55.8680, "lng": -4.5500},
    "Hyndland":       {"id": 229, "lat": 55.8760, "lng": -4.3110},
    "Ibrox":          {"id": 231, "lat": 55.8530, "lng": -4.3000},
    "Irvine":         {"id": 230, "lat": 55.6120, "lng": -4.6680},
    "Jordanhill":     {"id": 232, "lat": 55.8860, "lng": -4.3440},
    "Kelvindale":     {"id": 233, "lat": 55.8880, "lng": -4.3190},
    "Kelvingrove":    {"id": 234, "lat": 55.8680, "lng": -4.2830},
    "Kennishead":     {"id": 235, "lat": 55.8130, "lng": -4.3220},
    "Kilmarnock":     {"id": 238, "lat": 55.6111, "lng": -4.4960},
    "Kilmaurs":       {"id": 236, "lat": 55.6370, "lng": -4.5270},
    "Kilwinning":     {"id": 237, "lat": 55.6550, "lng": -4.7060},
    "Kinning Park":   {"id": 239, "lat": 55.8490, "lng": -4.2900},
    "Kirkintilloch":  {"id": 240, "lat": 55.9392, "lng": -4.1520},
    "Knightswood":    {"id": 241, "lat": 55.8920, "lng": -4.3660},
    "Lanark":         {"id": 242, "lat": 55.6730, "lng": -3.7770},
    "Langbank":       {"id": 243, "lat": 55.9250, "lng": -4.5800},
    "Langside":       {"id": 244, "lat": 55.8260, "lng": -4.2680},
    "Larkhall":       {"id": 245, "lat": 55.7370, "lng": -3.9710},
    "Livingston":     {"id": 246, "lat": 55.8832, "lng": -3.5157},
    "Lochwinnoch":    {"id": 247, "lat": 55.7930, "lng": -4.6310},
    "Luss":           {"id": 248, "lat": 56.0980, "lng": -4.6360},
    "Maryhill":       {"id": 249, "lat": 55.8949, "lng": -4.2804},
    "Milngavie":      {"id": 251, "lat": 55.9416, "lng": -4.3142},
    "Milton":         {"id": 252, "lat": 55.9010, "lng": -4.2190},
    "Motherwell":     {"id": 253, "lat": 55.7892, "lng": -3.9915},
    "Mount Florida":  {"id": 254, "lat": 55.8270, "lng": -4.2600},
    "Muirend":        {"id": 255, "lat": 55.8170, "lng": -4.2700},
    "Neilston":       {"id": 256, "lat": 55.7820, "lng": -4.4270},
    "Newton Mearns":  {"id": 257, "lat": 55.7700, "lng": -4.3360},
    "Paisley":        {"id": 258, "lat": 55.8456, "lng": -4.4239},
    "Parkhead":       {"id": 259, "lat": 55.8520, "lng": -4.1890},
    "Partick":        {"id": 260, "lat": 55.8700, "lng": -4.3100},
    "Pollok":         {"id": 261, "lat": 55.8280, "lng": -4.3370},
    "Pollokshaws":    {"id": 262, "lat": 55.8270, "lng": -4.2990},
    "Pollokshields":  {"id": 263, "lat": 55.8370, "lng": -4.2770},
    "Port Glasgow":   {"id": 264, "lat": 55.9340, "lng": -4.6890},
    "Possilpark":     {"id": 265, "lat": 55.8870, "lng": -4.2440},
    "Prestwick":      {"id": 266, "lat": 55.4940, "lng": -4.6150},
    "Rutherglen":     {"id": 267, "lat": 55.8291, "lng": -4.2141},
    "Shawlands":      {"id": 268, "lat": 55.8280, "lng": -4.2810},
    "Shettleston":    {"id": 269, "lat": 55.8530, "lng": -4.1600},
    "Springburn":     {"id": 270, "lat": 55.8850, "lng": -4.2280},
    "Stewarton":      {"id": 271, "lat": 55.6810, "lng": -4.5130},
    "Stirling":       {"id": 272, "lat": 56.1165, "lng": -3.9369},
    "Strathbungo":    {"id": 273, "lat": 55.8340, "lng": -4.2720},
    "Tarbet":         {"id": 274, "lat": 56.2130, "lng": -4.7210},
    "Tollcross":      {"id": 275, "lat": 55.8480, "lng": -4.1780},
    "Townhead":       {"id": 276, "lat": 55.8660, "lng": -4.2430},
    "Troon":          {"id": 277, "lat": 55.5430, "lng": -4.6600},
    "Uddingston":     {"id": 278, "lat": 55.8190, "lng": -4.0870},
    "Yoker":          {"id": 279, "lat": 55.8890, "lng": -4.3870},
    "Airdrie":        {"id": 173, "lat": 55.8608, "lng": -3.9806},
    "Alexandria":     {"id": 174, "lat": 55.9826, "lng": -4.5829},
}

def printf(msg): print(msg, flush=True)

def safe_filename(name):
    ascii_only = re.sub(r'[^\x00-\x7F]', '', name)
    safe = ascii_only.lower().replace(' ', '-').replace("'", '')
    safe = re.sub(r'[^a-z0-9\-]', '', safe)
    safe = re.sub(r'-+', '-', safe).strip('-')
    return safe[:40] if safe else 'listing'

def to_12h(t):
    h, m = int(t[:2]), t[2:]
    if h == 0:   return f"12:{m}am"
    elif h < 12: return f"{h}:{m}am"
    elif h == 12:return f"12:{m}pm"
    else:        return f"{h-12}:{m}pm"

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def fetch_existing_titles():
    printf("Loading existing listings for duplicate detection...")
    existing = set()
    page = 1
    while True:
        resp = requests.get(f"{WP_BASE_URL}/wp-json/wp/v2/listing",
            params={"per_page": 100, "page": page, "status": "publish,draft,pending"},
            auth=AUTH, timeout=30)
        if resp.status_code != 200: break
        listings = resp.json()
        if not listings: break
        for l in listings:
            existing.add(l.get("title", {}).get("rendered", "").strip().lower())
        page += 1
    printf(f"  Found {len(existing)} existing listings")
    return existing

def is_valid_builder(place, centre_lat, centre_lng):
    name    = place.get("name", "").lower()
    address = place.get("formatted_address", "")
    if "UK" not in address and "United Kingdom" not in address:
        return False, "Not in UK"
    loc  = place.get("geometry", {}).get("location", {})
    dist = haversine_km(centre_lat, centre_lng, loc.get("lat", 0), loc.get("lng", 0))
    if dist > MAX_DISTANCE_KM:
        return False, f"Too far {dist:.1f}km"
    has_keyword   = any(kw in name for kw in BUILDER_KEYWORDS)
    types         = place.get("types", [])
    is_builder_type = "general_contractor" in types or "roofing_contractor" in types
    if not has_keyword and not is_builder_type:
        return False, f"Not a builder: {name}"
    return True, "OK"

def discover_builders(location_name, centre_lat, centre_lng):
    all_results = []
    query = f"builder construction contractor in {location_name} Scotland"
    next_page_token = None
    for page in range(3):
        params = {"query": query, "key": GOOGLE_API_KEY}
        if next_page_token:
            params = {"pagetoken": next_page_token, "key": GOOGLE_API_KEY}
            time.sleep(2)
        resp = requests.get("https://maps.googleapis.com/maps/api/place/textsearch/json",
            params=params, timeout=15)
        data = resp.json()
        if data.get("status") not in ("OK", "ZERO_RESULTS"): break
        all_results.extend(data.get("results", []))
        next_page_token = data.get("next_page_token")
        if not next_page_token: break
    valid, filtered = [], 0
    for place in all_results:
        ok, reason = is_valid_builder(place, centre_lat, centre_lng)
        if ok:
            valid.append(place)
        else:
            filtered += 1
            printf(f"  FILTERED {place.get('name','')[:40]:40} - {reason}")
    return valid, len(all_results), filtered

def enrich_place(place_id):
    resp = requests.get("https://maps.googleapis.com/maps/api/place/details/json",
        params={"place_id": place_id,
                "fields": "name,formatted_address,formatted_phone_number,"
                          "international_phone_number,website,geometry,"
                          "photo,rating,user_ratings_total,opening_hours,"
                          "business_status,editorial_summary",
                "key": GOOGLE_API_KEY}, timeout=15)
    r = resp.json().get("result", {})
    if not r: return None
    photo_refs = [p["photo_reference"] for p in r.get("photos", [])[:4]]
    lp_hours   = {}
    all_days   = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    periods    = r.get("opening_hours", {}).get("periods", [])
    if periods:
        day_names = ["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"]
        is_247    = len(periods) == 1 and periods[0].get("open", {}).get("time") == "0000" and "close" not in periods[0]
        if is_247:
            for day in all_days: lp_hours[day] = {"open": "12:00am", "close": "11:59pm"}
        else:
            for period in periods:
                day_idx   = period.get("open", {}).get("day", 0)
                day_name  = day_names[day_idx]
                open_time = period.get("open",  {}).get("time", "0900")
                close_time= period.get("close", {}).get("time", "1700")
                lp_hours[day_name] = {"open": to_12h(open_time), "close": to_12h(close_time)}
            for day in all_days:
                if day not in lp_hours: lp_hours[day] = {"open": "closed", "close": "closed"}
    return {
        "name":            r.get("name", "Unknown"),
        "formatted_address": r.get("formatted_address", ""),
        "phone":           r.get("formatted_phone_number", ""),
        "intl_phone":      r.get("international_phone_number", ""),
        "website":         r.get("website", ""),
        "lat":             r.get("geometry", {}).get("location", {}).get("lat", 0),
        "lng":             r.get("geometry", {}).get("location", {}).get("lng", 0),
        "rating":          r.get("rating", 0),
        "review_count":    r.get("user_ratings_total", 0),
        "photo_refs":      photo_refs,
        "lp_hours":        lp_hours,
        "place_id":        place_id,
        "summary":         r.get("editorial_summary", {}).get("overview", ""),
    }

def upload_images(photo_refs, name):
    media_ids = []
    safe      = safe_filename(name)
    for i, ref in enumerate(photo_refs):
        photo_url = (f"https://maps.googleapis.com/maps/api/place/photo"
                     f"?maxwidth=1200&photo_reference={ref}&key={GOOGLE_API_KEY}")
        try:
            img = requests.get(photo_url, timeout=30)
            if img.status_code != 200: continue
            wp = requests.post(f"{WP_BASE_URL}/wp-json/wp/v2/media",
                data=img.content,
                headers={"Content-Disposition": f'attachment; filename="{safe}-{i+1}.jpg"',
                         "Content-Type": img.headers.get("Content-Type", "image/jpeg")},
                auth=AUTH, timeout=30)
            if wp.status_code == 201:
                media_ids.append(wp.json()["id"])
        except Exception as e:
            printf(f"    Photo {i+1} error: {e}")
    return media_ids

def create_listing(enriched, first_media_id, location_id, location_name):
    name    = enriched["name"]
    address = enriched["formatted_address"]
    rating  = enriched["rating"]
    reviews = enriched["review_count"]
    summary = enriched.get("summary", "")
    parts   = []
    if summary: parts.append(f"<p>{summary}</p>")
    parts.append(f"<p>{name} is a trusted building service located in {location_name}. "
                 f"Based at {address}, they provide professional building and construction solutions "
                 f"for both residential and commercial customers in the {location_name} area.</p>")
    if rating and reviews:
        parts.append(f"<p>With a {rating}/5 star rating from {reviews} customer reviews, "
                     f"{name} is one of {location_name}'s highest-rated building services.</p>")
    parts.append(f"<p>Contact {name} today for expert building assistance. "
                 f"You can find their phone number, opening hours, and location on this page.</p>")
    payload = {"title": name, "content": "".join(parts),
               "status": "publish", "listing-category": CAT_ID_BUILDER,
               "location": location_id}
    if first_media_id: payload["featured_media"] = first_media_id
    resp = requests.post(f"{WP_BASE_URL}/wp-json/wp/v2/listing",
        json=payload, auth=AUTH, timeout=30)
    if resp.status_code == 201:
        data = resp.json()
        return data["id"], data.get("link")
    return None, None

def inject_listing_pro(post_id, enriched, location_name):
    options = {
        "tagline_text":  f"Builder in {location_name}",
        "gAddress":      enriched["formatted_address"],
        "latitude":      str(enriched["lat"]),
        "longitude":     str(enriched["lng"]),
        "phone":         enriched["phone"],
        "website":       enriched["website"],
        "email": "", "twitter": "", "facebook": "", "linkedin": "",
        "youtube": "", "instagram": "", "video": "", "gallery": "",
        "price_status":  "notsay", "list_price": "", "list_price_to": "",
        "Planid": 0, "lp_purchase_days": "", "reviews_ids": "",
        "claimed_section": "notclaimed",
        "business_hours": enriched.get("lp_hours", {}),
    }
    requests.post(f"{WP_BASE_URL}/wp-json/glasgow-traders/v1/listing-options/{post_id}",
        json=options, auth=AUTH, timeout=15)

def set_gallery(post_id, media_ids):
    if not media_ids: return
    gallery_str = ",".join(str(m) for m in media_ids)
    requests.post(f"{WP_BASE_URL}/wp-json/wp/v2/listing/{post_id}",
        json={"meta": {"gallery_image_ids": gallery_str}}, auth=AUTH, timeout=15)
    requests.post(f"{WP_BASE_URL}/wp-json/glasgow-traders/v1/listing-options/{post_id}",
        json={"gallery": gallery_str}, auth=AUTH, timeout=15)

def publish_one(place, location_name, location_id, existing_titles):
    place_id = place["place_id"]
    name     = place.get("name", "Unknown")
    if name.lower() in existing_titles:
        return "skipped"
    try:
        enriched = enrich_place(place_id)
        if not enriched: return "failed"

        # IMAGE GATE — must have at least 1 photo
        if not enriched.get("photo_refs"):
            printf(f"  SKIP  No images on Google → {name}")
            return "skipped"

        media_ids  = upload_images(enriched["photo_refs"], name)
        if not media_ids:
            printf(f"  SKIP  Images failed to upload → {name}")
            return "skipped"

        first_media = media_ids[0]
        post_id, link = create_listing(enriched, first_media, location_id, location_name)
        if not post_id: return "failed"

        inject_listing_pro(post_id, enriched, location_name)
        set_gallery(post_id, media_ids)
        existing_titles.add(name.lower())
        printf(f"  ✓ {name[:45]:45} | {len(media_ids)} photos | {enriched['phone']} | Post {post_id}")
        return "published"
    except Exception as e:
        printf(f"  ERROR {name}: {e}")
        return "failed"

def main():
    dry_run    = "--dry-run" in sys.argv
    start_time = time.time()
    print("=" * 60)
    print("GLASGOW TRADERS - Bulk Builder Publisher")
    print(f"  Locations : {len(LOCATIONS)}")
    print(f"  Mode      : {'DRY RUN' if dry_run else 'LIVE PUBLISH'}")
    print(f"  Filter    : UK only, within {MAX_DISTANCE_KM}km, builder keywords")
    print("=" * 60)

    existing_titles  = fetch_existing_titles()
    stats = {"discovered": 0, "published": 0, "skipped": 0, "failed": 0}
    location_summary = []

    for loc_name, loc_data in LOCATIONS.items():
        printf(f"\n{'='*60}")
        printf(f"LOCATION: {loc_name}  (ID: {loc_data['id']})")
        printf(f"{'='*60}")
        valid, total, filtered = discover_builders(loc_name, loc_data["lat"], loc_data["lng"])
        stats["discovered"] += len(valid)
        new_count = sum(1 for p in valid if p.get("name","").lower() not in existing_titles)
        printf(f"  {total} found | {filtered} filtered | {len(valid)} valid | {new_count} new")

        if dry_run:
            location_summary.append((loc_name, len(valid), new_count))
            for i, p in enumerate(valid, 1):
                dupe = " [DUPLICATE]" if p.get("name","").lower() in existing_titles else ""
                printf(f"  {i}. {p.get('name','')}{dupe}")
            continue

        loc_published = 0
        for place in valid:
            result = publish_one(place, loc_name, loc_data["id"], existing_titles)
            stats[result] += 1
            if result == "published":
                loc_published += 1
                time.sleep(3)
        location_summary.append((loc_name, len(valid), loc_published))

    elapsed = time.time() - start_time
    minutes, seconds = int(elapsed // 60), int(elapsed % 60)
    print(f"\n{'='*60}")
    print("BULK PUBLISH COMPLETE")
    print(f"{'='*60}")
    print(f"  Locations  : {len(LOCATIONS)}")
    print(f"  Discovered : {stats['discovered']}")
    if dry_run:
        total_new = sum(n for _, _, n in location_summary)
        print(f"  New (would publish): {total_new}")
        print(f"\n  Top locations by new listings:")
        for name, valid, new in sorted(location_summary, key=lambda x: -x[2])[:20]:
            if new > 0: printf(f"    {name:20} {new:3} new / {valid} valid")
    else:
        print(f"  Published  : {stats['published']}")
        print(f"  Skipped    : {stats['skipped']}")
        print(f"  Failed     : {stats['failed']}")
        print(f"  Time       : {minutes}m {seconds}s")
    print("=" * 60)
    if dry_run:
        print("DRY RUN complete. Run without --dry-run to publish.")

if __name__ == "__main__":
    main()
