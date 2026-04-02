"""
Glasgow Traders AutoPipe - Full Discovery DAG
==============================================
Orchestrates the complete pipeline:
1. Discover all businesses in a category from Google Places
2. Enrich each with full details (phone, website, hours, photos)
3. Check which ones already exist on WordPress
4. Create new ListingPro listings with full sidebar + gallery
5. Send Slack summary

Schedule: Weekly on Sundays at 6am
"""

from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import json
import os
import hashlib
from requests.auth import HTTPBasicAuth


# ============================================================
# CONFIG - All from Airflow Variables or environment
# ============================================================
GOOGLE_API_KEY  = os.getenv("GOOGLE_PLACES_API_KEY", "")
WP_BASE_URL     = os.getenv("WP_BASE_URL", "")
WP_USER         = os.getenv("WP_USERNAME", "")
WP_APP_PASSWORD = os.getenv("WP_APP_PASSWORD", "")
SLACK_WEBHOOK   = os.getenv("SLACK_WEBHOOK_URL", "")

# Glasgow centre coordinates for nearby search
GLASGOW_CENTRE = (55.8642, -4.2518)
SEARCH_RADIUS  = 12000  # 12km covers greater Glasgow

# Category mapping: search term -> ListingPro category ID
# Add more categories here as you expand
CATEGORY_MAP = {
    "plumber": 22,
}

# Glasgow location taxonomy ID (from diagnostic)
GLASGOW_LOCATION_ID = 250

# How many photos to upload per listing
MAX_PHOTOS = 4


def get_auth():
    return HTTPBasicAuth(WP_USER, WP_APP_PASSWORD)


# ============================================================
# TASK 1: Discover all businesses in a category
# ============================================================
@task
def discover_businesses(category: str) -> list:
    """
    Search Google Places for all businesses in a category.
    Paginates through all results using next_page_token.
    Returns list of place_ids.
    """
    import time

    print(f"Discovering {category}s in Glasgow...")
    all_place_ids = []
    next_token = None

    while True:
        params = {
            "location": f"{GLASGOW_CENTRE[0]},{GLASGOW_CENTRE[1]}",
            "radius": SEARCH_RADIUS,
            "type": category,
            "key": GOOGLE_API_KEY,
        }
        if next_token:
            params["pagetoken"] = next_token
            time.sleep(2)  # Google requires delay between pages

        resp = requests.get(
            "https://maps.googleapis.com/maps/api/place/nearbysearch/json",
            params=params, timeout=15,
        )
        data = resp.json()

        if data.get("status") not in ["OK", "ZERO_RESULTS"]:
            print(f"  API error: {data.get('status')} {data.get('error_message', '')}")
            break

        for place in data.get("results", []):
            all_place_ids.append({
                "place_id": place["place_id"],
                "name": place.get("name", ""),
            })

        next_token = data.get("next_page_token")
        if not next_token:
            break

    print(f"  Found {len(all_place_ids)} {category}s in Glasgow")
    return all_place_ids


# ============================================================
# TASK 2: Get existing listings from WordPress
# ============================================================
@task
def get_existing_listings() -> dict:
    """
    Fetch all published listings from WordPress and build a
    lookup dict by title for duplicate detection.
    """
    print("Fetching existing WordPress listings...")
    auth = get_auth()
    existing = {}
    page = 1

    while True:
        resp = requests.get(
            f"{WP_BASE_URL}/wp-json/wp/v2/listing",
            params={"per_page": 100, "page": page, "status": "publish"},
            auth=auth, timeout=15,
        )
        if resp.status_code != 200:
            break

        listings = resp.json()
        if not listings:
            break

        for l in listings:
            title = l.get("title", {})
            if isinstance(title, dict):
                title = title.get("rendered", "")
            # Store by normalized title for matching
            key = title.lower().strip()
            existing[key] = {
                "id": l["id"],
                "title": title,
            }
        page += 1

    print(f"  Found {len(existing)} existing listings on WordPress")
    return existing


# ============================================================
# TASK 3: Filter to only new businesses
# ============================================================
@task
def filter_new_businesses(discovered: list, existing: dict) -> list:
    """
    Compare discovered businesses against existing WordPress listings.
    Only return businesses that aren't already on the site.
    """
    print("Filtering for new businesses...")
    existing_names = set(existing.keys())
    new_businesses = []

    for biz in discovered:
        name_key = biz["name"].lower().strip()
        if name_key not in existing_names:
            new_businesses.append(biz)
        else:
            print(f"  SKIP (exists): {biz['name']}")

    print(f"  {len(new_businesses)} new businesses to process")
    print(f"  {len(discovered) - len(new_businesses)} already exist")
    return new_businesses


# ============================================================
# TASK 4: Enrich, upload, and publish each new business
# ============================================================
@task
def process_new_business(business: dict, category: str) -> dict:
    """
    Full pipeline for a single business:
    1. Enrich with Google Place Details
    2. Upload photos to WordPress
    3. Create ListingPro listing
    4. Inject lp_listingpro_options
    5. Set gallery
    Returns summary dict.
    """
    auth = get_auth()
    place_id = business["place_id"]
    cat_id = CATEGORY_MAP.get(category, 22)

    # --- ENRICH ---
    print(f"\nProcessing: {business['name']}")
    detail_resp = requests.get(
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
    r = detail_resp.json().get("result", {})
    if not r:
        return {"name": business["name"], "status": "FAILED", "reason": "No details from Google"}

    # Skip closed businesses
    if r.get("business_status") == "CLOSED_PERMANENTLY":
        return {"name": business["name"], "status": "SKIPPED", "reason": "Permanently closed"}

    name = r.get("name", business["name"])
    address = r.get("formatted_address", "")
    phone = r.get("formatted_phone_number", "")
    website = r.get("website", "")
    lat = r.get("geometry", {}).get("location", {}).get("lat")
    lng = r.get("geometry", {}).get("location", {}).get("lng")
    rating = r.get("rating")
    review_count = r.get("user_ratings_total")
    about = r.get("editorial_summary", {}).get("overview", "")

    # Build hours in ListingPro format
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

    # --- UPLOAD PHOTOS ---
    photo_refs = [p["photo_reference"] for p in r.get("photos", [])[:MAX_PHOTOS]]
    media_ids = []
    for i, ref in enumerate(photo_refs):
        photo_url = (
            f"https://maps.googleapis.com/maps/api/place/photo"
            f"?maxwidth=1200&photoreference={ref}&key={GOOGLE_API_KEY}"
        )
        try:
            img = requests.get(photo_url, timeout=30)
            if img.status_code != 200:
                continue
            safe = name.lower().replace(" ", "-").replace("'", "")[:40]
            wp = requests.post(
                f"{WP_BASE_URL}/wp-json/wp/v2/media",
                data=img.content,
                headers={
                    "Content-Disposition": f'attachment; filename="{safe}-{i+1}.jpg"',
                    "Content-Type": img.headers.get("Content-Type", "image/jpeg"),
                },
                auth=auth, timeout=30,
            )
            if wp.status_code == 201:
                media_ids.append(wp.json()["id"])
        except Exception as e:
            print(f"  Photo {i+1} failed: {e}")

    print(f"  Uploaded {len(media_ids)} photos")

    # --- CREATE LISTING ---
    if not about:
        about = f"{name} is a professional {category} serving Glasgow and the surrounding areas."

    content = f"<p>{about}</p>"
    if hours_text:
        content += f"\n<h3>Opening Hours</h3>\n<p>{hours_text.replace(chr(10), '<br>')}</p>"

    payload = {
        "title": name,
        "content": content,
        "status": "publish",
        "featured_media": media_ids[0] if media_ids else 0,
        "listing-category": [cat_id],
        "location": [GLASGOW_LOCATION_ID],
    }

    resp = requests.post(
        f"{WP_BASE_URL}/wp-json/wp/v2/listing",
        json=payload, auth=auth, timeout=30,
    )

    if resp.status_code not in [200, 201]:
        return {"name": name, "status": "FAILED", "reason": f"WP create: {resp.status_code}"}

    resp_data = resp.json()
    if isinstance(resp_data, list):
        resp_data = resp_data[0]
    post_id = resp_data.get("id")
    link = resp_data.get("link", "")

    print(f"  Created post {post_id}: {link}")

    # --- INJECT LP OPTIONS ---
    options = {
        "phone": phone,
        "website": website,
        "gAddress": address,
        "latitude": str(lat) if lat else "",
        "longitude": str(lng) if lng else "",
        "tagline_text": f"{category.title()} in Glasgow",
        "email": "",
        "claimed_section": "not_claimed",
        "price_status": "notsay",
    }
    if lp_hours:
        options["business_hours"] = lp_hours

    requests.post(
        f"{WP_BASE_URL}/wp-json/glasgow-traders/v1/listing-options/{post_id}",
        json=options, auth=auth, timeout=15,
    )

    # --- SET GALLERY ---
    if media_ids:
        gallery_str = ",".join(str(m) for m in media_ids)
        requests.post(
            f"{WP_BASE_URL}/wp-json/wp/v2/listing/{post_id}",
            json={"meta": {"gallery_image_ids": gallery_str}},
            auth=auth, timeout=15,
        )

    return {
        "name": name,
        "status": "PUBLISHED",
        "post_id": post_id,
        "link": link,
        "phone": phone,
        "photos": len(media_ids),
        "rating": rating,
    }


# ============================================================
# TASK 5: Send Slack summary
# ============================================================
@task
def send_summary(results: list, category: str):
    """Send a Slack notification with the pipeline results."""
    if not SLACK_WEBHOOK:
        print("No Slack webhook configured, skipping notification.")
        return

    published = [r for r in results if r.get("status") == "PUBLISHED"]
    skipped = [r for r in results if r.get("status") == "SKIPPED"]
    failed = [r for r in results if r.get("status") == "FAILED"]

    message = f"*Weekly {category.title()} scan complete.*"
    fields = [
        {"title": "New Published", "value": str(len(published))},
        {"title": "Skipped (exists/closed)", "value": str(len(skipped))},
        {"title": "Failed", "value": str(len(failed))},
        {"title": "Category", "value": category.title()},
    ]

    # Add names of newly published
    if published:
        names = "\n".join([f"- {r['name']}" for r in published[:10]])
        fields.append({"title": "New Listings", "value": names})

    level = "success" if not failed else "warning"
    colours = {"success": "#22C55E", "warning": "#F59E0B", "error": "#EF4444"}
    icons = {"success": "✅", "warning": "⚠️", "error": "🚨"}

    payload = {
        "attachments": [{
            "color": colours.get(level, "#22C55E"),
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"{icons[level]} *Glasgow Traders AutoPipe*\n{message}"
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*{f['title']}*\n{f['value']}"}
                        for f in fields
                    ]
                },
                {
                    "type": "context",
                    "elements": [{
                        "type": "mrkdwn",
                        "text": f"🏴󠁧󠁢󠁳󠁣󠁴󠁿 {datetime.now().strftime('%d %b %Y, %H:%M')} UTC"
                    }]
                }
            ]
        }]
    }

    try:
        requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
        print("Slack notification sent.")
    except Exception as e:
        print(f"Slack failed: {e}")


# ============================================================
# DAG DEFINITION
# ============================================================
@dag(
    dag_id="glasgow_traders_full_pipeline",
    schedule="0 6 * * 0",  # Every Sunday at 6am
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["glasgow-traders", "production"],
    description="Full pipeline: Google Places -> PostgreSQL -> ListingPro",
)
def glasgow_traders_pipeline():

    # For now, run plumber category
    # To add more categories, expand this list
    category = "plumber"

    # Step 1: Discover all plumbers in Glasgow
    discovered = discover_businesses(category)

    # Step 2: Get existing listings from WordPress
    existing = get_existing_listings()

    # Step 3: Filter to new businesses only
    new_businesses = filter_new_businesses(discovered, existing)

    # Step 4: Process each new business (enrich + publish)
    # Using .map() for dynamic task mapping
    results = process_new_business.partial(category=category).expand(
        business=new_businesses,
    )

    # Step 5: Send Slack summary
    send_summary(results, category)


# Instantiate the DAG
glasgow_traders_pipeline()
