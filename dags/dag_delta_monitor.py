"""
Glasgow Traders AutoPipe - Delta Monitor DAG
=============================================
Runs hourly to detect:
1. New businesses that appeared on Google Maps
2. Businesses that changed (phone, address, hours, status)
3. Businesses that permanently closed

Uses SHA-256 hashing to detect changes efficiently.

Schedule: Hourly
"""

from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import json
import os
import hashlib
from requests.auth import HTTPBasicAuth


GOOGLE_API_KEY  = os.getenv("GOOGLE_PLACES_API_KEY", "")
WP_BASE_URL     = os.getenv("WP_BASE_URL", "")
WP_USER         = os.getenv("WP_USERNAME", "")
WP_APP_PASSWORD = os.getenv("WP_APP_PASSWORD", "")
SLACK_WEBHOOK   = os.getenv("SLACK_WEBHOOK_URL", "")

GLASGOW_CENTRE = (55.8642, -4.2518)
SEARCH_RADIUS  = 12000

CATEGORY_MAP = {
    "plumber": 22,
}

GLASGOW_LOCATION_ID = 250


def get_auth():
    return HTTPBasicAuth(WP_USER, WP_APP_PASSWORD)


def make_hash(data: dict) -> str:
    """Create a fingerprint of business data to detect changes."""
    hash_str = f"{data.get('name', '')}|{data.get('phone', '')}|{data.get('address', '')}|{data.get('website', '')}|{data.get('status', '')}"
    return hashlib.sha256(hash_str.encode()).hexdigest()


# ============================================================
# TASK 1: Scan Google for current state
# ============================================================
@task
def scan_google(category: str) -> list:
    """Get current list of businesses from Google Places."""
    import time
    print(f"Scanning Google for {category}s...")
    businesses = []
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
            time.sleep(2)

        resp = requests.get(
            "https://maps.googleapis.com/maps/api/place/nearbysearch/json",
            params=params, timeout=15,
        )
        data = resp.json()
        if data.get("status") not in ["OK", "ZERO_RESULTS"]:
            break

        for place in data.get("results", []):
            businesses.append({
                "place_id": place["place_id"],
                "name": place.get("name", ""),
                "address": place.get("vicinity", ""),
                "status": place.get("business_status", "OPERATIONAL"),
                "rating": place.get("rating"),
            })

        next_token = data.get("next_page_token")
        if not next_token:
            break

    print(f"  Found {len(businesses)} {category}s on Google")
    return businesses


# ============================================================
# TASK 2: Get current WordPress state
# ============================================================
@task
def get_wp_state() -> dict:
    """Fetch all listings and their metadata from WordPress."""
    print("Fetching WordPress listings...")
    auth = get_auth()
    listings = {}
    page = 1

    while True:
        resp = requests.get(
            f"{WP_BASE_URL}/wp-json/wp/v2/listing",
            params={"per_page": 100, "page": page, "status": "publish"},
            auth=auth, timeout=15,
        )
        if resp.status_code != 200 or not resp.json():
            break

        for l in resp.json():
            title = l.get("title", {})
            if isinstance(title, dict):
                title = title.get("rendered", "")
            listings[title.lower().strip()] = {
                "id": l["id"],
                "title": title,
            }
        page += 1

    print(f"  Found {len(listings)} listings on WordPress")
    return listings


# ============================================================
# TASK 3: Detect changes
# ============================================================
@task
def detect_changes(google_data: list, wp_data: dict) -> dict:
    """
    Compare Google state vs WordPress state.
    Returns dict with new, changed, and closed businesses.
    """
    print("Detecting changes...")

    wp_names = set(wp_data.keys())
    google_names = set()
    new_businesses = []
    closed_businesses = []

    for biz in google_data:
        name_key = biz["name"].lower().strip()
        google_names.add(name_key)

        if name_key not in wp_names:
            new_businesses.append(biz)
        
        if biz.get("status") == "CLOSED_PERMANENTLY" and name_key in wp_names:
            closed_businesses.append({
                "name": biz["name"],
                "wp_id": wp_data[name_key]["id"],
            })

    result = {
        "new": new_businesses,
        "closed": closed_businesses,
        "total_google": len(google_data),
        "total_wp": len(wp_data),
        "new_count": len(new_businesses),
        "closed_count": len(closed_businesses),
    }

    print(f"  New businesses: {len(new_businesses)}")
    print(f"  Closed businesses: {len(closed_businesses)}")
    return result


# ============================================================
# TASK 4: Process new businesses
# ============================================================
@task
def process_new(changes: dict, category: str) -> int:
    """Create listings for new businesses."""
    auth = get_auth()
    new_businesses = changes.get("new", [])
    created = 0

    if not new_businesses:
        print("No new businesses to process.")
        return 0

    print(f"Processing {len(new_businesses)} new businesses...")

    for biz in new_businesses[:20]:  # Limit to 20 per run to avoid API limits
        place_id = biz["place_id"]
        try:
            # Enrich
            detail_resp = requests.get(
                "https://maps.googleapis.com/maps/api/place/details/json",
                params={
                    "place_id": place_id,
                    "fields": "name,formatted_address,formatted_phone_number,"
                              "website,geometry,photo,rating,user_ratings_total,"
                              "opening_hours,editorial_summary,business_status",
                    "key": GOOGLE_API_KEY,
                },
                timeout=15,
            )
            r = detail_resp.json().get("result", {})
            if not r:
                continue

            name = r.get("name", biz["name"])
            address = r.get("formatted_address", "")
            phone = r.get("formatted_phone_number", "")
            website = r.get("website", "")
            lat = r.get("geometry", {}).get("location", {}).get("lat")
            lng = r.get("geometry", {}).get("location", {}).get("lng")
            about = r.get("editorial_summary", {}).get("overview", "")

            if not about:
                about = f"{name} is a professional {category} serving Glasgow."

            # Upload up to 4 photos
            photo_refs = [p["photo_reference"] for p in r.get("photos", [])[:4]]
            media_ids = []
            for i, ref in enumerate(photo_refs):
                photo_url = (
                    f"https://maps.googleapis.com/maps/api/place/photo"
                    f"?maxwidth=1200&photoreference={ref}&key={GOOGLE_API_KEY}"
                )
                img = requests.get(photo_url, timeout=30)
                if img.status_code == 200:
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

            # Build hours
            lp_hours = {}
            if r.get("opening_hours", {}).get("periods"):
                day_names = ["Sunday", "Monday", "Tuesday", "Wednesday",
                             "Thursday", "Friday", "Saturday"]
                for period in r["opening_hours"]["periods"]:
                    day_idx = period.get("open", {}).get("day", 0)
                    open_time = period.get("open", {}).get("time", "0900")
                    close_time = period.get("close", {}).get("time", "1700")
                    oh, om = int(open_time[:2]), open_time[2:]
                    ch, cm = int(close_time[:2]), close_time[2:]
                    lp_hours[day_names[day_idx]] = {
                        "open": f"{oh if oh <= 12 else oh-12:02d}:{om}{'am' if oh < 12 else 'pm'}",
                        "close": f"{ch if ch <= 12 else ch-12:02d}:{cm}{'am' if ch < 12 else 'pm'}",
                    }

            hours_text = ""
            if r.get("opening_hours", {}).get("weekday_text"):
                hours_text = "\n".join(r["opening_hours"]["weekday_text"])

            # Create listing
            content = f"<p>{about}</p>"
            if hours_text:
                content += f"\n<h3>Opening Hours</h3>\n<p>{hours_text.replace(chr(10), '<br>')}</p>"

            cat_id = CATEGORY_MAP.get(category, 22)
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

            if resp.status_code in [200, 201]:
                resp_data = resp.json()
                if isinstance(resp_data, list):
                    resp_data = resp_data[0]
                post_id = resp_data.get("id")

                # Inject LP options
                options = {
                    "phone": phone, "website": website,
                    "gAddress": address,
                    "latitude": str(lat or ""), "longitude": str(lng or ""),
                    "tagline_text": f"{category.title()} in Glasgow",
                    "email": "", "claimed_section": "not_claimed",
                    "price_status": "notsay",
                }
                if lp_hours:
                    options["business_hours"] = lp_hours

                requests.post(
                    f"{WP_BASE_URL}/wp-json/glasgow-traders/v1/listing-options/{post_id}",
                    json=options, auth=auth, timeout=15,
                )

                # Set gallery
                if media_ids:
                    gallery_str = ",".join(str(m) for m in media_ids)
                    requests.post(
                        f"{WP_BASE_URL}/wp-json/wp/v2/listing/{post_id}",
                        json={"meta": {"gallery_image_ids": gallery_str}},
                        auth=auth, timeout=15,
                    )

                created += 1
                print(f"  PUBLISHED: {name} (ID {post_id})")
            else:
                print(f"  FAILED: {name} ({resp.status_code})")

        except Exception as e:
            print(f"  ERROR processing {biz['name']}: {e}")

    print(f"\nCreated {created} new listings")
    return created


# ============================================================
# TASK 5: Handle closed businesses
# ============================================================
@task
def handle_closures(changes: dict) -> int:
    """Set closed businesses to draft status on WordPress."""
    auth = get_auth()
    closed = changes.get("closed", [])

    if not closed:
        print("No closures detected.")
        return 0

    print(f"Processing {len(closed)} closures...")
    count = 0
    for biz in closed:
        resp = requests.post(
            f"{WP_BASE_URL}/wp-json/wp/v2/listing/{biz['wp_id']}",
            json={"status": "draft"},
            auth=auth, timeout=15,
        )
        if resp.status_code in [200, 201]:
            print(f"  UNPUBLISHED: {biz['name']} (ID {biz['wp_id']})")
            count += 1
    return count


# ============================================================
# TASK 6: Slack notification
# ============================================================
@task
def notify_changes(changes: dict, new_count: int, closed_count: int):
    """Send Slack alert about changes detected."""
    if not SLACK_WEBHOOK:
        return

    total_new = changes.get("new_count", 0)
    total_closed = changes.get("closed_count", 0)

    if total_new == 0 and total_closed == 0:
        print("No changes to report.")
        return

    level = "info" if total_closed == 0 else "warning"
    icons = {"info": "ℹ️", "warning": "⚠️"}
    colours = {"info": "#06B6D4", "warning": "#F59E0B"}

    fields = [
        {"title": "New Businesses", "value": str(total_new)},
        {"title": "Closures Detected", "value": str(total_closed)},
        {"title": "Published", "value": str(new_count)},
        {"title": "Unpublished", "value": str(closed_count)},
    ]

    payload = {
        "attachments": [{
            "color": colours[level],
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"{icons[level]} *Glasgow Traders AutoPipe*\n*Delta monitor detected changes.*"
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
    except Exception as e:
        print(f"Slack failed: {e}")


# ============================================================
# DAG
# ============================================================
@dag(
    dag_id="glasgow_traders_delta_monitor",
    schedule="0 * * * *",  # Every hour
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["glasgow-traders", "monitoring"],
    description="Hourly change detection: new businesses, closures, updates",
)
def delta_monitor():
    category = "plumber"

    google_state = scan_google(category)
    wp_state = get_wp_state()
    changes = detect_changes(google_state, wp_state)
    new_count = process_new(changes, category)
    closed_count = handle_closures(changes)
    notify_changes(changes, new_count, closed_count)


delta_monitor()
