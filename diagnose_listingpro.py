import requests
import json
import os
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

load_dotenv()

WP_BASE_URL     = os.getenv("WP_BASE_URL")
WP_USER         = os.getenv("WP_USERNAME")
WP_APP_PASSWORD = os.getenv("WP_APP_PASSWORD")
AUTH = HTTPBasicAuth(WP_USER, WP_APP_PASSWORD)

def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def probe_post_types():
    section("1. REGISTERED POST TYPES")
    resp = requests.get(f"{WP_BASE_URL}/wp-json/wp/v2/types", auth=AUTH, timeout=15)
    types = resp.json()
    for slug, info in types.items():
        rest_base = info.get("rest_base", "???")
        print(f"  {slug:<25} rest_base={rest_base:<20} name={info.get('name')}")

def probe_taxonomies():
    section("2. LISTING TAXONOMIES")
    resp = requests.get(f"{WP_BASE_URL}/wp-json/wp/v2/types/listing", auth=AUTH, timeout=15)
    if resp.status_code != 200:
        print(f"  Could not fetch listing type: {resp.status_code}")
        return
    info = resp.json()
    taxonomies = info.get("taxonomies", [])
    print(f"  Taxonomies: {taxonomies}")
    for tax in taxonomies:
        print(f"\n  --- {tax} ---")
        tax_resp = requests.get(f"{WP_BASE_URL}/wp-json/wp/v2/{tax}", params={"per_page": 10}, auth=AUTH, timeout=15)
        if tax_resp.status_code == 200:
            for term in tax_resp.json():
                print(f"    ID={term['id']:<6} slug={term.get('slug',''):<25} name={term.get('name','')}")

def probe_existing_listings():
    section("3. EXISTING LISTINGS")
    resp = requests.get(f"{WP_BASE_URL}/wp-json/wp/v2/listing", params={"per_page": 5, "status": "publish"}, auth=AUTH, timeout=15)
    if resp.status_code != 200:
        print(f"  Failed: {resp.status_code}")
        return
    for listing in resp.json()[:3]:
        post_id = listing.get("id")
        title = listing.get("title", {})
        if isinstance(title, dict):
            title = title.get("rendered", "???")
        print(f"\n  --- Post ID: {post_id} | Title: {title} ---")
        for key in sorted(listing.keys()):
            if key in ["content", "_links", "guid"]:
                continue
            val = listing[key]
            preview = str(val)[:100] if val else "(empty)"
            print(f"    {key:<30} = {preview}")

print("=" * 60)
print("  LISTINGPRO REST API DIAGNOSTIC")
print("=" * 60)
probe_post_types()
probe_taxonomies()
probe_existing_listings()
print("\n\nDONE.")
