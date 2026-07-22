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
import seo_fields
import tile_gen

GEN_TILE_DIR = "assets/featured/generated"


def build_and_upload_tile(enriched, cat_id, location_name):
    """Generate a unique branded featured tile for this business and upload it
    to WP media. Returns the media id, or None. Google photos are ToS-frozen
    (rule 5); these original tiles give each listing a distinct image."""
    import re
    os.makedirs(GEN_TILE_DIR, exist_ok=True)
    slug = re.sub(r"[^a-z0-9]+", "-", enriched["name"].lower()).strip("-")[:48] or "listing"
    town = seo_fields.town_from_address(enriched.get("formatted_address", ""),
                                        location_name)
    path = os.path.join(GEN_TILE_DIR, f"{slug}.jpg")
    tile_gen.make_business_tile(enriched["name"], cat_id, town, path)
    with open(path, "rb") as f:
        data = f.read()
    fname = (slug + ".jpg").encode("ascii", "ignore").decode("ascii")
    r = session.post(f"{WP_BASE_URL}/wp-json/wp/v2/media",
                     headers={"Content-Disposition": f'attachment; filename="{fname}"',
                              "Content-Type": "image/jpeg"},
                     data=data, timeout=60)
    if r.status_code != 201:
        print(f"    tile upload failed: {r.status_code}")
        return None
    mid = r.json()["id"]
    session.post(f"{WP_BASE_URL}/wp-json/wp/v2/media/{mid}",
                 json={"alt_text": f"{enriched['name']} — Glasgow Trader"}, timeout=30)
    time.sleep(0.3)
    return mid

# ── Trade registry ──────────────────────────────────────────
# Per-run parameters (CLAUDE.md): search queries, category mapping, junk
# filter and copy per trade. category_rules are (substring, category_id)
# checked in order against the lowercased business name; first hit wins,
# otherwise default_category.
TRADES = {
    "plumber": {
        "queries": ["plumbers in {loc}, Scotland, UK"],
        "include_kw": ["plumb", "heating", "gas", "pipe", "drain", "boiler"],
        "exclude_kw": ["supplies", "merchant", "store", "wholesale",
                       "showroom", "plumb center", "plumb centre"],
        "category_rules": [],
        "default_category": PLUMBER_CAT_ID,
        "label": "Plumber",
        "service_phrase": "plumbing service",
    },
    "gardener": {
        "queries": ["gardeners in {loc}, Scotland, UK",
                    "landscapers in {loc}, Scotland, UK",
                    "tree surgeons in {loc}, Scotland, UK"],
        "include_kw": ["garden", "landscap", "lawn", "tree", "hedge",
                       "grounds", "horticult", "turf", "arbor"],
        "exclude_kw": ["centre", "center", "nursery", "supplies", "store",
                       "shop", "wholesale", "florist", "depot"],
        "category_rules": [("tree", 166), ("arbor", 166),
                           ("landscap", 161)],
        "default_category": 159,
        "label": "Gardener",
        "service_phrase": "gardening and landscaping service",
    },
    "roofer": {
        "queries": ["roofers in {loc}, Scotland, UK"],
        "include_kw": ["roof", "slater", "slating", "guttering"],
        "exclude_kw": ["supplies", "merchant", "store", "wholesale", "depot"],
        "category_rules": [],
        "default_category": 158,
        "label": "Roofer",
        "service_phrase": "roofing service",
    },
    "joiner": {
        "queries": ["joiners in {loc}, Scotland, UK",
                    "carpenters in {loc}, Scotland, UK"],
        "include_kw": ["joiner", "joinery", "carpent", "woodwork"],
        "exclude_kw": ["supplies", "merchant", "timber merchant", "store"],
        "category_rules": [],
        "default_category": 162,
        "label": "Joiner",
        "service_phrase": "joinery and carpentry service",
    },
    "painter": {
        "queries": ["painters and decorators in {loc}, Scotland, UK"],
        "include_kw": ["paint", "decorat", "decor "],
        "exclude_kw": ["supplies", "merchant", "store", "art gallery", "car "],
        "category_rules": [],
        "default_category": 160,
        "label": "Painter & Decorator",
        "service_phrase": "painting and decorating service",
    },
    "plasterer": {
        "queries": ["plasterers in {loc}, Scotland, UK"],
        "include_kw": ["plaster", "render", "screed", "harling"],
        "exclude_kw": ["supplies", "merchant", "store", "wholesale"],
        "category_rules": [],
        "default_category": 163,
        "label": "Plasterer",
        "service_phrase": "plastering service",
    },
    "kitchen": {
        "queries": ["kitchen fitters in {loc}, Scotland, UK"],
        "include_kw": ["kitchen", "worktop"],
        "exclude_kw": ["supplies", "showroom", "store", "restaurant",
                       "takeaway", "cafe", "wholesale"],
        "category_rules": [],
        "default_category": 293,
        "label": "Kitchen Fitter",
        "service_phrase": "kitchen fitting service",
    },
    "bathroom": {
        "queries": ["bathroom fitters in {loc}, Scotland, UK"],
        "include_kw": ["bathroom", "wet room", "wetroom", "shower"],
        "exclude_kw": ["supplies", "showroom", "store", "wholesale", "depot"],
        "category_rules": [],
        "default_category": 169,
        "label": "Bathroom Fitter",
        "service_phrase": "bathroom fitting service",
    },
    "tiler": {
        "queries": ["tilers in {loc}, Scotland, UK"],
        "include_kw": ["tiler", "tiling", "tile"],
        "exclude_kw": ["supplies", "showroom", "store", "wholesale", "depot"],
        "category_rules": [],
        "default_category": 170,
        "label": "Tiler",
        "service_phrase": "tiling service",
    },
    "handyman": {
        "queries": ["handyman in {loc}, Scotland, UK",
                    "property maintenance in {loc}, Scotland, UK"],
        "include_kw": ["handyman", "handy man", "property maintenance",
                       "home repair", "odd job", "maintenance"],
        "exclude_kw": ["supplies", "merchant", "store", "letting", "estate agent"],
        "category_rules": [],
        "default_category": 167,
        "label": "Handyman",
        "service_phrase": "handyman and property maintenance service",
    },
    "heating": {
        "queries": ["heating engineers in {loc}, Scotland, UK",
                    "gas boiler service in {loc}, Scotland, UK"],
        "include_kw": ["heating", "boiler", "gas", "central heating", "radiator"],
        "exclude_kw": ["supplies", "merchant", "store", "wholesale"],
        "category_rules": [("boiler", 172), ("central heating", 171)],
        "default_category": 291,
        "label": "Heating Engineer",
        "service_phrase": "heating and boiler service",
    },
    "locksmith": {
        "queries": ["locksmiths in {loc}, Scotland, UK"],
        "include_kw": ["locksmith", "lock", "key"],
        "exclude_kw": ["supplies", "store", "car key programming supplies"],
        "category_rules": [],
        "default_category": 168,
        "label": "Locksmith",
        "service_phrase": "locksmith service",
    },
    "window": {
        "queries": ["window fitters in {loc}, Scotland, UK",
                    "double glazing in {loc}, Scotland, UK"],
        "include_kw": ["window", "glazing", "glazier"],
        "exclude_kw": ["supplies", "merchant", "store", "cleaning", "cleaner",
                       "tint"],
        "category_rules": [],
        "default_category": 297,
        "label": "Window Fitter",
        "service_phrase": "window and glazing service",
    },
    "fencing": {
        "queries": ["fencing contractors in {loc}, Scotland, UK"],
        "include_kw": ["fenc", "gate", "decking", "railing"],
        "exclude_kw": ["supplies", "merchant", "store", "wholesale", "depot"],
        "category_rules": [],
        "default_category": 165,
        "label": "Fencing & Gates",
        "service_phrase": "fencing and gates service",
    },
    # ── Phase 2 trades ──────────────────────────────────────
    "bricklayer": {
        "queries": ["bricklayers in {loc}, Scotland, UK"],
        "include_kw": ["bricklay", "brickwork", "brick "],
        "exclude_kw": ["supplies", "merchant", "store", "wholesale"],
        "category_rules": [], "default_category": 280,
        "label": "Bricklayer", "service_phrase": "bricklaying service",
    },
    "stonemason": {
        "queries": ["stonemasons in {loc}, Scotland, UK"],
        "include_kw": ["stonemason", "masonry", "stone "],
        "exclude_kw": ["supplies", "merchant", "store", "memorial park"],
        "category_rules": [], "default_category": 306,
        "label": "Stonemason", "service_phrase": "stonemasonry service",
    },
    "construction": {
        "queries": ["construction contractors in {loc}, Scotland, UK"],
        "include_kw": ["construction", "contractor"],
        "exclude_kw": ["supplies", "merchant", "recruitment", "training"],
        "category_rules": [], "default_category": 298,
        "label": "Construction Contractor", "service_phrase": "construction service",
    },
    "extension": {
        "queries": ["house extension builders in {loc}, Scotland, UK"],
        "include_kw": ["extension", "build", "renovat"],
        "exclude_kw": ["supplies", "merchant", "hair", "eyelash", "salon"],
        "category_rules": [], "default_category": 286,
        "label": "Extension Builder", "service_phrase": "home extension service",
    },
    "demolition": {
        "queries": ["demolition contractors in {loc}, Scotland, UK"],
        "include_kw": ["demolition", "demoli", "site clearance"],
        "exclude_kw": ["supplies", "merchant", "store"],
        "category_rules": [], "default_category": 285,
        "label": "Demolition Contractor", "service_phrase": "demolition service",
    },
    "scaffolder": {
        "queries": ["scaffolding in {loc}, Scotland, UK"],
        "include_kw": ["scaffold"],
        "exclude_kw": ["supplies", "hire shop", "training"],
        "category_rules": [], "default_category": 300,
        "label": "Scaffolder", "service_phrase": "scaffolding service",
    },
    "garage_builder": {
        "queries": ["garage and shed builders in {loc}, Scotland, UK"],
        "include_kw": ["garage", "shed", "outbuilding", "garden room"],
        "exclude_kw": ["supplies", "mot", "car repair", "servicing", "tyre"],
        "category_rules": [], "default_category": 288,
        "label": "Garage & Shed Builder", "service_phrase": "garage and shed building service",
    },
    "loft": {
        "queries": ["loft conversions in {loc}, Scotland, UK"],
        "include_kw": ["loft", "attic", "dormer"],
        "exclude_kw": ["supplies", "storage", "self storage"],
        "category_rules": [], "default_category": 294,
        "label": "Loft Conversion Specialist", "service_phrase": "loft conversion service",
    },
    "guttering": {
        "queries": ["guttering installers in {loc}, Scotland, UK"],
        "include_kw": ["gutter", "fascia", "soffit", "rainwater"],
        "exclude_kw": ["supplies", "merchant", "store"],
        "category_rules": [], "default_category": 290,
        "label": "Guttering Installer", "service_phrase": "guttering and fascia service",
    },
    "damp": {
        "queries": ["damp proofing specialists in {loc}, Scotland, UK"],
        "include_kw": ["damp", "rot", "woodworm", "timber treatment", "preservation"],
        "exclude_kw": ["supplies", "merchant", "store"],
        "category_rules": [], "default_category": 284,
        "label": "Damp Proofing Specialist", "service_phrase": "damp proofing service",
    },
    "insulation": {
        "queries": ["insulation installers in {loc}, Scotland, UK"],
        "include_kw": ["insulation", "cavity wall", "loft insulation"],
        "exclude_kw": ["supplies", "merchant", "store"],
        "category_rules": [], "default_category": 292,
        "label": "Insulation Installer", "service_phrase": "insulation service",
    },
    "conservatory": {
        "queries": ["conservatory installers in {loc}, Scotland, UK"],
        "include_kw": ["conservator", "orangery", "sunroom", "garden room"],
        "exclude_kw": ["supplies", "merchant", "store"],
        "category_rules": [], "default_category": 283,
        "label": "Conservatory Installer", "service_phrase": "conservatory installation service",
    },
    "driveway": {
        "queries": ["driveway contractors in {loc}, Scotland, UK"],
        "include_kw": ["driveway", "monoblock", "paving", "patio", "tarmac", "block paving"],
        "exclude_kw": ["supplies", "merchant", "store", "quarry"],
        "category_rules": [], "default_category": 164,
        "label": "Driveways & Patios", "service_phrase": "driveway and patio service",
    },
    "flooring": {
        "queries": ["flooring fitters in {loc}, Scotland, UK"],
        "include_kw": ["floor", "laminate", "hardwood", "vinyl", "lvt"],
        "exclude_kw": ["supplies", "showroom", "store", "wholesale", "carpet warehouse"],
        "category_rules": [], "default_category": 287,
        "label": "Flooring Specialist", "service_phrase": "flooring service",
    },
    "carpet": {
        "queries": ["carpet fitters in {loc}, Scotland, UK"],
        "include_kw": ["carpet", "rug fitting"],
        "exclude_kw": ["supplies", "showroom", "warehouse", "store", "cleaning", "cleaner"],
        "category_rules": [], "default_category": 281,
        "label": "Carpet Fitter", "service_phrase": "carpet fitting service",
    },
    "wallpaper": {
        "queries": ["wallpaper hanging in {loc}, Scotland, UK"],
        "include_kw": ["wallpaper", "wall covering", "paperhang"],
        "exclude_kw": ["supplies", "showroom", "store", "wholesale"],
        "category_rules": [], "default_category": 313,
        "label": "Wallpaper & Wall Coverings", "service_phrase": "wallpapering service",
    },
    "hvac": {
        "queries": ["HVAC engineers in {loc}, Scotland, UK"],
        "include_kw": ["hvac", "ventilation", "air handling", "ductwork"],
        "exclude_kw": ["supplies", "merchant", "store"],
        "category_rules": [], "default_category": 309,
        "label": "HVAC Technician", "service_phrase": "HVAC and ventilation service",
    },
    "aircon": {
        "queries": ["air conditioning installers in {loc}, Scotland, UK"],
        "include_kw": ["air conditioning", "aircon", "air con", "refrigeration"],
        "exclude_kw": ["supplies", "merchant", "store", "hire"],
        "category_rules": [], "default_category": 307,
        "label": "Air Conditioning Mechanic", "service_phrase": "air conditioning service",
    },
    "solar": {
        "queries": ["solar panel installers in {loc}, Scotland, UK"],
        "include_kw": ["solar", "photovoltaic", "pv ", "renewable"],
        "exclude_kw": ["supplies", "merchant", "store"],
        "category_rules": [], "default_category": 305,
        "label": "Solar Panel Installer", "service_phrase": "solar panel installation service",
    },
    "hometech": {
        "queries": ["smart home installers in {loc}, Scotland, UK"],
        "include_kw": ["smart home", "home automation", "av install", "home cinema", "audio visual"],
        "exclude_kw": ["supplies", "store", "phone repair"],
        "category_rules": [], "default_category": 301,
        "label": "Home Technology", "service_phrase": "smart home installation service",
    },
    "cctv": {
        "queries": ["CCTV installers in {loc}, Scotland, UK"],
        "include_kw": ["cctv", "security system", "alarm", "access control", "surveillance"],
        "exclude_kw": ["supplies", "store", "guard", "manned"],
        "category_rules": [], "default_category": 304,
        "label": "CCTV & Video Security", "service_phrase": "CCTV and security system service",
    },
    "chimney": {
        "queries": ["chimney sweeps in {loc}, Scotland, UK"],
        "include_kw": ["chimney", "sweep", "fireplace", "stove", "flue"],
        "exclude_kw": ["supplies", "showroom", "store"],
        "category_rules": [], "default_category": 282,
        "label": "Chimney & Fireplace Specialist", "service_phrase": "chimney and fireplace service",
    },
    "removals": {
        "queries": ["removal companies in {loc}, Scotland, UK"],
        "include_kw": ["removal", "moving", "man and van", "house clearance"],
        "exclude_kw": ["supplies", "store", "storage only"],
        "category_rules": [], "default_category": 308,
        "label": "Removals", "service_phrase": "removals service",
    },
    "auto": {
        "queries": ["car garages in {loc}, Scotland, UK"],
        "include_kw": ["garage", "mot", "auto", "car repair", "servicing", "tyre", "bodyshop"],
        "exclude_kw": ["supplies", "parts store", "dealership", "showroom", "car wash"],
        "category_rules": [], "default_category": 311,
        "label": "Auto Workshop", "service_phrase": "vehicle servicing and repair",
    },
}

# Core towns for Phase 1 (Glasgow + substantive towns) — captures ~all of a
# trade's real businesses; suburb location-pages fill via cross-town attaches.
CORE_TOWNS = ["Glasgow", "Paisley", "Airdrie", "Coatbridge", "Bellshill",
              "Hamilton", "East Kilbride", "Clydebank", "Cumbernauld",
              "Dumbarton", "Ayr", "Greenock", "Falkirk", "Stirling",
              "Bathgate", "Carluke"]


def is_uk_address(addr):
    """Google formats UK addresses ending in ', UK'. Ambiguous town names
    (Houston, Hamilton, Airdrie, Alexandria, Irvine...) match large non-UK
    places, so this guards against publishing foreign businesses."""
    a = addr or ""
    return ("UK" in a) or ("United Kingdom" in a)


def categorize(name, trade_cfg):
    """listing-category id for a business name under this trade's rules."""
    low = (name or "").lower()
    for substring, cat_id in trade_cfg["category_rules"]:
        if substring in low:
            return cat_id
    return trade_cfg["default_category"]


def is_valid_trade_business(name, trade_cfg):
    """Keep tradespeople; drop merchants/shops and off-trade results."""
    low = (name or "").lower()
    for kw in trade_cfg["exclude_kw"]:
        if kw in low:
            return False, f"merchant/shop keyword: {kw}"
    if not any(kw in low for kw in trade_cfg["include_kw"]):
        return False, "no trade keyword in name"
    return True, "OK"

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


# ── Step 1: Search Google Places for the trade ──────────────
def search_trade(location_name, trade_cfg):
    """Text Search across the trade's queries; merge by place_id and apply
    the junk filter. Returns [{place_id, name}]."""
    print(f"\n[SEARCH] Finding {trade_cfg['label'].lower()}s in {location_name}...")
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    seen, merged, filtered = set(), [], 0

    for query_tpl in trade_cfg["queries"]:
        params = {"query": query_tpl.format(loc=location_name),
                  "key": GOOGLE_API_KEY}
        while True:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            for r in data.get("results", []):
                pid = r["place_id"]
                if pid in seen:
                    continue
                seen.add(pid)
                name = r.get("name", "Unknown")
                ok, reason = is_valid_trade_business(name, trade_cfg)
                if not ok:
                    filtered += 1
                    continue
                merged.append({"place_id": pid, "name": name})

            next_token = data.get("next_page_token")
            if not next_token:
                break
            # Google requires a short delay before using next_page_token
            time.sleep(2)
            params = {"pagetoken": next_token, "key": GOOGLE_API_KEY}

    print(f"  Found {len(merged)} candidates "
          f"({filtered} filtered as merchants/off-trade)")
    return merged


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
def build_description(enriched, location_name, trade_cfg):
    """Generate a professional business description."""
    name = enriched["name"]
    address = enriched["formatted_address"]
    rating = enriched.get("rating")
    reviews = enriched.get("review_count")
    website = enriched.get("website", "")
    about = enriched.get("about", "")
    service = trade_cfg["service_phrase"]

    # Start with Google's editorial summary if available
    if about:
        desc = f"{about}\n\n"
    else:
        desc = f"{name} is a professional {service} serving {location_name} and the surrounding Glasgow area. "

    # Add rating info
    if rating and reviews:
        desc += f"They have an excellent {rating}-star rating from {reviews} customer reviews on Google. "

    # Add website mention
    if website:
        desc += f"Visit their website for more information about their services."

    # Add opening hours section
    if enriched.get("hours_text"):
        desc += f"\n\n<strong>Opening Hours</strong>\n{enriched['hours_text']}"

    return desc


# Human-facing label per category id (taglines)
CAT_LABELS = {22: "Plumber", 159: "Gardener", 161: "Landscaper",
              166: "Tree Surgeon", 26: "Electrician", 82: "Builder",
              158: "Roofer", 162: "Joiner", 160: "Painter & Decorator",
              163: "Plasterer", 293: "Kitchen Fitter", 169: "Bathroom Fitter",
              170: "Tiler", 167: "Handyman", 291: "Heating Engineer",
              171: "Central Heating", 172: "Gas Boiler Service",
              168: "Locksmith", 297: "Window Fitter", 165: "Fencing & Gates",
              280: "Bricklayer", 306: "Stonemason",
              298: "Construction Contractor", 286: "Extension Builder",
              285: "Demolition Contractor", 300: "Scaffolder",
              288: "Garage & Shed Builder", 294: "Loft Conversion Specialist",
              290: "Guttering Installer", 284: "Damp Proofing Specialist",
              292: "Insulation Installer", 283: "Conservatory Installer",
              164: "Driveways & Patios", 287: "Flooring Specialist",
              281: "Carpet Fitter", 313: "Wallpaper & Wall Coverings",
              309: "HVAC Technician", 307: "Air Conditioning Mechanic",
              305: "Solar Panel Installer", 301: "Home Technology",
              304: "CCTV & Video Security",
              282: "Chimney & Fireplace Specialist",
              308: "Removals", 311: "Auto Workshop"}


# ── Step 5: Create WordPress listing ────────────────────────
def create_listing(enriched, location_id, featured_media_id, location_name,
                   trade_cfg):
    """Create the listing post in WordPress."""
    description = build_description(enriched, location_name, trade_cfg)
    category_id = categorize(enriched["name"], trade_cfg)

    payload = {
        "title": enriched["name"],
        "content": description,
        "status": "publish",
        # taxonomy rest_base is "listing-category" (hyphen); the underscore
        # form is silently ignored and leaves the listing uncategorized.
        "listing-category": [category_id],
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
def inject_listingpro_options(post_id, enriched, gallery_ids, location_name,
                              trade_cfg):
    """Write all data into lp_listingpro_options serialized array."""
    label = CAT_LABELS.get(categorize(enriched["name"], trade_cfg),
                           trade_cfg["label"])
    options_data = {
        "phone":            enriched.get("phone", ""),
        "website":          enriched.get("website", ""),
        "gAddress":         enriched.get("formatted_address", ""),
        "latitude":         str(enriched.get("lat", "")),
        "longitude":        str(enriched.get("lng", "")),
        "tagline_text":     f"{label} in {location_name}",
        "email":            "",
        "claimed_section":  "not_claimed",
        "price_status":     "notsay",
        # Dedup key (rule 3); bridge v2.2 mirrors this to standalone meta.
        "google_place_id":  enriched.get("place_id", ""),
    }

    # Yoast SEO (bridge v2.3 writes these to protected _yoast_wpseo_* meta).
    label = CAT_LABELS.get(categorize(enriched["name"], trade_cfg),
                           trade_cfg["label"])
    town = seo_fields.town_from_address(enriched.get("formatted_address", ""),
                                        location_name)
    options_data["yoast_focuskw"] = seo_fields.focus_keyphrase(enriched["name"])
    options_data["yoast_metadesc"] = seo_fields.meta_description(
        enriched["name"], label, town,
        rating=enriched.get("rating") or 0,
        reviews=enriched.get("review_count") or 0)

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
    parser = argparse.ArgumentParser(description="Batch snipe a trade by location")
    parser.add_argument("--location", required=True, help="Location name (e.g. Airdrie)")
    parser.add_argument("--trade", default="plumber", choices=sorted(TRADES),
                        help="Trade to discover (default: plumber)")
    parser.add_argument("--placeholder-media", type=int, default=0,
                        help="WP media ID of a licensed stock image to set as "
                             "featured image (owners replace via claim flow)")
    parser.add_argument("--dry-run", action="store_true", help="Search only, don't publish")
    args = parser.parse_args()

    location_name = args.location
    trade_cfg = TRADES[args.trade]

    print("=" * 60)
    print(f"  Glasgow Traders — Batch Sniper")
    print(f"  Location: {location_name}")
    print(f"  Trade: {trade_cfg['label']}")
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


    # Search for the trade in this location
    results = search_trade(location_name, trade_cfg)

    if not results:
        sys.exit(f"No {trade_cfg['label'].lower()}s found in {location_name}")

    if args.dry_run:
        print(f"\n[DRY RUN] Would process {len(results)} candidates:")
        for i, r in enumerate(results, 1):
            cat = CAT_LABELS.get(categorize(r["name"], trade_cfg),
                                 trade_cfg["label"])
            held_by = ledger_utils.existing_post_for(by_place, r["place_id"])
            if held_by:
                note = f"(SKIP - ledger post {held_by}; would attach location)"
            elif r["name"].lower().strip() in existing:
                note = "(SKIP - title already exists)"
            else:
                note = ""
            print(f"  {i}. [{cat}] {r['name']} {note}")
        print(f"\nRun without --dry-run to publish these listings.")
        return

    # Process candidates (≤25 creates per run; ledger makes re-runs resume
    # where the last batch stopped)
    MAX_CREATES = 25
    created = 0
    skipped = 0
    failed = 0

    for i, result in enumerate(results, 1):
        if created >= MAX_CREATES:
            print(f"\n  Batch cap reached ({MAX_CREATES} creates). "
                  f"Re-run (new token) to continue — the ledger skips "
                  f"everything published so far.")
            break
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

        # Hard UK guard: ambiguous town names (Houston, Hamilton, Airdrie,
        # Alexandria, Irvine, Livingston...) match large non-UK places, so
        # never publish a business whose address isn't in the UK.
        if not is_uk_address(enriched.get("formatted_address", "")):
            print(f"  SKIP: non-UK address — "
                  f"{enriched.get('formatted_address', '')[:50]}")
            skipped += 1
            continue

        print(f"    Address: {enriched['formatted_address']}")
        print(f"    Phone:   {enriched['phone'] or 'N/A'}")
        print(f"    Website: {enriched['website'] or 'N/A'}")
        print(f"    Rating:  {enriched['rating']} ({enriched['review_count']} reviews)")
        print(f"    Hours:   {len(enriched.get('lp_hours', {}))} days")
        print(f"    Photos:  {len(enriched.get('photo_refs', []))}")

        # Featured image: unique branded tile per business (Google photos are
        # ToS-frozen); owners replace it via the claim flow.
        cat_id = categorize(enriched["name"], trade_cfg)
        if args.placeholder_media:
            featured_id = args.placeholder_media
        else:
            print(f"  Generating featured tile...")
            featured_id = build_and_upload_tile(enriched, cat_id, location_name)
        gallery_ids = []

        # Create listing
        print(f"  Creating listing...")
        post_id, link = create_listing(enriched, location_id, featured_id,
                                       location_name, trade_cfg)
        if not post_id:
            failed += 1
            continue
        print(f"    Post ID: {post_id}")
        print(f"    URL: {link}")

        # Inject ListingPro options
        print(f"  Injecting ListingPro sidebar data...")
        ok = inject_listingpro_options(post_id, enriched, gallery_ids,
                                       location_name, trade_cfg)
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
