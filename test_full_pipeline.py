from ingestion.google_places_client import GlasgowPlacesClient
from staging.change_detector import upsert_to_staging
from publisher.wordpress_client import WordPressPublisher
from dotenv import load_dotenv
import redis, psycopg2, os, json

load_dotenv()

# Step 1: Discover businesses
print("=== STEP 1: Discovering Glasgow businesses ===")
r = redis.Redis.from_url(os.getenv("REDIS_URL"))
client = GlasgowPlacesClient(os.getenv("GOOGLE_PLACES_API_KEY"), r)
businesses = client.search_category("plumber")
print(f"Found {len(businesses)} plumbers")

# Step 2: Upsert to staging database
print("\n=== STEP 2: Upserting to PostgreSQL staging ===")
conn = psycopg2.connect(
    host="localhost",
    port=5433,
    dbname="glasgow_traders",
    user="pipeline",
    password="Glasgow190",
)

for b in businesses:
    data = {
        "place_id": b.place_id,
        "name": b.name,
        "address": b.address,
        "phone": b.phone,
        "website": b.website,
        "postcode": b.postcode,
        "lat": b.lat,
        "lng": b.lng,
        "categories": json.dumps(b.categories),
        "hours": json.dumps(b.hours),
        "rating": b.rating,
        "review_count": b.review_count,
        "status": b.status,
    }
    upsert_to_staging(conn, data)
print(f"Upserted {len(businesses)} records to staging")

# Verify staging
with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM staging.raw_listings")
    count = cur.fetchone()[0]
    print(f"Total records in staging: {count}")

# Step 3: Publish top 5 to WordPress
print("\n=== STEP 3: Publishing to WordPress ===")
pub = WordPressPublisher(
    os.getenv("WP_BASE_URL"),
    os.getenv("WP_USERNAME"),
    os.getenv("WP_APP_PASSWORD"),
)

published = 0
for b in businesses[:5]:
    try:
        listing = {
            "place_id": b.place_id,
            "name": b.name,
            "address": b.address,
            "postcode": b.postcode,
            "latitude": b.lat,
            "longitude": b.lng,
            "phone_e164": b.phone or "",
            "website": b.website or "",
            "health_score": 80,
        }
        post_id = pub.publish(listing)
        print(f"  Published: {b.name} -> Post ID: {post_id}")
        published += 1
    except Exception as e:
        print(f"  Failed: {b.name} -> {e}")

conn.close()
print("\n=== PIPELINE COMPLETE ===")
print(f"Discovered: {len(businesses)}")
print(f"Staged: {len(businesses)}")
print(f"Published: {published}")
