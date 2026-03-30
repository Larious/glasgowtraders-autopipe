from ingestion.google_places_client import GlasgowPlacesClient
from dotenv import load_dotenv
import redis, os

load_dotenv()

r = redis.Redis.from_url(os.getenv("REDIS_URL"))
client = GlasgowPlacesClient(os.getenv("GOOGLE_PLACES_API_KEY"), r)

results = client.search_category("plumber")
print(f"Found {len(results)} Glasgow plumbers")
for b in results[:3]:
    print(b.name, b.postcode, b.status)
