from airflow.decorators import dag, task
from datetime import datetime, timedelta

CATEGORIES = [
    "plumber","electrician","restaurant","hair_salon",
    "garage","accountant","solicitor","dentist","gym",
    "supermarket","pharmacy","builder","cleaner",
    "locksmith","painter","roofer","flooring_store",
]

@dag(
    schedule="0 6 * * 0",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    tags=["glasgow-traders", "discovery"],
)
def discovery_full_scan():

    @task
    def scrape_category(category: str) -> int:
        from ingestion.google_places_client import GlasgowPlacesClient
        from staging.change_detector import upsert_to_staging
        import redis, psycopg2, os

        r = redis.Redis.from_url(os.getenv("REDIS_URL"))
        client = GlasgowPlacesClient(os.getenv("GOOGLE_PLACES_API_KEY"), r)
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )
        businesses = client.search_category(category)
        for b in businesses:
            upsert_to_staging(conn, vars(b))
        conn.close()
        return len(businesses)

    @task
    def summarise(counts: list[int]) -> int:
        total = sum(counts)
        print(f"Total records upserted: {total}")
        return total

    counts = scrape_category.expand(category=CATEGORIES)
    summarise(counts)

discovery_full_scan()
```

Then create the GitHub Actions CI:
```
mkdir -p .github/workflows
nano .github/workflows/ci.yml
