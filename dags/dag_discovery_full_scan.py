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
        print(f"Scraping category: {category}")
        return 0

    @task
    def summarise(counts: list) -> int:
        total = sum(counts)
        print(f"Total records upserted: {total}")
        return total

    counts = scrape_category.expand(category=CATEGORIES)
    summarise(counts)

discovery_full_scan()
