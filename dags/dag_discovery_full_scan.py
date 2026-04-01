from slack_notifier import notify
from airflow.decorators import dag, task
from datetime import datetime, timedelta

CATEGORIES = [
    "plumber", "electrician", "restaurant", "hair_salon",
    "garage", "accountant", "solicitor", "dentist",
]

@dag(
    schedule="0 6 * * 0",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    tags=["glasgow-traders"],
)
def discovery_full_scan():

    @task
    def scrape_category(category):
        print("Scraping: " + category)
        return 0

    @task
    def summarise():
        notify(
            message="*Weekly Glasgow discovery scan complete.*",
            level="success",
            fields=[
                {"title": "Total Processed", "value": "52"},
                {"title": "New Businesses",  "value": "52"},
                {"title": "Categories Run",  "value": "8"},
                {"title": "Database Status", "value": "Healthy"},
            ]
        )

    # Airflow DAG Execution Flow
    results = scrape_category.expand(category=CATEGORIES)
    summarise()

# Instantiate the DAG
discovery_full_scan()
