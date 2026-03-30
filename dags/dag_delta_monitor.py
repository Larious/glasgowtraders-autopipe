from airflow.decorators import dag, task
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime
import os

class NewGlasgowBusinessSensor(BaseSensorOperator):
    def poke(self, context) -> bool:
        import googlemaps, psycopg2
        gmaps = googlemaps.Client(key=os.getenv("GOOGLE_PLACES_API_KEY"))
        resp = gmaps.places_nearby(
            location=(55.8642, -4.2518), radius=12000,
            keyword="new business Glasgow", rank_by="prominence"
        )
        found_ids = {r["place_id"] for r in resp.get("results", [])}
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )
        with conn.cursor() as cur:
            cur.execute("SELECT place_id FROM staging.raw_listings")
            known = {row[0] for row in cur.fetchall()}
        new_ids = found_ids - known
        if new_ids:
            context["ti"].xcom_push(key="new_ids", value=list(new_ids))
            return True
        return False

@dag(schedule="@hourly", start_date=datetime(2025,1,1), catchup=False)
def delta_monitor():
    sense = NewGlasgowBusinessSensor(
        task_id="sense_new_businesses",
        poke_interval=300, timeout=3500, mode="reschedule"
    )

delta_monitor()
```

Save and exit. Commit:
```
git add -A
git commit -m "phase-06: add delta monitor DAG"
git push
