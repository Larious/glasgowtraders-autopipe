from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule="@hourly", start_date=datetime(2025,1,1), catchup=False)
def delta_monitor():

    @task
    def check_new_businesses():
        print("Checking for new Glasgow businesses")
        return 0

    check_new_businesses()

delta_monitor()
