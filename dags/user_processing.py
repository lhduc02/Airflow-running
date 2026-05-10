from datetime import datetime
from airflow.sdk import dag, task
from airflow.sdk.bases.sensor import PokeReturnValue

@dag(
    start_date=datetime(2026, 5, 1),
    schedule="@daily",
    catchup=False
)
def user_processing():

    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue:
        import requests

        response = requests.get(
            "https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json"
        )

        print(response.status_code)

        if response.status_code == 200:
            condition = True
            fake_user = response.json()
        else:
            condition = False
            fake_user = None

        return PokeReturnValue(
            is_done=condition,
            xcom_value=fake_user
        )
    is_api_available()

user_processing()
