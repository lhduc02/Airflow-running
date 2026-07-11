import csv
from datetime import datetime
from airflow.sdk import dag, task
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


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
    
    @task
    def extract_user(fake_users):
        return {
            "id": fake_users["id"],
            "firstname": fake_users["personalInfo"]["firstName"],
            "lastname": fake_users["personalInfo"]["lastName"],
            "email": fake_users["personalInfo"]["email"]
        }
    
    @task
    def process_user(user_info):
        user_info["loaded_at"] - datetime.now().strftime("%Y-%M-%D %H:%M%S")
        
        with open("/tmp/user_info.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=user_info.keys())
            writer.writeheader()
            writer.writerow(user_info)
        print("Write success")
    
    @task
    def store_user():
        hook = BigQueryHook(gcp_conn_id = "google_cloud_default")
        
        local_file_path = "/tmp/user_info.csv"
        project_id = hook.project_id
        dataset_id = 'raw_data'
        table_id = 'user_info'

        # Sử dụng run_load để đẩy file local
        hook.run_load(
            destination_project_dataset_table=f"{project_id}.{dataset_id}.{table_id}",
            source_format='CSV',
            autodetect=True,
            write_disposition='WRITE_APPEND',
            skip_leading_rows=1,
            src_data=local_file_path
        )
    
    fake_user = is_api_available()
    user_info = extract_user(fake_user)
    process_user(user_info)
    store_user()
    
    # create_table >> process_user(extract_user(is_api_available())) >> store_user()

user_processing()

"""
Kết quả kiểm tra dữ liệu

(engineering_env) lhduc02@MyComputer:~/My_Repo/Airflow-running$ docker exec -it airflow-running-airflow-worker-1 bash
default@a2e5bee81959:/tmp$ cat user_info.csv 
id,email,lastname,firstname
1293234,johndoe@example.com,Doe,John
"""
