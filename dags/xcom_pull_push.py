from airflow.sdk import dag, task, get_current_context
from typing import Dict, Any

@dag(
    schedule="0 3 * * *"
)
def xcom_pull_push():
    
    @task
    def t1() -> Dict[str, Any]:
        context = get_current_context()
        val = 42
        context['ti'].xcom_push(key="my_key", value=val)
    
    @task
    def t2():
        context = get_current_context()
        val = context['ti'].xcom_pull(task_ids='t1', key="my_key")
        print(val+10)
    
    t1() >> t2()
    
xcom_pull_push()
