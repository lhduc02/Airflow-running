from airflow.sdk import dag, task, get_current_context
from typing import Dict, Any

@dag(
    schedule="0 3 * * *"
)
def xcom():
    
    @task
    def t1() -> Dict[str, Any]:
        my_val = 42
        my_sentence = "Hello, World!"
        return {
            "my_val": my_val,
            "my_sentence": my_sentence
        }
    
    @task
    def t2(data: Dict[str, Any]):
        print(data['my_val'])
        print(data['my_sentence'])
    
    data = t1()
    t2(data)

xcom()
