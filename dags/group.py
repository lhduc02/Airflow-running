from airflow.sdk import dag, task, task_group

@dag(
    schedule="0 2 * * *"
)
def group():
    
    @task
    def a():
        print("a")
    
    @task_group(default_args={"retries":2})
    def my_group():
        @task
        def b():
            print("b")
        
        @task(default_args={"retries":3})
        def c():
            print("c")
        
        b() >> c()
    
    a() >> my_group()

group()


