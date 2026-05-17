from airflow.sdk import dag, task

@dag
def branch():
    
    @task
    def a():
        return 1
    
    # Khi return thì giá trị trả về phải là tên của task tiếp theo muốn kiểm tra điều kiện
    # Ví dụ bên dưới sẽ kiểm tra điều kiện ở 2 task là equal_1 và different_1
    @task.branch
    def b(val: int):
        if val == 1:
            return "equal_1"
        else:
            return "different_1"
    
    @task
    def equal_1():
        print("Equal to 1")
    
    @task
    def different_1():
        print("Different than 1")
    
    val = a()
    b(val) >> [equal_1(), different_1()]

branch()

