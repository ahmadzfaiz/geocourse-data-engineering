from datetime import datetime
from time import sleep
from airflow.sdk import DAG, task

with DAG(
    dag_id="contoh1",
    start_date=datetime(2025, 6, 1),
    schedule="@daily",
    tags=["geocourse"]
) as dag:
    
    @task
    def task_a():
        sleep(5)
        print("Halo saya task A")

    @task
    def task_b():
        sleep(10)
        print("Halo saya task B")

    @task
    def task_c():
        sleep(3)
        print("Halo saya task C")
    
    @task
    def task_d():
        sleep(6)
        print("Halo saya task D")

    a = task_a()
    b = task_b()
    c = task_c()
    d = task_d()
    
    a >> b
    c << b
    a >> d >> c
