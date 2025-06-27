import random
from datetime import datetime
from time import sleep
from airflow.sdk import DAG, task
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id="contoh2",
    start_date=datetime(2025, 6, 1),
    schedule="@daily",
    tags=["geocourse"]
) as dag:
    
    @task.branch
    def task_choose_path():
        sleep(5)
        choosen_path = random.randint(1,3)
        
        if choosen_path == 1:
            return "red_path"
        elif choosen_path == 2:
            return "blue_path"
        else:
            return "yellow_path"

    @task
    def red_path():
        sleep(10)
        print("Halo saya jalur merah")

    @task
    def blue_path():
        sleep(3)
        print("Halo saya jalur biru")

    @task
    def yellow_path():
        sleep(6)
        print("Halo saya jalur kuning")

    @task(trigger_rule=TriggerRule.ONE_SUCCESS)
    def data_cleaning():
        sleep(12)
        print("Data sedang dibersihkan")


    choose_path = task_choose_path()
    red = red_path()
    blue = blue_path()
    yellow = yellow_path()
    cleaning = data_cleaning()

    choose_path >> [red, blue, yellow]
    [red, yellow] >> cleaning