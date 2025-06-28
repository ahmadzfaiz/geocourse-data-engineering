import random
from datetime import datetime
from time import sleep
from airflow.sdk import DAG, task, task_group

with DAG(
    dag_id="contoh5",
    start_date=datetime(2025, 6, 1),
    schedule="@daily",
    tags=["geocourse"]
) as dag:
    
    @task
    def access_data_source():
        print("Mendapatkan daftar sistem kenegaraan di negara ASEAN")
        sleep(12)
        return [
            (1, "Indonesia", "Republik"),
            (2, "Malaysia", "Kerajaan"),
            (3, "Filipina", "Republik"),
            (4, "Brunei Darussalam", "Kerajaan"),
            (5, "Singapura", "Republik"),
            (6, "Thailand", "Kerajaan"),
            (7, "Vietnam", "Republik"),
            (8, "Kamboja", "Kerajaan"),
            (9, "Laos", "Republik"),
            (10, "Myanmar", "Republik")
        ]
    
    @task
    def task_process_country(data):
        country_id = data[0]
        country_name = data[1]
        country_type = data[2]
        print(f"Memproses negara ke-{country_id}: {country_name} dengan tipe negara {country_type}")
        sleep(6)
        return {
            "name": country_name,
            "type": country_type
        }
    
    @task
    def task_process_republik(data):
        sleep(4)
        return [
            item["name"] 
            for item in data 
            if item["type"] == "Republik"
        ]
    
    @task
    def task_process_kerajaan(data):
        sleep(4)
        return [
            item["name"] 
            for item in data 
            if item["type"] == "Kerajaan"
        ]
    
    @task
    def calculating_succession_line():
        sleep(7)
        return [
            "Pangeran A",
            "Pangeran B",
            "Pangeran C",
            "Pangeran D"
        ]
    
    @task
    def calculate_dynasty():
        sleep(5)
        return [
            "Dinasti X",
            "Dinasti Y",
            "Dinasti Z"
        ]
    
    @task 
    def calculating_president_candidate():
        sleep(3)
        return [
            "Mr. Abcde",
            "Mrs. Qwerty",
            "Mr. Asdfgh",
            "Ms. Cvbnm",
            "Mr. Poiuyt",
        ]
    
    @task
    def future_kingdom_gov(country, kings, dynasties):
        sleep(10)
        king = random.choice(kings)
        dynasty = random.choice(dynasties)
        print(f"Masa depan kerajaan {country} dipimpin oleh Raja {king} dari dinasti {dynasty}.")

    @task
    def future_republic_gov(country, presidents):
        sleep(10)
        president = random.choice(presidents)
        print(f"Masa depan republik {country} dipimpin oleh Presiden {president}.")

    @task_group
    def group_republik():
        process_republik = task_process_republik(data=process_country)
        presidents = calculating_president_candidate()
        future_republic_gov.partial(presidents=presidents).expand(country=process_republik)

    @task_group
    def group_kerajaan():
        process_kerajaan = task_process_kerajaan(data=process_country)
        succession_line = calculating_succession_line()
        dynasties = calculate_dynasty()
        future_kingdom_gov.partial(kings=succession_line, dynasties=dynasties).expand(country=process_kerajaan)


    data_source = access_data_source()
    process_country = task_process_country.expand(data=data_source)
    group_republik()
    group_kerajaan()
