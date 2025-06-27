from datetime import datetime
from time import sleep
from airflow.sdk import DAG, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.smtp.operators.smtp import EmailOperator
import pandas as pd

with DAG(
    dag_id="contoh4",
    start_date=datetime(2025, 6, 1),
    schedule="@daily",
    tags=["geocourse"]
) as dag:
    
    CSV_FILE_FULL_PATH = "/opt/airflow/data/output/country.csv"

    @task
    def process_country_to_csv(sql_result: dict):
        data = sql_result['data']
        columns = sql_result.get('columns', [])
        df = pd.DataFrame(data, columns=columns)
        print(f"DataFrame berhasil dibuat dengan {len(df)} baris dan {len(df.columns)} kolom.")
        print(f"{df.head().to_string()}")
        sleep(20)
        df.to_csv(CSV_FILE_FULL_PATH)
    
    country = SQLExecuteQueryOperator(
        task_id="query_country",
        conn_id="natural_earth_db",
        sql="""
            SELECT 
                NAME as name,
                SUBREGION as subregion,
                POP_EST as population,
                POP_YEAR as population_year
            FROM country;
        """,
        handler=lambda cursor: {
            'data': cursor.fetchall(),
            'columns': [col[0] for col in cursor.description] if cursor.description else []
        },
    )

    country_csv_file = FileSensor(
        task_id="country_csv_file_waiting", 
        fs_conn_id="natural_earth_fs",
        filepath="country.csv",
        poke_interval=5,
        timeout=25,
        mode="reschedule",
        soft_fail=True
    )

    send_email_with_csv = EmailOperator(
        task_id="send_country_data_email",
        conn_id="natural_earth_email",
        to="test.email513@gmail.com",
        from_email="ertim.geoportal@gmail.com",
        subject="Airflow: Daily Country Data CSV",
        html_content="""
            <h3>Hello Team,</h3>
            <p>The daily country data CSV file has been generated and is attached.</p>
            <p>Pipeline: <b>{{ dag.dag_id }}</b><br>
            Run ID: <b>{{ run_id }}</b></p>
            <p>Best regards,<br>
            Faiz Airflow Automation</p>
        """,
        files=[CSV_FILE_FULL_PATH],
    )

    process_country = process_country_to_csv(sql_result=country.output)
    country_csv_file >> send_email_with_csv
