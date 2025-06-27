import uuid
from datetime import datetime
from time import sleep
from airflow.sdk import DAG, task, Variable

with DAG(
    dag_id="contoh3",
    start_date=datetime(2025, 6, 1),
    schedule="@daily",
    tags=["geocourse"]
) as dag:
    
    @task
    def task_generate_uuid():
        uuid_value = uuid.uuid4().hex
        print("Nilai UUID yang dihasilkan:", uuid_value)
        sleep(5)
        return uuid_value
    
    @task
    def data_receiver1(uuid):
        sleep(6)
        print("Nilai UUID yang didapatkan dari XCom:", uuid)

    @task
    def task_get_current_date():
        today = datetime.today().strftime("%d %B %Y")
        print("Tanggal hari ini:", today)
        sleep(4)
        return today
    
    @task
    def data_receiver2(uuid, date):
        sleep(8)
        print(f"Memproses XCom dengan UUID: {uuid} pada tanggal {date}")

    @task
    def data_receiver3(uuid, date):
        negara = Variable.get("negara")
        lembaga = Variable.get("lembaga")
        identitas = Variable.get("identitas_diri", deserialize_json=True)
        sleep(12)
        print(f"Memproses data dengan ID: {uuid} pada tanggal {date}")
        print(f"Diproses di negara {negara} dalam pelatihan yang diselenggarakan oleh {lembaga}")
        print(f"Pemroses data: {identitas['nama']}")
        if identitas["perempuan"]:
            print(f"Pemroses data adalah perempuan berusia {identitas['umur']} tahun")
        else:
            print(f"Pemroses data adalah laki-laki berusia {identitas['umur']} tahun")

    
    generate_uuid = task_generate_uuid()
    receiver1 = data_receiver1(uuid=generate_uuid)
    
    get_today_date = task_get_current_date()
    receiver2 = data_receiver2(uuid=generate_uuid, date=get_today_date)

    receiver3 = data_receiver3(uuid=generate_uuid, date=get_today_date)
