from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta


default_args = {
    'owner': 'Ramon Medeiro',
    'email_on_failure': False,
    'email_on_retry': False,
    'email': 'admin@local.com',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id = 'exemplo1',
    start_date = datetime(2021, 1, 1),
    schedule_interval='*/2 * * * *',
    default_args = default_args,
    catchup = False
) as dag:

    def check(response):
        if response == 200:
            print("Returning True")
            return True
        else:
            print("Returning False")
            return False

    sensor_api = HttpSensor(
        task_id = 'api_sensor',
        http_conn_id = 'real_dolar_api',
        endpoint = 'last/USD-BRL',
        response_check = lambda response : True if check(response.status_code) is True else False
    )

    file_sensor = FileSensor(
        task_id = 'file_sensor',
        fs_conn_id = 'sensor_file',
        filepath = 'teste-airflow.csv'
    )