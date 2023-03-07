import requests
import json
import pandas as pd
import numpy as np

from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta


def request_api():
    r = requests.get('https://economia.awesomeapi.com.br/last/USD-BRL')
    content = json.loads(r.text)
    high = content["USDBRL"]["high"]
    low = content["USDBRL"]["low"]
    var = content["USDBRL"]["varBid"]

    df_dolar = pd.DataFrame(np.array([[high, low, var]]), columns = ['high', 'low', 'variance'])
    df_dolar.to_csv('teste-airflow.csv', index=False)

    return 


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