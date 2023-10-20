from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import csv, requests, json

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


# Download forex rates according to the currencies we want to watch
# described in the file forex_currencies.csv
def download_rates():
    BASE_URL = "https://gist.githubusercontent.com/marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b/raw/"
    ENDPOINTS = {
        'USD': 'api_forex_exchange_usd.json',
        'EUR': 'api_forex_exchange_eur.json'
    }
    with open('/opt/airflow/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')
        for idx, row in enumerate(reader):
            base = row['base']
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get(f"{BASE_URL}{ENDPOINTS[base]}").json()
            outdata = {'base': base, 'rates': {}, 'last_update': indata['date']}
            for pair in with_pairs:
                outdata['rates'][pair] = indata['rates'][pair]
            with open('/opt/airflow/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')

with DAG("forex_data_pipeline", 
         start_date= datetime(2021, 1, 1), 
         schedule_interval = "@daily", 
         default_args= default_args, 
         catchup=False ) as dag:
    
    # check availability of forex rates

    is_forex_rates_available = HttpSensor(
        task_id="is_forex_rates_available",
        http_conn_id="forex_api",
        endpoint="marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b",
        response_check=lambda response: "rates" in response.text,
        poke_interval=5,
        timeout=20,
    )

    # Steps to test task
    # docker ps
    #   1. docker exec -it <container_id of airflow container> /bin/bash

    #   2. airflow tasks test forex_data_pipeline is_forex_rates_available 2021-01-01

    # aurflow ui - admin - connections - coonn_id =forex_path, conn_type= File(path), extra = {"path":"/opt/airflow/dags/files"}
    is_forex_currencies_file_available = FileSensor(
        task_id="is_forex_currencies_file_available",
        fs_conn_id="forex_path",
        filepath="/forex_currencies.csv",
        poke_interval=5,
        timeout=20
    )

    # Download forex rates with python
    downloading_rates = PythonOperator(
        task_id="downloading_rates",
        python_callable=download_rates
    )

    