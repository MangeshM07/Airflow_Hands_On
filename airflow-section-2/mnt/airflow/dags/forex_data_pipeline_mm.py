from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.operators.email import EmailOperator

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

def _get_message() -> str:
    return "Hi from forex_data_pipeline"

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

    # Save the forex rates in HDFS
    # to open HUE : localhost:32762 root root
    saving_rates = BashOperator(
        task_id="saving_rates"
        bash_command="""
            hdfs dfs -mkdir -p /forex && \
            hdfc dfs -put -f $AIRFLOW_HOME/dags/files/forex_rates.json /forex
        """
    )

    # Create a hive table to store forex rates from the hdfs
    creating_forex_rates_table = HiveOperator(
        task_id="creating_forex_rates_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS forex_rates(
                base STRING,
                last_update DATE,
                eur DOUBLE,
                usd DOUBLE,
                nzd DOUBLE,
                gbp DOUBLE,
                jpy DOUBLE,
                cad DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
    )

    # to create hive connection
    # open airflow in browser localhost:8080
    # admin -> connections -> +  
    # conn_id = hive_conn
    # conn_type = Hive server 2 Thrift
    # host = hive-server
    # login= hive
    # password= hive
    # port = 10000

    # Process forex rates with spark
    forex_processing = SparkSubmitOperator(
        task_id = "forex_processing",
        application = "/opt/airflow/dags/scripts/forex_processing.py",
        conn_id = "spark_conn",
        verbose=False
    )

    # to create spark connection
    # open airflow in browser localhost:8080
    # admin -> connections -> +  
    # conn_id = spark_conn
    # conn_type = spark
    # host = spark://spark-master
    # port = 7077

    # Send an email notification
        # configure emai provider
            # Open https://security.google.com/settings/security/apppasswords
            # select app -> mail   device--> windows
            # generate
            # /mnt/airflow.cfg
            # search smtp
            # smtp_host = smtp.gmail.com
            # smtp_user = gmail address
            # smtp_password = generated password
            # smtp_mail_from = your email address
            # restart airflow instance
            # docker-compsoe restart airflow
    send_email_notification = EmailOperator(
        task_id = "send_email_notification",
        to="airflow_course@yopmail.com",
        subject = "forex_data_pipeline",
        html_content = "<h3>forex_data_pipeline</h3>"
    )
  
    # Send a slack notification
    send_slack_notification = SlackWebhookOperator(
        task_id = 'send_slack_notification',
        http_conn_id = "slack_conn",
        message= _get_message(),
        channel = "#monitoring"
    )
    # to create slack connection
    # open airflow in browser localhost:8080
    # admin -> connections -> +  
    # conn_id = slack_conn
    # conn_type = HTTP
    # password = wehbhook url

    is_forex_rates_available >> is_forex_currencies_file_available >> downloading_rates >> saving_rates
    saving_rates >> creating_forex_rates_table >> forex_processing >> send_email_notification
    send_email_notification >> send_slack_notification