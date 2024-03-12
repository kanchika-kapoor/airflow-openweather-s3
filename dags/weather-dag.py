from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor

from datetime import datetime, timedelta
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024,3,12),
    'email': ['someain@mail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay':timedelta(minutes=2)
}

with DAG('weather_dag',
         default_args = default_args,
         schedule_interval = '@daily',
         catchup=False
         ) as dag:
    
    is_weather_api_ready = HttpSensor(
        task_id = 'is_weather_api_ready',
        http_conn_id = 'weathermap_api',
        endpoint = '/data/2.5/weather?q=Portland&appid=8e3cbb1f2bc6309fdf07b24e597bf2af'
    )