from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import json
import pandas as pd

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

def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin*1.8) - 459.67
    return temp_in_fahrenheit


def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids = 'extract_weather_data')
    city = data['name']
    weather_description = data['weather'][0]['description']
    temp_fahrenheit = kelvin_to_fahrenheit(data['main']['temp'])
    feels_like_fahrenheit = kelvin_to_fahrenheit(data['main']['feels_like'])
    min_temp_fahrenheit = kelvin_to_fahrenheit(data['main']['temp_min'])
    max_temp_fahrenheit = kelvin_to_fahrenheit(data['main']['temp_max'])
    pressure = data['main']['pressure']
    humidity = data['main']['humidity']
    wind_speed = data['wind']['speed']
    time_of_record = datetime.utcfromtimestamp(data['dt']+data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise']+data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset']+data['timezone'])

    transformed_data = {
        'City': city,
        'Description': weather_description,
        'Temperature (F)': temp_fahrenheit,
        'Feels Like (F)': feels_like_fahrenheit,
        'Min Temperature (F)': min_temp_fahrenheit,
        'Max Temperature (F)': max_temp_fahrenheit,
        'Pressure': pressure,
        'Humidity': humidity,
        'Wind Speed': wind_speed,
        'Time of Record': time_of_record,
        'Sunrise (LocalTime)': sunrise_time,
        'Sunset (LocalTime)': sunset_time
    }

    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)

    now = datetime.now()
    dt_string = now.strftime('%d%m%Y%H%M%S')
    dt_string = 'current_weather_data_portland_'+dt_string

    df_data.to_csv(f"{dt_string}.csv", index=False)


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

    extract_weather_data = SimpleHttpOperator(
        task_id = 'extract_weather_data',
        http_conn_id = 'weathermap_api',
        method = 'GET',
        response_filter = lambda resp: json.loads(resp.text),
        log_response = True,
        endpoint = '/data/2.5/weather?q=Portland&appid=8e3cbb1f2bc6309fdf07b24e597bf2af'
    )

    transform_extract_load_data = PythonOperator(
        task_id = 'transform_extract_data',
        python_callable = transform_load_data

    )


    is_weather_api_ready >> extract_weather_data >> transform_extract_load_data