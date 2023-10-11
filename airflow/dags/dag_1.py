import pprint
import requests
import json

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from confluent_kafka import Producer

def fetch_weather_data():
    try:
        # Realiza la solicitud a la API de clima
        api_url = 'https://api.openweathermap.org/data/2.5/weather?lat=4&lon=74&appid=ce1726a8ed29022108be4c2c3555f028'
        response = requests.get(api_url)
        weather_data = response.json()
        return weather_data
    except Exception as e:
        print(f"Error al obtener datos del clima: {str(e)}")
        return None

def print_json(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='fetch_weather_task')  # Obtener el JSON de la tarea anterior
    if json_data:
        pprint.pprint(json_data)  # Imprimir el JSON utilizando pprint
    else:
        print("No se pudo obtener el JSON de datos del clima")

def json_serialization(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='fetch_weather_task')  # Obtener el JSON de la tarea anterior
    if json_data:
        producer_config = {
            'bootstrap.servers': 'localhost:9092',
            'client.id': 'airflow'
        }
        producer = Producer(producer_config)
        json_message = json.dumps(json_data)
        producer.produce('airflow-spark', value=json_message)
        producer.flush()
    else:
        print("No se pudo obtener el JSON de datos del clima")


dag = DAG('mi_dag_api', 
          schedule_interval=timedelta(seconds=5),
          start_date=datetime(2023, 10, 6))

task1 = PythonOperator(
    task_id='fetch_weather_task',
    python_callable=fetch_weather_data,
    dag=dag,
    provide_context=True,
)

task2 = PythonOperator(
    task_id='get_json_from_task1',
    python_callable=print_json,
    dag=dag,
    provide_context=True,
)

task3 = PythonOperator(
    task_id='run_kafka_producer',
    python_callable=json_serialization,
    dag=dag,
    provide_context=True,
)

task1 >> task2 >> task3
