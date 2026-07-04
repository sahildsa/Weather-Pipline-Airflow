import pandas as pd
import numpy as np
import requests

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, task
import datetime as dt

@dag
def data_pipeline():
    @task
    def check_api_status()->str:
        response = requests.get("https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m")
        if response.status_code == 200:
            import csv
            forecast = response.json()
            forecast_data = {
                "timestamp": forecast["current"]["time"],
                "temperature": forecast["current"]["temperature_2m"],
                "wind_speed": forecast["current"]["wind_speed_10m"]
            }
            import datetime as dt
            date=dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            file_path = f"/opt/airflow/result/sample_data_{date}.csv"
            with open(file_path, mode='w', newline="") as file:
                writer = csv.DictWriter(file, fieldnames=forecast_data.keys())
                writer.writeheader()
                writer.writerow(forecast_data)
            return file_path
        else:
            print(f"API request failed with status code: {response.status_code}")
            raise Exception("API request failed")
    
    test_db_connection = SQLExecuteQueryOperator(
        task_id="test_db_connection",
        conn_id="pipeline_db",
        sql="SELECT 1;",
    )

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="pipeline_db",
        sql="""
            CREATE TABLE IF NOT EXISTS weather_data (
                id SERIAL PRIMARY KEY,
                temperature FLOAT,
                wind_speed FLOAT,
                relative_humidity FLOAT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
    )

    @task
    def push_data_to_db(filepath:str):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id="pipeline_db")
        hook.copy_expert(
            sql="""
                COPY weather_data (timestamp, temperature, wind_speed)
                FROM STDIN WITH CSV HEADER
            """,
            filename=filepath
        )


    csv_filepath = check_api_status()
    csv_filepath >> test_db_connection >> create_table >> push_data_to_db(csv_filepath)

data_pipeline()

    