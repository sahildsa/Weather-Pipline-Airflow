from __future__ import annotations

import datetime as dt
from docker.types import Mount

from airflow.sdk import dag
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


@dag(start_date=dt.datetime(2026, 1, 1), schedule="@daily", catchup=False)
def weather_pipeline():
    """A simple weather pipeline that runs a Docker container."""
    create_data=DockerOperator(
        task_id="api-worker",
        image="api-worker:latest",
        auto_remove='force',
        mounts=[Mount(source="D:/Airflow_Udemy/result", target="/app/result", type="bind")],
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        do_xcom_push=True,
    )

    process_data=DockerOperator(
        task_id="pyspark-worker",
        image="pyspark-worker:latest",
        auto_remove='force',
        mounts=[Mount(source="D:/Airflow_Udemy/result", target="/app/result", type="bind"),Mount(source="D:/Airflow_Udemy/silver_layer", target="/app/silver_layer", type="bind")],
        environment= {"INPUT_FILE":'{{ ti.xcom_pull(task_ids="api-worker") }}'},
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        do_xcom_push=True,
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


    
    create_data >> process_data >> create_table


weather_pipeline()  