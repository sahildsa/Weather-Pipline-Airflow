from __future__ import annotations

import datetime as dt
from docker.types import Mount

from airflow.sdk import dag
from airflow.providers.docker.operators.docker import DockerOperator


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

    create_data


weather_pipeline()  