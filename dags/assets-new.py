from airflow.sdk import asset,Asset,Context

@asset(schedule="@daily",
       uri="https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"
       )

def extract_data_from_api(self) -> dict[str]:
    import requests
    response = requests.get(self.uri)
    if response.status_code == 200:
        return response.json()
    else:          
        raise Exception(f"API request failed with status code {response.status_code}")

@asset(schedule=extract_data_from_api)
def temperature_data(extract_data_from_api:Asset,context:Context) -> dict[str]:
    data = context['ti' ].xcom_pull(
        dag_id=extract_data_from_api.name,
        task_ids=extract_data_from_api.name,
        include_prior_dates=True)
    return data["current"]["temperature_2m"]