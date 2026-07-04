from __future__ import annotations

import pendulum
import pandas as pd

from airflow.sdk import dag, task


@dag(
    dag_id="pandas_local_file_example",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["Example", "pandas"],
)
def pandas_local_file_dag():
    """
    ### Pandas Local File Processing Example
    This DAG demonstrates reading a file from the local (container) filesystem,
    processing it with pandas, and writing it back to a temporary location.
    """

    @task
    def process_with_pandas() -> str:
        """Reads the local CSV, adds a new column, and writes to a new file."""
        # Path is relative to the container's filesystem.
        input_path = "/opt/airflow/dags/sample_data.csv"
        output_path = "/opt/airflow/result/sample_data.csv"

        df = pd.read_csv(input_path)
        df["new_value"] = df["value"] * 2
        df.to_csv(output_path, index=False)
        print(f"Processed data written to {output_path}")
        return output_path

    @task
    def read_processed_data(file_path: str):
        """Reads the processed data from the given file path."""
        df = pd.read_csv(file_path)
        print("Successfully read processed data:")
        print(df)

    processed_file_path = process_with_pandas()
    read_processed_data(processed_file_path)

pandas_local_file_dag()
