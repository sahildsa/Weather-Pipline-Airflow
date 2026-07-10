
from pyspark.sql import SparkSession
import pyspark
import os


spark = SparkSession.builder \
    .appName("Datacleansing") \
    .getOrCreate()

def read_data(spark,file_path: str)-> pyspark.sql.DataFrame:
    """
    Reads data from a CSV file into a Spark DataFrame.

    Args:
        spark (SparkSession): The SparkSession instance.
        file_path (str): The path to the CSV file.

    Returns:
        pyspark.sql.DataFrame: The DataFrame containing the data from the CSV file.
    """
    try:
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        print(f"Data read successfully from {file_path}")
        return df
    except Exception as e:
        print(f"Error reading data from {file_path}: {e}")
        return None
    

def write_data(df: pyspark.sql.DataFrame)-> str:
    import os
    cws=os.getcwd() #CWD =current working directory
    cwf =os.path.join(cws, "silver_layer")
    fpth =os.path.join(cwf, "output.csv") #results folder in the current working directory
    """
    Writes a Spark DataFrame to a CSV file.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame to be written.
        file_path (str): The path where the CSV file will be saved.
    """
    try:
        df.write.csv(fpth, header=True, mode='overwrite')
        print(f"Data written successfully to {fpth}")
    except Exception as e:
        print(f"Error writing data to {fpth}: {e}")
cwd=os.getcwd() #CWD =current working directory
cwdfolder=os.path.join(cwd, "result") #results folder in the current working directory
cwdreport=os.path.join(cwdfolder, "sample_data_2026-06-18_09-56-22.csv") #results folder in the current working directory
df = read_data(spark,cwdreport)
df.show()
write_data(df)
spark.stop()