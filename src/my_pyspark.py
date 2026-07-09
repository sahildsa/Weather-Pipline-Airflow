
from pyspark.sql import SparkSession

def run_spark_job():
    """
    Initializes a Spark session, prints a success message, and stops the session.
    """
    # Initialize a SparkSession.
    # The SparkSession is the entry point for any Spark functionality.
    # .appName() gives your application a name to display in the Spark UI.
    # .getOrCreate() gets an existing SparkSession or creates a new one if none exists.
    spark = SparkSession.builder \
        .appName("SimpleSuccessJob") \
        .getOrCreate()

    print("--- Starting Spark Job ---")

    try:
        # Your main Spark processing logic would typically go here.
        # For this request, we will just print a success message.
        print("ran successfully")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Stop the SparkSession to release the cluster resources.
        print("--- Stopping Spark Job ---")
        spark.stop()

if __name__ == "__main__":
    run_spark_job()
