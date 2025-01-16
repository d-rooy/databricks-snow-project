# main.py

from spark_utils import create_spark_session
from pipeline import pipeline

def main():
    # 1. Create the Spark Session
    spark = create_spark_session()

    # 2. Run the data pipeline
    run_data_pipeline(spark)

    # 3. Stop the Spark Session (optional in Databricks, but good practice)
    spark.stop()

if __name__ == "__main__":
    main()