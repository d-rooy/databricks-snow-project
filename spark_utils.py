# spark_utils.py
from pyspark.sql import SparkSession

def create_spark_session(app_name: str = "NOAA Snow Data Processing") -> SparkSession:

    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.connection.maximum", "200")
        .config("spark.hadoop.fs.s3a.threads.max", "200")
        .config("spark.hadoop.fs.s3a.multipart.size", "104857600")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.speculation", "true")
        .getOrCreate()
    )