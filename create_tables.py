#Create Delta Tables for Processed Files and Snow Data
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType


def create_tables():
    
# Delta table for processed files
processed_files_path = "/FileStore/tables/processed_files"
snow_data_delta_path = "/FileStore/tables/snow_data_delta"

# Schema for processed files tracking
processed_files_schema = StructType([
    StructField("filename", StringType(), True),
    StructField("processed_timestamp", TimestampType(), True)
])

# Schema for snow data Delta table
snow_data_schema = StructType([
    StructField("weather_station_usaf_id", StringType(), True),
    StructField("weather_station_wban_id", StringType(), True),
    StructField("observation_date", StringType(), True),
    StructField("observation_time", StringType(), True),
    StructField("latitude_dec", DoubleType(), True),
    StructField("longitude_dec", DoubleType(), True),
    StructField("elevation", IntegerType(), True),
    StructField("identifier", StringType(), True),
    StructField("snow_depth", StringType(), True),
    StructField("source_file", StringType(), True)
])

# Initialize empty Delta tables (run once to initialize)
spark.createDataFrame([], processed_files_schema).write.format("delta").mode("overwrite").save(processed_files_path)
spark.createDataFrame([], snow_data_schema).write.format("delta").mode("overwrite").save(snow_data_delta_path)
