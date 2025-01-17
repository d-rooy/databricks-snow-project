from pyspark.sql.functions import col, expr, substring, when, lit
from pyspark.sql.types import DoubleType, IntegerType
from datetime import datetime


def pipeline():

# Paths
processed_files_path = "/FileStore/tables/processed_files"
snow_data_delta_path = "/FileStore/tables/snow_data_delta"
s3_path = "s3a://noaa-isd-pds/data/2025/"

# Helper function for substring extraction
def extract_field(value_col, marker, offset, length):
    return expr(f"substring({value_col}, instr({value_col}, '{marker}') + {offset}, {length})")

# List all files in the S3 bucket
file_paths = dbutils.fs.ls(s3_path)
all_filenames = [file.name for file in file_paths]

# Load processed files table
processed_files = spark.read.format("delta").load(processed_files_path)

# Identify new files by excluding already processed ones
new_files_df = spark.createDataFrame([(file,) for file in all_filenames], ["filename"])
new_files_to_process = new_files_df.join(processed_files, on="filename", how="left_anti")

# Collect new files to process into a list
files_to_process = [f"{s3_path}/{row['filename']}" for row in new_files_to_process.collect()]

# Process all new files in a single batch
if files_to_process:
    print(f"Processing {len(files_to_process)} new files...")

    # Read and process the data in a single batch
    df = spark.read.text(files_to_process).filter(
        col("value").rlike("AJ1|AK1|AL1|AL2|AL3|AL4|AM1|AN1")
    ).select(
        substring(col("value"), 5, 6).alias("weather_station_usaf_id"),
        substring(col("value"), 11, 5).alias("weather_station_wban_id"),
        substring(col("value"), 16, 8).alias("observation_date"),
        substring(col("value"), 24, 4).alias("observation_time"),
        (substring(col("value"), 29, 6).cast(DoubleType()) / 1000).alias("latitude_dec"),
        (substring(col("value"), 35, 7).cast(DoubleType()) / 1000).alias("longitude_dec"),
        substring(col("value"), 47, 5).cast(IntegerType()).alias("elevation"),
        when(col("value").contains("AJ1"), lit("AJ1"))
        .when(col("value").contains("AK1"), lit("AK1"))
        .alias("identifier"),
        extract_field("value", "AJ1", 3, 4).alias("snow_depth"),
    ).withColumn("source_file", lit("batched_files"))

    # Write processed data to Delta table in a single write
    df.write.format("delta").mode("append").save(snow_data_delta_path)

    # Update the processed files Delta table with filenames
    new_processed_entries = [(file.split("/")[-1], datetime.now()) for file in files_to_process]
    new_processed_df = spark.createDataFrame(new_processed_entries, ["filename", "processed_timestamp"])
    new_processed_df.write.format("delta").mode("append").save(processed_files_path)

    print(f"Successfully processed {len(files_to_process)} files.")
else:
    print("No new files to process.")
