# pipeline
def pipeline():
    # Define S3 path
    # s3_path = "s3a://noaa-isd-pds/data/2025/"
    s3_path = "s3a://noaa-isd-pds/data/2025/010010-99999-2025.gz"

    # Helper function for substring extraction
    def extract_field(value_col, marker, offset, length):
        # Extracts a substring from the value column based on a marker, offset, and length.
        return expr(f"substring({value_col}, instr({value_col}, '{marker}') + {offset}, {length})")

    # Read and process the data in a single pipeline
    df = spark.read.text(s3_path).filter(
        col("value").rlike("AJ1|AK1|AL1|AL2|AL3|AL4|AM1|AN1")
    ).select(
        # Mandatory fields
        substring(col("value"), 5, 6).alias("weather_station_usaf_id"),
        substring(col("value"), 11, 5).alias("weather_station_wban_id"),
        substring(col("value"), 16, 8).alias("observation_date"),
        substring(col("value"), 24, 4).alias("observation_time"),
        (substring(col("value"), 29, 6).cast(DoubleType()) / 1000).alias("latitude_dec"),
        (substring(col("value"), 35, 7).cast(DoubleType()) / 1000).alias("longitude_dec"),
        substring(col("value"), 47, 5).cast(IntegerType()).alias("elevation"),

        # Snow-related fields
        when(col("value").contains("AJ1"), lit("AJ1"))
        .when(col("value").contains("AK1"), lit("AK1"))
        .when(col("value").contains("AL1"), lit("AL1"))
        .when(col("value").contains("AL2"), lit("AL2"))
        .when(col("value").contains("AL3"), lit("AL3"))
        .when(col("value").contains("AL4"), lit("AL4"))
        .when(col("value").contains("AM1"), lit("AM1"))
        .when(col("value").contains("AN1"), lit("AN1"))
        .alias("identifier"),

        # AJ1 Fields
        extract_field("value", "AJ1", 3, 4).alias("snow_depth"),
        extract_field("value", "AJ1", 7, 1).alias("snow_condition_code"),
        extract_field("value", "AJ1", 8, 1).alias("snow_quality_code"),
        extract_field("value", "AJ1", 9, 6).alias("equiv_water_depth"),
        extract_field("value", "AJ1", 15, 1).alias("equiv_water_condition_code"),
        extract_field("value", "AJ1", 16, 1).alias("equiv_water_quality_code"),

        # AK1 Fields
        extract_field("value", "AK1", 3, 4).alias("greatest_snow_depth"),
        extract_field("value", "AK1", 7, 1).alias("greatest_condition_code"),
        extract_field("value", "AK1", 8, 6).alias("greatest_dates_of_occurrence"),
        extract_field("value", "AK1", 14, 1).alias("greatest_quality_code"),

        # AL1 to AL4 Fields
        extract_field("value", "AL1", 3, 2).alias("al1_accumulation_period_quantity"),
        extract_field("value", "AL1", 5, 3).alias("al1_accumulation_depth"),
        extract_field("value", "AL1", 8, 1).alias("al1_accumulation_condition_code"),
        extract_field("value", "AL1", 9, 1).alias("al1_accumulation_quality_code"),
        extract_field("value", "AL2", 3, 2).alias("al2_accumulation_period_quantity"),
        extract_field("value", "AL2", 5, 3).alias("al2_accumulation_depth"),
        extract_field("value", "AL2", 8, 1).alias("al2_accumulation_condition_code"),
        extract_field("value", "AL2", 9, 1).alias("al2_accumulation_quality_code"),
        extract_field("value", "AL3", 3, 2).alias("al3_accumulation_period_quantity"),
        extract_field("value", "AL3", 5, 3).alias("al3_accumulation_depth"),
        extract_field("value", "AL3", 8, 1).alias("al3_accumulation_condition_code"),
        extract_field("value", "AL3", 9, 1).alias("al3_accumulation_quality_code"),
        extract_field("value", "AL4", 3, 2).alias("al4_accumulation_period_quantity"),
        extract_field("value", "AL4", 5, 3).alias("al4_accumulation_depth"),
        extract_field("value", "AL4", 8, 1).alias("al4_accumulation_condition_code"),
        extract_field("value", "AL4", 9, 1).alias("al4_accumulation_quality_code"),

        # AM1 Fields
        extract_field("value", "AM1", 3, 4).alias("greatest_accumulation_depth"),
        extract_field("value", "AM1", 7, 1).alias("am1_accumulation_condition_code"),
        extract_field("value", "AM1", 8, 4).alias("am1_dates_of_occurrence_1"),
        extract_field("value", "AM1", 12, 4).alias("am1_dates_of_occurrence_2"),
        extract_field("value", "AM1", 16, 4).alias("am1_dates_of_occurrence_3"),
        extract_field("value", "AM1", 20, 1).alias("am1_accumulation_quality_code"),

        # AN1 Fields
        extract_field("value", "AN1", 3, 3).alias("monthly_period_quantity"),
        extract_field("value", "AN1", 6, 4).alias("monthly_accumulation_depth"),
        extract_field("value", "AN1", 10, 1).alias("monthly_condition_code"),
        extract_field("value", "AN1", 11, 1).alias("monthly_quality_code")
    ).select(
        # Cast all fields to strings for consistency
        *(col(field).cast("string").alias(field) for field in [
            "weather_station_usaf_id", "weather_station_wban_id", "observation_date", "observation_time",
            "latitude_dec", "longitude_dec", "elevation", "identifier", "snow_depth", "snow_condition_code",
            "snow_quality_code", "equiv_water_depth", "equiv_water_condition_code", "equiv_water_quality_code",
            "greatest_snow_depth", "greatest_condition_code", "greatest_dates_of_occurrence", "greatest_quality_code",
            "al1_accumulation_period_quantity", "al1_accumulation_depth", "al1_accumulation_condition_code",
            "al1_accumulation_quality_code", "al2_accumulation_period_quantity", "al2_accumulation_depth",
            "al2_accumulation_condition_code", "al2_accumulation_quality_code", "al3_accumulation_period_quantity",
            "al3_accumulation_depth", "al3_accumulation_condition_code", "al3_accumulation_quality_code",
            "al4_accumulation_period_quantity", "al4_accumulation_depth", "al4_accumulation_condition_code",
            "al4_accumulation_quality_code", "greatest_accumulation_depth", "am1_accumulation_condition_code",
            "am1_dates_of_occurrence_1", "am1_dates_of_occurrence_2", "am1_dates_of_occurrence_3",
            "am1_accumulation_quality_code", "monthly_period_quantity", "monthly_accumulation_depth",
            "monthly_condition_code", "monthly_quality_code"
        ])
    )

    # Register the DataFrame as a temporary view in Spark
    df.createOrReplaceTempView("df_temp")

    # Create or replace a Delta table in the Databricks metastore
    spark.sql(f"CREATE OR REPLACE TABLE df_delta USING DELTA AS SELECT * FROM df_temp")