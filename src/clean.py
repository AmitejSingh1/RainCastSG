"""Data cleaning and quality auditing transformations for Singapore Rainfall records."""

from typing import Dict, Any, Tuple
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
)


def get_standard_raw_schema() -> StructType:
    """Returns the expected raw CSV schema for historical rainfall files."""
    return StructType([
        StructField("date", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("update_timestamp", StringType(), True),
        StructField("station_id", StringType(), True),
        StructField("station_name", StringType(), True),
        StructField("station_device_id", StringType(), True),
        StructField("location_longitude", StringType(), True),
        StructField("location_latitude", StringType(), True),
        StructField("reading_update_timestamp", StringType(), True),
        StructField("reading_value", StringType(), True),
        StructField("reading_type", StringType(), True),
        StructField("reading_unit", StringType(), True),
    ])


def standardize_columns(df: DataFrame) -> DataFrame:
    """Standardizes column names to lowercase and strips whitespace."""
    for col in df.columns:
        clean_col = col.strip().lower()
        if clean_col != col:
            df = df.withColumnRenamed(col, clean_col)
    return df


def clean_dataframe(
    df: DataFrame,
    expected_type: str = "TB1 Rainfall 5 Minute Total F",
    expected_unit: str = "mm"
) -> Tuple[DataFrame, Dict[str, Any]]:
    """Applies strict data quality validation rules and resolves duplicates.

    Returns:
        cleaned_df: Validated DataFrame with standardized types
        audit_stats: Dictionary recording rejected record counts and validation metrics
    """
    df = standardize_columns(df)
    initial_count = df.count()

    # Rule 1 & 2: Non-null station_id and timestamp
    df_with_types = (
        df
        .withColumn("station_id", F.upper(F.trim(F.col("station_id"))))
        .withColumn("timestamp_clean", F.to_timestamp(F.col("timestamp")))
        .withColumn("reading_value_num", F.col("reading_value").cast(DoubleType()))
        .withColumn("longitude_num", F.col("location_longitude").cast(DoubleType()))
        .withColumn("latitude_num", F.col("location_latitude").cast(DoubleType()))
        .withColumn("update_ts_clean", F.coalesce(
            F.to_timestamp(F.col("reading_update_timestamp")),
            F.to_timestamp(F.col("update_timestamp")),
            F.to_timestamp(F.col("timestamp"))
        ))
    )

    null_station_count = df_with_types.filter(
        F.col("station_id").isNull() | (F.col("station_id") == "")
    ).count()

    null_timestamp_count = df_with_types.filter(
        F.col("timestamp_clean").isNull()
    ).count()

    # Filter usable stations and timestamps
    valid_keys_df = df_with_types.filter(
        F.col("station_id").isNotNull() &
        (F.col("station_id") != "") &
        F.col("timestamp_clean").isNotNull()
    )

    # Rule 3 & 4: Numeric, non-null, non-negative rainfall values
    # Note: reading_value_num == 0.0 is explicitly retained!
    invalid_rainfall_count = valid_keys_df.filter(
        F.col("reading_value_num").isNull() |
        F.isnan(F.col("reading_value_num")) |
        (F.col("reading_value_num") < 0.0)
    ).count()

    valid_rainfall_df = valid_keys_df.filter(
        F.col("reading_value_num").isNotNull() &
        (~F.isnan(F.col("reading_value_num"))) &
        (F.col("reading_value_num") >= 0.0)
    )

    # Rule 5: Validate measurement type and unit if present
    unexpected_type_count = valid_rainfall_df.filter(
        F.col("reading_type").isNotNull() & (F.trim(F.col("reading_type")) != expected_type)
    ).count()

    unexpected_unit_count = valid_rainfall_df.filter(
        F.col("reading_unit").isNotNull() & (F.lower(F.trim(F.col("reading_unit"))) != expected_unit.lower())
    ).count()

    valid_type_df = valid_rainfall_df.filter(
        (F.col("reading_type").isNull() | (F.trim(F.col("reading_type")) == expected_type)) &
        (F.col("reading_unit").isNull() | (F.lower(F.trim(F.col("reading_unit"))) == expected_unit.lower()))
    )

    # Rule 8 & 9: Identify duplicate (station_id, timestamp) groups and resolve
    window_spec = Window.partitionBy("station_id", "timestamp_clean")
    df_counted = valid_type_df.withColumn("group_count", F.count("*").over(window_spec))

    total_in_duplicate_groups = df_counted.filter(F.col("group_count") > 1).count()

    # Check for conflicting reading values within duplicates
    df_with_min_max = (
        df_counted
        .withColumn("group_min_val", F.min("reading_value_num").over(window_spec))
        .withColumn("group_max_val", F.max("reading_value_num").over(window_spec))
    )
    conflicting_duplicates_count = df_with_min_max.filter(
        (F.col("group_count") > 1) & (F.col("group_min_val") != F.col("group_max_val"))
    ).count()

    # Duplicate resolution policy:
    # Order by update_ts_clean DESC, reading_value_num DESC (or first deterministically)
    rank_window = Window.partitionBy("station_id", "timestamp_clean").orderBy(
        F.col("update_ts_clean").desc_nulls_last(),
        F.col("reading_value_num").desc(),
        F.col("station_device_id").desc_nulls_last()
    )

    deduped_df = (
        valid_type_df
        .withColumn("rank", F.row_number().over(rank_window))
        .filter(F.col("rank") == 1)
        .drop("rank")
    )

    # Select final standardized columns
    cleaned_df = deduped_df.select(
        F.col("timestamp_clean").alias("timestamp"),
        F.col("station_id"),
        F.coalesce(F.col("station_name"), F.col("station_id")).alias("station_name"),
        F.col("reading_value_num").alias("rainfall"),
        F.col("longitude_num").alias("longitude"),
        F.col("latitude_num").alias("latitude"),
        F.col("update_ts_clean").alias("update_timestamp"),
        F.year(F.col("timestamp_clean")).alias("year"),
        F.month(F.col("timestamp_clean")).alias("month"),
        F.dayofmonth(F.col("timestamp_clean")).alias("day"),
        F.hour(F.col("timestamp_clean")).alias("hour"),
        F.minute(F.col("timestamp_clean")).alias("minute")
    )

    final_count = cleaned_df.count()

    audit_stats = {
        "raw_record_count": initial_count,
        "null_or_empty_station_count": null_station_count,
        "null_or_invalid_timestamp_count": null_timestamp_count,
        "invalid_rainfall_count": invalid_rainfall_count,
        "unexpected_measurement_type_count": unexpected_type_count,
        "unexpected_unit_count": unexpected_unit_count,
        "total_records_in_duplicate_groups": total_in_duplicate_groups,
        "conflicting_duplicate_records": conflicting_duplicates_count,
        "final_cleaned_record_count": final_count,
        "total_rejected_records": initial_count - final_count,
    }

    return cleaned_df, audit_stats
