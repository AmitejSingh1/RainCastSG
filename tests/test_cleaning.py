"""Unit tests for data cleaning and validation transformations."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType
)
from src.clean import clean_dataframe, standardize_columns
from src.utils import configure_environment


@pytest.fixture(scope="session")
def spark():
    """Initializes a local lightweight SparkSession for unit testing."""
    configure_environment()
    spark_session = (
        SparkSession.builder
        .appName("RainCastSG-Tests")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


def test_standardize_columns(spark):
    data = [("2017-01-01", "S06")]
    schema = StructType([
        StructField(" Date ", StringType(), True),
        StructField("Station_ID", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    clean_df = standardize_columns(df)
    assert "date" in clean_df.columns
    assert "station_id" in clean_df.columns


def test_clean_dataframe_retains_zeros(spark):
    """Verifies that genuine zero-rainfall readings are preserved and not dropped or treated as missing."""
    schema = StructType([
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

    data = [
        ("2017-01-01", "2017-01-01T00:00:00+08:00", "2017-01-01T00:01:00+08:00", "S06", "Paya Lebar", "S06", "103.9", "1.3", "2017-01-01T00:01:00+08:00", "0.0", "TB1 Rainfall 5 Minute Total F", "mm"),
        ("2017-01-01", "2017-01-01T00:05:00+08:00", "2017-01-01T00:06:00+08:00", "S06", "Paya Lebar", "S06", "103.9", "1.3", "2017-01-01T00:06:00+08:00", "1.2", "TB1 Rainfall 5 Minute Total F", "mm"),
    ]

    df = spark.createDataFrame(data, schema)
    cleaned_df, stats = clean_dataframe(df)

    assert cleaned_df.count() == 2
    row0 = cleaned_df.filter(cleaned_df.minute == 0).first()
    assert row0.rainfall == 0.0
    assert stats["invalid_rainfall_count"] == 0
    assert stats["final_cleaned_record_count"] == 2


def test_clean_dataframe_rejects_invalids(spark):
    """Verifies that negative rainfall, missing station IDs, and invalid timestamps are excluded."""
    schema = StructType([
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

    data = [
        # Valid record
        ("2017-01-01", "2017-01-01T00:00:00+08:00", "2017-01-01T00:01:00+08:00", "S06", "Station A", "S06", "103.9", "1.3", "2017-01-01T00:01:00+08:00", "0.4", "TB1 Rainfall 5 Minute Total F", "mm"),
        # Negative rainfall -> rejected
        ("2017-01-01", "2017-01-01T00:05:00+08:00", "2017-01-01T00:06:00+08:00", "S06", "Station A", "S06", "103.9", "1.3", "2017-01-01T00:06:00+08:00", "-0.5", "TB1 Rainfall 5 Minute Total F", "mm"),
        # Null station_id -> rejected
        ("2017-01-01", "2017-01-01T00:10:00+08:00", "2017-01-01T00:11:00+08:00", None, "Station Unknown", "S06", "103.9", "1.3", "2017-01-01T00:11:00+08:00", "0.2", "TB1 Rainfall 5 Minute Total F", "mm"),
        # Invalid timestamp string -> rejected
        ("2017-01-01", "not_a_timestamp", "2017-01-01T00:16:00+08:00", "S06", "Station A", "S06", "103.9", "1.3", "2017-01-01T00:16:00+08:00", "0.0", "TB1 Rainfall 5 Minute Total F", "mm"),
        # Non-numeric reading -> rejected
        ("2017-01-01", "2017-01-01T00:20:00+08:00", "2017-01-01T00:21:00+08:00", "S06", "Station A", "S06", "103.9", "1.3", "2017-01-01T00:21:00+08:00", "INVALID", "TB1 Rainfall 5 Minute Total F", "mm"),
    ]

    df = spark.createDataFrame(data, schema)
    cleaned_df, stats = clean_dataframe(df)

    assert cleaned_df.count() == 1
    assert stats["null_or_empty_station_count"] == 1
    assert stats["null_or_invalid_timestamp_count"] == 1
    assert stats["invalid_rainfall_count"] == 2
    assert stats["final_cleaned_record_count"] == 1


def test_clean_dataframe_duplicate_resolution(spark):
    """Verifies that duplicate (station_id, timestamp) records are resolved by latest update timestamp."""
    schema = StructType([
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

    data = [
        # Older version: rainfall = 0.0, updated at 00:01
        ("2017-01-01", "2017-01-01T00:00:00+08:00", "2017-01-01T00:01:00+08:00", "S06", "Station A", "S06", "103.9", "1.3", "2017-01-01T00:01:00+08:00", "0.0", "TB1 Rainfall 5 Minute Total F", "mm"),
        # Newer corrected version: rainfall = 2.4, updated at 00:15
        ("2017-01-01", "2017-01-01T00:00:00+08:00", "2017-01-01T00:15:00+08:00", "S06", "Station A", "S06", "103.9", "1.3", "2017-01-01T00:15:00+08:00", "2.4", "TB1 Rainfall 5 Minute Total F", "mm"),
    ]

    df = spark.createDataFrame(data, schema)
    cleaned_df, stats = clean_dataframe(df)

    assert cleaned_df.count() == 1
    retained = cleaned_df.first()
    assert retained.rainfall == 2.4
    assert stats["total_records_in_duplicate_groups"] == 2
    assert stats["conflicting_duplicate_records"] == 2
