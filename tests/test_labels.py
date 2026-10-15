"""Unit tests for future 30-minute target accumulation and threshold labeling."""

import pytest
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from src.features import construct_30min_target, assign_splits_and_labels
from src.utils import configure_environment


@pytest.fixture(scope="session")
def spark():
    configure_environment()
    session = (
        SparkSession.builder
        .appName("RainCastSG-LabelTests")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_future_target_accumulation(spark):
    """Verifies that R30 is exactly the sum of 6 consecutive future intervals."""
    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("station_id", StringType(), True),
        StructField("rainfall", DoubleType(), True),
    ])

    base_time = datetime(2020, 1, 1, 12, 0, 0)
    # 7 intervals: prediction time t + 6 future intervals
    # rainfall: t has 0.0, next 6 intervals have [0.5, 1.0, 1.5, 0.5, 0.5, 1.0] -> sum = 5.0 mm
    rain_values = [0.0, 0.5, 1.0, 1.5, 0.5, 0.5, 1.0]
    data = []
    for i, r in enumerate(rain_values):
        ts = base_time + timedelta(minutes=5 * i)
        data.append((ts, "S06", r))

    df = spark.createDataFrame(data, schema)
    df_target = construct_30min_target(df, lead_intervals=6, interval_minutes=5)

    first_row = df_target.orderBy("timestamp").first()
    assert first_row.future_rainfall_30m == 5.0
    assert first_row.future_span_seconds == 1800 # 30 minutes in seconds
    assert first_row.future_is_complete == True


def test_future_gap_handling(spark):
    """Verifies that future_is_complete is False if any future interval is missing."""
    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("station_id", StringType(), True),
        StructField("rainfall", DoubleType(), True),
    ])

    base_time = datetime(2020, 1, 1, 12, 0, 0)
    # 7 records but one timestamp is skipped (interval 3 is missing, replaced with a gap)
    rain_values = [0.0, 0.5, 1.0, 1.5, 0.5, 0.5, 1.0]
    data = []
    for i, r in enumerate(rain_values):
        minutes = 5 * i if i < 3 else (5 * i) + 15
        ts = base_time + timedelta(minutes=minutes)
        data.append((ts, "S06", r))

    df = spark.createDataFrame(data, schema)
    df_target = construct_30min_target(df, lead_intervals=6, interval_minutes=5)

    first_row = df_target.orderBy("timestamp").first()
    assert first_row.future_span_seconds > 1800
    assert first_row.future_is_complete == False


def test_strict_greater_than_threshold(spark):
    """Verifies strict inequality: y = 1 if R30 > threshold, else y = 0."""
    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("station_id", StringType(), True),
        StructField("year", DoubleType(), True),
        StructField("month", DoubleType(), True),
        StructField("day", DoubleType(), True),
        StructField("hour", DoubleType(), True),
        StructField("minute", DoubleType(), True),
        StructField("future_rainfall_30m", DoubleType(), True),
        StructField("future_is_complete", StringType(), True),
        StructField("past_is_complete", StringType(), True),
    ])

    # Three test scenarios:
    # 1. R30 = 1.0 mm (equal to 1.0mm threshold -> must be 0)
    # 2. R30 = 1.01 mm (strictly greater than 1.0mm -> must be 1)
    # 3. R30 = 0.99 mm (less than 1.0mm -> must be 0)
    t = datetime(2020, 5, 1, 12, 0, 0)
    data = [
        (t, "S06", 2020.0, 5.0, 1.0, 12.0, 0.0, 1.00, "true", "true"),
        (t, "S06", 2020.0, 5.0, 1.0, 12.0, 5.0, 1.01, "true", "true"),
        (t, "S06", 2020.0, 5.0, 1.0, 12.0, 10.0, 0.99, "true", "true"),
    ]

    df = spark.createDataFrame(data, schema)
    df = df.withColumn("future_is_complete", df["future_is_complete"].cast("boolean"))
    df = df.withColumn("past_is_complete", df["past_is_complete"].cast("boolean"))

    df_labeled = assign_splits_and_labels(df, thresholds=[1.0], train_years=[2020], val_years=[], test_years=[])
    rows = df_labeled.orderBy("minute").collect()

    # R30 == 1.00 -> label_gt_1_0mm must be 0.0 (strict inequality requirement!)
    assert rows[0]["label_gt_1_0mm"] == 0.0
    # R30 == 1.01 -> label_gt_1_0mm must be 1.0
    assert rows[1]["label_gt_1_0mm"] == 1.0
    # R30 == 0.99 -> label_gt_1_0mm must be 0.0
    assert rows[2]["label_gt_1_0mm"] == 0.0
