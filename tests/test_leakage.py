"""Unit tests for temporal leakage prevention and chronological split boundary isolation."""

import pytest
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from src.features import construct_historical_features, assign_splits_and_labels
from src.utils import configure_environment


@pytest.fixture(scope="session")
def spark():
    configure_environment()
    session = (
        SparkSession.builder
        .appName("RainCastSG-LeakageTests")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_no_future_information_in_historical_features(spark):
    """Verifies that historical features at time t are IDENTICAL regardless of future rainfall values."""
    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("station_id", StringType(), True),
        StructField("rainfall", DoubleType(), True),
        StructField("hour", DoubleType(), True),
        StructField("month", DoubleType(), True),
    ])

    base_time = datetime(2020, 1, 1, 10, 0, 0)
    # 12 past intervals + 3 future intervals
    data_a = []
    data_b = []

    for i in range(15):
        ts = base_time + timedelta(minutes=5 * i)
        # Past 12 intervals (0..11) are identical
        if i < 12:
            val = float(i)
            data_a.append((ts, "S06", val, float(ts.hour), float(ts.month)))
            data_b.append((ts, "S06", val, float(ts.hour), float(ts.month)))
        else:
            # Future intervals (12, 13, 14) differ drastically between dataset A and B
            data_a.append((ts, "S06", 0.0, float(ts.hour), float(ts.month)))
            data_b.append((ts, "S06", 50.0, float(ts.hour), float(ts.month))) # Extreme torrential rain in future

    df_a = spark.createDataFrame(data_a, schema)
    df_b = spark.createDataFrame(data_b, schema)

    feat_a = construct_historical_features(df_a, max_history_intervals=12, interval_minutes=5)
    feat_b = construct_historical_features(df_b, max_history_intervals=12, interval_minutes=5)

    # Inspect features at index 11 (the prediction timestamp)
    target_ts = base_time + timedelta(minutes=5 * 11)
    row_a = feat_a.filter(feat_a.timestamp == target_ts).first()
    row_b = feat_b.filter(feat_b.timestamp == target_ts).first()

    # All historical features must be perfectly identical:
    assert row_a.rain_hist_5m == row_b.rain_hist_5m
    assert row_a.rain_hist_15m == row_b.rain_hist_15m
    assert row_a.rain_hist_30m == row_b.rain_hist_30m
    assert row_a.rain_hist_60m == row_b.rain_hist_60m
    assert row_a.rain_hist_mean_60m == row_b.rain_hist_mean_60m
    assert row_a.rain_hist_max_60m == row_b.rain_hist_max_60m
    assert row_a.wet_intervals_past_60m == row_b.wet_intervals_past_60m
    assert row_a.rain_trend_15m == row_b.rain_trend_15m


def test_boundary_cutoff_prevents_split_crossing(spark):
    """Verifies that observations within 30 minutes of a split transition are excluded from the earlier split."""
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

    # Case 1: 2022-12-31 23:25 (future target ends at 23:55 -> stays within 2022 -> permitted)
    # Case 2: 2022-12-31 23:35 (future target ends at 2023-01-01 00:05 -> crosses into val split -> must be excluded)
    t1 = datetime(2022, 12, 31, 23, 25, 0)
    t2 = datetime(2022, 12, 31, 23, 35, 0)

    data = [
        (t1, "S06", 2022.0, 12.0, 31.0, 23.0, 25.0, 1.2, "true", "true"),
        (t2, "S06", 2022.0, 12.0, 31.0, 23.0, 35.0, 1.5, "true", "true"),
    ]

    df = spark.createDataFrame(data, schema)
    df = df.withColumn("future_is_complete", df["future_is_complete"].cast("boolean"))
    df = df.withColumn("past_is_complete", df["past_is_complete"].cast("boolean"))

    df_safe = assign_splits_and_labels(
        df,
        thresholds=[1.0],
        train_years=[2022],
        val_years=[2023],
        test_years=[2024]
    )

    remaining = df_safe.collect()
    assert len(remaining) == 1
    assert remaining[0]["minute"] == 25.0
