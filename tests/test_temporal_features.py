"""Unit tests for historical feature engineering: rolling sums, completeness, and trend."""

import pytest
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
import pyspark.sql.functions as F
from src.features import construct_historical_features
from src.utils import configure_environment


@pytest.fixture(scope="session")
def spark():
    configure_environment()
    session = (
        SparkSession.builder
        .appName("RainCastSG-FeatureTests")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_historical_rolling_accumulations(spark):
    """Verifies that 5m, 15m, 30m, and 60m historical rolling sums match exact expected values."""
    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("station_id", StringType(), True),
        StructField("rainfall", DoubleType(), True),
        StructField("hour", DoubleType(), True),
        StructField("month", DoubleType(), True),
    ])

    # Create 12 consecutive 5-minute intervals for station S06
    # Each interval has rainfall 1.0 mm
    base_time = datetime(2020, 1, 1, 10, 0, 0)
    data = []
    for i in range(12):
        ts = base_time + timedelta(minutes=5 * i)
        data.append((ts, "S06", 1.0, float(ts.hour), float(ts.month)))

    df = spark.createDataFrame(data, schema)
    df_feat = construct_historical_features(df, max_history_intervals=12, interval_minutes=5)

    # For the 12th row (index 11, after 55 min of history):
    # rain_hist_5m: 1.0 (1 interval)
    # rain_hist_15m: 3.0 (3 intervals)
    # rain_hist_30m: 6.0 (6 intervals)
    # rain_hist_60m: 12.0 (12 intervals)
    # rain_hist_mean_60m: 1.0
    # rain_hist_max_60m: 1.0
    # wet_intervals_past_60m: 12
    # rain_trend_15m: 1.0 - (3.0 / 3) = 0.0
    # past_is_complete: True
    last_row = df_feat.orderBy("timestamp", ascending=False).first()

    assert last_row.rain_hist_5m == 1.0
    assert last_row.rain_hist_15m == 3.0
    assert last_row.rain_hist_30m == 6.0
    assert last_row.rain_hist_60m == 12.0
    assert last_row.rain_hist_mean_60m == 1.0
    assert last_row.rain_hist_max_60m == 1.0
    assert last_row.wet_intervals_past_60m == 12
    assert last_row.rain_trend_15m == 0.0
    assert last_row.past_is_complete == True


def test_historical_gap_detection(spark):
    """Verifies that past_is_complete is False if any intermediate interval is missing."""
    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("station_id", StringType(), True),
        StructField("rainfall", DoubleType(), True),
        StructField("hour", DoubleType(), True),
        StructField("month", DoubleType(), True),
    ])

    base_time = datetime(2020, 1, 1, 10, 0, 0)
    data = []
    for i in range(12):
        # Introduce a 10-minute gap at interval 5
        minutes = 5 * i if i < 5 else (5 * i) + 10
        ts = base_time + timedelta(minutes=minutes)
        data.append((ts, "S06", 0.5, float(ts.hour), float(ts.month)))

    df = spark.createDataFrame(data, schema)
    df_feat = construct_historical_features(df, max_history_intervals=12, interval_minutes=5)

    last_row = df_feat.orderBy("timestamp", ascending=False).first()
    # Span is longer than 55 minutes, so past_is_complete must be False
    assert last_row.past_is_complete == False
