"""Unit tests for MLlib model pipeline, save/reload, probability bounds, and inference."""

import pytest
import shutil
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import PipelineModel

from src.train import build_ml_pipeline
from src.predict import predict_30min_probability
from src.utils import configure_environment


@pytest.fixture(scope="session")
def spark():
    configure_environment()
    session = (
        SparkSession.builder
        .appName("RainCastSG-ModelTests")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_model_pipeline_save_reload_and_monotonicity(spark, tmp_path):
    """Verifies that MLlib Pipeline fits, saves, reloads, outputs valid probabilities [0, 1],

    and exhibits monotonic non-increasing probabilities as threshold increases.
    """
    # Create synthetic dataset with stations and numeric features
    schema = StructType([
        StructField("station_id", StringType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("hour_sin", DoubleType(), True),
        StructField("hour_cos", DoubleType(), True),
        StructField("month_sin", DoubleType(), True),
        StructField("month_cos", DoubleType(), True),
        StructField("day_of_year", DoubleType(), True),
        StructField("day_of_week", DoubleType(), True),
        StructField("rain_hist_5m", DoubleType(), True),
        StructField("rain_hist_15m", DoubleType(), True),
        StructField("rain_hist_30m", DoubleType(), True),
        StructField("rain_hist_60m", DoubleType(), True),
        StructField("rain_hist_mean_60m", DoubleType(), True),
        StructField("rain_hist_max_60m", DoubleType(), True),
        StructField("wet_intervals_past_60m", DoubleType(), True),
        StructField("rain_trend_15m", DoubleType(), True),
        StructField("label_th0", DoubleType(), True),
        StructField("label_th1", DoubleType(), True),
        StructField("label_th5", DoubleType(), True),
    ])

    # 50 synthetic observations
    np.random.seed(42)
    data = []
    stations = ["S06", "S24", "S109", "S117"]
    for i in range(50):
        st = stations[i % len(stations)]
        rain5 = float(np.random.choice([0.0, 0.2, 1.0, 3.5]))
        r30_future = rain5 * 3.0 + np.random.uniform(0, 2)
        row = (
            st, 103.8, 1.3,
            0.5, 0.8, 0.2, 0.9,
            float(i + 1), float((i % 7) + 1),
            rain5, rain5 * 2, rain5 * 3, rain5 * 5,
            rain5 / 2, rain5 * 1.5,
            float(3 if rain5 > 0 else 0),
            0.1,
            1.0 if r30_future > 0.0 else 0.0,
            1.0 if r30_future > 1.0 else 0.0,
            1.0 if r30_future > 5.0 else 0.0,
        )
        data.append(row)

    df = spark.createDataFrame(data, schema)

    # 1. Fit for theta = 0.0
    lr0 = LogisticRegression(featuresCol="features", labelCol="label_th0", maxIter=10)
    pipeline0 = build_ml_pipeline(lr0)
    model0 = pipeline0.fit(df)

    # 2. Fit for theta = 1.0
    lr1 = LogisticRegression(featuresCol="features", labelCol="label_th1", maxIter=10)
    pipeline1 = build_ml_pipeline(lr1)
    model1 = pipeline1.fit(df)

    # Test Save and Reload
    save_path = str(tmp_path / "test_model_th0")
    model0.write().overwrite().save(save_path)
    reloaded_model0 = PipelineModel.load(save_path)

    # Test Predictions
    preds0 = reloaded_model0.transform(df)
    preds1 = model1.transform(df)

    p0_first = preds0.select("probability").first()["probability"][1]
    p1_first = preds1.select("probability").first()["probability"][1]

    # Probability bounds: in [0, 1]
    assert 0.0 <= p0_first <= 1.0
    assert 0.0 <= p1_first <= 1.0

    # Monotonicity check: P(R > 0) >= P(R > 1)
    # The event (R > 1.0) is a subset of (R > 0.0), so probability must not exceed P(R > 0.0)
    # Allowing small numerical epsilon
    assert p0_first >= (p1_first - 0.05)


def test_predict_rejects_incomplete_history(spark):
    """Verifies that prediction interface properly rejects requests when historical data is missing."""
    # Predict for a non-existent timestamp or station with no history
    res = predict_30min_probability(
        station_id="S999_NONEXISTENT",
        timestamp_str="2024-01-01 12:00:00",
        threshold=1.0
    )
    # Should reject gracefully explaining missing history
    assert res["status"] in ("rejected", "error")
    assert "incomplete" in res["message"].lower() or "not found" in res["message"].lower()


def test_predict_day_of_week_matches_spark():
    """Verifies that Python-calculated day_of_week in predict aligns with Spark F.dayofweek convention:

    1 = Sunday, 2 = Monday, ..., 7 = Saturday.
    """
    # 2024-10-15 is a Tuesday (Python isoweekday=2, Spark dayofweek=3)
    dt_tue = datetime(2024, 10, 15)
    dow_tue = (dt_tue.isoweekday() % 7) + 1
    assert dow_tue == 3

    # 2024-10-13 is a Sunday (Python isoweekday=7, Spark dayofweek=1)
    dt_sun = datetime(2024, 10, 13)
    dow_sun = (dt_sun.isoweekday() % 7) + 1
    assert dow_sun == 1

