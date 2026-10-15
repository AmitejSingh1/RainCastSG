"""Task 2: Historical Feature Engineering and 30-Minute Target Construction.

Builds strictly causal historical features, verifies temporal completeness,
enforces boundary constraints to prevent temporal leakage, and saves feature partitions.
"""

import math
from pathlib import Path
from typing import List, Dict, Any, Tuple
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import json

from src.utils import get_logger, load_config, get_spark_session


def construct_30min_target(
    df: DataFrame,
    lead_intervals: int = 6,
    interval_minutes: int = 5
) -> DataFrame:
    """Constructs future 30-minute accumulated rainfall target:

    R30(s,t) = sum of rainfall at t+5, t+10, t+15, t+20, t+25, t+30 minutes.
    Requires exactly 6 consecutive, complete future intervals.
    """
    station_time_window = Window.partitionBy("station_id").orderBy("timestamp")

    # Future timestamps and rainfall values
    lead_cols = [F.lead("rainfall", k).over(station_time_window) for k in range(1, lead_intervals + 1)]
    future_final_ts = F.lead("timestamp", lead_intervals).over(station_time_window)

    # Accumulation sum
    future_accumulation = lead_cols[0]
    for c in lead_cols[1:]:
        future_accumulation = future_accumulation + c

    # Exact time span check in seconds: 6 intervals * 5 min * 60 sec = 1800 sec
    expected_future_seconds = lead_intervals * interval_minutes * 60
    future_seconds_diff = future_final_ts.cast("long") - F.col("timestamp").cast("long")

    df_target = (
        df
        .withColumn("future_rainfall_30m", future_accumulation)
        .withColumn("future_span_seconds", future_seconds_diff)
        .withColumn(
            "future_is_complete",
            (F.col("future_span_seconds") == expected_future_seconds) &
            (F.col("future_rainfall_30m").isNotNull())
        )
    )

    return df_target


def construct_historical_features(
    df: DataFrame,
    max_history_intervals: int = 12,
    interval_minutes: int = 5
) -> DataFrame:
    """Constructs causal historical features using only information at or before prediction time t.

    Computes 5m, 15m, 30m, and 60m historical accumulation, rolling statistics,
    recent trend, wet-interval counts, and cyclic calendar encodings.
    """
    station_time_window = Window.partitionBy("station_id").orderBy("timestamp")

    # Current interval (lag 0 = most recent 5 minutes at prediction time)
    lags = [F.lag("rainfall", k).over(station_time_window) for k in range(max_history_intervals)]
    lags[0] = F.col("rainfall") # lag 0 is current rainfall

    # Historical accumulations:
    # 5m: lag 0
    # 15m: lag 0 + lag 1 + lag 2 (3 intervals)
    # 30m: lag 0..5 (6 intervals)
    # 60m: lag 0..11 (12 intervals)
    hist_5m = lags[0]
    hist_15m = lags[0] + lags[1] + lags[2]
    hist_30m = hist_15m + lags[3] + lags[4] + lags[5]
    hist_60m = hist_30m + sum(lags[6:12])

    # Past 12-interval completeness check: 11 intervals * 5 min * 60 sec = 3300 sec
    past_start_ts = F.lag("timestamp", max_history_intervals - 1).over(station_time_window)
    expected_past_seconds = (max_history_intervals - 1) * interval_minutes * 60
    past_seconds_diff = F.col("timestamp").cast("long") - past_start_ts.cast("long")

    # Rolling statistics over past 12 intervals
    # Spark SQL array expressions for row-level rolling min, max, mean, count of wet intervals
    lags_array = F.array(*lags[:12])

    # Wet intervals count in past 60 min (rainfall > 0)
    wet_count_expr = F.aggregate(
        lags_array,
        F.lit(0),
        lambda acc, x: F.when(x > 0.0, acc + 1).otherwise(acc)
    )

    # Rolling max and mean over past 60 min
    rolling_max_expr = F.array_max(lags_array)
    rolling_mean_expr = F.round(hist_60m / 12.0, 4)

    # Trend: difference between recent 5m and past 15m average
    trend_expr = F.round(hist_5m - (hist_15m / 3.0), 4)

    # Cyclic time features
    # hour: 0 to 23
    # month: 1 to 12
    two_pi = 2.0 * math.pi
    hour_sin = F.round(F.sin(F.col("hour") * (two_pi / 24.0)), 5)
    hour_cos = F.round(F.cos(F.col("hour") * (two_pi / 24.0)), 5)
    month_sin = F.round(F.sin((F.col("month") - 1) * (two_pi / 12.0)), 5)
    month_cos = F.round(F.cos((F.col("month") - 1) * (two_pi / 12.0)), 5)

    df_features = (
        df
        .withColumn("rain_hist_5m", F.round(hist_5m, 3))
        .withColumn("rain_hist_15m", F.round(hist_15m, 3))
        .withColumn("rain_hist_30m", F.round(hist_30m, 3))
        .withColumn("rain_hist_60m", F.round(hist_60m, 3))
        .withColumn("rain_hist_mean_60m", rolling_mean_expr)
        .withColumn("rain_hist_max_60m", F.round(rolling_max_expr, 3))
        .withColumn("wet_intervals_past_60m", wet_count_expr)
        .withColumn("rain_trend_15m", trend_expr)
        .withColumn("past_span_seconds", past_seconds_diff)
        .withColumn("past_is_complete", (F.col("past_span_seconds") == expected_past_seconds) & (F.col("rain_hist_60m").isNotNull()))
        .withColumn("hour_sin", hour_sin)
        .withColumn("hour_cos", hour_cos)
        .withColumn("month_sin", month_sin)
        .withColumn("month_cos", month_cos)
        .withColumn("day_of_year", F.dayofyear("timestamp"))
        .withColumn("day_of_week", F.dayofweek("timestamp"))
    )

    return df_features


def assign_splits_and_labels(
    df: DataFrame,
    thresholds: List[float] = [0.0, 1.0, 2.0, 5.0, 10.0],
    train_years: List[int] = [2017, 2018, 2019, 2020, 2021, 2022],
    val_years: List[int] = [2023],
    test_years: List[int] = [2024]
) -> DataFrame:
    """Assigns chronological split (train, val, test) and creates binary classification labels

    for each rainfall threshold. Enforces boundary conditions so future targets do not cross splits.
    """
    # Assign split
    df_split = df.withColumn(
        "split",
        F.when(F.col("year").isin(train_years), F.lit("train"))
        .when(F.col("year").isin(val_years), F.lit("val"))
        .when(F.col("year").isin(test_years), F.lit("test"))
        .otherwise(F.lit("unknown"))
    )

    # Filter to only records with complete future target AND complete past history
    df_valid = df_split.filter(
        (F.col("future_is_complete") == True) &
        (F.col("past_is_complete") == True) &
        (F.col("split") != "unknown")
    )

    # Boundary safety: Target window is 30 minutes.
    # Exclude prediction timestamps on Dec 31 after 23:30 for boundary years (e.g. 2022 and 2023)
    # so targets NEVER leak across dataset split boundaries!
    df_safe = df_valid.filter(
        ~(
            (F.col("year").isin([2022, 2023])) &
            (F.col("month") == 12) &
            (F.col("day") == 31) &
            (F.col("hour") == 23) &
            (F.col("minute") > 30)
        )
    )

    # Create binary target labels for each threshold theta:
    # y = 1.0 if R30 > theta else 0.0 (strict greater than per spec)
    for theta in thresholds:
        label_col = f"label_gt_{str(theta).replace('.', '_')}mm"
        df_safe = df_safe.withColumn(
            label_col,
            F.when(F.col("future_rainfall_30m") > float(theta), 1.0).otherwise(0.0)
        )

    return df_safe


def generate_feature_dataset(config_path: str = "config/project_config.yaml") -> None:
    """Orchestrates feature engineering, target construction, validation, and Parquet persistence."""
    config = load_config(config_path)
    logger = get_logger("RainCastSG.Features")

    spark = get_spark_session(
        app_name="RainCastSG-Features",
        master=config["environment"]["spark_master"],
        driver_memory=config["environment"]["driver_memory"],
        shuffle_partitions=config["environment"]["shuffle_partitions"],
        timezone=config["project"]["timezone"],
        log_level=config["environment"]["log_level"]
    )

    clean_parquet_path = f"{config['data']['processed_dir']}/rainfall_clean"
    logger.info(f"Loading cleaned observations from {clean_parquet_path}...")
    df_clean = spark.read.parquet(clean_parquet_path)

    # 1. Construct 30-minute future target accumulation
    logger.info("Constructing 30-minute future target accumulation (6 complete 5-min intervals)...")
    df_with_target = construct_30min_target(
        df_clean,
        lead_intervals=config["features"]["target_lead_intervals"],
        interval_minutes=config["data"]["interval_minutes"]
    )

    # 2. Construct historical features (up to 60-min past)
    logger.info("Constructing historical features (5m, 15m, 30m, 60m lags and rolling statistics)...")
    df_with_features = construct_historical_features(
        df_with_target,
        max_history_intervals=12,
        interval_minutes=config["data"]["interval_minutes"]
    )

    # 3. Assign chronological splits and binary labels
    thresholds = config["features"]["thresholds"]
    logger.info(f"Assigning chronological splits and generating binary labels for thresholds: {thresholds}...")
    df_final = assign_splits_and_labels(
        df_with_features,
        thresholds=thresholds,
        train_years=config["splits"]["train_years"],
        val_years=config["splits"]["val_years"],
        test_years=config["splits"]["test_years"]
    )

    # Select final modeling columns
    label_cols = [f"label_gt_{str(t).replace('.', '_')}mm" for t in thresholds]
    feature_cols = [
        "station_id", "station_name", "longitude", "latitude", "timestamp",
        "year", "month", "day", "hour", "minute", "day_of_year", "day_of_week",
        "hour_sin", "hour_cos", "month_sin", "month_cos",
        "rain_hist_5m", "rain_hist_15m", "rain_hist_30m", "rain_hist_60m",
        "rain_hist_mean_60m", "rain_hist_max_60m", "wet_intervals_past_60m", "rain_trend_15m",
        "future_rainfall_30m", "split"
    ] + label_cols

    df_out = df_final.select(feature_cols)

    # Save partitioned by split
    features_out_path = f"{config['data']['processed_dir']}/features"
    logger.info(f"Persisting feature dataset to {features_out_path} partitioned by split...")
    (
        df_out
        .write
        .mode("overwrite")
        .partitionBy("split")
        .parquet(features_out_path)
    )

    # Compute and log split statistics
    split_counts = df_out.groupBy("split").count().collect()
    logger.info("Feature dataset generation complete! Record counts by split:")
    summary_dict = {}
    for row in split_counts:
        logger.info(f"  Split: {row['split']:<6} Count: {row['count']:,}")
        summary_dict[row["split"]] = row["count"]

    # Save summary
    manifest_path = Path("outputs/quality/feature_summary.json")
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(summary_dict, f, indent=2)

    spark.stop()


def main():
    generate_feature_dataset()


if __name__ == "__main__":
    main()
