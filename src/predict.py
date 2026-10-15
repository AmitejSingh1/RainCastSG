"""Prediction inference interface for RainCastSG.

Accepts station_id, prediction_timestamp (SGT), and rainfall threshold (mm).
Validates historical completeness, extracts causal features, applies trained MLlib pipeline,
and applies calibration to return accurate 30-minute rainfall exceedance probabilities.
"""

import argparse
import json
import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

from pyspark.ml import PipelineModel
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
import pyspark.sql.functions as F

from src.utils import get_logger, load_config, get_spark_session


def load_calibration_params(model_type: str, threshold: float, models_dir: str = "models") -> Optional[Tuple[float, float]]:
    """Loads fitted Platt scaling parameters (a, b) for the requested model and threshold."""
    cal_file = Path(models_dir) / f"calibration_{model_type}.json"
    if cal_file.exists():
        with open(cal_file, "r", encoding="utf-8") as f:
            params = json.load(f)
            th_key = str(threshold)
            if th_key in params:
                return params[th_key]["a"], params[th_key]["b"]
    return None


def predict_30min_probability(
    station_id: str,
    timestamp_str: str,
    threshold: float,
    model_type: str = "logistic_regression",
    config_path: str = "config/project_config.yaml"
) -> Dict[str, Any]:
    """Generates 30-minute rainfall exceedance probability:

    P(R(s, t, t+30m) > theta | history at or before t)
    """
    config = load_config(config_path)
    logger = get_logger("RainCastSG.Predict")

    # 1. Validate threshold
    supported_thresholds = config["features"]["thresholds"]
    if threshold not in supported_thresholds:
        return {
            "status": "error",
            "message": f"Threshold {threshold} mm is unsupported. Supported thresholds: {supported_thresholds}"
        }

    # 2. Check model existence
    th_str = str(threshold).replace('.', '_')
    model_path = Path(config["models"]["save_dir"]) / f"{model_type}_threshold_{th_str}"
    if not model_path.exists():
        return {
            "status": "error",
            "message": f"Trained model not found at {model_path}. Please train model first: python -m src.train --model {model_type} --thresholds {threshold}"
        }

    # 3. Parse timestamp
    try:
        req_dt = datetime.fromisoformat(timestamp_str.replace("Z", "+08:00"))
    except Exception as e:
        return {
            "status": "error",
            "message": f"Invalid timestamp format '{timestamp_str}'. Expected ISO 8601 string (e.g. '2024-11-07 16:45:00'). Error: {e}"
        }

    # 4. Initialize Spark and fetch historical records
    spark = get_spark_session(
        app_name="RainCastSG-Predict",
        master="local[2]",
        driver_memory="2g",
        shuffle_partitions=4,
        timezone=config["project"]["timezone"],
        log_level="ERROR"
    )

    clean_parquet_path = f"{config['data']['processed_dir']}/rainfall_clean"
    df_clean = spark.read.parquet(clean_parquet_path)

    # Historical window: past 60 minutes ending at req_dt (12 intervals of 5 minutes: t - 55m to t)
    window_start = req_dt - timedelta(minutes=55)

    hist_records = (
        df_clean
        .filter(
            (F.col("station_id") == station_id.upper()) &
            (F.col("timestamp") >= window_start) &
            (F.col("timestamp") <= req_dt)
        )
        .orderBy("timestamp")
        .collect()
    )

    if len(hist_records) < 12:
        spark.stop()
        return {
            "status": "rejected",
            "message": (
                f"Historical observations are incomplete for station {station_id} at {timestamp_str}. "
                f"Found {len(hist_records)}/12 required 5-minute intervals in the past 60 minutes. "
                "Causal forecast cannot be reliably computed."
            )
        }

    # Verify exact 5-minute interval spacing across all 12 records:
    # Latest record must match req_dt exactly, and span between oldest and latest must be 3300 seconds (55 min)
    actual_span_seconds = (hist_records[-1]["timestamp"] - hist_records[0]["timestamp"]).total_seconds()
    if hist_records[-1]["timestamp"] != req_dt or actual_span_seconds != 3300:
        spark.stop()
        return {
            "status": "rejected",
            "message": (
                f"Historical observations for station {station_id} at {timestamp_str} contain temporal gaps. "
                f"Span from oldest to latest record is {actual_span_seconds}s (expected 3300s). "
                "Causal forecast cannot be reliably computed."
            )
        }

    # Extract historical rainfall array
    rain_lags = [r["rainfall"] for r in hist_records] # index 0 is oldest (t-55m), index 11 is latest (t)
    latest_rec = hist_records[-1]

    # Compute causal features
    hist_5m = rain_lags[11]
    hist_15m = sum(rain_lags[9:12])
    hist_30m = sum(rain_lags[6:12])
    hist_60m = sum(rain_lags[0:12])
    mean_60m = hist_60m / 12.0
    max_60m = max(rain_lags)
    wet_intervals_past_60m = sum(1 for r in rain_lags if r > 0.0)
    rain_trend_15m = hist_5m - (hist_15m / 3.0)

    # Calendar features
    hour = req_dt.hour
    month = req_dt.month
    two_pi = 2.0 * math.pi
    hour_sin = math.sin(hour * (two_pi / 24.0))
    hour_cos = math.cos(hour * (two_pi / 24.0))
    month_sin = math.sin((month - 1) * (two_pi / 12.0))
    month_cos = math.cos((month - 1) * (two_pi / 12.0))
    day_of_year = req_dt.timetuple().tm_yday
    # Align day_of_week with Spark F.dayofweek: 1 = Sunday, 2 = Monday, ..., 7 = Saturday
    # Python isoweekday() returns 1 = Monday, ..., 7 = Sunday
    day_of_week = (req_dt.isoweekday() % 7) + 1

    feature_row = [{
        "station_id": station_id.upper(),
        "longitude": float(latest_rec["longitude"]) if latest_rec["longitude"] is not None else 103.8,
        "latitude": float(latest_rec["latitude"]) if latest_rec["latitude"] is not None else 1.3,
        "hour_sin": hour_sin,
        "hour_cos": hour_cos,
        "month_sin": month_sin,
        "month_cos": month_cos,
        "day_of_year": float(day_of_year),
        "day_of_week": float(day_of_week),
        "rain_hist_5m": float(hist_5m),
        "rain_hist_15m": float(hist_15m),
        "rain_hist_30m": float(hist_30m),
        "rain_hist_60m": float(hist_60m),
        "rain_hist_mean_60m": float(mean_60m),
        "rain_hist_max_60m": float(max_60m),
        "wet_intervals_past_60m": wet_intervals_past_60m,
        "rain_trend_15m": float(rain_trend_15m),
        f"label_gt_{th_str}mm": 0.0 # dummy label for pipeline compatibility
    }]

    # Convert to single-row Spark DataFrame
    input_df = spark.createDataFrame(feature_row)

    # Load fitted PipelineModel and score
    model = PipelineModel.load(str(model_path))
    prediction_df = model.transform(input_df)
    prob_vec = prediction_df.select("probability").first()["probability"]
    raw_prob = float(prob_vec[1])

    # Apply calibration if parameters exist
    cal_params = load_calibration_params(model_type, threshold, config["models"]["save_dir"])
    if cal_params is not None:
        a, b = cal_params
        eps = 1e-6
        p_clip = min(max(raw_prob, eps), 1.0 - eps)
        logit = math.log(p_clip / (1.0 - p_clip))
        calibrated_prob = 1.0 / (1.0 + math.exp(-(a * logit + b)))
    else:
        calibrated_prob = raw_prob

    spark.stop()

    return {
        "status": "success",
        "station_id": station_id.upper(),
        "station_name": latest_rec["station_name"],
        "prediction_timestamp_sgt": req_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "target_horizon": "Next 30 minutes",
        "threshold_mm": threshold,
        "raw_probability": round(raw_prob, 4),
        "calibrated_probability": round(calibrated_prob, 4),
        "recent_rainfall_summary": {
            "past_5m_mm": round(hist_5m, 2),
            "past_15m_mm": round(hist_15m, 2),
            "past_30m_mm": round(hist_30m, 2),
            "past_60m_mm": round(hist_60m, 2),
            "wet_intervals_past_60m": wet_intervals_past_60m
        }
    }


def main():
    parser = argparse.ArgumentParser(description="Predict 30-Minute Singapore Rainfall Probability")
    parser.add_argument("--station-id", type=str, required=True, help="Monitoring station ID (e.g. S06, S24)")
    parser.add_argument("--timestamp", type=str, required=True, help="Prediction timestamp in SGT (e.g. '2024-11-07 16:45:00')")
    parser.add_argument("--threshold", type=float, default=1.0, help="Rainfall threshold in mm (e.g. 0.0, 1.0, 2.0, 5.0, 10.0)")
    parser.add_argument("--model", type=str, default="logistic_regression", choices=["logistic_regression", "random_forest", "gbt"])
    args = parser.parse_args()

    result = predict_30min_probability(
        station_id=args.station_id,
        timestamp_str=args.timestamp,
        threshold=args.threshold,
        model_type=args.model
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
