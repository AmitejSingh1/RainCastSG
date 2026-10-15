"""Quality auditing module: station coverage, missing 5-minute intervals, and quality reporting."""

from typing import Dict, Any, List, Tuple
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import pandas as pd
from pathlib import Path
import json


def audit_station_coverage(df: DataFrame, interval_minutes: int = 5) -> DataFrame:
    """Computes station-level observation span, expected interval count, actual count,

    and missing interval counts.
    """
    station_stats = (
        df
        .groupBy("station_id", "station_name")
        .agg(
            F.min("timestamp").alias("first_obs"),
            F.max("timestamp").alias("last_obs"),
            F.count("*").alias("valid_obs_count"),
            F.round(F.mean("rainfall"), 4).alias("mean_rainfall"),
            F.round(F.sum("rainfall"), 2).alias("total_rainfall"),
            F.round(F.mean(F.when(F.col("rainfall") > 0.0, 1.0).otherwise(0.0)), 4).alias("wet_fraction")
        )
    )

    # Calculate expected intervals: (last_obs - first_obs) in seconds / (interval_minutes * 60) + 1
    station_coverage = (
        station_stats
        .withColumn(
            "span_seconds",
            F.col("last_obs").cast("long") - F.col("first_obs").cast("long")
        )
        .withColumn(
            "expected_intervals",
            F.when(F.col("span_seconds") >= 0, (F.col("span_seconds") / (interval_minutes * 60)).cast("long") + 1)
            .otherwise(1)
        )
        .withColumn(
            "missing_intervals",
            F.when(
                F.col("expected_intervals") > F.col("valid_obs_count"),
                F.col("expected_intervals") - F.col("valid_obs_count")
            ).otherwise(0)
        )
        .withColumn(
            "coverage_pct",
            F.round(
                F.when(
                    F.col("expected_intervals") > 0,
                    (F.col("valid_obs_count") / F.col("expected_intervals")) * 100.0
                ).otherwise(100.0),
                2
            )
        )
    )

    return station_coverage


def save_quality_reports(
    annual_audit_records: List[Dict[str, Any]],
    station_coverage_df: DataFrame,
    output_dir: str = "outputs/quality"
) -> Tuple[Path, Path, Path]:
    """Saves machine-readable audit reports in JSON and CSV formats."""
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    # 1. Annual Audit Summary (JSON)
    json_path = out_path / "quality_audit_summary.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(annual_audit_records, f, indent=2)

    # 2. Annual Audit Summary (CSV)
    csv_path = out_path / "quality_audit_summary.csv"
    audit_df = pd.DataFrame(annual_audit_records)
    audit_df.to_csv(csv_path, index=False)

    # 3. Station Coverage Table (CSV)
    station_csv_path = out_path / "station_coverage.csv"
    station_pdf = station_coverage_df.toPandas()
    station_pdf.to_csv(station_csv_path, index=False)

    return json_path, csv_path, station_csv_path
