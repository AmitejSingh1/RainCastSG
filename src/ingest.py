"""Ingestion and cleaning pipeline: reads raw annual CSVs, applies cleaning rules,

audits data quality, and persists Snappy-compressed Parquet partitioned by year.
"""

import argparse
import time
from pathlib import Path
from typing import List, Dict, Any
import pyspark.sql.functions as F

from src.utils import get_logger, load_config, get_spark_session
from src.clean import get_standard_raw_schema, clean_dataframe
from src.quality_audit import audit_station_coverage, save_quality_reports


def ingest_and_clean_year(
    spark,
    year: int,
    raw_dir: str = "data/raw",
    processed_dir: str = "data/processed",
    expected_type: str = "TB1 Rainfall 5 Minute Total F",
    expected_unit: str = "mm"
) -> Dict[str, Any]:
    """Ingests a single annual raw rainfall CSV, applies validation and duplicate resolution,

    persists Parquet partition, and returns audit metrics.
    """
    logger = get_logger("RainCastSG.Ingest")
    raw_file = Path(raw_dir) / f"rainfall_{year}.csv"

    if not raw_file.exists():
        logger.error(f"Raw file {raw_file} does not exist.")
        raise FileNotFoundError(f"Missing raw dataset for year {year}: {raw_file}")

    start_time = time.time()
    logger.info(f"[{year}] Starting ingestion of {raw_file} ({raw_file.stat().st_size:,} bytes)...")

    # Read CSV with standardized raw schema
    raw_schema = get_standard_raw_schema()
    raw_df = (
        spark.read
        .option("header", "true")
        .schema(raw_schema)
        .csv(str(raw_file))
    )

    # Clean DataFrame and collect audit metrics
    cleaned_df, audit_stats = clean_dataframe(
        raw_df,
        expected_type=expected_type,
        expected_unit=expected_unit
    )

    # Write cleaned data to partitioned Parquet
    target_parquet_partition = Path(processed_dir) / f"rainfall_clean/year={year}"
    target_parquet_partition.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"[{year}] Writing {audit_stats['final_cleaned_record_count']:,} cleaned records to Parquet...")
    (
        cleaned_df
        .drop("year") # Already partitioned by directory
        .write
        .mode("overwrite")
        .parquet(str(target_parquet_partition))
    )

    elapsed_time = time.time() - start_time
    output_size = sum(f.stat().st_size for f in target_parquet_partition.glob("*.parquet"))

    # Count unique active stations
    unique_stations = cleaned_df.select("station_id").distinct().count()

    audit_stats.update({
        "year": year,
        "input_csv_bytes": raw_file.stat().st_size,
        "output_parquet_bytes": output_size,
        "unique_stations_count": unique_stations,
        "processing_time_seconds": round(elapsed_time, 2)
    })

    logger.info(
        f"[{year}] Completed in {elapsed_time:.1f}s. "
        f"Retained: {audit_stats['final_cleaned_record_count']:,} records "
        f"({audit_stats['total_rejected_records']:,} rejected). "
        f"Output size: {output_size / 1024 / 1024:.2f} MB."
    )

    return audit_stats


def run_pipeline(years: List[int], config_path: str = "config/project_config.yaml") -> None:
    """Executes ingestion, cleaning, and quality auditing across requested years."""
    config = load_config(config_path)
    logger = get_logger("RainCastSG.Ingest")

    spark = get_spark_session(
        app_name=config["environment"]["spark_app_name"],
        master=config["environment"]["spark_master"],
        driver_memory=config["environment"]["driver_memory"],
        shuffle_partitions=config["environment"]["shuffle_partitions"],
        timezone=config["project"]["timezone"],
        log_level=config["environment"]["log_level"]
    )

    annual_audits = []
    processed_dir = config["data"]["processed_dir"]
    raw_dir = config["data"]["raw_dir"]
    expected_type = config["data"]["expected_measurement_type"]
    expected_unit = config["data"]["expected_unit"]

    for year in years:
        try:
            stats = ingest_and_clean_year(
                spark=spark,
                year=year,
                raw_dir=raw_dir,
                processed_dir=processed_dir,
                expected_type=expected_type,
                expected_unit=expected_unit
            )
            annual_audits.append(stats)
        except Exception as e:
            logger.error(f"Error processing year {year}: {e}", exc_info=True)

    if annual_audits:
        # Load all processed partitions to compute global station coverage
        logger.info("Computing global station-level coverage and missing intervals...")
        full_cleaned_df = spark.read.parquet(f"{processed_dir}/rainfall_clean")
        station_coverage = audit_station_coverage(
            full_cleaned_df,
            interval_minutes=config["data"]["interval_minutes"]
        )

        json_out, csv_out, station_out = save_quality_reports(
            annual_audit_records=annual_audits,
            station_coverage_df=station_coverage,
            output_dir=f"{config['models']['outputs_dir']}/quality"
        )
        logger.info(f"Quality audit summary saved to {json_out} and {csv_out}")
        logger.info(f"Station coverage report saved to {station_out}")

    spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Ingest and clean Singapore Rainfall datasets")
    parser.add_argument("--years", nargs="+", type=int, help="Years to ingest, e.g. 2017 2018")
    parser.add_argument("--all", action="store_true", help="Ingest all configured years (2017-2024)")
    args = parser.parse_args()

    config = load_config()
    if args.all or not args.years:
        years = config["data"]["years"]
    else:
        years = args.years

    run_pipeline(years)


if __name__ == "__main__":
    main()
