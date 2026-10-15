"""Task 1: Exploratory Data Analysis of Singapore Rainfall Patterns (2017-2024).

Computes Spark SQL aggregations, exports analytical tables, and generates figures.
"""

import os
from pathlib import Path
from typing import Dict, Any, Tuple
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pyspark.sql.functions as F

from src.utils import get_logger, load_config, get_spark_session


def setup_plotting_style():
    """Sets a consistent, clean style for publication-ready figures."""
    sns.set_theme(style="whitegrid", font="sans-serif")
    plt.rcParams.update({
        "font.size": 11,
        "axes.labelsize": 12,
        "axes.titlesize": 13,
        "xtick.labelsize": 10,
        "ytick.labelsize": 10,
        "legend.fontsize": 10,
        "figure.titlesize": 14,
        "figure.dpi": 300,
        "savefig.dpi": 300,
        "savefig.bbox": "tight",
    })


def compute_annual_summary(df) -> pd.DataFrame:
    """Computes annual rainfall statistics normalized by station count and coverage."""
    # Use a single percentile_approx call with array of probabilities and accuracy 1000 for memory efficiency
    annual_df = (
        df
        .groupBy("year")
        .agg(
            F.count("*").alias("total_valid_observations"),
            F.countDistinct("station_id").alias("active_stations"),
            F.round(F.mean("rainfall"), 5).alias("mean_5min_rainfall_mm"),
            F.round(F.stddev("rainfall"), 5).alias("std_5min_rainfall_mm"),
            F.round(F.max("rainfall"), 2).alias("max_5min_rainfall_mm"),
            F.sum(F.when(F.col("rainfall") > 0.0, 1).otherwise(0)).alias("wet_intervals_count"),
            F.round(F.mean(F.when(F.col("rainfall") > 0.0, 1.0).otherwise(0.0)) * 100, 2).alias("wet_interval_pct"),
            F.expr("percentile_approx(rainfall, array(0.90, 0.95, 0.99, 0.999), 1000)").alias("percentiles_arr"),
            F.round(F.sum("rainfall") / F.countDistinct("station_id"), 2).alias("station_avg_annual_accumulation_mm")
        )
        .withColumn("p90_rainfall_mm", F.round(F.col("percentiles_arr")[0], 3))
        .withColumn("p95_rainfall_mm", F.round(F.col("percentiles_arr")[1], 3))
        .withColumn("p99_rainfall_mm", F.round(F.col("percentiles_arr")[2], 3))
        .withColumn("p999_rainfall_mm", F.round(F.col("percentiles_arr")[3], 3))
        .drop("percentiles_arr")
        .orderBy("year")
    )
    return annual_df.toPandas()


def compute_monthly_summary(df) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Computes monthly rainfall metrics both across all years and year-by-month."""
    # 1. Year-by-month
    year_month_df = (
        df
        .groupBy("year", "month")
        .agg(
            F.count("*").alias("valid_observations"),
            F.countDistinct("station_id").alias("active_stations"),
            F.round(F.mean("rainfall"), 5).alias("mean_5min_rainfall_mm"),
            F.round(F.sum("rainfall") / F.countDistinct("station_id"), 2).alias("avg_station_accumulation_mm"),
            F.round(F.mean(F.when(F.col("rainfall") > 0.0, 1.0).otherwise(0.0)) * 100, 2).alias("wet_interval_pct")
        )
        .orderBy("year", "month")
    )
    ym_pdf = year_month_df.toPandas()

    # 2. Aggregated 8-year monthly profile
    monthly_profile = (
        ym_pdf
        .groupby("month")
        .agg(
            mean_monthly_accumulation_mm=("avg_station_accumulation_mm", "mean"),
            std_monthly_accumulation_mm=("avg_station_accumulation_mm", "std"),
            mean_wet_interval_pct=("wet_interval_pct", "mean"),
            mean_5min_rainfall_mm=("mean_5min_rainfall_mm", "mean")
        )
        .reset_index()
    )

    # Assign meteorological seasons in Singapore
    # Dec-Mar: Northeast Monsoon; Apr-May: Inter-Monsoon 1; Jun-Sep: Southwest Monsoon; Oct-Nov: Inter-Monsoon 2
    def get_season(m):
        if m in (12, 1, 2, 3):
            return "Northeast Monsoon"
        elif m in (4, 5):
            return "Inter-Monsoon 1"
        elif m in (6, 7, 8, 9):
            return "Southwest Monsoon"
        else:
            return "Inter-Monsoon 2"

    monthly_profile["season"] = monthly_profile["month"].apply(get_season)
    return ym_pdf, monthly_profile


def compute_diurnal_summary(df) -> pd.DataFrame:
    """Computes hourly rainfall occurrence and intensity in Singapore Local Time (SGT)."""
    hourly_df = (
        df
        .groupBy("hour")
        .agg(
            F.count("*").alias("valid_observations"),
            F.sum(F.when(F.col("rainfall") > 0.0, 1).otherwise(0)).alias("wet_observations"),
            F.round(F.mean(F.when(F.col("rainfall") > 0.0, 1.0).otherwise(0.0)) * 100, 2).alias("wet_frequency_pct"),
            F.round(F.mean("rainfall"), 5).alias("mean_5min_rainfall_mm"),
            F.round(F.mean(F.when(F.col("rainfall") > 0.0, F.col("rainfall"))), 4).alias("mean_wet_intensity_mm"),
            F.expr("percentile_approx(rainfall, array(0.95, 0.99), 500)").alias("p_arr")
        )
        .withColumn("p95_rainfall_mm", F.round(F.col("p_arr")[0], 3))
        .withColumn("p99_rainfall_mm", F.round(F.col("p_arr")[1], 3))
        .drop("p_arr")
        .orderBy("hour")
    )
    return hourly_df.toPandas()


def compute_intensity_percentiles(df) -> pd.DataFrame:
    """Computes extreme percentile distribution for all observations and wet intervals."""
    quantiles = [0.50, 0.75, 0.90, 0.95, 0.98, 0.99, 0.995, 0.999]
    q_str = ", ".join([str(q) for q in quantiles])

    row_all = df.select(
        F.expr(f"percentile_approx(rainfall, array({q_str}), 1000)").alias("all_arr")
    ).first()["all_arr"]

    wet_df = df.filter(F.col("rainfall") > 0.0)
    row_wet = wet_df.select(
        F.expr(f"percentile_approx(rainfall, array({q_str}), 1000)").alias("wet_arr")
    ).first()["wet_arr"]

    summary_records = []
    for i, q in enumerate(quantiles):
        summary_records.append({
            "percentile": f"{q*100:.1f}%",
            "quantile": q,
            "all_observations_mm": round(float(row_all[i]), 4) if row_all is not None else 0.0,
            "wet_intervals_only_mm": round(float(row_wet[i]), 4) if row_wet is not None else 0.0
        })

    return pd.DataFrame(summary_records)


def compute_spatial_summary(df) -> pd.DataFrame:
    """Computes station-level spatial metrics: coordinates, normalized rainfall, and coverage."""
    spatial_df = (
        df
        .groupBy("station_id", "station_name")
        .agg(
            F.first("longitude", ignorenulls=True).alias("longitude"),
            F.first("latitude", ignorenulls=True).alias("latitude"),
            F.count("*").alias("valid_observations"),
            F.round(F.mean("rainfall"), 4).alias("mean_5min_rainfall_mm"),
            F.round(F.sum("rainfall"), 2).alias("total_rainfall_mm"),
            F.round(F.mean(F.when(F.col("rainfall") > 0.0, 1.0).otherwise(0.0)) * 100, 2).alias("wet_frequency_pct"),
            F.round(F.max("rainfall"), 2).alias("max_5min_rainfall_mm")
        )
        .filter(F.col("longitude").isNotNull() & F.col("latitude").isNotNull())
    )
    return spatial_df.toPandas()


def generate_figures(
    annual_pdf: pd.DataFrame,
    ym_pdf: pd.DataFrame,
    monthly_pdf: pd.DataFrame,
    hourly_pdf: pd.DataFrame,
    intensity_pdf: pd.DataFrame,
    spatial_pdf: pd.DataFrame,
    figures_dir: str = "figures"
):
    """Generates and saves Task 1 exploratory figures."""
    setup_plotting_style()
    fig_path = Path(figures_dir)
    fig_path.mkdir(parents=True, exist_ok=True)

    # 1. Figure 1: Annual Rainfall Summary
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    bars = ax1.bar(
        annual_pdf["year"],
        annual_pdf["station_avg_annual_accumulation_mm"],
        color="#2b5c8f",
        alpha=0.85,
        edgecolor="black"
    )
    ax1.set_title("Annual Station-Normalized Rainfall Accumulation (2017–2024)", fontweight="bold")
    ax1.set_xlabel("Year")
    ax1.set_ylabel("Accumulation per Station (mm)")
    for bar in bars:
        yval = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2, yval + 30, f"{yval:.0f} mm", ha="center", va="bottom", fontsize=9)

    ax2.plot(annual_pdf["year"], annual_pdf["wet_interval_pct"], marker="o", color="#d95f02", lw=2.2, label="Wet Interval %")
    ax2.set_title("Annual Wet-Interval Occurrence Frequency (%)", fontweight="bold")
    ax2.set_xlabel("Year")
    ax2.set_ylabel("Wet Intervals (% of Valid Observations)")
    ax2.grid(True, linestyle="--", alpha=0.6)
    plt.tight_layout()
    plt.savefig(fig_path / "fig1_annual_rainfall_summary.png")
    plt.close()

    # 2. Figure 2: Monthly Seasonal Profile
    fig, ax = plt.subplots(figsize=(10, 5))
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    colors = {
        "Northeast Monsoon": "#1f78b4",
        "Inter-Monsoon 1": "#33a02c",
        "Southwest Monsoon": "#ff7f00",
        "Inter-Monsoon 2": "#6a3d9a"
    }
    bar_colors = [colors[s] for s in monthly_pdf["season"]]
    ax.bar(months, monthly_pdf["mean_monthly_accumulation_mm"], yerr=monthly_pdf["std_monthly_accumulation_mm"],
           color=bar_colors, alpha=0.85, capsize=4, edgecolor="black")
    ax.set_title("Average Monthly Rainfall Accumulation with Monsoon Seasons", fontweight="bold")
    ax.set_xlabel("Month")
    ax.set_ylabel("Mean Monthly Accumulation per Station (mm)")

    # Custom legend for seasons
    handles = [plt.Rectangle((0, 0), 1, 1, color=colors[s]) for s in colors]
    ax.legend(handles, colors.keys(), title="Monsoon Season", loc="upper left")
    plt.tight_layout()
    plt.savefig(fig_path / "fig2_monthly_seasonal_patterns.png")
    plt.close()

    # 3. Figure 3: Year-by-Month Heatmap
    pivot_df = ym_pdf.pivot(index="year", columns="month", values="avg_station_accumulation_mm")
    pivot_df.columns = months
    fig, ax = plt.subplots(figsize=(11, 6))
    sns.heatmap(pivot_df, annot=True, fmt=".0f", cmap="YlGnBu", cbar_kws={'label': 'Accumulation (mm)'}, ax=ax)
    ax.set_title("Singapore Monthly Rainfall Accumulation Heatmap (2017–2024)", fontweight="bold")
    ax.set_xlabel("Month")
    ax.set_ylabel("Year")
    plt.tight_layout()
    plt.savefig(fig_path / "fig3_year_month_heatmap.png")
    plt.close()

    # 4. Figure 4: Diurnal Hourly Occurrence and Intensity
    fig, ax1 = plt.subplots(figsize=(11, 5))
    color_freq = "#2166ac"
    color_int = "#b2182b"

    ax1.bar(hourly_pdf["hour"], hourly_pdf["wet_frequency_pct"], color=color_freq, alpha=0.65, label="Wet Interval Frequency (%)")
    ax1.set_xlabel("Hour of Day (SGT - Singapore Local Time)")
    ax1.set_ylabel("Wet Interval Frequency (%)", color=color_freq)
    ax1.tick_params(axis="y", labelcolor=color_freq)
    ax1.set_xticks(range(0, 24))

    ax2 = ax1.twinx()
    ax2.plot(hourly_pdf["hour"], hourly_pdf["mean_wet_intensity_mm"], color=color_int, marker="s", lw=2.5, label="Mean Wet Intensity (mm/5-min)")
    ax2.set_ylabel("Mean Wet Intensity (mm / 5-min)", color=color_int)
    ax2.tick_params(axis="y", labelcolor=color_int)
    ax2.grid(False)

    plt.title("Diurnal Rainfall Profile: Afternoon Convective Peak (SGT)", fontweight="bold")
    plt.tight_layout()
    plt.savefig(fig_path / "fig4_diurnal_hourly_rainfall.png")
    plt.close()

    # 5. Figure 5: Rainfall Intensity Distribution (Log scale)
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(intensity_pdf["quantile"] * 100, intensity_pdf["wet_intervals_only_mm"], marker="o", lw=2.2, color="#08519c")
    ax.set_title("Rainfall Intensity Percentile Curve (Wet Intervals Only)", fontweight="bold")
    ax.set_xlabel("Percentile (%)")
    ax.set_ylabel("Recorded 5-Minute Rainfall (mm)")
    ax.set_yscale("log")
    for _, r in intensity_pdf.iterrows():
        if r["quantile"] in (0.50, 0.90, 0.95, 0.99, 0.999):
            ax.annotate(f"{r['percentile']}: {r['wet_intervals_only_mm']:.2f}mm",
                        xy=(r["quantile"]*100, r["wet_intervals_only_mm"]),
                        xytext=(10, -5), textcoords="offset points", fontsize=8)
    ax.grid(True, which="both", ls="--", alpha=0.5)
    plt.tight_layout()
    plt.savefig(fig_path / "fig5_rainfall_intensity_distribution.png")
    plt.close()

    # 6. Figure 6: Spatial Rainfall Distribution Map
    fig, ax = plt.subplots(figsize=(10, 6))
    scatter = ax.scatter(
        spatial_pdf["longitude"],
        spatial_pdf["latitude"],
        c=spatial_pdf["mean_5min_rainfall_mm"] * 12, # convert to equivalent mm/hr rate
        s=spatial_pdf["valid_observations"] / 15000,
        cmap="Blues",
        edgecolors="black",
        linewidth=0.8,
        alpha=0.85
    )
    cbar = plt.colorbar(scatter, ax=ax)
    cbar.set_label("Mean Equivalent Rate (mm / hour)")
    ax.set_title("Spatial Rainfall Distribution across Singapore Monitoring Stations", fontweight="bold")
    ax.set_xlabel("Longitude (°E)")
    ax.set_ylabel("Latitude (°N)")
    # Annotate a few prominent stations
    for _, row in spatial_pdf.nlargest(6, "mean_5min_rainfall_mm").iterrows():
        ax.annotate(row["station_name"], (row["longitude"], row["latitude"]),
                    xytext=(5, 5), textcoords="offset points", fontsize=8, alpha=0.8)
    plt.tight_layout()
    plt.savefig(fig_path / "fig6_spatial_rainfall_distribution.png")
    plt.close()


def run_exploration(config_path: str = "config/project_config.yaml"):
    """Runs all Task 1 computations, exports summary tables and figures."""
    config = load_config(config_path)
    logger = get_logger("RainCastSG.Explore")

    spark = get_spark_session(
        app_name="RainCastSG-Explore",
        master=config["environment"]["spark_master"],
        driver_memory=config["environment"]["driver_memory"],
        shuffle_partitions=config["environment"]["shuffle_partitions"],
        timezone=config["project"]["timezone"],
        log_level=config["environment"]["log_level"]
    )

    clean_parquet_path = f"{config['data']['processed_dir']}/rainfall_clean"
    logger.info(f"Loading cleaned Parquet from {clean_parquet_path}...")
    df = spark.read.parquet(clean_parquet_path)

    total_records = df.count()
    logger.info(f"Loaded {total_records:,} cleaned records for Task 1 exploratory analysis.")

    analysis_out = Path(config["models"]["outputs_dir"]) / "analysis"
    analysis_out.mkdir(parents=True, exist_ok=True)

    # 1. Annual Summary
    logger.info("Computing Annual Rainfall variation...")
    annual_pdf = compute_annual_summary(df)
    annual_pdf.to_csv(analysis_out / "annual_summary.csv", index=False)

    # 2. Monthly and Seasonal Patterns
    logger.info("Computing Monthly and Seasonal patterns...")
    ym_pdf, monthly_pdf = compute_monthly_summary(df)
    ym_pdf.to_csv(analysis_out / "year_month_summary.csv", index=False)
    monthly_pdf.to_csv(analysis_out / "monthly_profile.csv", index=False)

    # 3. Diurnal Profiles
    logger.info("Computing Diurnal (hourly) profiles in SGT...")
    hourly_pdf = compute_diurnal_summary(df)
    hourly_pdf.to_csv(analysis_out / "hourly_diurnal_summary.csv", index=False)

    # 4. Intensity Distribution
    logger.info("Computing Rainfall Intensity percentiles...")
    intensity_pdf = compute_intensity_percentiles(df)
    intensity_pdf.to_csv(analysis_out / "intensity_percentiles.csv", index=False)

    # 5. Spatial Variation
    logger.info("Computing Station Spatial distribution...")
    spatial_pdf = compute_spatial_summary(df)
    spatial_pdf.to_csv(analysis_out / "spatial_summary.csv", index=False)

    # 6. Figures Generation
    logger.info(f"Rendering Task 1 visualization figures in {config['models']['figures_dir']}...")
    generate_figures(
        annual_pdf=annual_pdf,
        ym_pdf=ym_pdf,
        monthly_pdf=monthly_pdf,
        hourly_pdf=hourly_pdf,
        intensity_pdf=intensity_pdf,
        spatial_pdf=spatial_pdf,
        figures_dir=config["models"]["figures_dir"]
    )

    logger.info("Task 1 Exploratory Analysis completed successfully!")
    spark.stop()


def main():
    run_exploration()


if __name__ == "__main__":
    main()
