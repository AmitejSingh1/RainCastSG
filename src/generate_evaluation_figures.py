"""Evaluation figures generator for RainCastSG.

Generates publication-quality model evaluation figures:
1. Fig 10: ROC Curves comparing Logistic Regression vs Random Forest across all 5 thresholds.
2. Fig 11: Precision-Recall Curves comparing both models with prevalence baselines.
3. Fig 12: Grouped Bar Charts comparing ROC-AUC and PR-AUC across thresholds.
4. Fig 13: Event Prevalence & Class Imbalance distribution.

Caches computed test curve points to outputs/evaluation/test_curve_points.json
to avoid re-evaluating Spark models on subsequent runs.
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Tuple, Any
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import roc_curve, precision_recall_curve

from pyspark.ml import PipelineModel
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType

from src.utils import get_logger, load_config, get_spark_session


def load_authoritative_metrics():
    """Loads authoritative full-test metrics from evaluation CSVs."""
    lr_csv = Path("outputs/evaluation/evaluation_logistic_regression_test.csv")
    rf_csv = Path("outputs/evaluation/evaluation_random_forest_test.csv")
    df_lr = pd.read_csv(lr_csv)
    df_rf = pd.read_csv(rf_csv)
    return df_lr, df_rf


def extract_or_load_curve_points(
    config_path: str = "config/project_config.yaml",
    cache_path: str = "outputs/evaluation/test_curve_points.json",
    sample_fraction: float = 0.05
) -> Dict[str, Any]:
    """Scores a representative sample of test split to compute ROC and PR curve points."""
    cache_file = Path(cache_path)
    if cache_file.exists():
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    config = load_config(config_path)
    logger = get_logger("RainCastSG.Figures")
    thresholds = config["features"]["thresholds"]
    models_dir = Path(config["models"]["save_dir"])

    spark = get_spark_session(
        app_name="RainCastSG-CurvePoints",
        master=config["environment"]["spark_master"],
        driver_memory=config["environment"]["driver_memory"],
        shuffle_partitions=8,
        timezone=config["project"]["timezone"],
        log_level="ERROR"
    )

    logger.info(f"Loading 2024 test partition for curve extraction (sample_fraction={sample_fraction})...")
    features_path = Path(config["data"]["processed_dir"]) / "features"
    df_test = spark.read.parquet(str(features_path)).filter(F.col("split") == "test")
    sample_df = df_test.sample(withReplacement=False, fraction=sample_fraction, seed=config["project"]["seed"]).cache()
    n_sample = sample_df.count()
    logger.info(f"Sampled {n_sample:,} test observations for curve extraction.")

    extract_prob_udf = F.udf(lambda v: float(v[1]) if v is not None and len(v) > 1 else 0.0, DoubleType())
    curve_data = {}

    for th in thresholds:
        th_str = str(th).replace('.', '_')
        label_col = f"label_gt_{th_str}mm"
        curve_data[str(th)] = {}

        for model_name in ["logistic_regression", "random_forest"]:
            model_path = models_dir / f"{model_name}_threshold_{th_str}"
            if not model_path.exists():
                logger.warning(f"Model path {model_path} not found.")
                continue

            logger.info(f"Scoring {model_name} at theta = {th} mm...")
            model = PipelineModel.load(str(model_path))
            pred_df = model.transform(sample_df).withColumn("prob_positive", extract_prob_udf(F.col("probability")))
            pdf = pred_df.select(label_col, "prob_positive").toPandas()

            y_true = pdf[label_col].values.astype(int)
            y_prob = pdf["prob_positive"].values.astype(float)

            # Compute ROC curve
            fpr, tpr, _ = roc_curve(y_true, y_prob)
            # Compute Precision-Recall curve
            precision, recall, _ = precision_recall_curve(y_true, y_prob)

            # Downsample points for efficient storage and clean plotting if large
            max_points = 500
            if len(fpr) > max_points:
                indices = np.linspace(0, len(fpr) - 1, max_points, dtype=int)
                fpr = fpr[indices]
                tpr = tpr[indices]
            if len(recall) > max_points:
                indices = np.linspace(0, len(recall) - 1, max_points, dtype=int)
                recall = recall[indices]
                precision = precision[indices]

            curve_data[str(th)][model_name] = {
                "fpr": [round(float(x), 5) for x in fpr],
                "tpr": [round(float(x), 5) for x in tpr],
                "recall": [round(float(x), 5) for x in recall],
                "precision": [round(float(x), 5) for x in precision]
            }

    spark.stop()

    cache_file.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(curve_data, f, indent=2)
    logger.info(f"Saved curve points to {cache_file}")

    return curve_data


def plot_roc_curves(curve_data: Dict[str, Any], df_lr: pd.DataFrame, df_rf: pd.DataFrame, out_path: Path):
    """Plot Fig 10: 5-panel ROC curves comparing Logistic Regression and Random Forest."""
    thresholds = [0.0, 1.0, 2.0, 5.0, 10.0]
    fig, axes = plt.subplots(1, 5, figsize=(20, 4.3), sharey=True)

    for i, th in enumerate(thresholds):
        ax = axes[i]
        th_key = str(th)
        lr_row = df_lr[df_lr["threshold_mm"] == th].iloc[0]
        rf_row = df_rf[df_rf["threshold_mm"] == th].iloc[0]

        # Random chance diagonal
        ax.plot([0, 1], [0, 1], linestyle="--", color="#555555", lw=1.2, label="Chance (AUC=0.50)")

        if th_key in curve_data:
            # Logistic Regression
            if "logistic_regression" in curve_data[th_key]:
                data = curve_data[th_key]["logistic_regression"]
                ax.plot(data["fpr"], data["tpr"], color="#1f77b4", lw=2,
                        label=f"LR (AUC={lr_row['roc_auc']:.4f})")

            # Random Forest
            if "random_forest" in curve_data[th_key]:
                data = curve_data[th_key]["random_forest"]
                ax.plot(data["fpr"], data["tpr"], color="#2ca02c", lw=2,
                        label=f"RF (AUC={rf_row['roc_auc']:.4f})")

        ax.set_title(f"Threshold $\\theta > {th}$ mm", fontsize=11, fontweight="bold")
        ax.set_xlabel("False Positive Rate (1 - Specificity)", fontsize=9.5)
        if i == 0:
            ax.set_ylabel("True Positive Rate (Sensitivity)", fontsize=9.5)
        ax.set_xlim([-0.02, 1.02])
        ax.set_ylim([-0.02, 1.02])
        ax.grid(True, linestyle=":", alpha=0.6)
        ax.legend(loc="lower right", fontsize=8.5, framealpha=0.9)

    plt.suptitle("Figure 10: Receiver Operating Characteristic (ROC) Curves on 2024 Held-Out Test Set (6.3M records)",
                 fontsize=12.5, fontweight="bold", y=1.03)
    plt.tight_layout()
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Generated: {out_path}")


def plot_pr_curves(curve_data: Dict[str, Any], df_lr: pd.DataFrame, df_rf: pd.DataFrame, out_path: Path):
    """Plot Fig 11: 5-panel Precision-Recall curves comparing both models with prevalence baseline."""
    thresholds = [0.0, 1.0, 2.0, 5.0, 10.0]
    fig, axes = plt.subplots(1, 5, figsize=(20, 4.3), sharey=True)

    for i, th in enumerate(thresholds):
        ax = axes[i]
        th_key = str(th)
        lr_row = df_lr[df_lr["threshold_mm"] == th].iloc[0]
        rf_row = df_rf[df_rf["threshold_mm"] == th].iloc[0]
        prevalence = lr_row["prevalence"]

        # Horizontal baseline at empirical prevalence
        ax.axhline(y=prevalence, linestyle="--", color="#d62728", lw=1.2,
                   label=f"Baseline ($\\pi$={prevalence:.4f})")

        if th_key in curve_data:
            # Logistic Regression
            if "logistic_regression" in curve_data[th_key]:
                data = curve_data[th_key]["logistic_regression"]
                ax.plot(data["recall"], data["precision"], color="#1f77b4", lw=2,
                        label=f"LR (PR-AUC={lr_row['pr_auc']:.4f})")

            # Random Forest
            if "random_forest" in curve_data[th_key]:
                data = curve_data[th_key]["random_forest"]
                ax.plot(data["recall"], data["precision"], color="#2ca02c", lw=2,
                        label=f"RF (PR-AUC={rf_row['pr_auc']:.4f})")

        ax.set_title(f"Threshold $\\theta > {th}$ mm", fontsize=11, fontweight="bold")
        ax.set_xlabel("Recall (Sensitivity)", fontsize=9.5)
        if i == 0:
            ax.set_ylabel("Precision (Positive Predictive Value)", fontsize=9.5)
        ax.set_xlim([-0.02, 1.02])
        ax.set_ylim([-0.02, 1.02])
        ax.grid(True, linestyle=":", alpha=0.6)
        ax.legend(loc="upper right", fontsize=8.5, framealpha=0.9)

    plt.suptitle("Figure 11: Precision-Recall (PR) Curves on 2024 Held-Out Test Set (6.3M records)",
                 fontsize=12.5, fontweight="bold", y=1.03)
    plt.tight_layout()
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Generated: {out_path}")


def plot_model_comparison_bars(df_lr: pd.DataFrame, df_rf: pd.DataFrame, out_path: Path):
    """Plot Fig 12: Grouped bar charts comparing ROC-AUC and PR-AUC across all thresholds."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 4.8))
    thresholds = [0.0, 1.0, 2.0, 5.0, 10.0]
    th_labels = [f"> {th} mm" for th in thresholds]
    x = np.arange(len(thresholds))
    width = 0.35

    # Panel A: ROC-AUC
    lr_roc = [df_lr[df_lr["threshold_mm"] == th].iloc[0]["roc_auc"] for th in thresholds]
    rf_roc = [df_rf[df_rf["threshold_mm"] == th].iloc[0]["roc_auc"] for th in thresholds]

    rects1 = ax1.bar(x - width/2, lr_roc, width, label="Logistic Regression", color="#1f77b4", edgecolor="black", alpha=0.9)
    rects2 = ax1.bar(x + width/2, rf_roc, width, label="Random Forest (50T)", color="#2ca02c", edgecolor="black", alpha=0.9)

    ax1.set_title("(A) Area Under ROC Curve (ROC-AUC)", fontsize=11, fontweight="bold")
    ax1.set_ylabel("ROC-AUC", fontsize=10)
    ax1.set_xticks(x)
    ax1.set_xticklabels(th_labels, fontsize=9.5)
    ax1.set_ylim([0.75, 1.00])
    ax1.axhline(0.5, color="gray", linestyle="--", lw=1)
    ax1.legend(loc="lower right", fontsize=9)
    ax1.grid(True, axis="y", linestyle=":", alpha=0.6)

    # Annotate bars
    for rect in rects1:
        h = rect.get_height()
        ax1.annotate(f"{h:.3f}", xy=(rect.get_x() + rect.get_width()/2, h), xytext=(0, 2),
                     textcoords="offset points", ha="center", va="bottom", fontsize=7.5)
    for rect in rects2:
        h = rect.get_height()
        ax1.annotate(f"{h:.3f}", xy=(rect.get_x() + rect.get_width()/2, h), xytext=(0, 2),
                     textcoords="offset points", ha="center", va="bottom", fontsize=7.5)

    # Panel B: PR-AUC
    lr_pr = [df_lr[df_lr["threshold_mm"] == th].iloc[0]["pr_auc"] for th in thresholds]
    rf_pr = [df_rf[df_rf["threshold_mm"] == th].iloc[0]["pr_auc"] for th in thresholds]
    prevs = [df_lr[df_lr["threshold_mm"] == th].iloc[0]["prevalence"] for th in thresholds]

    rects3 = ax2.bar(x - width/2, lr_pr, width, label="Logistic Regression", color="#1f77b4", edgecolor="black", alpha=0.9)
    rects4 = ax2.bar(x + width/2, rf_pr, width, label="Random Forest (50T)", color="#2ca02c", edgecolor="black", alpha=0.9)

    # Plot baseline markers
    ax2.plot(x, prevs, "ro--", label="Random Prevalence Baseline", markersize=5, lw=1.2)

    ax2.set_title("(B) Area Under Precision-Recall Curve (PR-AUC)", fontsize=11, fontweight="bold")
    ax2.set_ylabel("PR-AUC", fontsize=10)
    ax2.set_xticks(x)
    ax2.set_xticklabels(th_labels, fontsize=9.5)
    ax2.set_ylim([0.0, 0.75])
    ax2.legend(loc="upper right", fontsize=9)
    ax2.grid(True, axis="y", linestyle=":", alpha=0.6)

    for rect in rects3:
        h = rect.get_height()
        ax2.annotate(f"{h:.3f}", xy=(rect.get_x() + rect.get_width()/2, h), xytext=(0, 2),
                     textcoords="offset points", ha="center", va="bottom", fontsize=7.5)
    for rect in rects4:
        h = rect.get_height()
        ax2.annotate(f"{h:.3f}", xy=(rect.get_x() + rect.get_width()/2, h), xytext=(0, 2),
                     textcoords="offset points", ha="center", va="bottom", fontsize=7.5)

    plt.suptitle("Figure 12: Model Performance Comparison Across Precipitation Exceedance Thresholds (2024 Test Set)",
                 fontsize=12, fontweight="bold", y=1.02)
    plt.tight_layout()
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Generated: {out_path}")


def plot_event_prevalence(df_lr: pd.DataFrame, out_path: Path):
    """Plot Fig 13: Event prevalence across thresholds highlighting class imbalance."""
    fig, ax = plt.subplots(figsize=(7.5, 4.2))
    thresholds = [0.0, 1.0, 2.0, 5.0, 10.0]
    th_labels = [f"> {th} mm" for th in thresholds]
    prev_pct = [df_lr[df_lr["threshold_mm"] == th].iloc[0]["prevalence"] * 100 for th in thresholds]
    counts = [df_lr[df_lr["threshold_mm"] == th].iloc[0]["positive_event_count"] for th in thresholds]
    total = df_lr.iloc[0]["evaluation_records"]

    colors = ["#4292c6", "#2171b5", "#08519c", "#08306b", "#4a148c"]
    bars = ax.bar(th_labels, prev_pct, color=colors, edgecolor="black", width=0.55, alpha=0.9)

    ax.set_title("Figure 13: Positive Event Prevalence by Threshold (2024 Held-Out Test Set, N=6,338,426)",
                 fontsize=11, fontweight="bold")
    ax.set_xlabel("Rainfall Exceedance Threshold (30-Minute Accumulation $R_{30}$)", fontsize=10)
    ax.set_ylabel("Positive Event Prevalence (%)", fontsize=10)
    ax.set_ylim([0, 7.5])
    ax.grid(True, axis="y", linestyle=":", alpha=0.6)

    for bar, count, pct in zip(bars, counts, prev_pct):
        y = bar.get_height()
        ax.annotate(f"{pct:.2f}%\n({count:,})",
                    xy=(bar.get_x() + bar.get_width()/2, y),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha="center", va="bottom", fontsize=8.5, fontweight="bold")

    plt.tight_layout()
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Generated: {out_path}")


def main():
    df_lr, df_rf = load_authoritative_metrics()
    curve_data = extract_or_load_curve_points(sample_fraction=0.05)

    fig_dir = Path("figures")
    fig_dir.mkdir(parents=True, exist_ok=True)

    plot_roc_curves(curve_data, df_lr, df_rf, fig_dir / "fig10_roc_curves.png")
    plot_pr_curves(curve_data, df_lr, df_rf, fig_dir / "fig11_precision_recall_curves.png")
    plot_model_comparison_bars(df_lr, df_rf, fig_dir / "fig12_model_comparison_bars.png")
    plot_event_prevalence(df_lr, fig_dir / "fig13_event_prevalence.png")


if __name__ == "__main__":
    main()
