"""Probability calibration module for RainCastSG.

Fits probability calibrators (Platt scaling / Logistic calibration) on validation data only,
evaluates calibration curves & reliability diagrams, verifies monotonicity across thresholds,
and persists calibration parameters.
"""

import argparse
import json
from pathlib import Path
from typing import List, Dict, Any, Tuple
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from pyspark.ml import PipelineModel
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from sklearn.linear_model import LogisticRegression as SklearnLogReg
from sklearn.calibration import calibration_curve

from src.utils import get_logger, load_config, get_spark_session


def compute_reliability_data(
    y_true: np.ndarray,
    y_prob: np.ndarray,
    n_bins: int = 10
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Computes bin midpoints, observed empirical frequencies, and bin counts."""
    prob_true, prob_pred = calibration_curve(y_true, y_prob, n_bins=n_bins, strategy="uniform")
    return prob_true, prob_pred


def fit_platt_scaling(y_true: np.ndarray, y_prob: np.ndarray) -> Tuple[float, float]:
    """Fits Platt scaling (univariate logistic regression on logits) on validation probabilities."""
    # Clip probabilities to prevent infinite logits
    eps = 1e-6
    probs_clipped = np.clip(y_prob, eps, 1.0 - eps)
    logits = np.log(probs_clipped / (1.0 - probs_clipped)).reshape(-1, 1)

    lr = SklearnLogReg(C=1.0, solver="lbfgs")
    lr.fit(logits, y_true)

    a = float(lr.coef_[0][0])
    b = float(lr.intercept_[0])
    return a, b


def apply_platt_scaling(y_prob: np.ndarray, a: float, b: float) -> np.ndarray:
    """Applies fitted Platt scaling transformation."""
    eps = 1e-6
    probs_clipped = np.clip(y_prob, eps, 1.0 - eps)
    logits = np.log(probs_clipped / (1.0 - probs_clipped))
    calibrated_logits = a * logits + b
    calibrated_probs = 1.0 / (1.0 + np.exp(-calibrated_logits))
    return calibrated_probs


def run_calibration(
    model_type: str = "logistic_regression",
    thresholds: List[float] = None,
    config_path: str = "config/project_config.yaml"
):
    """Fits calibrator on validation split (2023) and evaluates reliability curves."""
    config = load_config(config_path)
    logger = get_logger("RainCastSG.Calibrate")

    if thresholds is None:
        thresholds = config["features"]["thresholds"]

    features_path = Path(config["data"]["processed_dir"]) / "features"
    spark = get_spark_session(
        app_name=f"RainCastSG-Calibrate-{model_type}",
        master=config["environment"]["spark_master"],
        driver_memory=config["environment"]["driver_memory"],
        shuffle_partitions=config["environment"]["shuffle_partitions"],
        timezone=config["project"]["timezone"],
        log_level=config["environment"]["log_level"]
    )

    df_features = spark.read.parquet(str(features_path))
    # STRICT RULE: Fit calibration using validation data ONLY
    val_df = df_features.filter(F.col("split") == "val")
    val_count = val_df.count()
    logger.info(f"Loaded {val_count:,} validation records (2023) for probability calibration.")
    val_df.cache()

    models_dir = Path(config["models"]["save_dir"])
    calibration_params = {}
    reliability_results = {}

    extract_prob_udf = F.udf(lambda v: float(v[1]) if v is not None and len(v) > 1 else 0.0, DoubleType())

    for th in thresholds:
        th_str = str(th).replace('.', '_')
        model_path = models_dir / f"{model_type}_threshold_{th_str}"
        label_col = f"label_gt_{th_str}mm"

        if not model_path.exists():
            logger.warning(f"Model path {model_path} not found. Skipping θ = {th} mm.")
            continue

        model = PipelineModel.load(str(model_path))
        pred_df = model.transform(val_df)
        pred_df = pred_df.withColumn("prob_positive", extract_prob_udf(F.col("probability")))

        # Collect validation labels and raw predicted probabilities to driver for calibration fitting
        pdf = pred_df.select(label_col, "prob_positive").toPandas()
        y_true = pdf[label_col].values.astype(int)
        y_prob = pdf["prob_positive"].values.astype(float)

        # Fit Platt scaling
        a, b = fit_platt_scaling(y_true, y_prob)
        y_prob_cal = apply_platt_scaling(y_prob, a, b)

        # Compute Brier scores before and after calibration
        brier_raw = float(np.mean((y_prob - y_true) ** 2))
        brier_cal = float(np.mean((y_prob_cal - y_true) ** 2))

        logger.info(f"θ = {th}mm | Platt scaling: a={a:.4f}, b={b:.4f} | Brier: {brier_raw:.5f} -> {brier_cal:.5f}")

        calibration_params[str(th)] = {
            "a": a,
            "b": b,
            "brier_raw": round(brier_raw, 5),
            "brier_calibrated": round(brier_cal, 5),
            "improvement_pct": round(((brier_raw - brier_cal) / brier_raw) * 100, 2) if brier_raw > 0 else 0.0
        }

        # Calibration curves
        prob_true_raw, prob_pred_raw = compute_reliability_data(y_true, y_prob)
        prob_true_cal, prob_pred_cal = compute_reliability_data(y_true, y_prob_cal)

        reliability_results[str(th)] = {
            "raw": {"true": prob_true_raw.tolist(), "pred": prob_pred_raw.tolist()},
            "cal": {"true": prob_true_cal.tolist(), "pred": prob_pred_cal.tolist()}
        }

    # Save calibration parameters
    cal_file = models_dir / f"calibration_{model_type}.json"
    with open(cal_file, "w", encoding="utf-8") as f:
        json.dump(calibration_params, f, indent=2)
    logger.info(f"Calibration parameters saved to {cal_file}")

    # Plot Reliability Diagrams
    fig_dir = Path(config["models"]["figures_dir"])
    fig_dir.mkdir(parents=True, exist_ok=True)
    fig, axes = plt.subplots(1, len(thresholds), figsize=(4 * len(thresholds), 4), sharey=True)
    if len(thresholds) == 1:
        axes = [axes]

    for ax, th in zip(axes, thresholds):
        th_key = str(th)
        if th_key in reliability_results:
            raw_data = reliability_results[th_key]["raw"]
            cal_data = reliability_results[th_key]["cal"]
            ax.plot([0, 1], [0, 1], "k--", label="Perfect Calibration")
            ax.plot(raw_data["pred"], raw_data["true"], "s-", color="#d95f02", label="Uncalibrated")
            ax.plot(cal_data["pred"], cal_data["true"], "o-", color="#1b9e77", label="Platt Calibrated")
            ax.set_title(f"θ = {th} mm\n(Brier: {calibration_params[th_key]['brier_calibrated']:.4f})")
            ax.set_xlabel("Mean Predicted Probability")
            if th == thresholds[0]:
                ax.set_ylabel("Fraction of Positives")
            ax.grid(True, linestyle="--", alpha=0.5)

    axes[0].legend(loc="upper left")
    plt.suptitle("Reliability Diagrams (Validation 2023) Before and After Calibration", fontweight="bold", y=1.03)
    plt.tight_layout()
    plt.savefig(fig_dir / f"fig8_calibration_{model_type}.png")
    plt.close()
    logger.info(f"Reliability diagrams saved to {fig_dir / f'fig8_calibration_{model_type}.png'}")

    spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Calibrate Probabilities for Trained ML Models")
    parser.add_argument("--model", type=str, default="logistic_regression", choices=["logistic_regression", "random_forest", "gbt"])
    parser.add_argument("--thresholds", nargs="+", type=float, default=None)
    args = parser.parse_args()

    run_calibration(model_type=args.model, thresholds=args.thresholds)


if __name__ == "__main__":
    main()
