"""Model evaluation module for RainCastSG.

Evaluates trained MLlib models on held-out validation (2023) or test (2024) partitions.
Computes ROC-AUC, PR-AUC, Brier score, Baseline Brier score, Precision, Recall, F1,
and Confusion Matrix across all supported rainfall thresholds.
"""

import argparse
import json
from pathlib import Path
from typing import List, Dict, Any
import numpy as np
import pandas as pd

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator, MulticlassClassificationEvaluator
)
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType

from src.utils import get_logger, load_config, get_spark_session


def evaluate_model_threshold(
    model,
    df_eval,
    threshold: float,
    label_col: str,
    logger
) -> Dict[str, Any]:
    """Generates predictions and computes rigorous probabilistic and classification metrics."""
    # Transform evaluation dataset
    predictions = model.transform(df_eval)

    # Extract probability of positive class (index 1 of vector)
    extract_prob_udf = F.udf(lambda v: float(v[1]) if v is not None and len(v) > 1 else 0.0, DoubleType())
    predictions = predictions.withColumn("prob_positive", extract_prob_udf(F.col("probability")))

    # 1. Prevalence
    total_eval = predictions.count()
    positive_count = predictions.filter(F.col(label_col) == 1.0).count()
    prevalence = positive_count / total_eval if total_eval > 0 else 0.0

    # 2. ROC-AUC & PR-AUC
    evaluator_roc = BinaryClassificationEvaluator(
        rawPredictionCol="probability",
        labelCol=label_col,
        metricName="areaUnderROC"
    )
    roc_auc = evaluator_roc.evaluate(predictions)

    evaluator_pr = BinaryClassificationEvaluator(
        rawPredictionCol="probability",
        labelCol=label_col,
        metricName="areaUnderPR"
    )
    pr_auc = evaluator_pr.evaluate(predictions)

    # 3. Brier Score: Mean squared error of predicted probabilities vs binary labels
    # Baseline Brier Score: Predicting constant historical prevalence
    brier_df = predictions.select(
        F.pow(F.col("prob_positive") - F.col(label_col), 2).alias("sq_err"),
        F.pow(F.lit(prevalence) - F.col(label_col), 2).alias("baseline_sq_err")
    ).agg(
        F.mean("sq_err").alias("brier_score"),
        F.mean("baseline_sq_err").alias("baseline_brier_score")
    ).first()

    brier_score = float(brier_df["brier_score"])
    baseline_brier = float(brier_df["baseline_brier_score"])

    # 4. Standard Classification Metrics (at default operating threshold 0.5)
    tp = predictions.filter((F.col("prediction") == 1.0) & (F.col(label_col) == 1.0)).count()
    fp = predictions.filter((F.col("prediction") == 1.0) & (F.col(label_col) == 0.0)).count()
    tn = predictions.filter((F.col("prediction") == 0.0) & (F.col(label_col) == 0.0)).count()
    fn = predictions.filter((F.col("prediction") == 0.0) & (F.col(label_col) == 1.0)).count()

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = (2 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
    accuracy = (tp + tn) / total_eval if total_eval > 0 else 0.0

    metrics = {
        "threshold_mm": threshold,
        "evaluation_records": total_eval,
        "positive_event_count": positive_count,
        "prevalence": round(prevalence, 4),
        "roc_auc": round(roc_auc, 4),
        "pr_auc": round(pr_auc, 4),
        "brier_score": round(brier_score, 5),
        "baseline_brier_score": round(baseline_brier, 5),
        "brier_skill_score": round(1.0 - (brier_score / baseline_brier), 4) if baseline_brier > 0 else 0.0,
        "accuracy": round(accuracy, 4),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1_score": round(f1, 4),
        "confusion_matrix": {"tp": tp, "fp": fp, "tn": tn, "fn": fn}
    }

    logger.info(
        f"θ = {threshold}mm | Prevalence: {prevalence:.2%} | ROC-AUC: {roc_auc:.4f} | "
        f"PR-AUC: {pr_auc:.4f} | Brier: {brier_score:.5f} (Base: {baseline_brier:.5f}) | "
        f"Recall: {recall:.2%} | Precision: {precision:.2%}"
    )

    return metrics


def run_evaluation(
    model_type: str = "logistic_regression",
    split: str = "val",
    thresholds: List[float] = None,
    config_path: str = "config/project_config.yaml"
):
    """Evaluates all trained models for the specified split."""
    config = load_config(config_path)
    logger = get_logger("RainCastSG.Evaluate")

    if thresholds is None:
        thresholds = config["features"]["thresholds"]

    features_path = Path(config["data"]["processed_dir"]) / "features"
    if not features_path.exists():
        logger.error(f"Features dataset not found at {features_path}!")
        return

    spark = get_spark_session(
        app_name=f"RainCastSG-Eval-{model_type}",
        master=config["environment"]["spark_master"],
        driver_memory=config["environment"]["driver_memory"],
        shuffle_partitions=config["environment"]["shuffle_partitions"],
        timezone=config["project"]["timezone"],
        log_level=config["environment"]["log_level"]
    )

    df_features = spark.read.parquet(str(features_path))
    eval_df = df_features.filter(F.col("split") == split)
    logger.info(f"Loaded {eval_df.count():,} records for evaluation split '{split}'.")
    eval_df.cache()

    evaluation_records = []
    models_dir = Path(config["models"]["save_dir"])

    for th in thresholds:
        th_str = str(th).replace('.', '_')
        model_path = models_dir / f"{model_type}_threshold_{th_str}"

        if not model_path.exists():
            logger.warning(f"Trained model not found at {model_path}. Skipping θ = {th} mm.")
            continue

        logger.info(f"Loading trained pipeline from {model_path}...")
        model = PipelineModel.load(str(model_path))
        label_col = f"label_gt_{th_str}mm"

        metrics = evaluate_model_threshold(
            model=model,
            df_eval=eval_df,
            threshold=th,
            label_col=label_col,
            logger=logger
        )
        metrics["model_type"] = model_type
        metrics["split"] = split
        evaluation_records.append(metrics)

    if evaluation_records:
        # Save evaluation results
        out_dir = Path(config["models"]["outputs_dir"]) / "evaluation"
        out_dir.mkdir(parents=True, exist_ok=True)

        json_out = out_dir / f"evaluation_{model_type}_{split}.json"
        with open(json_out, "w", encoding="utf-8") as f:
            json.dump(evaluation_records, f, indent=2)

        csv_out = out_dir / f"evaluation_{model_type}_{split}.csv"
        # Flatten confusion matrix for CSV
        flat_records = []
        for r in evaluation_records:
            item = dict(r)
            cm = item.pop("confusion_matrix")
            item.update({f"cm_{k}": v for k, v in cm.items()})
            flat_records.append(item)
        pd.DataFrame(flat_records).to_csv(csv_out, index=False)

        logger.info(f"Evaluation completed successfully! Reports saved to {json_out} and {csv_out}")

    spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Evaluate Trained Spark MLlib Models")
    parser.add_argument("--model", type=str, default="logistic_regression", choices=["logistic_regression", "random_forest", "gbt"])
    parser.add_argument("--split", type=str, default="val", choices=["val", "test", "train"])
    parser.add_argument("--thresholds", nargs="+", type=float, default=None)
    args = parser.parse_args()

    run_evaluation(
        model_type=args.model,
        split=args.split,
        thresholds=args.thresholds
    )


if __name__ == "__main__":
    main()
