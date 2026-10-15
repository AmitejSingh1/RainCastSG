"""Model training module for RainCastSG.

Trains Spark MLlib classifiers (Logistic Regression, Random Forest, GBT)
for 30-minute rainfall probability prediction across configurable thresholds.

NOTE: Manual execution requirement - this script is executed explicitly from
the terminal by the user when ready.
"""

import argparse
import json
import time
from pathlib import Path
from typing import List, Dict, Any

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
)
from pyspark.ml.classification import (
    LogisticRegression, RandomForestClassifier, GBTClassifier
)
import pyspark.sql.functions as F

from src.utils import get_logger, load_config, get_spark_session


def build_ml_pipeline(
    classifier,
    categorical_col: str = "station_id",
    numeric_cols: List[str] = None
) -> Pipeline:
    """Builds a complete, reproducible Spark MLlib Pipeline:

    StringIndexer -> OneHotEncoder -> VectorAssembler -> Classifier.
    """
    if numeric_cols is None:
        numeric_cols = [
            "longitude", "latitude",
            "hour_sin", "hour_cos", "month_sin", "month_cos",
            "day_of_year", "day_of_week",
            "rain_hist_5m", "rain_hist_15m", "rain_hist_30m", "rain_hist_60m",
            "rain_hist_mean_60m", "rain_hist_max_60m",
            "wet_intervals_past_60m", "rain_trend_15m"
        ]

    indexer = (
        StringIndexer(inputCol=categorical_col, outputCol="station_idx", handleInvalid="keep")
    )
    encoder = (
        OneHotEncoder(inputCol="station_idx", outputCol="station_ohe", handleInvalid="keep")
    )
    assembler = (
        VectorAssembler(
            inputCols=["station_ohe"] + numeric_cols,
            outputCol="features",
            handleInvalid="keep"
        )
    )

    pipeline = Pipeline(stages=[indexer, encoder, assembler, classifier])
    return pipeline


def train_single_model(
    train_df,
    model_type: str,
    threshold: float,
    config: Dict[str, Any],
    logger
):
    """Trains a classifier for a specific threshold and saves the fitted PipelineModel."""
    label_col = f"label_gt_{str(threshold).replace('.', '_')}mm"
    logger.info(f"--- Training {model_type.upper()} for threshold θ = {threshold} mm (label: {label_col}) ---")

    # Select and configure classifier
    if model_type == "logistic_regression":
        lr_conf = config["models"]["logistic_regression"]
        classifier = LogisticRegression(
            featuresCol="features",
            labelCol=label_col,
            predictionCol="prediction",
            probabilityCol="probability",
            rawPredictionCol="rawPrediction",
            maxIter=lr_conf["max_iter"],
            regParam=lr_conf["reg_param"],
            elasticNetParam=lr_conf["elastic_net_param"]
        )
    elif model_type == "random_forest":
        rf_conf = config["models"]["random_forest"]
        classifier = RandomForestClassifier(
            featuresCol="features",
            labelCol=label_col,
            predictionCol="prediction",
            probabilityCol="probability",
            rawPredictionCol="rawPrediction",
            numTrees=rf_conf["num_trees"],
            maxDepth=rf_conf["max_depth"],
            featureSubsetStrategy=rf_conf["feature_subset_strategy"],
            seed=config["project"]["seed"]
        )
    elif model_type == "gbt":
        gbt_conf = config["models"]["gbt"]
        classifier = GBTClassifier(
            featuresCol="features",
            labelCol=label_col,
            predictionCol="prediction",
            rawPredictionCol="rawPrediction",
            maxIter=gbt_conf["max_iter"],
            maxDepth=gbt_conf["max_depth"],
            stepSize=gbt_conf["step_size"],
            seed=config["project"]["seed"]
        )
    else:
        raise ValueError(f"Unsupported model type: {model_type}")

    pipeline = build_ml_pipeline(classifier)

    # Train model
    start_t = time.time()
    fitted_model = pipeline.fit(train_df)
    train_duration = time.time() - start_t
    logger.info(f"Fitted {model_type} for θ = {threshold} mm in {train_duration:.1f} seconds.")

    # Save PipelineModel
    save_dir = Path(config["models"]["save_dir"]) / f"{model_type}_threshold_{str(threshold).replace('.', '_')}"
    save_dir.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Saving model to {save_dir}...")
    fitted_model.write().overwrite().save(str(save_dir))

    # Save metadata
    meta = {
        "model_type": model_type,
        "threshold_mm": threshold,
        "label_col": label_col,
        "training_duration_seconds": round(train_duration, 2),
        "save_path": str(save_dir),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    meta_path = save_dir / "metadata.json"
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    return meta


def run_training(
    model_type: str = "logistic_regression",
    thresholds: List[float] = None,
    sample_fraction: float = 1.0,
    config_path: str = "config/project_config.yaml"
):
    """Entry point for manual model training."""
    config = load_config(config_path)
    logger = get_logger("RainCastSG.Train")

    if thresholds is None:
        thresholds = config["features"]["thresholds"]

    features_path = Path(config["data"]["processed_dir"]) / "features"
    if not features_path.exists():
        logger.error(f"Features dataset not found at {features_path}!")
        print(f"\n[ERROR] Missing features dataset: {features_path}")
        print("Please run feature engineering first: python -m src.features\n")
        return

    spark = get_spark_session(
        app_name=f"RainCastSG-Train-{model_type}",
        master=config["environment"]["spark_master"],
        driver_memory=config["environment"]["driver_memory"],
        shuffle_partitions=config["environment"]["shuffle_partitions"],
        timezone=config["project"]["timezone"],
        log_level=config["environment"]["log_level"]
    )

    logger.info(f"Loading features dataset from {features_path}...")
    df_features = spark.read.parquet(str(features_path))

    # Restrict strictly to training partition (2017-2022)
    train_df = df_features.filter(F.col("split") == "train")
    total_train_records = train_df.count()
    logger.info(f"Total training partition records (2017–2022): {total_train_records:,}")

    # Optional deterministic sampling for fast experimentation
    if sample_fraction < 1.0:
        logger.info(f"Applying deterministic sample fraction: {sample_fraction} (seed={config['project']['seed']})...")
        train_df = train_df.sample(withReplacement=False, fraction=sample_fraction, seed=config["project"]["seed"])
        sampled_count = train_df.count()
        logger.info(f"Sampled training records: {sampled_count:,}")

    train_df.cache()

    training_manifest = []
    models_to_train = [model_type] if model_type != "all" else ["logistic_regression", "random_forest"]

    for m in models_to_train:
        for th in thresholds:
            meta = train_single_model(
                train_df=train_df,
                model_type=m,
                threshold=th,
                config=config,
                logger=logger
            )
            training_manifest.append(meta)

    # Save training session summary
    summary_path = Path(config["models"]["outputs_dir"]) / "evaluation" / "training_session_manifest.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(training_manifest, f, indent=2)

    logger.info(f"Training session complete! Manifest saved to {summary_path}")
    spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Train Spark MLlib Rainfall Probability Models")
    parser.add_argument(
        "--model",
        type=str,
        default="logistic_regression",
        choices=["logistic_regression", "random_forest", "gbt", "all"],
        help="Classifier architecture to train (default: logistic_regression)"
    )
    parser.add_argument(
        "--thresholds",
        nargs="+",
        type=float,
        default=None,
        help="Rainfall thresholds in mm (e.g. 0.0 1.0 2.0 5.0 10.0)"
    )
    parser.add_argument(
        "--sample-fraction",
        type=float,
        default=1.0,
        help="Deterministic sampling fraction for development (e.g. 0.1 for 10%% sample, 1.0 for full data)"
    )
    args = parser.parse_args()

    run_training(
        model_type=args.model,
        thresholds=args.thresholds,
        sample_fraction=args.sample_fraction
    )


if __name__ == "__main__":
    main()
