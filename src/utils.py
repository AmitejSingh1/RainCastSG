"""Utility functions for RainCastSG: configuration, logging, and SparkSession management."""

import os
import sys
import logging
from pathlib import Path
from typing import Any, Dict
import yaml

# Ensure JAVA_HOME and HADOOP_HOME are set on Windows
def configure_environment() -> None:
    """Configures Java and Hadoop paths for Spark on Windows if unset."""
    if "JAVA_HOME" not in os.environ or not os.path.exists(os.environ["JAVA_HOME"]):
        default_jdk = r"C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot"
        if os.path.exists(default_jdk):
            os.environ["JAVA_HOME"] = default_jdk
            os.environ["PATH"] = os.path.join(default_jdk, "bin") + os.pathsep + os.environ.get("PATH", "")

    if "HADOOP_HOME" not in os.environ or not os.path.exists(os.environ["HADOOP_HOME"]):
        default_hadoop = r"C:\hadoop"
        if os.path.exists(default_hadoop):
            os.environ["HADOOP_HOME"] = default_hadoop
            os.environ["PATH"] = os.path.join(default_hadoop, "bin") + os.pathsep + os.environ.get("PATH", "")

    # Ensure Spark Python worker uses the current Python interpreter
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def load_config(config_path: str = "config/project_config.yaml") -> Dict[str, Any]:
    """Loads YAML project configuration."""
    resolved_path = Path(config_path).resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"Configuration file not found at: {resolved_path}")
    with open(resolved_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config


def get_logger(name: str = "RainCastSG", log_file: str = "logs/pipeline.log") -> logging.Logger:
    """Configures and returns a standard logger."""
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        # Console handler
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        # File handler
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger


def get_spark_session(
    app_name: str = "RainCastSG",
    master: str = "local[*]",
    driver_memory: str = "4g",
    shuffle_partitions: int = 16,
    timezone: str = "Asia/Singapore",
    log_level: str = "WARN"
):
    """Initializes and returns a configured SparkSession."""
    configure_environment()

    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", timezone)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.maxResultSize", "2g")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    return spark
