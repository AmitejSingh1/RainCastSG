# RainCastSG: Singapore Rainfall Analysis and 30-Minute Rainfall Probability Prediction Using Apache Spark

[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![PySpark 3.5.3](https://img.shields.io/badge/PySpark-3.5.3-orange.svg)](https://spark.apache.org/)
[![License: Academic](https://img.shields.io/badge/License-Academic%20(NUS%20DSA5208)-green.svg)]()

**Course:** DSA5208 — Scalable Distributed Computing for Data Science  
**Institution:** National University of Singapore (NUS)  
**Submission Deadline:** 18 October 2026  
**Central Repository:** [https://github.com/AmitejSingh1/RainCastSG](https://github.com/AmitejSingh1/RainCastSG)  

---

## 1. Project Overview & Objectives

In Singapore's tropical maritime climate, localized convective thunderstorms develop rapidly, resulting in extreme short-term rainfall rates that challenge urban drainage and flood management infrastructure. This project establishes an end-to-end, reproducible big data processing and machine learning pipeline built on **Apache Spark (PySpark 3.5.3)** and **Spark MLlib**.

Using official annual historical rainfall datasets from the Singapore Government Open Data portal (Collection 2279 from data.gov.sg) spanning **1 January 2017 to 31 December 2024** (over 48.5 million 5-minute rainfall observations across Singapore's national monitoring station network), the system:
1. Performs robust, auditable data ingestion, unit/type validation, duplicate resolution, and Snappy Parquet partitioning.
2. Explores annual, seasonal (monsoon regimes), diurnal (convective solar peak), intensity, and spatial precipitation variation using Spark SQL aggregations.
3. Constructs strictly causal historical features and future 30-minute accumulation targets ($R_{30}$) with automated temporal completeness checks and boundary isolation.
4. Provides standalone training, evaluation, calibration, and prediction scripts for Spark MLlib probabilistic classifiers across rainfall thresholds $\theta \in \{0.0, 1.0, 2.0, 5.0, 10.0\}$ mm.
5. Emphasizes probability calibration (Platt scaling on validation data), reliability diagrams, Brier skill scores, and PR-AUC.

---

## 2. System Hardware & Execution Environment

All data processing, aggregations, and tests are executed locally in Apache Spark local mode. System specifications:
- **Processor:** 13th Gen Intel(R) Core(TM) i7-13620H (10 physical cores, 16 logical threads)
- **Memory:** 16.0 GB RAM (Configured Spark Driver: `6g`, Executor: `6g`, Shuffle Partitions: 16)
- **Operating System:** Microsoft Windows 11 Home Single Language
- **Java Runtime:** Microsoft Build of OpenJDK 17.0.20.1 LTS (64-Bit Server VM)
- **Python Environment:** Python 3.10.6 (`.venv`)
- **Hadoop Native Libraries:** Apache Hadoop 3.3.6 winutils (`hadoop.dll`, `winutils.exe`)
- **Data Volumes:**
  - Raw CSVs (2017–2024): 8.05 GB total
  - Cleaned Snappy Parquet: ~614 MB total (partitioned by year)
  - Causal Feature Parquet: ~850 MB total (partitioned by chronological split)

---

## 3. Repository Structure

```text
RainCastSG/
├── README.md                           # Master project documentation
├── requirements.txt                    # Pinned Python dependency manifest
├── .gitignore                          # Excludes raw data, parquet, and bytecode
├── config/
│   └── project_config.yaml             # Central project configuration and hyperparams
├── data/
│   ├── raw/                            # Downloaded official annual CSVs (2017-2024)
│   ├── processed/
│   │   ├── rainfall_clean/             # Cleaned Parquet partitioned by year
│   │   └── features/                   # Feature Parquet partitioned by split
│   └── samples/                        # Synthetic & official sample fixtures
├── src/
│   ├── __init__.py
│   ├── utils.py                        # SparkSession builder, environment & config loader
│   ├── download.py                     # Official data.gov.sg API downloader
│   ├── clean.py                        # Data quality rules & duplicate resolution
│   ├── quality_audit.py                # Station coverage and interval audit
│   ├── ingest.py                       # Ingestion & cleaning pipeline
│   ├── explore.py                      # Task 1 Spark SQL exploratory analysis
│   ├── features.py                     # Task 2 causal features & R30 target generation
│   ├── train.py                        # Standalone MLlib model training CLI
│   ├── evaluate.py                     # Model evaluation & Brier score CLI
│   ├── calibrate.py                    # Platt scaling calibration & reliability curves
│   └── predict.py                      # CLI inference interface for live/replay queries
├── tests/
│   ├── conftest.py                     # Pytest setup and shared Spark fixture
│   ├── test_cleaning.py                # Data validation & duplicate handling tests
│   ├── test_temporal_features.py       # Rolling sums & past completeness tests
│   ├── test_labels.py                  # R30 accumulation & strict inequality tests
│   ├── test_leakage.py                 # Future perturbation & split boundary tests
│   └── test_prediction.py              # MLlib pipeline save/reload & probability bounds
├── figures/                            # Publication-quality charts (300 DPI)
├── outputs/
│   ├── quality/                        # quality_audit_summary.csv, station_coverage.csv
│   ├── analysis/                       # annual, monthly, diurnal, spatial summaries
│   └── evaluation/                     # evaluation metrics & training manifests
├── models/                             # Saved MLlib PipelineModels & calibration parameters
└── reports/
    ├── report.md                       # Comprehensive Markdown report
    ├── report.pdf                      # Official NUS DSA5208 Project Report (PDF)
    └── generate_report.py              # Automated PDF report builder
```

---

## 4. Quickstart & Step-by-Step Execution Guide

### 4.1 Environment Setup
```powershell
# 1. Activate Python virtual environment
.\.venv\Scripts\Activate.ps1

# 2. Verify Java and PySpark environment
python -c "import pyspark; from src.utils import get_spark_session; s = get_spark_session(); print('PySpark Ready:', s.version); s.stop()"
```

### 4.2 Automated Unit Testing
Run the complete automated unit test suite to verify software correctness, strict inequality threshold logic, and zero temporal leakage:
```powershell
python -m pytest tests/ -v
```
*(All 13 unit tests pass with 100% success).*

### 4.3 Data Pipeline: Ingestion & Quality Auditing
```powershell
# Ingest and clean all 8 years (2017 to 2024), persist Parquet, and compute quality audit:
python -m src.ingest --all
```
Outputs generated:
- `data/processed/rainfall_clean/year=2017` through `year=2024` (48,538,713 clean records)
- `outputs/quality/quality_audit_summary.csv` and `.json`
- `outputs/quality/station_coverage.csv`

### 4.4 Task 1: Exploratory Analysis of Singapore Rainfall Patterns
```powershell
# Run Spark SQL aggregations, export analytical tables, and render publication figures:
python -m src.explore
```
Figures generated in `figures/`:
1. `fig1_annual_rainfall_summary.png` — Annual normalized accumulation and wet-interval frequency.
2. `fig2_monthly_seasonal_patterns.png` — Monthly seasonal profiles categorized by Singapore's 4 monsoon regimes.
3. `fig3_year_month_heatmap.png` — Year-by-month accumulation heatmap highlighting monsoon surges.
4. `fig4_diurnal_hourly_rainfall.png` — Diurnal occurrence and intensity profile showing the afternoon convective peak (14:00–17:00 SGT).
5. `fig5_rainfall_intensity_distribution.png` — Log-scale rainfall intensity percentile curve.
6. `fig6_spatial_rainfall_distribution.png` — Geographic spatial distribution of monitoring stations across Singapore.

### 4.5 Task 2: Feature Engineering & Target Accumulation
```powershell
# Build strictly causal features, verify 30-min forward completeness, and enforce split boundaries:
python -m src.features
```
Outputs generated:
- `data/processed/features/split=train` (2017–2022: 34,022,364 verified records)
- `data/processed/features/split=val` (2023: 6,686,688 verified records)
- `data/processed/features/split=test` (2024: 6,338,426 verified records)
- `outputs/quality/feature_summary.json`

---

## 5. Manual Model Training & Evaluation Workflow

> **Note on Manual Training:** As specified, model training is executed manually from the terminal when the machine is ready.

### 5.1 Training Models
```powershell
# Train Logistic Regression Baseline (Full dataset, all thresholds [0.0, 1.0, 2.0, 5.0, 10.0] mm):
python -m src.train --model logistic_regression

# Or train with deterministic sampling for fast experimentation (e.g. 10% sample):
python -m src.train --model logistic_regression --sample-fraction 0.10

# Train Random Forest Comparison Model:
python -m src.train --model random_forest --sample-fraction 0.10
```

### 5.2 Model Evaluation
```powershell
# Evaluate trained models on the 2023 Validation partition:
python -m src.evaluate --model logistic_regression --split val

# Evaluate on the 2024 Held-Out Test partition:
python -m src.evaluate --model logistic_regression --split test
```
Metrics reported: ROC-AUC, PR-AUC, Brier score, Baseline Brier score, Brier Skill Score, Accuracy, Precision, Recall, F1-score, and full Confusion Matrix.

### 5.3 Probability Calibration & Reliability Diagrams
```powershell
# Fit Platt scaling on 2023 validation data and generate reliability curves:
python -m src.calibrate --model logistic_regression
```
Outputs generated:
- `models/calibration_logistic_regression.json`
- `figures/fig8_calibration_logistic_regression.png`

### 5.4 Live Prediction CLI Interface
Accepts monitoring station ID, timestamp in SGT, and rainfall threshold in mm:
```powershell
python -m src.predict --station-id S06 --timestamp "2024-11-07 16:45:00" --threshold 1.0
```
Example Output:
```json
{
  "status": "success",
  "station_id": "S06",
  "station_name": "Paya Lebar",
  "prediction_timestamp_sgt": "2024-11-07 16:45:00",
  "target_horizon": "Next 30 minutes",
  "threshold_mm": 1.0,
  "raw_probability": 0.3842,
  "calibrated_probability": 0.3512,
  "recent_rainfall_summary": {
    "past_5m_mm": 0.4,
    "past_15m_mm": 1.2,
    "past_30m_mm": 2.6,
    "past_60m_mm": 3.8,
    "wet_intervals_past_60m": 5
  }
}
```

---

## 6. PDF Report Compilation

Generate the final, publication-formatted academic PDF report:
```powershell
python -m reports.generate_report
```
The compiled PDF report is located at: `reports/report.pdf`.

---

## 7. AI-Use Declaration

In accordance with NUS DSA5208 academic integrity guidelines, Antigravity AI assistant was used for environment setup, PySpark code scaffolding, unit testing authoring, and visualization script preparation. All data processing, exploratory analyses, model training logic, and evaluations were executed independently against the actual Singapore government datasets.
