# RainCastSG: Singapore Rainfall Analysis and 30-Minute Rainfall Probability Prediction Using Apache Spark

[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![PySpark 3.5.3](https://img.shields.io/badge/PySpark-3.5.3-orange.svg)](https://spark.apache.org/)
[![License: Academic](https://img.shields.io/badge/License-Academic%20(NUS%20DSA5208)-green.svg)]()

**Course:** DSA5208 — Scalable Distributed Computing for Data Science  
**Institution:** National University of Singapore (NUS)  
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
└── models/                             # Saved MLlib PipelineModels & calibration parameters
```

---

## 4. Dataset Download and Setup

The raw Singapore historical rainfall datasets (Collection 2279 from data.gov.sg) contain **over 48.5 million 5-minute records** spanning 2017 through 2024, totaling **~8.05 GB** in uncompressed CSV format. Due to GitHub's file-size constraints and repository best practices, raw datasets and processed Parquet directories are intentionally excluded from Git tracking via `.gitignore`.

However, **all 10 trained Spark MLlib PipelineModels and their fitted Platt calibration parameters are committed directly in the `models/` directory**. You do not need to retrain models to evaluate performance or run prediction inference.

---

### 4.1 Official Dataset Source

Download the official annual rainfall datasets directly from the Singapore Government Open Data portal:
- **Collection URL:** [https://data.gov.sg/collections/2279/view](https://data.gov.sg/collections/2279/view)
- **Measurement Type:** `TB1 Rainfall 5 Minute Total F` (5-minute cumulative rainfall)
- **Unit:** Millimeters (`mm`)
- **Years Required:** 2017 to 2024 (8 annual files)

---

### 4.2 Download Workflow

You can obtain the datasets using either the automated script provided in this repository or via manual download:

#### Option A: Automated Download (Recommended)
Run the built-in API downloader to query data.gov.sg and download the files:
```powershell
# Download all 8 years (2017–2024):
python -m src.download --all

# Or download a single year (e.g. 2024 to test prediction inference):
python -m src.download --years 2024
```

#### Option B: Manual Web Download
1. Navigate to [https://data.gov.sg/collections/2279/view](https://data.gov.sg/collections/2279/view).
2. Download the annual rainfall CSV for each year from 2017 to 2024.
3. Place each downloaded CSV file into the `data/raw/` directory using the exact filename format specified below.

---

### 4.3 Expected Directory Layout & Filenames

The ingestion pipeline (`src/ingest.py`) strictly expects raw datasets to reside in `data/raw/` with the following naming convention:

```text
RainCastSG/
└── data/
    └── raw/
        ├── rainfall_2017.csv   (~1.01 GB, ~6.1M rows)
        ├── rainfall_2018.csv   (~1.01 GB, ~6.1M rows)
        ├── rainfall_2019.csv   (~1.01 GB, ~6.1M rows)
        ├── rainfall_2020.csv   (~1.01 GB, ~6.1M rows)
        ├── rainfall_2021.csv   (~1.01 GB, ~6.1M rows)
        ├── rainfall_2022.csv   (~1.01 GB, ~6.1M rows)
        ├── rainfall_2023.csv   (~1.00 GB, ~6.1M rows)
        └── rainfall_2024.csv   (~1.01 GB, ~6.1M rows)
```

Each raw CSV matches the standardized schema: `Timestamp`, `Station_ID`, `Station_Name`, `Latitude`, `Longitude`, `Measurement_Type`, `Unit`, and `Value`.

---

### 4.4 Running Pretrained Model Inference (No Model Retraining)

The repository includes pre-fitted models in `models/`. Running inference requires only the cleaned observations for the requested station and timestamp:

1. **Clone the repository and install dependencies:**
   ```powershell
   git clone https://github.com/AmitejSingh1/RainCastSG.git
   cd RainCastSG
   pip install -r requirements.txt
   ```

2. **Download the raw rainfall data** (or at minimum 2024 for testing):
   Place `rainfall_2024.csv` into `data/raw/` (or run `python -m src.download --years 2024`).

3. **Ingest and clean the raw data (Preprocessing prerequisite):**
   The prediction CLI (`src/predict.py`) extracts causal features from recent historical intervals stored in `data/processed/rainfall_clean`. Run ingestion to generate this cleaned store:
   ```powershell
   # Ingest and clean 2024 (or --all for full dataset):
   python -m src.ingest --years 2024
   ```
   > **Note:** Ingestion performs data quality auditing, type enforcement, duplicate resolution, and Parquet persistence. **It does NOT train any machine learning models.**

4. **Execute the prediction CLI with committed pretrained models:**
   Execute 30-minute rainfall exceedance probability estimation using the committed models:
   ```powershell
   # Pretrained Logistic Regression model (Threshold = 1.0 mm, Station S06):
   python -m src.predict --station-id S06 --timestamp "2024-11-07 16:45:00" --threshold 1.0 --model logistic_regression

   # Pretrained Random Forest model (Threshold = 1.0 mm, Station S06):
   python -m src.predict --station-id S06 --timestamp "2024-11-07 16:45:00" --threshold 1.0 --model random_forest
   ```

---

### 4.5 Pipeline Execution Matrix: Pretrained Inference vs. Full Pipeline Rebuild

To ensure clarity on which commands require model training and which use existing artifacts:

| Step / Workflow Stage | Command | Requires Model Training? | Required Input Artifacts | Output Artifacts |
|---|---|:---:|---|---|
| **Unit Verification** | `python -m pytest tests/ -v` | **No** | None (self-contained fixtures) | Test suite verification report |
| **Data Download** | `python -m src.download --all` | **No** | Internet connection to data.gov.sg | `data/raw/rainfall_<year>.csv` |
| **Ingestion & Cleaning** | `python -m src.ingest --all` | **No** | `data/raw/rainfall_<year>.csv` | `data/processed/rainfall_clean/` |
| **Pretrained Prediction CLI** | `python -m src.predict --station-id S06 ...` | **No** *(Uses committed models)* | `data/processed/rainfall_clean` + `models/` | JSON prediction response |
| **Exploratory Analysis (EDA)** | `python -m src.explore` | **No** | `data/processed/rainfall_clean/` | Figures 1–6 in `figures/`, summary CSVs |
| **Feature Store Generation** | `python -m src.features` | **No** | `data/processed/rainfall_clean/` | `data/processed/features/split=*` |
| **Model Training (Optional Re-fit)**| `python -m src.train --model logistic_regression` | **Yes** *(Retrains models)* | `data/processed/features/` | Updates `models/` PipelineModels |
| **Model Calibration** | `python -m src.calibrate --model logistic_regression` | **No** *(Fits Platt scaling on val logits)*| `data/processed/features/` + `models/` | Updates `models/calibration_*.json` |
| **Model Evaluation** | `python -m src.evaluate --model logistic_regression --split test`| **No** *(Evaluates saved models)* | `data/processed/features/` + `models/` | Evaluation metrics in `outputs/evaluation/` |

---

## 5. Quickstart & Step-by-Step Execution Guide

### 5.1 Environment Setup
```powershell
# 1. Activate Python virtual environment
.\.venv\Scripts\Activate.ps1

# 2. Verify Java and PySpark environment
python -c "import pyspark; from src.utils import get_spark_session; s = get_spark_session(); print('PySpark Ready:', s.version); s.stop()"
```

### 5.2 Automated Unit Testing
Run the complete automated unit test suite to verify software correctness, strict inequality threshold logic, and zero temporal leakage:
```powershell
python -m pytest tests/ -v
```
*(All 14 unit tests pass with 100% success).*

### 5.3 Data Pipeline: Ingestion & Quality Auditing
```powershell
# Ingest and clean all 8 years (2017 to 2024), persist Parquet, and compute quality audit:
python -m src.ingest --all
```
Outputs generated:
- `data/processed/rainfall_clean/year=2017` through `year=2024` (48,538,713 clean records)
- `outputs/quality/quality_audit_summary.csv` and `.json`
- `outputs/quality/station_coverage.csv`

### 5.4 Task 1: Exploratory Analysis of Singapore Rainfall Patterns
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

### 5.5 Task 2: Feature Engineering & Target Accumulation
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

## 6. Manual Model Training & Evaluation Workflow

> **Note on Manual Training:** As specified, model training is executed manually from the terminal when the machine is ready.

### 6.1 Training Models
```powershell
# Train Logistic Regression Baseline (Full dataset, all thresholds [0.0, 1.0, 2.0, 5.0, 10.0] mm):
python -m src.train --model logistic_regression

# Or train with deterministic sampling for fast experimentation (e.g. 10% sample):
python -m src.train --model logistic_regression --sample-fraction 0.10

# Train Random Forest Comparison Model:
python -m src.train --model random_forest --sample-fraction 0.10
```

### 6.2 Model Evaluation
```powershell
# Evaluate trained models on the 2023 Validation partition:
python -m src.evaluate --model logistic_regression --split val

# Evaluate on the 2024 Held-Out Test partition:
python -m src.evaluate --model logistic_regression --split test
```
Metrics reported: ROC-AUC, PR-AUC, Brier score, Baseline Brier score, Brier Skill Score, Accuracy, Precision, Recall, F1-score, and full Confusion Matrix.

### 6.3 Probability Calibration & Reliability Diagrams
```powershell
# Fit Platt scaling on 2023 validation data and generate reliability curves:
python -m src.calibrate --model logistic_regression
```
Outputs generated:
- `models/calibration_logistic_regression.json`
- `figures/fig8_calibration_logistic_regression.png`

### 6.4 Operational Prediction CLI Interface
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

## 7. Technical Outputs & Artifacts

All computed experimental outputs, models, and figures are persisted in their respective directories:
- `figures/`: Publication-quality analytical and model evaluation figures (Figures 1–6 for EDA; Figures 8–13 for model comparison, ROC/PR curves, event prevalence, and calibration diagrams).
- `outputs/quality/`: `quality_audit_summary.csv`, `download_manifest.json`, and `station_coverage.csv`.
- `outputs/analysis/`: Summary CSV tables across annual, monthly, diurnal, spatial, and intensity distributions.
- `outputs/evaluation/`: Test and validation evaluation CSVs/JSONs across all thresholds for Logistic Regression and Random Forest models.
- `models/`: Saved Spark MLlib `PipelineModel` directories and Platt scaling calibration parameters (`calibration_logistic_regression.json`, `calibration_random_forest.json`).

---

## 8. AI-Use Declaration

In accordance with NUS DSA5208 academic integrity guidelines, assistance from artificial intelligence tools (Google DeepMind's Antigravity coding assistant and OpenAI's ChatGPT) was utilized for environment configuration, PySpark pipeline development and debugging, unit testing scaffolding, and mathematical interpretation of probabilistic metrics. All data ingestion, exploratory analyses, feature engineering, model training logic, and empirical evaluations were independently executed and verified against the actual Singapore government datasets.
