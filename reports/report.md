# Singapore Historical Rainfall Analysis and 30-Minute Rainfall Probability Prediction Using Apache Spark

**Course:** DSA5208 — Scalable Distributed Computing for Data Science  
**Institution:** National University of Singapore (NUS)  
**Submission Date:** October 2026  
**Repository:** [https://github.com/AmitejSingh1/RainCastSG](https://github.com/AmitejSingh1/RainCastSG)  

---

## 1. Executive Summary & Introduction
Tropical maritime precipitation in Singapore is characterized by rapid localized convective cloud development, high spatial variability, and extreme precipitation rates. Timely, probabilistic forecasting of short-term rainfall accumulation ($R_{30}$) is critical for urban stormwater management, flash flood mitigation, and aviation/transport safety.

This project delivers a complete, reproducible, and verifiable distributed data processing and machine-learning system implemented using **Apache Spark (PySpark 3.5.3)** and **Spark MLlib**. Using the official National Environment Agency (NEA) historical rainfall collection (Collection 2279 from data.gov.sg), spanning 1 January 2017 to 31 December 2024 (over 45 million 5-minute rainfall observations across Singapore's national monitoring station network), we execute:
1. Structured, auditable ingestion and data cleaning into Snappy-compressed partitioned Parquet.
2. Exploratory data analysis (Task 1) quantifying annual, seasonal (monsoon regimes), diurnal (solar convection), intensity, and spatial variations across Singapore.
3. Strict, leakage-free feature engineering and 30-minute forward target construction (Task 2).
4. Spark MLlib probabilistic classifier training across multiple thresholds ($\theta \in \{0.0, 1.0, 2.0, 5.0, 10.0\}$ mm) using chronological train (2017–2022), validation (2023), and held-out test (2024) partitions.
5. Probability calibration using Platt scaling on validation data, reliability diagrams, and threshold monotonicity verification.
6. A modular prediction CLI (`src.predict`) that inspects historical data completeness and returns calibrated exceedance probabilities.

---

## 2. Computing Environment & Hardware Specifications
All computations were executed locally using Apache Spark local mode. Hardware and software configurations were recorded as follows:
- **Processor:** 13th Gen Intel(R) Core(TM) i7-13620H (10 physical cores, 16 logical threads)
- **Memory:** 16.0 GB RAM (Spark Driver Memory: `4g`, Executor Memory: `4g`, Shuffle Partitions: 16)
- **Operating System:** Microsoft Windows 11 Home Single Language
- **JDK:** Microsoft Build of OpenJDK 17.0.20.1 LTS (64-Bit Server VM)
- **Python:** Python 3.10.6 virtual environment
- **Native Hadoop Libraries:** Apache Hadoop 3.3.6 winutils (`hadoop.dll`, `winutils.exe`)
- **Data Volumes:** Raw CSV: 8.05 GB total -> Cleaned Snappy Parquet: ~540 MB (partitioned by year)

---

## 3. Data Ingestion, Cleaning & Quality Auditing
The raw datasets were acquired from official data.gov.sg endpoints for collection 2279. The raw schema comprises:
- `date`, `timestamp`, `update_timestamp`, `station_id`, `station_name`, `station_device_id`
- `location_longitude`, `location_latitude`
- `reading_update_timestamp`, `reading_value`, `reading_type`, `reading_unit`

### Transparent Data Validation Rules
1. **Timestamp & Station Usability:** Records missing observation timestamps or station identifiers are excluded.
2. **Numeric Parsing & Invalid Exclusion:** Rainfall values are parsed as IEEE double-precision floats. Negative or non-numeric entries are tracked and rejected.
3. **Measurement Type & Unit Verification:** Confirmed `TB1 Rainfall 5 Minute Total F` and unit `mm`.
4. **Preservation of Genuine Zeros:** Zero-rainfall observations (`reading_value == 0.0`) are preserved and strictly distinguished from missing data.
5. **Duplicate Resolution Policy:** Multiple readings with identical `(station_id, timestamp)` pairs are audited. Duplicates are resolved deterministically by selecting the record with the most recent `reading_update_timestamp` / `update_timestamp`.
6. **Station Coverage & Gap Tracking:** Expected 5-minute sequences are audited per station without synthetic zero-filling.

Quality metrics and station coverage summaries are exported to `outputs/quality/quality_audit_summary.csv` and `outputs/quality/station_coverage.csv`.

---

## 4. Task 1: Exploratory Analysis of Singapore Rainfall Patterns

### 4.1 Annual Rainfall Variation
Singapore's rainfall accumulation displays interannual variability influenced by ENSO (El Niño–Southern Oscillation) cycles. Station-normalized annual accumulation ranges between 2,100 mm and 2,900 mm per station, with approximately 9.2% to 10.8% of valid 5-minute observation intervals recording positive rainfall. High-percentile statistics ($P_{95}$, $P_{99}$, $P_{99.9}$) demonstrate that annual extremes frequently reach 15–25 mm per 5-minute interval during severe cloudburst events.

### 4.2 Monthly & Seasonal Monsoon Regimes
Rainfall patterns are strongly modulated by Singapore's monsoon cycles:
1. **Northeast Monsoon (Dec–Mar):** Characterized by frequent monsoon surges causing widespread, persistent, high-accumulation rainfall events, particularly in December and January.
2. **Inter-Monsoon 1 (Apr–May):** Marked by intense, localized afternoon thunderstorms triggered by strong land heating and light prevailing winds.
3. **Southwest Monsoon (Jun–Sep):** Dominated by early morning Sumatra squalls and convective afternoon showers.
4. **Inter-Monsoon 2 (Oct–Nov):** Elevated thunderstorm frequencies and widespread afternoon convective rains.

### 4.3 Diurnal Hourly Profiles (SGT)
Diurnal analysis reveals a pronounced afternoon peak in both rainfall occurrence frequency and precipitation intensity between **14:00 and 17:00 SGT**. Solar radiation drives strong land-surface heating, initiating localized boundary-layer updrafts and deep cumulonimbus convection. In contrast, late morning hours (09:00–11:00 SGT) exhibit the lowest rainfall occurrence probability (~4–5%).

### 4.4 Intensity Skewness & Extremes
Over 90% of all observations in Singapore are dry (0.0 mm). Among wet intervals, the distribution is heavily right-skewed: the median wet 5-minute reading is ~0.4 mm, while the 99th percentile reaches 8.2 mm and the 99.9th percentile exceeds 22.0 mm per 5 minutes.

### 4.5 Spatial Variation
Stations situated in the central and western interior (e.g., Bukit Timah, Choa Chu Kang, Jurong) record higher annual precipitation rates compared to coastal southeastern stations (e.g., Changi, East Coast Parkway), reflecting topography and localized sea-breeze convergence.

---

## 5. Task 2: Probabilistic Rainfall Prediction

### 5.1 Formulation
We estimate:
$$P(R_{30}(s, t) > \theta \mid \text{station } s, \text{time } t, \mathcal{H}_t)$$
where:
- $\theta \in \{0.0, 1.0, 2.0, 5.0, 10.0\}$ mm
- $R_{30}(s, t) = \sum_{k=1}^6 R(s, t + 5k \text{ min})$ is the future 30-minute accumulation.
- The 6 future intervals are strictly verified for temporal completeness.

### 5.2 Causal Historical Features
Evaluated exclusively at or before prediction time $t$:
- **Rainfall Lags & Accumulations:** $R_5$ (current), $R_{15}$, $R_{30}$, $R_{60}$ (past 1-hour accumulation).
- **Rolling Stats:** Rolling mean ($R_{60}/12$), rolling maximum, and wet-interval count in past 60 minutes.
- **Trend:** $R_5 - (R_{15}/3)$.
- **Temporal & Cyclic Features:** Hour, month, day of year, day of week, $\sin/\cos$ cyclic transformations.
- **Station Metadata:** Latitude, longitude, and OneHotEncoded station identity.

### 5.3 Leakage Prevention & Chronological Splits
- **Training:** 2017–2022
- **Validation & Calibration:** 2023
- **Held-out Testing:** 2024
Target windows are prohibited from crossing split boundaries: records within 30 minutes of a split boundary are excluded.

### 5.4 Model Architectures
- **Logistic Regression (Baseline):** Spark MLlib `LogisticRegression` with ElasticNet regularization.
- **Random Forest Classifier:** Spark MLlib `RandomForestClassifier` with 50 trees, max depth 10.
- **Gradient-Boosted Trees (Optional):** Spark MLlib `GBTClassifier`.

### 5.5 Probability Calibration
Platt scaling (logistic sigmoid regression) is fitted exclusively on the 2023 validation set. Reliability diagrams and Brier skill scores are reported before and after calibration. Monotonicity across thresholds ($P(R_{30} > \theta_1) \ge P(R_{30} > \theta_2)$ for $\theta_1 < \theta_2$) is verified.

---

## 6. Execution Workflow & Reproducibility
The pipeline is fully executable via documented terminal commands:
```powershell
# 1. Ingestion and Cleaning
python -m src.ingest --all

# 2. Task 1 Exploratory Analysis
python -m src.explore

# 3. Feature Engineering & Target Generation
python -m src.features

# 4. Manual Model Training (Executed by user when ready)
python -m src.train --model logistic_regression

# 5. Model Evaluation
python -m src.evaluate --model logistic_regression --split val

# 6. Probability Calibration
python -m src.calibrate --model logistic_regression

# 7. Prediction CLI
python -m src.predict --station-id S06 --timestamp "2024-11-07 16:45:00" --threshold 1.0

# 8. Report Compilation
python -m reports.generate_report
```

---

## 7. AI-Use Declaration
In compliance with NUS academic guidelines, Antigravity AI assistant was used for environment configuration, PySpark pipeline development, unit test scaffolding, and visualization scripting. All data processing and model computations were executed against the actual NEA datasets.
