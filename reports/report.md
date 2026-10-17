# Singapore Historical Rainfall Analysis and 30-Minute Rainfall Probability Prediction Using Apache Spark

**Course:** DSA5208 — Scalable Distributed Computing for Data Science  
**Institution:** National University of Singapore (NUS)  
**Project Team / Authors:** [Student Names & Matriculation Numbers]  
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

![Figure 1: Annual Rainfall Summary](figures/fig1_annual_rainfall_summary.png)
*Figure 1: Annual station-normalized rainfall accumulation (mm) and wet-interval occurrence frequency across Singapore (2017–2024).*

### 4.2 Monthly & Seasonal Monsoon Regimes
Rainfall patterns are strongly modulated by Singapore's monsoon cycles:
1. **Northeast Monsoon (Dec–Mar):** Characterized by frequent monsoon surges causing widespread, persistent, high-accumulation rainfall events, particularly in December and January.
2. **Inter-Monsoon 1 (Apr–May):** Marked by intense, localized afternoon thunderstorms triggered by strong land heating and light prevailing winds.
3. **Southwest Monsoon (Jun–Sep):** Dominated by early morning Sumatra squalls and convective afternoon showers.
4. **Inter-Monsoon 2 (Oct–Nov):** Elevated thunderstorm frequencies and widespread afternoon convective rains.

![Figure 2: Monthly Seasonal Patterns](figures/fig2_monthly_seasonal_patterns.png)
*Figure 2: Average monthly rainfall accumulation categorized by Singapore's four meteorological monsoon regimes.*

![Figure 3: Year-Month Heatmap](figures/fig3_year_month_heatmap.png)
*Figure 3: Year-by-month rainfall accumulation heatmap (2017–2024), highlighting extreme monsoon surge events.*

### 4.3 Diurnal Hourly Profiles (SGT)
Diurnal analysis reveals a pronounced afternoon peak in both rainfall occurrence frequency and precipitation intensity between **14:00 and 17:00 SGT**. Solar radiation drives strong land-surface heating, initiating localized boundary-layer updrafts and deep cumulonimbus convection. In contrast, late morning hours (09:00–11:00 SGT) exhibit the lowest rainfall occurrence probability (~4–5%).

![Figure 4: Diurnal Hourly Rainfall](figures/fig4_diurnal_hourly_rainfall.png)
*Figure 4: Diurnal rainfall occurrence frequency (%) and mean intensity (mm/5-min) in Singapore Local Time (SGT), illustrating the afternoon solar heating convective peak (14:00–17:00).*

### 4.4 Intensity Skewness & Extremes
Over 90% of all observations in Singapore are dry (0.0 mm). Among wet intervals, the distribution is heavily right-skewed: the median wet 5-minute reading is ~0.4 mm, while the 99th percentile reaches 8.2 mm and the 99.9th percentile exceeds 22.0 mm per 5 minutes.

![Figure 5: Rainfall Intensity Distribution](figures/fig5_rainfall_intensity_distribution.png)
*Figure 5: Empirical rainfall intensity distribution (log scale) showing the extreme right skewness of tropical precipitation.*

### 4.5 Spatial Variation
Stations situated in the central and western interior (e.g., Bukit Timah, Choa Chu Kang, Jurong) record higher annual precipitation rates compared to coastal southeastern stations (e.g., Changi, East Coast Parkway), reflecting topography and localized sea-breeze convergence.

![Figure 6: Spatial Rainfall Distribution](figures/fig6_spatial_rainfall_distribution.png)
*Figure 6: Geographic spatial distribution of monitoring stations across Singapore with normalized mean equivalent rainfall rate.*

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
To reflect real-world operational deployment and prevent temporal target lookahead, the dataset is split chronologically:
- **Training partition (2017–2022):** 34,022,364 valid feature observations.
- **Validation partition (2023):** 6,686,688 valid feature observations (used exclusively for model comparison, diagnostic evaluation, and probability calibration).
- **Held-out Test partition (2024):** 6,338,426 valid feature observations (reserved strictly for final benchmark evaluation).
- **Total Verified Feature Dataset:** Exactly **47,047,478** observations across Singapore's station network (persisted in `data/processed/features` and verified via `outputs/quality/feature_summary.json`). Note that an earlier preliminary count of 47,048,915 included 1,437 boundary records situated within the 30-minute buffer of split boundaries; these were strictly pruned during final feature construction to ensure no future target window crossed between chronological partitions.

![Figure 7: Positive Event Prevalence by Threshold](figures/fig13_event_prevalence.png)
*Figure 7: Positive 30-minute rainfall exceedance prevalence across thresholds on the 2024 held-out test partition (N = 6,338,426), illustrating severe extreme-tail class imbalance.*

### 5.4 Model Architectures
- **Logistic Regression (Baseline):** Spark MLlib `LogisticRegression` with ElasticNet regularization.
- **Random Forest Classifier:** Spark MLlib `RandomForestClassifier` with 50 trees, max depth 10.
- **Gradient-Boosted Trees (Optional):** Spark MLlib `GBTClassifier`.

### 5.5 Probability Calibration
Platt scaling (logistic regression on log-odds) is fitted exclusively on the 2023 validation set ($N = 6,686,688$, subsampled at deterministic fraction 0.05 / $N = 333,703$ for optimization stability):
$$P_{\text{cal}} = \frac{1}{1 + \exp\bigl(-(a \cdot \text{logit}(p_{\text{raw}}) + b)\bigr)}, \quad \text{where } \text{logit}(p) = \ln\left(\frac{p}{1 - p}\right)$$
The slope parameter $a$ and intercept $b$ are fitted via univariate logistic regression on validation logits and saved to `models/calibration_<model_type>.json`. Reliability diagrams and Brier skill scores are evaluated before and after calibration. Monotonicity across thresholds ($P(R_{30} > \theta_1) \ge P(R_{30} > \theta_2)$ for $\theta_1 < \theta_2$) is strictly verified for downstream inference.

---

## 6. Model Evaluation & Architecture Comparison

### 6.1 Performance on Held-Out Test Set (2024, 6,338,426 Observations)
Both model architectures were evaluated across all five precipitation thresholds on the completely unseen 2024 test partition. The table below presents the **raw MLlib classifier probabilities** at the standard decision cutoff of $0.50$:

| Threshold ($\theta$) | Architecture | ROC-AUC | PR-AUC | Brier Score | Baseline Brier | Brier Skill Score (BSS) | Recall (0.5) | Precision (0.5) | F1-Score |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **> 0.0 mm** | Logistic Regression | 0.8760 | 0.6284 | 0.03693 | 0.05956 | +38.0% | 38.7% | 82.6% | 0.5268 |
| | **Random Forest (50T)** | **0.8801** | **0.6595** | **0.03293** | 0.05956 | **+44.7%** | **51.0%** | 79.4% | **0.6210** |
| **> 1.0 mm** | Logistic Regression | 0.8954 | 0.5435 | 0.01628 | 0.02505 | +35.0% | 33.3% | 82.8% | 0.4750 |
| | **Random Forest (50T)** | **0.9030** | **0.5690** | **0.01526** | 0.02505 | **+39.1%** | **44.0%** | 76.0% | **0.5573** |
| **> 2.0 mm** | Logistic Regression | 0.8901 | 0.4681 | 0.01178 | 0.01660 | +29.1% | 25.8% | 81.1% | 0.3918 |
| | **Random Forest (50T)** | **0.8970** | **0.4899** | **0.01108** | 0.01660 | **+33.3%** | **34.5%** | 74.9% | **0.4726** |
| **> 5.0 mm** | Logistic Regression | 0.8862 | 0.3622 | 0.00653 | 0.00831 | +21.4% | **21.6%** | **71.6%** | **0.3325** |
| | **Random Forest (50T)** | **0.8940** | **0.3673** | **0.00624** | 0.00831 | **+24.9%** | 20.0% | 66.5% | 0.3073 |
| **> 10.0 mm** | **Logistic Regression** | 0.8901 | **0.2746** | 0.00324 | 0.00381 | +14.9% | **16.9%** | **60.9%** | **0.2640** |
| | Random Forest (50T) | **0.8984** | 0.2529 | **0.00316** | 0.00381 | **+17.1%** | 0.0% | 0.0% | 0.0000 |

![Figure 8: Model Performance Comparison Across Thresholds](figures/fig12_model_comparison_bars.png)
*Figure 8: Model performance comparison across rainfall thresholds on the 2024 held-out test set: (A) ROC-AUC and (B) PR-AUC relative to random prevalence baselines.*

![Figure 9: ROC Curves](figures/fig10_roc_curves.png)
*Figure 9: Receiver Operating Characteristic (ROC) curves across five exceedance thresholds comparing Logistic Regression and Random Forest (50 trees) on the 2024 held-out test set.*

![Figure 10: Precision-Recall Curves](figures/fig11_precision_recall_curves.png)
*Figure 10: Precision-Recall (PR) curves across five exceedance thresholds comparing Logistic Regression and Random Forest against empirical prevalence baselines.*

![Figure 11: Reliability Diagrams](figures/fig8_calibration_logistic_regression.png)
*Figure 11: Reliability diagrams before and after Platt scaling calibration fitted on the 2023 validation dataset.*

### 6.2 Key Analysis & The $\theta = 10.0$ mm Cutoff Phenomenon
1. **Ranking & Probabilistic Calibration:** Random Forest outperforms Logistic Regression across discrimination metrics (higher ROC-AUC and PR-AUC for thresholds 0.0 to 5.0 mm) and probabilistic accuracy (lower Brier score and higher positive Brier Skill Score across all five thresholds, including 10.0 mm).
2. **Raw vs. Calibrated Metrics:** The test evaluation table directly reports the raw MLlib output probabilities. Calibration (Platt scaling) was fitted exclusively on the 2023 validation partition to prevent test data snooping and is applied at inference time in the prediction CLI (`src.predict`).
3. **Analysis of Random Forest Predictions at $\theta = 10.0$ mm:**
   - At $\theta = 10.0$ mm, 30-minute rainfall exceedance is an extreme-tail event with empirical test prevalence $\pi = 0.0038$ (0.38%, exactly 24,265 positive events out of 6,338,426 held-out test observations).
   - **Threshold-Independent Ranking (PR-AUC):** Across all possible decision thresholds, Random Forest achieves a Precision-Recall AUC of **0.2529**, which is approximately **66× above the uninformative random-guess baseline** ($\pi = 24,265 / 6,338,426 \approx 0.00383$). Combined with an **ROC-AUC of 0.8984** and a **Brier Skill Score of +17.1%** over climatology, this confirms that the ensemble assigns significantly higher relative probabilities to heavy-rain intervals than to dry intervals.
   - **Threshold-Dependent Decision Assignment (Recall at 0.50):** In contrast to ranking metrics, classification recall depends strictly on an assigned decision cutoff. Random Forest aggregates 50 unweighted decision trees trained on natural class frequencies without artificial oversampling. Because severe rain occurs in only 0.38% of intervals, individual tree leaves rarely predict high probabilities in rare regions, and the ensemble average probability across 50 trees never reaches the conventional 0.50 cutoff for any test record ($p < 0.50$ across all 6,338,426 test observations). Consequently, evaluating at the fixed 0.50 cutoff assigns every test observation to the negative class ($\text{TP} = 0, \text{FP} = 0, \text{TN} = 6,314,161, \text{FN} = 24,265$), producing zero positive predictions and 0.0% recall.
   - **Operational Implication:** The contrast between a high PR-AUC (66× baseline) and zero recall at 0.50 highlights that for extreme-tail precipitation, a generic 0.50 decision threshold is unsuited for binary classification; operational alerting requires operating thresholds calibrated on validation data or aligned with specific cost-loss matrices.

---

## 7. Execution Workflow & Reproducibility
The pipeline is fully executable via documented terminal commands. In strict adherence to leakage-prevention standards, model comparison, diagnostic checks, and probability calibration are conducted on the **2023 validation split**. The **2024 held-out test partition is reserved strictly for final benchmark evaluation** and must never be used for hyperparameter tuning or model selection.

```powershell
# 1. Ingestion and Cleaning (Raw CSVs -> Clean Parquet)
python -m src.ingest --all

# 2. Task 1 Exploratory Analysis (Generates figures and summary tables)
python -m src.explore

# 3. Feature Engineering & Target Generation (Generates features and labels)
python -m src.features

# 4. Model Training (Chronological training split: 2017–2022)
python -m src.train --model logistic_regression --thresholds 0.0 1.0 2.0 5.0 10.0
python -m src.train --model random_forest --thresholds 0.0 1.0 2.0 5.0 10.0

# 5. Validation Evaluation & Model Selection (Validation split: 2023)
# Note: Use validation split exclusively for diagnostics and model comparison.
python -m src.evaluate --model logistic_regression --split val
python -m src.evaluate --model random_forest --split val

# 6. Probability Calibration (Fitted strictly on 2023 validation split)
python -m src.calibrate --model logistic_regression
python -m src.calibrate --model random_forest

# 7. Final Held-Out Test Evaluation (Benchmark split: 2024)
# Executed once models and calibration parameters are finalized.
python -m src.evaluate --model logistic_regression --split test
python -m src.evaluate --model random_forest --split test

# 8. Prediction CLI (Interactive inference with calibrated probabilities)
python -m src.predict --station-id S06 --timestamp "2024-11-07 16:45:00" --threshold 1.0

# 9. Academic Report & PDF Compilation
python -m reports.generate_report
```

---

## 8. Limitations, AI-Use Declaration & References

### 8.1 Limitations
1. **Discrete Point-Gauge Spatial Coverage:** Rainfall observations are collected exclusively from surface tipping-bucket gauges at discrete stations (~60–80 operational stations). Tropical convective precipitation in Singapore frequently forms at microscale dimensions (1–3 km across); intense localized rainfall cells can initiate between monitoring stations without prior indication at adjacent gauges.
2. **Rainfall-Only Historical Predictors:** Feature engineering is restricted strictly to historical rainfall observations (lags, rolling accumulations, rolling maxima, intensity trends) and cyclic spatio-temporal indicators. The dataset (Collection 2279) does not include thermodynamic atmospheric state variables—such as relative humidity, ambient temperature, convective available potential energy (CAPE), surface pressure, or vertical wind velocity profiles—which govern the initial convective triggering of precipitation before surface rain occurs.
3. **Severe Imbalance at Heavy Rainfall Thresholds:** For rare events ($\theta = 10.0$ mm, 0.38% prevalence), standard unweighted tree ensembles output low absolute probabilities ($p < 0.50$ for all test records). Operational deployment for early warning requires decision thresholds calibrated to the empirical prior or asymmetric cost-utility matrices, rather than default 0.50 cutoffs.
4. **Strict Historical Continuity Requirement:** To prevent synthetic interpolation artifacts, the prediction interface strictly rejects inference requests if any of the preceding twelve 5-minute observation intervals (past 60 minutes) are missing, restricting forecast availability during gauge telemetry interruptions.

### 8.2 AI-Use Declaration
In compliance with NUS academic and course guidelines, assistance from artificial intelligence tools—specifically Google DeepMind's Antigravity coding assistant and OpenAI's ChatGPT—was utilized throughout the project lifecycle. These tools provided assistance with:
- High-level architectural planning, pipeline design, and literature/documentation review;
- Development, debugging, and environment configuration of the Apache Spark (PySpark 3.5.3) data processing and MLlib pipelines;
- Authoring, scaffolding, and debugging the automated unit test suite (`pytest`);
- Drafting, structuring, and formatting the academic project report and visualization scripts;
- Mathematical verification and interpretation of experimental results, including class imbalance phenomena at extreme thresholds, decision cutoff behavior, and Platt scaling probability calibration.

All data ingestion, exploratory data analyses, feature engineering, model training, probability calibration, and metric evaluations were independently executed, inspected, and verified against the actual Singapore government historical rainfall datasets (Collection 2279).

### 8.3 References
1. Singapore National Environment Agency (NEA), *Historical Rainfall across Singapore*, Collection 2279, data.gov.sg.
2. Apache Software Foundation, *Spark MLlib Guide: Pipelines and Classification*, Apache Spark 3.5.3 Documentation, 2024.
3. Wilks, D. S., *Statistical Methods in the Atmospheric Sciences*, 4th ed., Academic Press, 2019.

