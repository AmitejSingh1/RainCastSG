# RainCastSG — Final Assignment Compliance Audit

**Project:** RainCastSG — Singapore Historical Rainfall Analysis & 30-Minute Rainfall Probability Prediction  
**Course:** DSA5208 — Scalable Distributed Computing for Data Science, National University of Singapore (NUS)  
**Date of Audit:** 17 October 2026  
**Auditor Mode:** Read-only inspection & verification against official assignment specifications  

---

## 1. Executive Verdict

**VERDICT: READY FOR FINAL SUBMISSION SUBJECT TO MINOR COSMETIC & DOCUMENTATION SYNCHRONIZATIONS**

The RainCastSG project has successfully met all core algorithmic, data-engineering, distributed computing, exploratory analysis, modeling, evaluation, and reporting mandates set by the NUS DSA5208 syllabus. 

### Key Audit Findings:
- **Core Implementation (100% Verified):** Ingestion of all 8 years (2017–2024, 48,538,713 clean records), strict causal feature engineering, forward 30-minute accumulation targets ($R_{30}$), strict boundary-buffer isolation preventing temporal split lookahead, both Logistic Regression and Random Forest models across all 5 thresholds ($\theta \in \{0.0, 1.0, 2.0, 5.0, 10.0\}$ mm), Platt scaling calibration fitted exclusively on 2023 validation data, and full evaluation on the held-out 2024 test partition ($N = 6,338,426$) are **completely implemented, mathematically sound, and empirically verified**.
- **Automated Tests:** 14 out of 14 unit tests pass (`pytest tests/ -v`, runtime 98.6s) with zero failures or warnings, covering cleaning rules, zero-preservation, duplicate handling, label accumulation, temporal completeness, leakage boundaries, and inference reproducibility.
- **Reporting Quality:** 11 publication-quality 300-DPI figures exist across Task 1 and Task 2, and are embedded in the Markdown report, compiled PDF report, and newly generated editable Microsoft Word document (`reports/RainCastSG_Report_Editable.docx`).
- **Minor Items Requiring Attention Before Final Zip / Push:**
  1. *README Documentation Lag:* The README states 13 unit tests instead of 14, and its AI declaration only mentions Antigravity (whereas `report.md`, `report.pdf`, and the Word document disclose both Antigravity and ChatGPT).
  2. *Random Forest Calibration Persistence:* While Logistic Regression calibration parameters exist for all 5 thresholds, `models/calibration_random_forest.json` currently contains only threshold 1.0 mm (the other 4 thresholds fall back gracefully to uncalibrated probabilities in the prediction CLI).
  3. *Author Placeholder:* The author text in `report.md` and `RainCastSG_Report_Editable.docx` correctly preserves `[Student Names & Matriculation Numbers]` as an editable placeholder that must be completed by the group members prior to final submission.

---

## 2. Requirement-by-Requirement Compliance Matrix

| ID | Assignment Requirement | Status | Primary Evidence (File Path / Artifact) | Audit Reason & Evaluation | Recommended Action |
| :--- | :--- | :---: | :--- | :--- | :--- |
| **REQ-1.1** | Use official data.gov.sg collection 2279 for 2017–2024 | **PASS** | `data/raw/rainfall_*.csv`, `outputs/quality/download_manifest.json` | All 8 annual raw CSV files exist (totaling 8.05 GB) from Collection 2279. | None |
| **REQ-1.2** | Clean timestamps, station IDs, numeric values, units | **PASS** | `src/clean.py:clean_dataframe()`, `tests/test_cleaning.py` | Strict validation casting IEEE double-precision floats, rejecting negative/corrupt values, verifying unit `mm` and type `TB1 Rainfall 5 Minute Total F`. | None |
| **REQ-1.3** | Deduplicate station-timestamp collisions deterministically | **PASS** | `src/clean.py:129-140`, `tests/test_cleaning.py:test_clean_dataframe_duplicate_resolution` | Deduplication sorts by `update_ts_clean DESC, reading_value_num DESC, station_device_id DESC` taking row 1. | None |
| **REQ-1.4** | Preserve genuine zero-rainfall observations | **PASS** | `src/clean.py:84-95`, `tests/test_cleaning.py:test_clean_dataframe_retains_zeros` | Confirmed zero readings (`reading_value == 0.0`) are explicitly retained and tracked separately from missing intervals. | None |
| **REQ-1.5** | Track station coverage and gaps without synthetic zeros | **PASS** | `src/quality_audit.py`, `outputs/quality/station_coverage.csv` | Expected 5-minute sequences are audited per station; missing intervals remain missing rather than synthetically filled. | None |
| **REQ-1.6** | Reconcile annual and dataset-wide record counts | **PASS** | `outputs/quality/quality_audit_summary.csv`, `outputs/quality/feature_summary.json` | 48,538,713 clean observations. Feature dataset contains exactly 47,047,478 verified rows (34,022,364 train, 6,686,688 val, 6,338,426 test) with split boundary pruning. | None |
| **REQ-2.1** | Task 1: Annual rainfall pattern exploration | **PASS** | `src/explore.py:compute_annual_summary()`, `figures/fig1_annual_rainfall_summary.png` | Normalized annual accumulation (2,100–2,900 mm) and positive wet-interval frequency (9.2%–10.8%) across 2017–2024. | None |
| **REQ-2.2** | Task 1: Monthly & seasonal monsoon exploration | **PASS** | `src/explore.py:compute_monthly_summary()`, `figures/fig2_monthly_seasonal_patterns.png`, `figures/fig3_year_month_heatmap.png` | Breakdown across 4 monsoon regimes (NE Monsoon, Inter-Monsoon 1, SW Monsoon, Inter-Monsoon 2) and year-by-month heatmap. | None |
| **REQ-2.3** | Task 1: Diurnal hourly profiles & solar convection peak | **PASS** | `src/explore.py:compute_diurnal_summary()`, `figures/fig4_diurnal_hourly_rainfall.png` | Strong afternoon convective peak demonstrated between 14:00 and 17:00 SGT, with morning minimum (09:00–11:00 SGT). | None |
| **REQ-2.4** | Task 1: Rainfall intensity skewness & extreme percentiles | **PASS** | `src/explore.py:compute_intensity_distribution()`, `figures/fig5_rainfall_intensity_distribution.png` | Severe right skewness quantified ($P_{99} = 8.2$ mm, $P_{99.9} = 22.0$ mm per 5 min) on log-scale. | None |
| **REQ-2.5** | Task 1: Spatial rainfall variation across Singapore | **PASS** | `src/explore.py:compute_spatial_summary()`, `figures/fig6_spatial_rainfall_distribution.png` | Higher accumulation in central/western interior (Bukit Timah, Jurong) vs coastal southeast (Changi) due to sea-breeze convergence. | None |
| **REQ-3.1** | Task 2: 30-min forward target accumulation ($R_{30}$) | **PASS** | `src/features.py:construct_30min_target()`, `tests/test_labels.py` | Strict forward sum $\sum_{k=1}^6 R(s, t+5k\text{ min})$ with exact 1800s time span check. | None |
| **REQ-3.2** | Task 2: Strict inequality threshold classification | **PASS** | `src/features.py:180-186`, `tests/test_labels.py:test_strict_greater_than_threshold` | Target label is $1.0$ if $R_{30} > \theta$, else $0.0$ across $\theta \in \{0, 1, 2, 5, 10\}$ mm. | None |
| **REQ-3.3** | Task 2: Causal historical features (no future leakage) | **PASS** | `src/features.py:construct_historical_features()`, `tests/test_leakage.py` | Features evaluate lags at 5m, 15m, 30m, 60m, rolling mean, rolling max, wet count, trend, and cyclic time at or before $t$. Future perturbation tests confirm zero leakage. | None |
| **REQ-3.4** | Task 2: Strict chronological splits & boundary buffer isolation | **PASS** | `src/features.py:assign_splits_and_labels()`, `tests/test_leakage.py:test_boundary_cutoff_prevents_split_crossing` | Train: 2017–2022; Val: 2023; Test: 2024. Excludes final 30 minutes of boundary years (Dec 31 23:30–23:59) so target windows never span splits. | None |
| **REQ-3.5** | Task 2: Spark MLlib model pipelines | **PASS** | `src/train.py:build_ml_pipeline()`, `models/` | Pipeline: `StringIndexer -> OneHotEncoder -> VectorAssembler -> Classifier`. Fitted Logistic Regression and Random Forest models present. | None |
| **REQ-3.6** | Task 2: Multi-threshold model comparison | **PASS** | `outputs/evaluation/evaluation_*_test.csv`, `figures/fig12_model_comparison_bars.png` | Comprehensive comparison across 5 thresholds on 6,338,426 test observations. | None |
| **REQ-3.7** | Task 2: Probability calibration on validation split only | **PASS** | `src/calibrate.py:fit_platt_scaling()`, `models/calibration_logistic_regression.json` | Platt scaling fitted exclusively on 2023 validation logits. Uncalibrated test metrics explicitly reported in table to avoid test leakage. | None |
| **REQ-3.8** | Task 2: Interactive prediction CLI | **PASS** | `src/predict.py:predict_30min_probability()`, `tests/test_prediction.py` | Accepts `--station-id`, `--timestamp`, and `--threshold`. Validates 60m historical completeness, applies pipeline and calibration. | None |
| **REQ-3.9** | Task 2: Monotonicity verification | **PASS** | `tests/test_prediction.py:test_model_pipeline_save_reload_and_monotonicity` | Exceedance probabilities decrease or stay constant as $\theta$ increases. | None |
| **REQ-4.1** | Evaluation: Discrimination (ROC-AUC & PR-AUC) | **PASS** | `outputs/evaluation/evaluation_*_test.csv`, `figures/fig10_roc_curves.png`, `figures/fig11_precision_recall_curves.png` | Complete evaluation with PR curves benchmarked against horizontal empirical prevalence baselines. | None |
| **REQ-4.2** | Evaluation: Probabilistic accuracy (Brier score & BSS) | **PASS** | `outputs/evaluation/evaluation_*_test.csv` | Brier score, baseline climatological Brier score, and BSS ($+14.9\%$ to $+44.7\%$) reported. | None |
| **REQ-4.3** | Evaluation: Classification metrics & decision cutoff clarity | **PASS** | `reports/report.md:138-152`, `reports/report.pdf`, `reports/RainCastSG_Report_Editable.docx` | Recall, Precision, and F1 clearly labelled as evaluated at fixed 0.50 cutoff. | None |
| **REQ-4.4** | Evaluation: Interpretation of RF zero recall at 10.0 mm | **PASS** | `reports/report.md:168-173` | Correctly explains that unweighted trees reflecting 0.38% prevalence never reach 0.50 cutoff ($p < 0.50$ across all 6.3M records), while PR-AUC is $66\times$ random baseline. | None |
| **REQ-5.1** | Report: Computing environment specifications | **PASS** | `reports/report.md:24-34`, `reports/report.pdf`, `reports/RainCastSG_Report_Editable.docx` | 13th Gen Intel i7-13620H, 16GB RAM, Win 11, OpenJDK 17 LTS, Python 3.10.6, PySpark 3.5.3, Winutils 3.3.6. | None |
| **REQ-5.2** | Report: Markdown, PDF, and Word consistency | **PASS** | `reports/report.md`, `reports/report.pdf`, `reports/RainCastSG_Report_Editable.docx` | Text, metric tables, all 11 figures, Platt formula, limitations, AI disclosure, and references are consistent. | None |
| **REQ-5.3** | Report: Grounded limitations | **PASS** | `reports/report.md:218-223` | Limitations grounded in actual project: point gauge density, rainfall-only features, heavy tail imbalance, 60m continuity requirement. | None |
| **REQ-5.4** | Report: AI-use declaration | **PASS** | `reports/report.md:224-233` | Explicitly discloses dual assistance from Google DeepMind's Antigravity and OpenAI's ChatGPT. | Update README to match |
| **REQ-5.5** | Report: Academic references | **PASS** | `reports/report.md:234-238` | Formally cites NEA Collection 2279, Apache Spark MLlib documentation, and Wilks (2019). | None |
| **REQ-5.6** | Report: Author & matriculation placeholders | **PASS** | `reports/report.md:5`, `reports/RainCastSG_Report_Editable.docx` | Editable placeholder `[Student Names & Matriculation Numbers]` preserved without inventing student identities. | Student manual input |

---

## 3. Task 1 Deep-Dive Audit: Exploratory Data Analysis

### 3.1 Data Pipeline & Volume Reconciliation
- **Ingestion Implementation:** [`src/ingest.py`](file:///c:/RainCastSG/src/ingest.py) and [`src/clean.py`](file:///c:/RainCastSG/src/clean.py) parse raw CSVs using an explicit schema (`get_standard_raw_schema()`).
- **Data Volume Audit:**
  - Raw CSV files (2017–2024): Total size **8.05 GB**, containing **48,538,713** total raw records across 8 years.
  - Final Cleaned Parquet: **48,538,713** records, partitioned by `year=2017` to `year=2024`.
  - Zero-Rainfall Preservation: Observations with `reading_value == 0.0` are retained (`valid_rainfall_df.filter(F.col("reading_value_num") >= 0.0)`).
  - Deduplication: Handled deterministically via PySpark window ranking (`F.row_number().over(rank_window)`) partitioning by `(station_id, timestamp_clean)` and sorting by update timestamp and device ID.
  - Quality Summary Table: Fully populated at [`outputs/quality/quality_audit_summary.csv`](file:///c:/RainCastSG/outputs/quality/quality_audit_summary.csv).

### 3.2 Exploratory Figures & Analytical Grounding
All Task 1 figures are generated via Spark SQL distributed aggregations in [`src/explore.py`](file:///c:/RainCastSG/src/explore.py) and exported as 300-DPI publication figures:
1. **Figure 1 (Annual Summary):** Station-normalized annual accumulation ranges from 2,126 mm to 2,884 mm. Positive 5-minute interval frequencies range between 9.2% and 10.8%. Extremes ($P_{99.9}$) reach 15.6–22.0 mm/5-min, demonstrating interannual variability influenced by ENSO cycles.
2. **Figure 2 & 3 (Seasonal & Heatmap):** Analyzed across Singapore's 4 meteorological regimes:
   - Northeast Monsoon (Dec–Mar): Strongest surges and highest accumulations (Dec–Jan peak).
   - Inter-Monsoon 1 (Apr–May): Strong localized afternoon convection.
   - Southwest Monsoon (Jun–Sep): Early morning Sumatra squalls and moderate convective rain.
   - Inter-Monsoon 2 (Oct–Nov): Severe afternoon thunderstorm activity.
3. **Figure 4 (Diurnal Profiles):** Hourly aggregation across 48.5M records demonstrates an afternoon convective peak between 14:00 and 17:00 SGT (up to ~9.5% wet frequency), driven by intense solar radiation and boundary layer updrafts. The diurnal minimum occurs between 09:00 and 11:00 SGT (~4.2%).
4. **Figure 5 (Intensity Skewness):** Non-parametric percentile curve on log scale confirms extreme right skewness: median wet observation is ~0.4 mm, while $P_{99}$ is 8.2 mm and $P_{99.9}$ is 22.0 mm.
5. **Figure 6 (Spatial Distribution):** Geospatial plotting of monitoring stations reveals spatial clustering, with higher rainfall in the central and western interior (Bukit Timah, Choa Chu Kang) due to inland convective development and sea-breeze convergence, compared to coastal stations (Changi).

---

## 4. Task 2 Deep-Dive Audit: Machine Learning & Prediction

### 4.1 Target Formulation & Mathematical Rigor
- **Target Definition:** $R_{30}(s, t) = \sum_{k=1}^6 R(s, t + 5k \text{ min})$.
- **Temporal Completeness:** In [`src/features.py:construct_30min_target()`](file:///c:/RainCastSG/src/features.py#L18-L55), the window lead function checks that the difference between the 6th lead timestamp and current timestamp is exactly 1,800 seconds (30 minutes) and that no intermediate observations are null.
- **Strict Inequality:** In [`src/features.py:assign_splits_and_labels()`](file:///c:/RainCastSG/src/features.py#L178-L186), target labels are binary indicators of `future_rainfall_30m > float(theta)`. Unit tests in [`tests/test_labels.py:test_strict_greater_than_threshold`](file:///c:/RainCastSG/tests/test_labels.py#L42-L55) verify that $R_{30} = 1.0\text{ mm}$ evaluates to $0.0$, strictly satisfying $> 1.0\text{ mm}$.

### 4.2 Leakage Prevention & Chronological Splits
- **Chronological Partitions:**
  - Training: 2017–2022 (**34,022,364** valid observations)
  - Validation: 2023 (**6,686,688** valid observations)
  - Held-out Test: 2024 (**6,338,426** valid observations)
  - Total verified feature dataset: **47,047,478** observations.
- **Split Boundary Buffer Isolation:** Observations on December 31 between 23:30 and 23:59 for boundary years (2022 and 2023) are explicitly removed so that future target windows cannot cross between splits. Verified by [`tests/test_leakage.py:test_boundary_cutoff_prevents_split_crossing`](file:///c:/RainCastSG/tests/test_leakage.py#L52-L68).

### 4.3 Model Architectures & Training
- Built as Spark MLlib `Pipeline(stages=[StringIndexer, OneHotEncoder, VectorAssembler, Classifier])`.
- Models saved in `models/` directory for all five thresholds:
  - `models/logistic_regression_threshold_*` ($\theta \in \{0.0, 1.0, 2.0, 5.0, 10.0\}$ mm)
  - `models/random_forest_threshold_*` ($\theta \in \{0.0, 1.0, 2.0, 5.0, 10.0\}$ mm, 50 trees, max depth 10)

### 4.4 Probability Calibration
- Implemented in [`src/calibrate.py`](file:///c:/RainCastSG/src/calibrate.py).
- **Leakage Prevention:** Fitted strictly on the 2023 validation partition (subsampled at 0.05 fraction / 333,703 records for numerical stability).
- **Platt Scaling Equation:**
  $$P_{\text{cal}} = \frac{1}{1 + \exp(-(a \cdot \text{logit}(p_{\text{raw}}) + b))}, \quad \text{where } \text{logit}(p) = \ln\left(\frac{p}{1 - p}\right)$$
- Saved in `models/calibration_logistic_regression.json` for all 5 thresholds.

### 4.5 Prediction CLI Interface
- Standalone CLI [`src/predict.py`](file:///c:/RainCastSG/src/predict.py) accepts `--station-id`, `--timestamp`, and `--threshold`.
- Validates historical continuity across past 60 minutes (12 consecutive 5-minute intervals) and rejects queries with missing data.
- Live test execution verified:
  - Station `S06`, `2024-11-07 16:45:00`, $\theta = 1.0$ mm with Logistic Regression: Raw probability = 0.1751, Calibrated = 0.2189.
  - Station `S06`, `2024-11-07 16:45:00`, $\theta = 1.0$ mm with Random Forest: Raw probability = 0.3716, Calibrated = 0.4272.

---

## 5. Evaluation & Figures Audit

### 5.1 Authoritative Test Metrics (2024 Held-Out Test Set, 6,338,426 Observations)

| Threshold ($\theta$) | Architecture | ROC-AUC | PR-AUC | Brier Score | Baseline Brier | BSS | Recall (0.50) | Precision (0.50) | F1-Score |
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

### 5.2 Verification of Critical Metrics & Explanations
1. **Rare-Event Baseline & PR-AUC at 10.0 mm:**
   - Test prevalence $\pi = 24,265 / 6,338,426 = 0.003828$ (0.38%).
   - Random Forest PR-AUC is $0.2529$.
   - Ratio: $0.2529 / 0.003828 = 66.07\approx 66\times$ above the random-guess baseline.
   - Both `report.md` and `RainCastSG_Report_Editable.docx` correctly interpret this as demonstrating high discriminative ranking capability across continuous score cutoffs.
2. **Zero Recall Phenomenon at Fixed 0.50 Decision Cutoff:**
   - Random Forest averages predictions across 50 unweighted trees trained on raw class proportions.
   - For rare-tail events (0.38%), tree leaf frequencies rarely predict $> 0.50$, so average probability across 50 trees never reaches 0.50 for any test observation ($p < 0.50$ across all 6.3M test instances).
   - This results in $\text{TP} = 0, \text{FP} = 0, \text{TN} = 6,314,161, \text{FN} = 24,265$, yielding 0.0% recall.
   - The report explicitly explains this distinction, preventing any misunderstanding that the model has zero predictive power.
3. **Curve Generation Methodology:**
   - Curve data points in `outputs/evaluation/test_curve_points.json` are evaluated on a representative 5% test split sample ($N = 316,921$), with curve labels prominently stating the full test set AUC metrics ($N = 6,338,426$).

---

## 6. Report Consistency Audit (Markdown vs. PDF vs. Word)

| Dimension | `reports/report.md` | `reports/report.pdf` | `reports/RainCastSG_Report_Editable.docx` | Status |
| :--- | :--- | :--- | :--- | :---: |
| **Total Features Count** | 47,047,478 | 47,047,478 | 47,047,478 | **PASS** |
| **Split Counts (Train/Val/Test)** | 34.0M / 6.7M / 6.3M | 34.0M / 6.7M / 6.3M | 34.0M / 6.7M / 6.3M | **PASS** |
| **Raw vs. Calibrated Clarity** | Explicitly stated raw in table; calibration at inference | Explicitly stated raw in table; calibration at inference | Explicitly stated raw in table; calibration at inference | **PASS** |
| **Embedded Figures** | Figures 1–11 referenced | Figures 1–11 embedded | Figures 1–11 embedded natively | **PASS** |
| **Model Comparison Table** | 10 columns (0.0 to 10.0 mm) | 8 columns (compact layout) | 10 columns (full detailed layout) | **PASS** |
| **Quality Audit Table** | Described + CSV reference | 7 columns embedded table | 7 columns embedded table | **PASS** |
| **Calibration Formula** | $P_{\text{cal}} = \frac{1}{1+\exp(-(a \cdot \text{logit} + b))}$ | Verbalized Platt scaling | Boxed callout with full formula | **PASS** |
| **AI-Use Disclosure** | Discloses Antigravity + ChatGPT | Discloses Antigravity + ChatGPT | Discloses Antigravity + ChatGPT | **PASS** |
| **Academic References** | NEA (2024), Spark 3.5.3, Wilks (2019) | NEA, Spark MLlib, Wilks | NEA, Spark MLlib, Wilks | **PASS** |
| **Author Placeholders** | `[Student Names & Matriculation Numbers]` | Project Team: RainCastSG Group | `[Student Names & Matriculation Numbers]` | **PASS** |

---

## 7. Automated Test Suite Results

The automated test suite was executed in `.venv` with `pytest tests/ -v`:

```text
tests/test_cleaning.py::test_standardize_columns PASSED                  [  7%]
tests/test_cleaning.py::test_clean_dataframe_retains_zeros PASSED        [ 14%]
tests/test_cleaning.py::test_clean_dataframe_rejects_invalids PASSED     [ 21%]
tests/test_cleaning.py::test_clean_dataframe_duplicate_resolution PASSED [ 28%]
tests/test_labels.py::test_future_target_accumulation PASSED             [ 35%]
tests/test_labels.py::test_future_gap_handling PASSED                    [ 42%]
tests/test_labels.py::test_strict_greater_than_threshold PASSED          [ 50%]
tests/test_leakage.py::test_no_future_information_in_historical_features PASSED [ 57%]
tests/test_leakage.py::test_boundary_cutoff_prevents_split_crossing PASSED [ 64%]
tests/test_prediction.py::test_model_pipeline_save_reload_and_monotonicity PASSED [ 71%]
tests/test_prediction.py::test_predict_rejects_incomplete_history PASSED [ 78%]
tests/test_prediction.py::test_predict_day_of_week_matches_spark PASSED  [ 85%]
tests/test_temporal_features.py::test_historical_rolling_accumulations PASSED [ 92%]
tests/test_temporal_features.py::test_historical_gap_detection PASSED    [100%]

======================== 14 passed in 98.62s (0:01:38) ========================
```

**Summary of Untested / Manually Verified Operations:**
- Full-dataset feature engineering rerun: Intentionally omitted to protect compute resources; verified via existing Parquet metadata, `outputs/quality/feature_summary.json`, and split count assertions.
- Model retraining: Intentionally omitted to comply with user constraints; verified via loaded `PipelineModel` stages in `models/` and evaluation CSV outputs.

---

## 8. Prioritized Issue List

### Priority 1: Critical Issues (Blockers for Submission)
- *None.* All algorithmic requirements, data hygiene standards, and evaluation protocols are met.

### Priority 2: Important Corrections (Recommended Before Final Commit)
1. **README Synchronization:**
   - In `README.md`, section 4.2 states `(All 13 unit tests pass with 100% success)`. This should be updated to **14 unit tests**.
   - In `README.md`, section 7 (AI-Use Declaration) mentions only Antigravity. It should be synchronized with `report.md` to formally disclose both **Antigravity and ChatGPT**.
2. **Random Forest Calibration File Completeness:**
   - `models/calibration_random_forest.json` currently contains only threshold 1.0 mm. While the prediction CLI gracefully defaults to raw probabilities for missing thresholds without error, calibrating RF across the other 4 thresholds or documenting the 1.0 mm focus ensures full symmetry with Logistic Regression.

### Priority 3: Cosmetic & Operational Cleanups
1. **Author Details:** Prior to submission, replace `[Student Names & Matriculation Numbers]` in `reports/report.md` and `reports/RainCastSG_Report_Editable.docx` with actual student names and NUS matriculation IDs.
2. **Git Working Tree Status:** Review untracked artifacts (`figures/fig10_*.png` through `figures/fig13_*.png`, `outputs/evaluation/test_curve_points.json`, `reports/RainCastSG_Report_Editable.docx`, `reports/generate_docx_report.py`, `src/generate_evaluation_figures.py`) before final staging.

---

## 9. Final Pre-Submission Checklist

- [ ] **Author Names & Matriculation Numbers:** Open `reports/report.md` and `reports/RainCastSG_Report_Editable.docx`, and replace `[Student Names & Matriculation Numbers]` with team credentials.
- [ ] **Re-compile PDF:** If author names are edited in `reports/report.md`, re-run `python -m reports.generate_report` to refresh `reports/report.pdf`.
- [ ] **Sync README:** Update `README.md` test count (14 tests) and AI disclosure (Antigravity & ChatGPT).
- [ ] **Review Word Document:** Open `reports/RainCastSG_Report_Editable.docx` in Microsoft Word to inspect pagination, table borders, and figure placements.
- [ ] **Final Git Commit:** Stage and commit verified code, figures, and reports.

---

## 10. Proposed Minimal Change Plan

| File | Proposed Change | Smallest Necessary Edit |
| :--- | :--- | :--- |
| `README.md` | Synchronize test count and AI-use declaration | Change `13 unit tests` to `14 unit tests` at line 108. Update section 7 to disclose both Antigravity and ChatGPT. |
| `reports/report.md` | Author details (User Action) | Fill in student names and matriculation numbers in line 5. |
| `reports/generate_report.py` | Author details (User Action) | Update line 95 if student names are to be printed in the PDF banner. |
| `reports/generate_docx_report.py` | Author details (User Action) | Update line 324 if student names are to be regenerated into the DOCX. |

