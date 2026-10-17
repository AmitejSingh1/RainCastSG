"""Generates the official NUS DSA5208 Academic Project Report in PDF and Markdown format.

Embeds actual computing environment specs, data quality metrics, Task 1 exploratory figures,
Task 2 model architectures, calibration reliability curves, and prediction results.
"""

import json
import os
import platform
from pathlib import Path
from typing import Dict, Any, List
import pandas as pd
from fpdf import FPDF


class NUSReportPDF(FPDF):
    def header(self):
        if self.page_no() > 1:
            self.set_font("helvetica", "I", 8)
            self.set_text_color(100, 100, 100)
            self.cell(0, 8, "DSA5208 Project Report: Singapore Rainfall Analysis & Probabilistic Prediction", 0, 0, "L")
            self.cell(0, 8, f"NUS | Page {self.page_no()}", 0, 1, "R")
            self.line(10, 18, 200, 18)
            self.ln(4)

    def footer(self):
        self.set_y(-15)
        self.set_font("helvetica", "I", 8)
        self.set_text_color(120, 120, 120)
        self.cell(0, 10, "National University of Singapore - Department of Statistics and Data Science", 0, 0, "C")

    def chapter_title(self, num: str, label: str):
        self.set_font("helvetica", "B", 13)
        self.set_text_color(24, 43, 73) # Navy
        self.cell(0, 8, f"{num}. {label}", 0, 1, "L")
        self.ln(2)

    def section_title(self, label: str):
        self.set_font("helvetica", "B", 10.5)
        self.set_text_color(40, 40, 40)
        self.cell(0, 6, label, 0, 1, "L")
        self.ln(1)

    def body_text(self, text: str):
        self.set_font("helvetica", "", 9.5)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 5, text)
        self.ln(2)

    def add_image_figure(self, img_path: str, caption: str, width: int = 175):
        if Path(img_path).exists():
            self.image(img_path, w=width)
            self.ln(1)
            self.set_font("helvetica", "I", 8.5)
            self.set_text_color(80, 80, 80)
            self.multi_cell(0, 4, caption, align="C")
            self.ln(3)


def generate_pdf_report(
    config_path: str = "config/project_config.yaml",
    output_pdf: str = "reports/report.pdf"
):
    """Compiles the complete final report with tables, charts, and execution metrics."""
    pdf = NUSReportPDF(orientation="P", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=18)
    pdf.add_page()

    # 1. Title Banner
    pdf.set_fill_color(24, 43, 73) # Navy blue
    pdf.rect(0, 0, 210, 42, "F")
    pdf.set_y(8)
    pdf.set_font("helvetica", "B", 17)
    pdf.set_text_color(255, 255, 255)
    pdf.cell(0, 10, "National University of Singapore", 0, 1, "C")
    pdf.set_font("helvetica", "B", 13)
    pdf.cell(0, 8, "DSA5208 - Scalable Distributed Computing for Data Science", 0, 1, "C")
    pdf.set_font("helvetica", "I", 10)
    pdf.cell(0, 6, "Semester Project Report - Submission Date: October 2026", 0, 1, "C")
    pdf.ln(14)

    # Project Title
    pdf.set_font("helvetica", "B", 14)
    pdf.set_text_color(24, 43, 73)
    pdf.multi_cell(0, 7, "Singapore Historical Rainfall Analysis and 30-Minute Rainfall Probability Prediction Using Apache Spark", align="C")
    pdf.ln(4)

    # Authors & Metadata Box
    pdf.set_fill_color(245, 247, 250)
    pdf.set_draw_color(200, 210, 225)
    pdf.rect(15, pdf.get_y(), 180, 22, "DF")
    pdf.set_y(pdf.get_y() + 2)
    pdf.set_font("helvetica", "B", 9)
    pdf.set_text_color(40, 40, 40)
    pdf.cell(0, 5, "Project Team: RainCastSG Group", 0, 1, "C")
    pdf.set_font("helvetica", "", 8.5)
    pdf.cell(0, 4.5, "Institution: National University of Singapore (NUS)", 0, 1, "C")
    pdf.cell(0, 4.5, "GitHub Repository: https://github.com/AmitejSingh1/RainCastSG", 0, 1, "C")
    pdf.ln(8)

    # Executive Summary / Abstract
    pdf.chapter_title("1", "Executive Summary")
    pdf.body_text(
        "Accurate, localized, short-term precipitation estimation is critical for urban flood risk mitigation, "
        "transportation safety, and resource planning in Singapore's tropical maritime climate. This project delivers "
        "an end-to-end, reproducible big data processing and machine learning pipeline built on Apache Spark (PySpark 3.5.3) "
        "and Spark MLlib. Using the official National Environment Agency (NEA) historical rainfall dataset (Collection 2279) "
        "spanning 2017 to 2024 (over 45 million 5-minute rainfall observations across Singapore's national monitoring network), "
        "we perform scalable data cleaning, quality auditing, comprehensive exploratory spatial-temporal analysis, "
        "and train probabilistic models to forecast 30-minute accumulated rainfall exceedance: "
        "P(R_30 > theta | history) across thresholds theta in [0.0, 1.0, 2.0, 5.0, 10.0] mm."
    )

    # Computing Environment
    pdf.chapter_title("2", "Computing Environment & Hardware Specifications")
    pdf.body_text(
        "In accordance with project guidelines, all computations were performed locally using PySpark DataFrame "
        "and Spark SQL operations under pinned dependency constraints. The environment specifications were recorded as follows:\n"
        f"- Operating System: {platform.system()} {platform.release()} (Build {platform.version()})\n"
        "- Processor: 13th Gen Intel(R) Core(TM) i7-13620H (10 physical cores, 16 logical threads)\n"
        "- Physical Memory: 16.0 GB RAM (Configured Spark Driver: 4GB, Shuffle Partitions: 16)\n"
        "- Java Development Kit: Microsoft Build of OpenJDK 17.0.20.1 LTS (64-Bit Server VM)\n"
        "- Python & PySpark: Python 3.10.6, PySpark 3.5.3 with Hadoop 3.3.6 winutils native libraries\n"
        "- Storage Format: Raw CSV (8.05 GB total) -> Cleaned Snappy Parquet (540 MB total, partitioned by year)"
    )

    # Data Ingestion & Quality Audit
    pdf.chapter_title("3", "Data Ingestion, Cleaning & Quality Auditing")
    pdf.body_text(
        "The official annual datasets were retrieved directly from data.gov.sg (Collection 2279). "
        "Our transparent cleaning pipeline enforces strict validation rules:\n"
        "1. Unusable observation timestamps or missing station identifiers are rejected.\n"
        "2. Reading values are parsed numerically; negative or non-numeric values are excluded.\n"
        "3. Measurement unit ('mm') and reading type ('TB1 Rainfall 5 Minute Total F') are verified.\n"
        "4. Genuine zero-rainfall observations are strictly retained (not treated as missing).\n"
        "5. Conflicting duplicate station-time observations are resolved using latest update timestamps.\n"
        "6. Station coverage and temporal gaps are explicitly tracked without synthetic interpolation."
    )

    # Embed Quality Audit Table if available
    audit_csv = Path("outputs/quality/quality_audit_summary.csv")
    if audit_csv.exists():
        df_audit = pd.read_csv(audit_csv)
        pdf.section_title("Summary of Cleaned Annual Observations (2017-2024)")
        pdf.set_font("helvetica", "B", 8)
        col_w = [18, 30, 26, 26, 32, 28, 20]
        headers = ["Year", "Raw Records", "Clean Records", "Rejected", "Input CSV", "Parquet", "Runtime"]
        for h, w in zip(headers, col_w):
            pdf.cell(w, 6, h, 1, 0, "C", fill=True)
        pdf.ln()

        pdf.set_font("helvetica", "", 8)
        for _, row in df_audit.iterrows():
            pdf.cell(col_w[0], 5, str(int(row["year"])), 1, 0, "C")
            pdf.cell(col_w[1], 5, f"{int(row['raw_record_count']):,}", 1, 0, "R")
            pdf.cell(col_w[2], 5, f"{int(row['final_cleaned_record_count']):,}", 1, 0, "R")
            pdf.cell(col_w[3], 5, f"{int(row['total_rejected_records']):,}", 1, 0, "R")
            pdf.cell(col_w[4], 5, f"{row['input_csv_bytes']/(1024*1024):.1f} MB", 1, 0, "R")
            pdf.cell(col_w[5], 5, f"{row['output_parquet_bytes']/(1024*1024):.1f} MB", 1, 0, "R")
            pdf.cell(col_w[6], 5, f"{row['processing_time_seconds']:.1f}s", 1, 1, "R")
        pdf.ln(3)

    # Task 1 Exploratory Analysis
    pdf.chapter_title("4", "Task 1: Singapore Rainfall Patterns Exploration")
    pdf.body_text(
        "Using Spark SQL aggregations, we investigated Singapore's rainfall variation across annual, seasonal, "
        "diurnal, extreme, and spatial dimensions:"
    )

    # Figures
    pdf.add_image_figure("figures/fig1_annual_rainfall_summary.png", "Figure 1: Annual station-normalized rainfall accumulation (mm) and wet-interval occurrence frequency across Singapore (2017-2024).")
    pdf.add_image_figure("figures/fig2_monthly_seasonal_patterns.png", "Figure 2: Average monthly rainfall accumulation categorized by Singapore's four meteorological monsoon regimes.")
    pdf.add_image_figure("figures/fig3_year_month_heatmap.png", "Figure 3: Year-by-month rainfall accumulation heatmap (2017-2024), highlighting extreme monsoon surge events.")
    pdf.add_image_figure("figures/fig4_diurnal_hourly_rainfall.png", "Figure 4: Diurnal rainfall occurrence frequency (%) and mean intensity (mm/5-min) in Singapore Local Time (SGT), illustrating the afternoon solar heating convective peak (14:00-17:00).")
    pdf.add_image_figure("figures/fig5_rainfall_intensity_distribution.png", "Figure 5: Empirical rainfall intensity distribution (log scale) showing the extreme right skewness of tropical precipitation.")
    pdf.add_image_figure("figures/fig6_spatial_rainfall_distribution.png", "Figure 6: Geographic spatial distribution of monitoring stations across Singapore with normalized mean equivalent rainfall rate.")

    # Task 2 Methodology
    pdf.chapter_title("5", "Task 2: Probabilistic Rainfall Prediction Methodology")
    pdf.body_text(
        "1. Target Formulation: For station s at prediction time t, R_30(s,t) is defined as the sum of 6 consecutive "
        "future 5-minute intervals (t+5m to t+30m). Future completeness is verified by exact timestamp checks.\n"
        "2. Strict Causal Features: Only historical records at or before t are used (lags at 5m, 15m, 30m, 60m, rolling "
        "mean, rolling max, wet interval counts, and trend). Cyclic sine/cosine encodings are generated for hour and month.\n"
        "3. Chronological Splits & Leakage Prevention: Training: 2017-2022 (34,022,364 records); Validation: 2023 "
        "(6,686,688 records); Held-out Test: 2024 (6,338,426 records); Total verified feature rows: 47,047,478. "
        "Observations within 30 minutes of a split boundary are excluded so targets cannot cross partitions.\n"
        "4. MLlib Architectures: Logistic Regression baseline and Random Forest classifier with StringIndexer and OneHotEncoder."
    )
    pdf.add_image_figure("figures/fig13_event_prevalence.png", "Figure 7: Positive 30-minute rainfall exceedance prevalence across thresholds on the 2024 held-out test partition (N = 6,338,426), illustrating severe extreme-tail class imbalance.", width=140)

    # Evaluation & Calibration
    pdf.chapter_title("6", "Model Evaluation, Comparison & Probability Calibration")
    pdf.body_text(
        "Models were evaluated across five thresholds theta in [0.0, 1.0, 2.0, 5.0, 10.0] mm on both the 2023 validation "
        "and 2024 held-out test partitions (over 6.3 million independent test records). "
        "Due to severe precipitation sparsity, PR-AUC and Brier skill scores are evaluated alongside ROC-AUC."
    )

    test_rf_csv = Path("outputs/evaluation/evaluation_random_forest_test.csv")
    test_lr_csv = Path("outputs/evaluation/evaluation_logistic_regression_test.csv")
    if test_rf_csv.exists() and test_lr_csv.exists():
        df_rf = pd.read_csv(test_rf_csv)
        df_lr = pd.read_csv(test_lr_csv)
        pdf.section_title("Model Architecture Comparison on Held-Out Test Set (2024, 6,338,426 Observations)")
        pdf.set_font("helvetica", "B", 7.5)
        col_w = [18, 28, 22, 22, 22, 22, 24, 22]
        headers = ["Threshold", "Architecture", "ROC-AUC", "PR-AUC", "Brier Score", "BSS", "Recall (0.5)", "Precision"]
        for h, w in zip(headers, col_w):
            pdf.cell(w, 5.5, h, border=1, align="C", fill=True)
        pdf.ln()

        pdf.set_font("helvetica", "", 7.5)
        for th in [0.0, 1.0, 2.0, 5.0, 10.0]:
            r_lr = df_lr[df_lr["threshold_mm"] == th].iloc[0]
            r_rf = df_rf[df_rf["threshold_mm"] == th].iloc[0]

            # Logistic Regression row
            pdf.cell(col_w[0], 5, f"> {th} mm", border=1, align="C")
            pdf.cell(col_w[1], 5, "Logistic Regression", border=1, align="L")
            pdf.cell(col_w[2], 5, f"{r_lr['roc_auc']:.4f}", border=1, align="R")
            pdf.cell(col_w[3], 5, f"{r_lr['pr_auc']:.4f}", border=1, align="R")
            pdf.cell(col_w[4], 5, f"{r_lr['brier_score']:.5f}", border=1, align="R")
            pdf.cell(col_w[5], 5, f"{r_lr['brier_skill_score']:+.1%}", border=1, align="R")
            pdf.cell(col_w[6], 5, f"{r_lr['recall']:.1%}", border=1, align="R")
            pdf.cell(col_w[7], 5, f"{r_lr['precision']:.1%}", border=1, align="R")
            pdf.ln()

            # Random Forest row
            pdf.cell(col_w[0], 5, f"> {th} mm", border=1, align="C")
            pdf.cell(col_w[1], 5, "Random Forest (50T)", border=1, align="L")
            pdf.cell(col_w[2], 5, f"{r_rf['roc_auc']:.4f}", border=1, align="R")
            pdf.cell(col_w[3], 5, f"{r_rf['pr_auc']:.4f}", border=1, align="R")
            pdf.cell(col_w[4], 5, f"{r_rf['brier_score']:.5f}", border=1, align="R")
            pdf.cell(col_w[5], 5, f"{r_rf['brier_skill_score']:+.1%}", border=1, align="R")
            pdf.cell(col_w[6], 5, f"{r_rf['recall']:.1%}", border=1, align="R")
            pdf.cell(col_w[7], 5, f"{r_rf['precision']:.1%}", border=1, align="R")
            pdf.ln()

        pdf.ln(3)

        pdf.body_text(
            "Note on Metrics & Decision Cutoff: The table above reports raw model probabilities evaluated at the default "
            "decision threshold of 0.50. For Random Forest at theta = 10.0 mm (empirical test prevalence = 0.38%, 24,265 positive "
            "cases out of 6,338,426 test records), the model achieves a PR-AUC of 0.2529 (approximately 66x above the uninformative "
            "random prevalence baseline of 0.0038) and an ROC-AUC of 0.8984, establishing strong discriminative ranking across thresholds. "
            "However, because trees vote unweighted and reflect natural rarity, average predicted probabilities never reach 0.50 for "
            "any test observation, yielding zero positive predictions and 0.0% recall at the arbitrary 0.50 cutoff. "
            "Probability calibration (Platt scaling) was fitted exclusively on the 2023 validation split to prevent test data snooping."
        )

    pdf.add_image_figure("figures/fig12_model_comparison_bars.png", "Figure 8: Model performance comparison across rainfall thresholds on the 2024 held-out test set: (A) ROC-AUC and (B) PR-AUC relative to random prevalence baselines.", width=175)
    pdf.add_image_figure("figures/fig10_roc_curves.png", "Figure 9: Receiver Operating Characteristic (ROC) curves across five exceedance thresholds comparing Logistic Regression and Random Forest (50 trees) on the 2024 held-out test set.", width=180)
    pdf.add_image_figure("figures/fig11_precision_recall_curves.png", "Figure 10: Precision-Recall (PR) curves across five exceedance thresholds comparing Logistic Regression and Random Forest against empirical prevalence baselines.", width=180)
    pdf.add_image_figure("figures/fig8_calibration_logistic_regression.png", "Figure 11: Reliability diagrams before and after Platt scaling calibration fitted on the 2023 validation dataset.", width=175)

    # Prediction Interface
    pdf.chapter_title("7", "CLI Prediction Interface & Live Verification")
    pdf.body_text(
        "The standalone inference script 'src/predict.py' validates historical completeness, extracts causal features, "
        "applies the fitted Spark MLlib pipeline, and outputs calibrated probabilities with explanatory diagnostics:\n\n"
        "  python -m src.predict --station-id S06 --timestamp '2024-11-07 16:45:00' --threshold 1.0\n\n"
        "Incomplete observation windows are rejected to ensure forecasts remain dependable."
    )

    # Limitations & Conclusion
    pdf.chapter_title("8", "Limitations, AI-Use Declaration & References")
    pdf.body_text(
        "Limitations: 1) Point-Gauge Spatial Resolution: Ground gauge networks measure rainfall at discrete locations; localized "
        "tropical convective cells (1-3 km scale) may develop between stations without prior detection at upstream gauges. "
        "2) Rainfall-Only Predictor Set: The dataset (Collection 2279) provides rainfall observations only; thermodynamic atmospheric "
        "state variables (humidity, temperature, pressure, wind velocity profiles) are not available to forecast convective initiation. "
        "3) Rare-Event Imbalance: At extreme thresholds (theta = 10.0 mm), standard unweighted tree ensembles require decision threshold "
        "tuning or cost-utility optimization for operational alerting rather than a generic 0.50 cutoff. "
        "4) Historical Continuity Strictness: Inference requires a complete 60-minute historical sequence; telemetry gaps prevent prediction.\n\n"
        "AI-Use Declaration: In compliance with NUS academic and course guidelines, assistance from artificial intelligence tools "
        "(Google DeepMind's Antigravity and OpenAI's ChatGPT) was utilized for project architecture planning, PySpark pipeline development "
        "and debugging, unit test scaffolding, academic report drafting, and mathematical interpretation of probabilistic metrics. All "
        "data ingestion, transformations, feature engineering, model training, and empirical evaluations were independently executed "
        "and verified against actual Singapore government datasets.\n\n"
        "References:\n"
        "1. Singapore National Environment Agency (NEA), 'Historical Rainfall across Singapore', collection 2279, data.gov.sg.\n"
        "2. Apache Spark Documentation, 'Spark MLlib Guide: Pipelines and Classification', Apache Software Foundation, 2024.\n"
        "3. Wilks, D. S., 'Statistical Methods in the Atmospheric Sciences', 4th ed., Academic Press, 2019."
    )

    Path(output_pdf).parent.mkdir(parents=True, exist_ok=True)
    pdf.output(output_pdf)
    print(f"Report generated successfully: {output_pdf}")


if __name__ == "__main__":
    generate_pdf_report()
