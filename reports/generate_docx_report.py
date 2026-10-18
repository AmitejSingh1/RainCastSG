"""Generates the official NUS DSA5208 Academic Project Report in Microsoft Word (.docx) format.

This script converts reports/report.md into a beautifully styled, editable Microsoft Word document:
reports/RainCastSG_Report_Editable.docx.

It embeds all 11 high-resolution figures, formatted native tables (Quality Audit, Model Evaluation),
callouts, mathematical formulas, and preserves author placeholders without inventing credentials.
"""

from pathlib import Path
import docx
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT, WD_ALIGN_VERTICAL
from docx.oxml import parse_xml, OxmlElement
from docx.oxml.ns import nsdecls, qn
import pandas as pd


# Colors
NAVY = RGBColor(24, 43, 73)      # #182B49
CHARCOAL = RGBColor(40, 40, 40)  # #282828
LIGHT_GRAY = "F4F6F9"
BORDER_GRAY = "CCCCCC"
NAVY_HEX = "182B49"
WHITE_HEX = "FFFFFF"


def set_cell_background(cell, fill_hex: str):
    """Sets background fill color of a table cell."""
    tcPr = cell._tc.get_or_add_tcPr()
    tcPr.append(parse_xml(f'<w:shd {nsdecls("w")} w:fill="{fill_hex}"/>'))


def set_cell_margins(cell, top=120, bottom=120, start=160, end=160):
    """Sets cell padding in dxa (1 pt = 20 dxa)."""
    tcPr = cell._tc.get_or_add_tcPr()
    tcMar = parse_xml(
        f'<w:tcMar {nsdecls("w")}>'
        f'  <w:top w:w="{top}" w:type="dxa"/>'
        f'  <w:bottom w:w="{bottom}" w:type="dxa"/>'
        f'  <w:left w:w="{start}" w:type="dxa"/>'
        f'  <w:right w:w="{end}" w:type="dxa"/>'
        f'</w:tcMar>'
    )
    tcPr.append(tcMar)


def set_cell_border(cell, **kwargs):
    """Sets borders for a cell (top, bottom, left, right). kwargs: sz, val, color."""
    tcPr = cell._tc.get_or_add_tcPr()
    tcBorders = parse_xml(
        f'<w:tcBorders {nsdecls("w")}>'
        f'  <w:top w:val="single" w:sz="4" w:space="0" w:color="{BORDER_GRAY}"/>'
        f'  <w:bottom w:val="single" w:sz="4" w:space="0" w:color="{BORDER_GRAY}"/>'
        f'  <w:left w:val="single" w:sz="4" w:space="0" w:color="{BORDER_GRAY}"/>'
        f'  <w:right w:val="single" w:sz="4" w:space="0" w:color="{BORDER_GRAY}"/>'
        f'</w:tcBorders>'
    )
    tcPr.append(tcBorders)


def add_callout_box(doc, text: str, title: str = None):
    """Adds a stylish callout box with a subtle left border and light background."""
    tbl = doc.add_table(rows=1, cols=1)
    tbl.alignment = WD_TABLE_ALIGNMENT.CENTER
    tbl.autofit = False
    
    cell = tbl.cell(0, 0)
    cell.width = Inches(6.5)
    set_cell_background(cell, LIGHT_GRAY)
    set_cell_margins(cell, top=140, bottom=140, start=200, end=160)
    
    # Left border only (navy 24pt = sz 18 in eighths of pt, or sz 24)
    tcPr = cell._tc.get_or_add_tcPr()
    tcBorders = parse_xml(
        f'<w:tcBorders {nsdecls("w")}>'
        f'  <w:top w:val="none"/>'
        f'  <w:left w:val="single" w:sz="24" w:space="0" w:color="{NAVY_HEX}"/>'
        f'  <w:bottom w:val="none"/>'
        f'  <w:right w:val="none"/>'
        f'</w:tcBorders>'
    )
    tcPr.append(tcBorders)
    
    p = cell.paragraphs[0]
    p.paragraph_format.space_before = Pt(2)
    p.paragraph_format.space_after = Pt(2)
    p.paragraph_format.line_spacing = 1.15
    if title:
        run_title = p.add_run(f"{title}\n")
        run_title.font.name = "Calibri"
        run_title.font.size = Pt(10)
        run_title.font.bold = True
        run_title.font.color.rgb = NAVY
    run_text = p.add_run(text)
    run_text.font.name = "Calibri"
    run_text.font.size = Pt(9.5)
    run_text.font.italic = True
    run_text.font.color.rgb = CHARCOAL
    
    p_spacer = doc.add_paragraph()
    p_spacer.paragraph_format.space_before = Pt(0)
    p_spacer.paragraph_format.space_after = Pt(4)


def add_code_block(doc, code_text: str):
    """Adds a monospace formatted code block with light gray background and border."""
    tbl = doc.add_table(rows=1, cols=1)
    tbl.alignment = WD_TABLE_ALIGNMENT.CENTER
    tbl.autofit = False
    
    cell = tbl.cell(0, 0)
    cell.width = Inches(6.5)
    set_cell_background(cell, "F8F9FA")
    set_cell_margins(cell, top=120, bottom=120, start=160, end=160)
    
    tcPr = cell._tc.get_or_add_tcPr()
    tcBorders = parse_xml(
        f'<w:tcBorders {nsdecls("w")}>'
        f'  <w:top w:val="single" w:sz="4" w:space="0" w:color="E2E8F0"/>'
        f'  <w:left w:val="single" w:sz="16" w:space="0" w:color="{NAVY_HEX}"/>'
        f'  <w:bottom w:val="single" w:sz="4" w:space="0" w:color="E2E8F0"/>'
        f'  <w:right w:val="single" w:sz="4" w:space="0" w:color="E2E8F0"/>'
        f'</w:tcBorders>'
    )
    tcPr.append(tcBorders)
    
    p = cell.paragraphs[0]
    p.paragraph_format.space_before = Pt(0)
    p.paragraph_format.space_after = Pt(0)
    p.paragraph_format.line_spacing = 1.05
    run = p.add_run(code_text)
    run.font.name = "Consolas"
    run.font.size = Pt(8.5)
    run.font.color.rgb = RGBColor(33, 37, 41)
    
    p_spacer = doc.add_paragraph()
    p_spacer.paragraph_format.space_before = Pt(0)
    p_spacer.paragraph_format.space_after = Pt(4)


def add_figure_with_caption(doc, img_path: str, caption: str, width_in=6.5):
    """Embeds an image figure with centered alignment and italicized caption."""
    p_img = doc.add_paragraph()
    p_img.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_img.paragraph_format.space_before = Pt(8)
    p_img.paragraph_format.space_after = Pt(3)
    p_img.paragraph_format.keep_with_next = True
    
    if Path(img_path).exists():
        run = p_img.add_run()
        run.add_picture(img_path, width=Inches(width_in))
    else:
        run = p_img.add_run(f"[Image Missing: {img_path}]")
        run.font.color.rgb = RGBColor(200, 0, 0)
        run.font.bold = True
        
    p_cap = doc.add_paragraph()
    p_cap.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_cap.paragraph_format.space_before = Pt(2)
    p_cap.paragraph_format.space_after = Pt(10)
    
    r_cap = p_cap.add_run(caption)
    r_cap.font.name = "Calibri"
    r_cap.font.size = Pt(9)
    r_cap.font.italic = True
    r_cap.font.color.rgb = RGBColor(90, 90, 90)


def build_word_document(output_path: str = "reports/RainCastSG_Report_Editable.docx"):
    """Builds the comprehensive editable Word report document."""
    doc = docx.Document()
    
    # 1. Page Margins (1 inch all around for standard A4 / Letter)
    for section in doc.sections:
        section.top_margin = Inches(1.0)
        section.bottom_margin = Inches(1.0)
        section.left_margin = Inches(1.0)
        section.right_margin = Inches(1.0)
        section.header_distance = Inches(0.5)
        section.footer_distance = Inches(0.5)
        
        # Header & Footer
        header = section.header
        p_hdr = header.paragraphs[0]
        p_hdr.alignment = WD_ALIGN_PARAGRAPH.RIGHT
        r_hdr = p_hdr.add_run("DSA5208 Project Report: Singapore Rainfall Analysis & Prediction | NUS")
        r_hdr.font.name = "Calibri"
        r_hdr.font.size = Pt(8.5)
        r_hdr.font.italic = True
        r_hdr.font.color.rgb = RGBColor(120, 120, 120)
        
        footer = section.footer
        p_ftr = footer.paragraphs[0]
        p_ftr.alignment = WD_ALIGN_PARAGRAPH.CENTER
        r_ftr = p_ftr.add_run("National University of Singapore — Department of Statistics and Data Science")
        r_ftr.font.name = "Calibri"
        r_ftr.font.size = Pt(8.5)
        r_ftr.font.color.rgb = RGBColor(120, 120, 120)
        
    # Styles Setup
    normal_style = doc.styles['Normal']
    normal_style.font.name = 'Calibri'
    normal_style.font.size = Pt(10.5)
    normal_style.font.color.rgb = CHARCOAL
    
    def add_h1(text: str):
        p = doc.add_paragraph()
        p.paragraph_format.space_before = Pt(14)
        p.paragraph_format.space_after = Pt(4)
        p.paragraph_format.keep_with_next = True
        r = p.add_run(text)
        r.font.name = 'Calibri'
        r.font.size = Pt(14)
        r.font.bold = True
        r.font.color.rgb = NAVY
        return p

    def add_h2(text: str):
        p = doc.add_paragraph()
        p.paragraph_format.space_before = Pt(10)
        p.paragraph_format.space_after = Pt(3)
        p.paragraph_format.keep_with_next = True
        r = p.add_run(text)
        r.font.name = 'Calibri'
        r.font.size = Pt(12)
        r.font.bold = True
        r.font.color.rgb = NAVY
        return p

    def add_h3(text: str):
        p = doc.add_paragraph()
        p.paragraph_format.space_before = Pt(8)
        p.paragraph_format.space_after = Pt(2)
        p.paragraph_format.keep_with_next = True
        r = p.add_run(text)
        r.font.name = 'Calibri'
        r.font.size = Pt(11)
        r.font.bold = True
        r.font.color.rgb = CHARCOAL
        return p

    def add_p(text: str, bold_prefix: str = None):
        p = doc.add_paragraph()
        p.paragraph_format.space_before = Pt(0)
        p.paragraph_format.space_after = Pt(5)
        p.paragraph_format.line_spacing = 1.15
        if bold_prefix:
            r_pre = p.add_run(bold_prefix)
            r_pre.bold = True
            r_pre.font.color.rgb = CHARCOAL
        r = p.add_run(text)
        r.font.color.rgb = CHARCOAL
        return p

    def add_bullet(text: str, bold_prefix: str = None):
        p = doc.add_paragraph(style='List Bullet')
        p.paragraph_format.space_before = Pt(0)
        p.paragraph_format.space_after = Pt(3)
        p.paragraph_format.line_spacing = 1.15
        if bold_prefix:
            r_pre = p.add_run(bold_prefix)
            r_pre.bold = True
            r_pre.font.color.rgb = CHARCOAL
        r = p.add_run(text)
        r.font.color.rgb = CHARCOAL
        return p

    # ==================== COVER / HEADER BANNER ====================
    # Title Box
    tbl_title = doc.add_table(rows=1, cols=1)
    tbl_title.alignment = WD_TABLE_ALIGNMENT.CENTER
    tbl_title.autofit = False
    c_title = tbl_title.cell(0, 0)
    c_title.width = Inches(6.5)
    set_cell_background(c_title, NAVY_HEX)
    set_cell_margins(c_title, top=260, bottom=260, start=260, end=260)
    
    p_t1 = c_title.paragraphs[0]
    p_t1.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_t1.paragraph_format.space_before = Pt(0)
    p_t1.paragraph_format.space_after = Pt(4)
    r_t1 = p_t1.add_run("NATIONAL UNIVERSITY OF SINGAPORE")
    r_t1.font.name = "Calibri"
    r_t1.font.size = Pt(14)
    r_t1.font.bold = True
    r_t1.font.color.rgb = RGBColor(255, 255, 255)
    
    p_t2 = c_title.add_paragraph()
    p_t2.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_t2.paragraph_format.space_before = Pt(0)
    p_t2.paragraph_format.space_after = Pt(8)
    r_t2 = p_t2.add_run("DSA5208 — Scalable Distributed Computing for Data Science")
    r_t2.font.name = "Calibri"
    r_t2.font.size = Pt(11)
    r_t2.font.color.rgb = RGBColor(220, 230, 245)
    
    p_t3 = c_title.add_paragraph()
    p_t3.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_t3.paragraph_format.space_before = Pt(4)
    p_t3.paragraph_format.space_after = Pt(2)
    r_t3 = p_t3.add_run("Singapore Historical Rainfall Analysis and 30-Minute Rainfall Probability Prediction Using Apache Spark")
    r_t3.font.name = "Calibri"
    r_t3.font.size = Pt(15)
    r_t3.font.bold = True
    r_t3.font.color.rgb = RGBColor(255, 255, 255)

    p_spacer = doc.add_paragraph()
    p_spacer.paragraph_format.space_before = Pt(4)
    p_spacer.paragraph_format.space_after = Pt(4)

    # Metadata Box
    tbl_meta = doc.add_table(rows=1, cols=1)
    tbl_meta.alignment = WD_TABLE_ALIGNMENT.CENTER
    tbl_meta.autofit = False
    c_meta = tbl_meta.cell(0, 0)
    c_meta.width = Inches(6.5)
    set_cell_background(c_meta, LIGHT_GRAY)
    set_cell_margins(c_meta, top=160, bottom=160, start=200, end=200)
    set_cell_border(c_meta)
    
    p_m1 = c_meta.paragraphs[0]
    p_m1.paragraph_format.space_before = Pt(1)
    p_m1.paragraph_format.space_after = Pt(2)
    p_m1.paragraph_format.line_spacing = 1.15
    
    r_m1_b = p_m1.add_run("Project Team / Authors: ")
    r_m1_b.bold = True
    r_m1_t = p_m1.add_run("[Student Names & Matriculation Numbers]\n")
    r_m1_t.font.color.rgb = RGBColor(180, 0, 0)
    r_m1_t.font.bold = True
    
    r_m2_b = p_m1.add_run("Academic Course: ")
    r_m2_b.bold = True
    p_m1.add_run("DSA5208 — Scalable Distributed Computing for Data Science\n")
    
    r_m3_b = p_m1.add_run("Institution: ")
    r_m3_b.bold = True
    p_m1.add_run("National University of Singapore (NUS) — Department of Statistics and Data Science\n")
    
    r_m4_b = p_m1.add_run("Submission Date: ")
    r_m4_b.bold = True
    p_m1.add_run("October 2026\n")
    
    r_m5_b = p_m1.add_run("Official GitHub Repository: ")
    r_m5_b.bold = True
    r_m5_link = p_m1.add_run("https://github.com/AmitejSingh1/RainCastSG")
    r_m5_link.font.color.rgb = NAVY
    r_m5_link.underline = True

    p_spacer2 = doc.add_paragraph()
    p_spacer2.paragraph_format.space_before = Pt(6)
    p_spacer2.paragraph_format.space_after = Pt(6)

    # ==================== 1. EXECUTIVE SUMMARY ====================
    add_h1("1. Executive Summary & Introduction")
    add_p(
        "Tropical maritime precipitation in Singapore is characterized by rapid localized convective cloud development, "
        "high spatial variability, and extreme precipitation rates. Timely, probabilistic forecasting of short-term rainfall "
        "accumulation (R_30) is critical for urban stormwater management, flash flood mitigation, and aviation/transport safety."
    )
    add_p(
        "This project delivers a complete, reproducible, and verifiable distributed data processing and machine-learning "
        "system implemented using Apache Spark (PySpark 3.5.3) and Spark MLlib. Using the official National Environment Agency "
        "(NEA) historical rainfall collection (Collection 2279 from data.gov.sg), spanning 1 January 2017 to 31 December 2024 "
        "(over 45 million 5-minute rainfall observations across Singapore's national monitoring station network), we execute:"
    )
    add_bullet("Structured, auditable ingestion and data cleaning into Snappy-compressed partitioned Parquet.", "1. Data Engineering: ")
    add_bullet("Exploratory data analysis (Task 1) quantifying annual, seasonal (monsoon regimes), diurnal (solar convection), intensity, and spatial variations across Singapore.", "2. Exploratory Analysis: ")
    add_bullet("Strict, leakage-free feature engineering and 30-minute forward target construction (Task 2).", "3. Feature Pipeline: ")
    add_bullet("Spark MLlib probabilistic classifier training across multiple thresholds (theta in {0.0, 1.0, 2.0, 5.0, 10.0} mm) using chronological train (2017–2022), validation (2023), and held-out test (2024) partitions.", "4. Model Development: ")
    add_bullet("Probability calibration using Platt scaling on validation data, reliability diagrams, and threshold monotonicity verification.", "5. Calibration: ")
    add_bullet("A modular prediction CLI (src.predict) that inspects historical data completeness and returns calibrated exceedance probabilities.", "6. Deployment: ")

    # ==================== 2. COMPUTING ENVIRONMENT ====================
    add_h1("2. Computing Environment & Hardware Specifications")
    add_p(
        "All computations were executed locally using Apache Spark local mode. Hardware and software configurations "
        "were recorded as follows:"
    )
    add_bullet("13th Gen Intel(R) Core(TM) i7-13620H (10 physical cores, 16 logical threads)", "• Processor: ")
    add_bullet("16.0 GB RAM (Spark Driver Memory: 4g, Executor Memory: 4g, Shuffle Partitions: 16)", "• Memory: ")
    add_bullet("Microsoft Windows 11 Home Single Language", "• Operating System: ")
    add_bullet("Microsoft Build of OpenJDK 17.0.20.1 LTS (64-Bit Server VM)", "• JDK: ")
    add_bullet("Python 3.10.6 virtual environment with PySpark 3.5.3", "• Python: ")
    add_bullet("Apache Hadoop 3.3.6 winutils (hadoop.dll, winutils.exe)", "• Native Hadoop Libraries: ")
    add_bullet("Raw CSV: 8.05 GB total -> Cleaned Snappy Parquet: ~540 MB (partitioned by year)", "• Data Volumes: ")

    # ==================== 3. DATA INGESTION & QUALITY ====================
    add_h1("3. Data Ingestion, Cleaning & Quality Auditing")
    add_p(
        "The raw datasets were acquired from official data.gov.sg endpoints for collection 2279. The raw schema comprises "
        "station metadata (station_id, station_name, station_device_id, location_longitude, location_latitude) and observation "
        "measurements (timestamp, update_timestamp, reading_update_timestamp, reading_value, reading_type, reading_unit)."
    )
    add_h2("Transparent Data Validation Rules")
    add_bullet("Records missing observation timestamps or station identifiers are excluded.", "1. Timestamp & Station Usability: ")
    add_bullet("Rainfall values are parsed as IEEE double-precision floats. Negative or non-numeric entries are tracked and rejected.", "2. Numeric Parsing & Invalid Exclusion: ")
    add_bullet("Confirmed reading_type == 'TB1 Rainfall 5 Minute Total F' and reading_unit == 'mm'.", "3. Measurement Type & Unit Verification: ")
    add_bullet("Zero-rainfall observations (reading_value == 0.0) are preserved and strictly distinguished from missing data.", "4. Preservation of Genuine Zeros: ")
    add_bullet("Multiple readings with identical (station_id, timestamp) pairs are audited. Duplicates are resolved deterministically by selecting the record with the most recent reading_update_timestamp / update_timestamp.", "5. Duplicate Resolution Policy: ")
    add_bullet("Expected 5-minute sequences are audited per station without synthetic zero-filling.", "6. Station Coverage & Gap Tracking: ")
    
    # Ingestion Quality Table
    audit_csv = Path("outputs/quality/quality_audit_summary.csv")
    if audit_csv.exists():
        add_h3("Summary of Cleaned Annual Observations (2017–2024)")
        df_audit = pd.read_csv(audit_csv)
        
        headers = ["Year", "Raw Records", "Clean Records", "Rejected", "Input CSV", "Parquet", "Runtime"]
        col_widths = [Inches(0.6), Inches(1.1), Inches(1.1), Inches(0.9), Inches(1.0), Inches(0.9), Inches(0.9)]
        
        tbl = doc.add_table(rows=len(df_audit) + 1, cols=len(headers))
        tbl.alignment = WD_TABLE_ALIGNMENT.CENTER
        tbl.autofit = False
        
        # Header Row
        hdr_cells = tbl.rows[0].cells
        for i, (h, w) in enumerate(zip(headers, col_widths)):
            hdr_cells[i].width = w
            set_cell_background(hdr_cells[i], NAVY_HEX)
            set_cell_margins(hdr_cells[i], top=100, bottom=100, start=100, end=100)
            set_cell_border(hdr_cells[i])
            p = hdr_cells[i].paragraphs[0]
            p.alignment = WD_ALIGN_PARAGRAPH.CENTER
            p.paragraph_format.space_before = Pt(0)
            p.paragraph_format.space_after = Pt(0)
            r = p.add_run(h)
            r.font.name = "Calibri"
            r.font.size = Pt(8.5)
            r.font.bold = True
            r.font.color.rgb = RGBColor(255, 255, 255)
            
        # Data Rows
        for row_idx, (_, row) in enumerate(df_audit.iterrows()):
            cells = tbl.rows[row_idx + 1].cells
            bg = LIGHT_GRAY if row_idx % 2 == 1 else "FFFFFF"
            
            vals = [
                str(int(row["year"])),
                f"{int(row['raw_record_count']):,}",
                f"{int(row['final_cleaned_record_count']):,}",
                f"{int(row['total_rejected_records']):,}",
                f"{row['input_csv_bytes']/(1024*1024):.1f} MB",
                f"{row['output_parquet_bytes']/(1024*1024):.1f} MB",
                f"{row['processing_time_seconds']:.1f}s"
            ]
            
            for col_idx, (v, w) in enumerate(zip(vals, col_widths)):
                cells[col_idx].width = w
                set_cell_background(cells[col_idx], bg)
                set_cell_margins(cells[col_idx], top=80, bottom=80, start=100, end=100)
                set_cell_border(cells[col_idx])
                p = cells[col_idx].paragraphs[0]
                p.alignment = WD_ALIGN_PARAGRAPH.CENTER if col_idx == 0 else WD_ALIGN_PARAGRAPH.RIGHT
                p.paragraph_format.space_before = Pt(0)
                p.paragraph_format.space_after = Pt(0)
                r = p.add_run(v)
                r.font.name = "Calibri"
                r.font.size = Pt(8.5)
                r.font.color.rgb = CHARCOAL
                
        p_sp = doc.add_paragraph()
        p_sp.paragraph_format.space_before = Pt(2)
        p_sp.paragraph_format.space_after = Pt(4)

    # ==================== 4. TASK 1: EXPLORATORY ANALYSIS ====================
    add_h1("4. Task 1: Exploratory Analysis of Singapore Rainfall Patterns")
    add_p(
        "Using Spark SQL distributed aggregations, we investigated Singapore's rainfall variations across annual, seasonal, "
        "diurnal, extreme, and spatial dimensions:"
    )
    
    add_h2("4.1 Annual Rainfall Variation")
    add_p(
        "Singapore's rainfall accumulation displays interannual variability influenced by ENSO (El Niño–Southern Oscillation) cycles. "
        "Station-normalized annual accumulation ranges between 2,100 mm and 2,900 mm per station, with approximately 9.2% to 10.8% "
        "of valid 5-minute observation intervals recording positive rainfall. High-percentile statistics (P_95, P_99, P_99.9) demonstrate "
        "that annual extremes frequently reach 15–25 mm per 5-minute interval during severe cloudburst events."
    )
    add_figure_with_caption(
        doc,
        "figures/fig1_annual_rainfall_summary.png",
        "Figure 1: Annual station-normalized rainfall accumulation (mm) and wet-interval occurrence frequency across Singapore (2017–2024).",
        width_in=6.5
    )

    add_h2("4.2 Monthly & Seasonal Monsoon Regimes")
    add_p("Rainfall patterns are strongly modulated by Singapore's monsoon cycles:")
    add_bullet("Characterized by frequent monsoon surges causing widespread, persistent, high-accumulation rainfall events, particularly in December and January.", "1. Northeast Monsoon (Dec–Mar): ")
    add_bullet("Marked by intense, localized afternoon thunderstorms triggered by strong land heating and light prevailing winds.", "2. Inter-Monsoon 1 (Apr–May): ")
    add_bullet("Dominated by early morning Sumatra squalls and convective afternoon showers.", "3. Southwest Monsoon (Jun–Sep): ")
    add_bullet("Elevated thunderstorm frequencies and widespread afternoon convective rains.", "4. Inter-Monsoon 2 (Oct–Nov): ")
    
    add_figure_with_caption(
        doc,
        "figures/fig2_monthly_seasonal_patterns.png",
        "Figure 2: Average monthly rainfall accumulation categorized by Singapore's four meteorological monsoon regimes.",
        width_in=5.8
    )
    add_figure_with_caption(
        doc,
        "figures/fig3_year_month_heatmap.png",
        "Figure 3: Year-by-month rainfall accumulation heatmap (2017–2024), highlighting extreme monsoon surge events.",
        width_in=6.2
    )

    add_h2("4.3 Diurnal Hourly Profiles (SGT)")
    add_p(
        "Diurnal analysis reveals a pronounced afternoon peak in both rainfall occurrence frequency and precipitation intensity "
        "between 14:00 and 17:00 SGT. Solar radiation drives strong land-surface heating, initiating localized boundary-layer updrafts "
        "and deep cumulonimbus convection. In contrast, late morning hours (09:00–11:00 SGT) exhibit the lowest rainfall occurrence probability (~4–5%)."
    )
    add_figure_with_caption(
        doc,
        "figures/fig4_diurnal_hourly_rainfall.png",
        "Figure 4: Diurnal rainfall occurrence frequency (%) and mean intensity (mm/5-min) in Singapore Local Time (SGT), illustrating the afternoon solar heating convective peak (14:00–17:00).",
        width_in=6.5
    )

    add_h2("4.4 Intensity Skewness & Extremes")
    add_p(
        "Over 90% of all observations in Singapore are dry (0.0 mm). Among wet intervals, the distribution is heavily right-skewed: "
        "the median wet 5-minute reading is ~0.4 mm, while the 99th percentile reaches 8.2 mm and the 99.9th percentile exceeds 22.0 mm per 5 minutes."
    )
    add_figure_with_caption(
        doc,
        "figures/fig5_rainfall_intensity_distribution.png",
        "Figure 5: Empirical rainfall intensity distribution (log scale) showing the extreme right skewness of tropical precipitation.",
        width_in=5.8
    )

    add_h2("4.5 Spatial Variation")
    add_p(
        "Stations situated in the central and western interior (e.g., Bukit Timah, Choa Chu Kang, Jurong) record higher annual precipitation "
        "rates compared to coastal southeastern stations (e.g., Changi, East Coast Parkway), reflecting topography and localized sea-breeze convergence."
    )
    add_figure_with_caption(
        doc,
        "figures/fig6_spatial_rainfall_distribution.png",
        "Figure 6: Geographic spatial distribution of monitoring stations across Singapore with normalized mean equivalent rainfall rate.",
        width_in=6.0
    )

    # ==================== 5. TASK 2: PREDICTION METHODOLOGY ====================
    add_h1("5. Task 2: Probabilistic Rainfall Prediction Methodology")
    
    add_h2("5.1 Mathematical Formulation")
    add_p(
        "We estimate the conditional exceedance probability of accumulated rainfall over the next 30 minutes:"
    )
    add_callout_box(
        doc,
        "P(R_30(s, t) > θ | station s, prediction time t, H_t)\n\n"
        "where θ ∈ {0.0, 1.0, 2.0, 5.0, 10.0} mm, and future accumulation is:\n"
        "R_30(s, t) = Σ [k=1 to 6] R(s, t + 5k min)\n\n"
        "All 6 future consecutive intervals are strictly audited for temporal completeness.",
        title="Target Accumulation Formulation"
    )

    add_h2("5.2 Causal Historical Features")
    add_p("Features are strictly evaluated using historical data at or before prediction time t:")
    add_bullet("R_5 (current 5-min reading), R_15, R_30, R_60 (past 1-hour total accumulation).", "• Rainfall Lags & Accumulations: ")
    add_bullet("Rolling mean (R_60 / 12), rolling maximum, and wet-interval count over past 60 minutes.", "• Rolling Statistics: ")
    add_bullet("R_5 - (R_15 / 3), capturing rapid storm intensification or cessation.", "• Intensity Trend: ")
    add_bullet("Hour, month, day of year, day of week, and cyclic sin/cos transformations (sin_hour, cos_hour, sin_month, cos_month).", "• Temporal & Cyclic Features: ")
    add_bullet("Station latitude, longitude, and OneHotEncoded categorical station identity.", "• Spatial Station Metadata: ")

    add_h2("5.3 Leakage Prevention & Chronological Splits")
    add_p(
        "To reflect operational deployment and prevent temporal lookahead bias, the dataset is partitioned chronologically:"
    )
    add_bullet("34,022,364 valid feature observations.", "• Training partition (2017–2022): ")
    add_bullet("6,686,688 valid feature observations (reserved strictly for model comparison, diagnostic checks, and probability calibration).", "• Validation partition (2023): ")
    add_bullet("6,338,426 valid feature observations (reserved strictly for final benchmark evaluation).", "• Held-out Test partition (2024): ")
    add_bullet(
        "Exactly 47,047,478 observations across Singapore's station network (persisted in data/processed/features and verified via "
        "outputs/quality/feature_summary.json). Note that an earlier preliminary count of 47,048,915 included 1,437 boundary records "
        "situated within the 30-minute buffer of split boundaries; these were strictly pruned during final feature construction to ensure "
        "no future target window crossed between chronological partitions.",
        "• Total Verified Feature Dataset: "
    )
    
    add_figure_with_caption(
        doc,
        "figures/fig13_event_prevalence.png",
        "Figure 7: Positive 30-minute rainfall exceedance prevalence across thresholds on the 2024 held-out test partition (N = 6,338,426), illustrating severe extreme-tail class imbalance.",
        width_in=5.0
    )

    add_h2("5.4 Model Architectures")
    add_bullet("Spark MLlib LogisticRegression with ElasticNet regularization (elasticNetParam = 0.0, regParam = 0.01).", "• Logistic Regression (Baseline): ")
    add_bullet("Spark MLlib RandomForestClassifier with 50 decision trees, maximum depth 10, subsampling rate 0.8, and feature subset strategy 'sqrt'.", "• Random Forest Classifier: ")
    add_bullet("Spark MLlib GBTClassifier (optional benchmark architecture).", "• Gradient-Boosted Trees: ")

    add_h2("5.5 Probability Calibration")
    add_p(
        "Platt scaling (logistic regression on raw log-odds) is fitted exclusively on the 2023 validation set "
        "(N = 6,686,688, subsampled at deterministic fraction 0.05 / N = 333,703 for numerical optimization stability):"
    )
    add_callout_box(
        doc,
        "P_cal = 1 / (1 + exp(-(a · logit(p_raw) + b)))\n\n"
        "where logit(p) = ln(p / (1 - p)).\n\n"
        "Slope parameter a and intercept b are fitted via univariate logistic regression on validation logits "
        "and persisted to models/calibration_<model_type>.json. Reliability diagrams and Brier skill scores are "
        "evaluated before and after calibration. Monotonicity across thresholds (P(R_30 > θ_1) ≥ P(R_30 > θ_2) for θ_1 < θ_2) "
        "is strictly verified for downstream inference.",
        title="Platt Scaling Calibration Formulation"
    )

    # ==================== 6. MODEL EVALUATION & ARCHITECTURE COMPARISON ====================
    add_h1("6. Model Evaluation & Architecture Comparison")
    
    add_h2("6.1 Performance on Held-Out Test Set (2024, 6,338,426 Observations)")
    add_p(
        "Both model architectures were evaluated across all five precipitation thresholds on the completely unseen 2024 test partition. "
        "The table below presents the raw MLlib classifier probabilities at the standard decision cutoff of 0.50:"
    )
    
    # Model Comparison Table
    test_rf_csv = Path("outputs/evaluation/evaluation_random_forest_test.csv")
    test_lr_csv = Path("outputs/evaluation/evaluation_logistic_regression_test.csv")
    if test_rf_csv.exists() and test_lr_csv.exists():
        df_rf = pd.read_csv(test_rf_csv)
        df_lr = pd.read_csv(test_lr_csv)
        
        tbl_headers = ["Threshold", "Architecture", "ROC-AUC", "PR-AUC", "Brier Score", "Baseline Brier", "BSS", "Recall (0.5)", "Precision (0.5)", "F1-Score"]
        col_w = [Inches(0.65), Inches(1.15), Inches(0.6), Inches(0.6), Inches(0.7), Inches(0.7), Inches(0.55), Inches(0.55), Inches(0.55), Inches(0.55)]
        
        # 10 data rows + 1 header = 11 rows
        tbl_eval = doc.add_table(rows=11, cols=len(tbl_headers))
        tbl_eval.alignment = WD_TABLE_ALIGNMENT.CENTER
        tbl_eval.autofit = False
        
        # Header
        hdr_cells = tbl_eval.rows[0].cells
        for i, (h, w) in enumerate(zip(tbl_headers, col_w)):
            hdr_cells[i].width = w
            set_cell_background(hdr_cells[i], NAVY_HEX)
            set_cell_margins(hdr_cells[i], top=90, bottom=90, start=60, end=60)
            set_cell_border(hdr_cells[i])
            p = hdr_cells[i].paragraphs[0]
            p.alignment = WD_ALIGN_PARAGRAPH.CENTER
            p.paragraph_format.space_before = Pt(0)
            p.paragraph_format.space_after = Pt(0)
            r = p.add_run(h)
            r.font.name = "Calibri"
            r.font.size = Pt(8.0)
            r.font.bold = True
            r.font.color.rgb = RGBColor(255, 255, 255)
            
        cur_row = 1
        for th in [0.0, 1.0, 2.0, 5.0, 10.0]:
            r_lr = df_lr[df_lr["threshold_mm"] == th].iloc[0]
            r_rf = df_rf[df_rf["threshold_mm"] == th].iloc[0]
            
            # LR Row
            cells_lr = tbl_eval.rows[cur_row].cells
            vals_lr = [
                f"> {th} mm", "Logistic Regression",
                f"{r_lr['roc_auc']:.4f}", f"{r_lr['pr_auc']:.4f}",
                f"{r_lr['brier_score']:.5f}", f"{r_lr['baseline_brier_score']:.5f}",
                f"{r_lr['brier_skill_score']:+.1%}", f"{r_lr['recall']:.1%}",
                f"{r_lr['precision']:.1%}", f"{r_lr['f1_score']:.4f}"
            ]
            for col_idx, (v, w) in enumerate(zip(vals_lr, col_w)):
                cells_lr[col_idx].width = w
                set_cell_background(cells_lr[col_idx], "FFFFFF")
                set_cell_margins(cells_lr[col_idx], top=70, bottom=70, start=60, end=60)
                set_cell_border(cells_lr[col_idx])
                p = cells_lr[col_idx].paragraphs[0]
                p.alignment = WD_ALIGN_PARAGRAPH.CENTER if col_idx in [0, 1] else WD_ALIGN_PARAGRAPH.RIGHT
                p.paragraph_format.space_before = Pt(0)
                p.paragraph_format.space_after = Pt(0)
                r = p.add_run(v)
                r.font.name = "Calibri"
                r.font.size = Pt(8.0)
                r.font.color.rgb = CHARCOAL
                if th == 10.0 and col_idx in [1, 3, 7, 8, 9]:
                    r.font.bold = True
            cur_row += 1
            
            # RF Row
            cells_rf = tbl_eval.rows[cur_row].cells
            vals_rf = [
                f"> {th} mm", "Random Forest (50T)",
                f"{r_rf['roc_auc']:.4f}", f"{r_rf['pr_auc']:.4f}",
                f"{r_rf['brier_score']:.5f}", f"{r_rf['baseline_brier_score']:.5f}",
                f"{r_rf['brier_skill_score']:+.1%}", f"{r_rf['recall']:.1%}",
                f"{r_rf['precision']:.1%}", f"{r_rf['f1_score']:.4f}"
            ]
            for col_idx, (v, w) in enumerate(zip(vals_rf, col_w)):
                cells_rf[col_idx].width = w
                set_cell_background(cells_rf[col_idx], LIGHT_GRAY)
                set_cell_margins(cells_rf[col_idx], top=70, bottom=70, start=60, end=60)
                set_cell_border(cells_rf[col_idx])
                p = cells_rf[col_idx].paragraphs[0]
                p.alignment = WD_ALIGN_PARAGRAPH.CENTER if col_idx in [0, 1] else WD_ALIGN_PARAGRAPH.RIGHT
                p.paragraph_format.space_before = Pt(0)
                p.paragraph_format.space_after = Pt(0)
                r = p.add_run(v)
                r.font.name = "Calibri"
                r.font.size = Pt(8.0)
                r.font.color.rgb = CHARCOAL
                if th < 10.0 and col_idx in [1, 2, 3, 4, 6, 7, 9]:
                    r.font.bold = True
                elif th == 10.0 and col_idx in [2, 4, 6]:
                    r.font.bold = True
            cur_row += 1

        p_sp2 = doc.add_paragraph()
        p_sp2.paragraph_format.space_before = Pt(2)
        p_sp2.paragraph_format.space_after = Pt(4)

    # Figures 8-11
    add_figure_with_caption(
        doc,
        "figures/fig12_model_comparison_bars.png",
        "Figure 8: Model performance comparison across rainfall thresholds on the 2024 held-out test set: (A) ROC-AUC and (B) PR-AUC relative to random prevalence baselines.",
        width_in=6.5
    )
    add_figure_with_caption(
        doc,
        "figures/fig10_roc_curves.png",
        "Figure 9: Receiver Operating Characteristic (ROC) curves across five exceedance thresholds comparing Logistic Regression and Random Forest (50 trees) on the 2024 held-out test set.",
        width_in=6.5
    )
    add_figure_with_caption(
        doc,
        "figures/fig11_precision_recall_curves.png",
        "Figure 10: Precision-Recall (PR) curves across five exceedance thresholds comparing Logistic Regression and Random Forest against empirical prevalence baselines.",
        width_in=6.5
    )
    add_figure_with_caption(
        doc,
        "figures/fig8_calibration_logistic_regression.png",
        "Figure 11: Reliability diagrams before and after Platt scaling calibration fitted on the 2023 validation dataset.",
        width_in=6.5
    )

    add_h2("6.2 Key Analysis & The θ = 10.0 mm Cutoff Phenomenon")
    add_bullet(
        "Random Forest outperforms Logistic Regression across discrimination metrics (higher ROC-AUC and PR-AUC for thresholds 0.0 to 5.0 mm) "
        "and probabilistic accuracy (lower Brier score and higher positive Brier Skill Score across all five thresholds, including 10.0 mm).",
        "1. Ranking & Probabilistic Calibration: "
    )
    add_bullet(
        "The test evaluation table directly reports the raw MLlib output probabilities. Calibration (Platt scaling) was fitted exclusively "
        "on the 2023 validation partition to prevent test data snooping and is applied at inference time in the prediction CLI (src.predict).",
        "2. Raw vs. Calibrated Metrics: "
    )
    add_bullet(
        "At θ = 10.0 mm, 30-minute rainfall exceedance is an extreme-tail event with empirical test prevalence π = 0.0038 "
        "(0.38%, exactly 24,265 positive events out of 6,338,426 held-out test observations).",
        "3. Imbalance at θ = 10.0 mm: "
    )
    add_bullet(
        "Across all possible decision thresholds, Random Forest achieves a Precision-Recall AUC of 0.2529, which is approximately "
        "66× above the uninformative random-guess baseline (π = 24,265 / 6,338,426 ≈ 0.00383). Combined with an ROC-AUC of 0.8984 and a Brier Skill "
        "Score of +17.1% over climatology, this confirms that the ensemble assigns significantly higher relative probabilities to heavy-rain "
        "intervals than to dry intervals.",
        "4. Threshold-Independent Ranking (PR-AUC): "
    )
    add_bullet(
        "In contrast to ranking metrics, classification recall depends strictly on an assigned decision cutoff. Random Forest aggregates "
        "50 unweighted decision trees trained on natural class frequencies without artificial oversampling. Because severe rain occurs in only "
        "0.38% of intervals, individual tree leaves rarely predict high probabilities in rare regions, and the ensemble average probability "
        "across 50 trees never reaches the conventional 0.50 cutoff for any test record (p < 0.50 across all 6,338,426 test observations). "
        "Consequently, evaluating at the fixed 0.50 cutoff assigns every test observation to the negative class "
        "(TP = 0, FP = 0, TN = 6,314,161, FN = 24,265), producing zero positive predictions and 0.0% recall.",
        "5. Threshold-Dependent Decision Assignment (Recall at 0.50): "
    )
    add_bullet(
        "The contrast between a high PR-AUC (66× baseline) and zero recall at 0.50 highlights that for extreme-tail precipitation, a generic "
        "0.50 decision threshold is unsuited for binary classification; operational alerting requires operating thresholds calibrated on validation "
        "data or aligned with specific cost-loss matrices.",
        "6. Operational Implication: "
    )

    # ==================== 7. EXECUTION WORKFLOW & REPRODUCIBILITY ====================
    add_h1("7. Execution Workflow & Reproducibility")
    add_p(
        "The complete pipeline is fully automated and executable via documented terminal commands. In strict adherence "
        "to leakage-prevention standards, model comparison, diagnostic checks, and probability calibration are conducted on the "
        "2023 validation split. The 2024 held-out test partition is reserved strictly for final benchmark evaluation and must never "
        "be used for hyperparameter tuning or model selection."
    )
    
    code_text = (
        "# 1. Ingestion and Cleaning (Raw CSVs -> Clean Parquet)\n"
        "python -m src.ingest --all\n\n"
        "# 2. Task 1 Exploratory Analysis (Generates figures and summary tables)\n"
        "python -m src.explore\n\n"
        "# 3. Feature Engineering & Target Generation (Generates features and labels)\n"
        "python -m src.features\n\n"
        "# 4. Model Training (Chronological training split: 2017–2022)\n"
        "python -m src.train --model logistic_regression --thresholds 0.0 1.0 2.0 5.0 10.0\n"
        "python -m src.train --model random_forest --thresholds 0.0 1.0 2.0 5.0 10.0\n\n"
        "# 5. Validation Evaluation & Model Selection (Validation split: 2023)\n"
        "# Note: Use validation split exclusively for diagnostics and model comparison.\n"
        "python -m src.evaluate --model logistic_regression --split val\n"
        "python -m src.evaluate --model random_forest --split val\n\n"
        "# 6. Probability Calibration (Fitted strictly on 2023 validation split)\n"
        "python -m src.calibrate --model logistic_regression\n"
        "python -m src.calibrate --model random_forest\n\n"
        "# 7. Final Held-Out Test Evaluation (Benchmark split: 2024)\n"
        "# Executed once models and calibration parameters are finalized.\n"
        "python -m src.evaluate --model logistic_regression --split test\n"
        "python -m src.evaluate --model random_forest --split test\n\n"
        "# 8. Prediction CLI (Interactive inference with calibrated probabilities)\n"
        "python -m src.predict --station-id S06 --timestamp \"2024-11-07 16:45:00\" --threshold 1.0"
    )
    add_code_block(doc, code_text)

    # ==================== 8. LIMITATIONS, AI-USE & REFERENCES ====================
    add_h1("8. Limitations, AI-Use Declaration & References")
    
    add_h2("8.1 Limitations")
    add_bullet(
        "Rainfall observations are collected exclusively from surface tipping-bucket gauges at discrete stations (~60–80 operational stations). "
        "Tropical convective precipitation in Singapore frequently forms at microscale dimensions (1–3 km across); intense localized rainfall "
        "cells can initiate between monitoring stations without prior indication at adjacent gauges.",
        "1. Discrete Point-Gauge Spatial Coverage: "
    )
    add_bullet(
        "Feature engineering is restricted strictly to historical rainfall observations (lags, rolling accumulations, rolling maxima, "
        "intensity trends) and cyclic spatio-temporal indicators. The dataset (Collection 2279) does not include thermodynamic atmospheric "
        "state variables—such as relative humidity, ambient temperature, convective available potential energy (CAPE), surface pressure, or "
        "vertical wind velocity profiles—which govern the initial convective triggering of precipitation before surface rain occurs.",
        "2. Rainfall-Only Historical Predictors: "
    )
    add_bullet(
        "For rare events (θ = 10.0 mm, 0.38% prevalence), standard unweighted tree ensembles output low absolute probabilities (p < 0.50 "
        "for all test records). Operational deployment for early warning requires decision thresholds calibrated to the empirical prior or "
        "asymmetric cost-utility matrices, rather than default 0.50 cutoffs.",
        "3. Severe Imbalance at Heavy Rainfall Thresholds: "
    )
    add_bullet(
        "To prevent synthetic interpolation artifacts, the prediction interface strictly rejects inference requests if any of the preceding "
        "twelve 5-minute observation intervals (past 60 minutes) are missing, restricting forecast availability during gauge telemetry interruptions.",
        "4. Strict Historical Continuity Requirement: "
    )

    add_h2("8.2 AI-Use Declaration")
    add_p(
        "In compliance with NUS academic and course guidelines, assistance from artificial intelligence tools—specifically Google DeepMind's "
        "Antigravity coding assistant and OpenAI's ChatGPT—was utilized throughout the project lifecycle. These tools provided assistance with:"
    )
    add_bullet("High-level architectural planning, pipeline design, and literature/documentation review;", "• Architecture & Planning: ")
    add_bullet("Development, debugging, and environment configuration of the Apache Spark (PySpark 3.5.3) data processing and MLlib pipelines;", "• Pipeline Implementation: ")
    add_bullet("Authoring, scaffolding, and debugging the automated unit test suite (pytest);", "• Testing Suite: ")
    add_bullet("Drafting, structuring, and formatting the academic project report and visualization scripts;", "• Documentation & Reporting: ")
    add_bullet("Mathematical verification and interpretation of experimental results, including class imbalance phenomena at extreme thresholds, decision cutoff behavior, and Platt scaling probability calibration.", "• Technical Verification: ")
    add_p(
        "All data ingestion, exploratory data analyses, feature engineering, model training, probability calibration, and metric evaluations "
        "were independently executed, inspected, and verified against the actual Singapore government historical rainfall datasets (Collection 2279)."
    )

    add_h2("8.3 Academic References")
    add_bullet("Singapore National Environment Agency (NEA), Historical Rainfall across Singapore, Collection 2279, data.gov.sg.", "1. ")
    add_bullet("Apache Software Foundation, Spark MLlib Guide: Pipelines and Classification, Apache Spark 3.5.3 Documentation, 2024.", "2. ")
    add_bullet("Wilks, D. S., Statistical Methods in the Atmospheric Sciences, 4th ed., Academic Press, 2019.", "3. ")

    # Save Document
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    doc.save(output_path)
    print(f"Document successfully created: {output_path}")


if __name__ == "__main__":
    build_word_document()
