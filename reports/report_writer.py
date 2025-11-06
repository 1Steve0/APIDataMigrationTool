import csv
import os
from datetime import datetime
from openpyxl import Workbook
from fpdf import FPDF

def generate_report_files(summary, adapter_name, entity, migration_type):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = f"{entity}_{migration_type}_{adapter_name}_{timestamp}"
    output_dir = "static/reports"
    os.makedirs(output_dir, exist_ok=True)

    csv_path = os.path.join(output_dir, f"{base_name}.csv")
    xlsx_path = os.path.join(output_dir, f"{base_name}.xlsx")
    pdf_path = os.path.join(output_dir, f"{base_name}.pdf")

    write_csv(summary["rows"], csv_path)
    write_xlsx(summary["rows"], xlsx_path)
    write_pdf(summary["rows"], pdf_path)

    return {
        "csv": os.path.basename(csv_path),
        "xlsx": os.path.basename(xlsx_path),
        "pdf": os.path.basename(pdf_path)
    }

def write_csv(rows, path):
    if not rows:
        return
    fieldnames = rows[0].keys()
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

def write_xlsx(rows, path):
    if not rows:
        return
    wb = Workbook()
    ws = wb.active
    ws.title = "Migration Results"

    headers = list(rows[0].keys())
    ws.append(headers)
    for row in rows:
        ws.append([row.get(h, "") for h in headers])

    wb.save(path)

def write_pdf(rows, path):
    if not rows:
        return
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=10)

    headers = list(rows[0].keys())
    col_width = 190 / len(headers)

    # Header
    for header in headers:
        pdf.cell(col_width, 10, header, border=1)
    pdf.ln()

    # Rows
    for row in rows:
        for header in headers:
            cell = str(row.get(header, ""))
            pdf.cell(col_width, 10, cell[:30], border=1)  # truncate long text
        pdf.ln()

    pdf.output(path)