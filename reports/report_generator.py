import os
import csv
from datetime import datetime
from openpyxl import Workbook
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from collections import defaultdict

def generate_csv_log(summary, filepath):
    headers = ["Row #", "Name", "Parent ID", "Description", "Status", "New_ID", "Reason"]
    with open(filepath, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in summary.get("rows", []):
            writer.writerow([
                row.get("row", ""),
                row.get("name", ""),
                row.get("parentId", ""),
                row.get("description", ""),
                row.get("status", ""),
                row.get("response_id", ""),
                row.get("reason", "")
            ])

def generate_report_files(summary, adapter_name, entity, migration_type):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    safe_entity = entity.replace(" ", "_")  # Optional: sanitize filename
    base_name = f"{safe_entity}_{migration_type}_{adapter_name}_{timestamp}"

    xlsx_filename = f"{base_name}.xlsx"
    pdf_filename = f"{base_name}.pdf"
    csv_filename = f"{base_name}.csv"

    # Match Flask route folder name exactly
    xlsx_path = os.path.join("reports", xlsx_filename)
    pdf_path = os.path.join("reports", pdf_filename)
    csv_path = os.path.join("reports", csv_filename)

    generate_xlsx_report(summary, xlsx_path)
    generate_pdf_summary(summary, pdf_path, adapter_name, entity, migration_type, timestamp)
    generate_csv_log(summary, csv_path)

    return {
        "xlsx": xlsx_filename,
        "pdf": pdf_filename,
        "csv": csv_filename
    }

def generate_xlsx_report(summary, filepath):
    wb = Workbook()
    ws = wb.active
    ws.title = "Migration Records"

    # Header row
    ws.append(["Row #", "Name", "Parent ID", "Description", "Status", "New_ID", "Reason"])

    # Per-record detail
    for i, row in enumerate(summary.get("rows", []), start=1):
        status = "Migrated" if row.get("error") in [None, ""] else "Skipped"
        new_id = row.get("response_id", "") if status == "Migrated" else ""
        reason = row.get("error", "") if status == "Skipped" else ""

        ws.append([    
            row.get("row", ""),
            row.get("name", ""),
            row.get("parentId", ""),
            row.get("description", ""),
            row.get("status", ""),
            row.get("response_id", ""),
            row.get("reason", "")
        ])

    # Optional summary sheet
    summary_ws = wb.create_sheet(title="Summary")
    summary_ws.append(["Total Rows", "Success", "Skipped", "Duration (s)"])
    summary_ws.append([
        summary["total"],
        summary["success"],
        summary["skipped"],
        summary["duration"]
    ])

    if summary["errors"]:
        summary_ws.append([])
        summary_ws.append(["Skipped Reasons"])
        for reason in summary["errors"]:
            summary_ws.append([reason])

    wb.save(filepath)

def generate_pdf_summary(summary, filepath, adapter_name, entity, migration_type, timestamp):
    c = canvas.Canvas(filepath, pagesize=A4)
    width, height = A4

    # === Header Section ===
    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, height - 50, "Migration Report")

    c.setFont("Helvetica", 12)
    c.drawString(50, height - 80, f"Adapter: {adapter_name}")
    c.drawString(50, height - 100, f"Entity: {entity}")
    c.drawString(50, height - 120, f"Migration Type: {migration_type}")
    c.drawString(50, height - 140, f"Timestamp: {timestamp}")

    c.drawString(50, height - 180, f"Total Rows: {summary['total']}")
    c.drawString(50, height - 200, f"Successfully Written: {summary['success']}")
    c.drawString(50, height - 220, f"Skipped: {summary['skipped']}")
    c.drawString(50, height - 240, f"Duration: {summary['duration']} seconds")

    # === Section: Migration - Success ===
    c.showPage()
    c.setFont("Helvetica-Bold", 14)
    c.drawString(50, height - 50, "✅ Migration – Success")

    y = height - 80
    for i, row in enumerate(summary.get("rows", []), start=1):
        if row.get("status") == "Migrated":
            line = f"{i}. Name: {row.get('name')} | Parent ID: {row.get('parentId')} | New ID: {row.get('response_id')} | Description: {row.get('description')}"
            c.setFont("Helvetica", 10)
            c.drawString(50, y, line)
            y -= 15
            if y < 50:
                c.showPage()
                y = height - 50

    # === Section: Skipped – Not Migrated ===
    c.showPage()
    c.setFont("Helvetica-Bold", 14)
    c.drawString(50, height - 50, "⚠️ Skipped – Not Migrated")

    grouped_errors = defaultdict(list)
    for i, row in enumerate(summary.get("rows", []), start=1):
        if row.get("status") == "Skipped":
            reason = row.get("reason", "Unknown reason")
            grouped_errors[reason].append((i, row))

    y = height - 80
    for reason, records in grouped_errors.items():
        c.setFont("Helvetica-Bold", 12)
        c.drawString(50, y, f"🔸 Reason: {reason}  ({len(records)} record{'s' if len(records) != 1 else ''})")
        y -= 20

        c.setFont("Helvetica", 10)
        for i, row in records:
            line = f"{i}. Name: {row.get('name')} | Parent ID: {row.get('parentId')} | Description: {row.get('description')}"
            c.drawString(70, y, line)
            y -= 15
            if y < 50:
                c.showPage()
                y = height - 50

    c.save()

