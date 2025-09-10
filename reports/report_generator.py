import os
import datetime
from openpyxl import Workbook
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

def generate_report_files(summary, adapter_name, entity, migration_type, output_dir="reports"):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    base_name = f"{entity}_{migration_type}_{adapter_name}_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)

    xlsx_path = os.path.join(output_dir, f"{base_name}.xlsx")
    pdf_path = os.path.join(output_dir, f"{base_name}.pdf")

    generate_xlsx_report(summary, xlsx_path)
    generate_pdf_summary(summary, pdf_path, adapter_name, entity, migration_type, timestamp)

    return {"xlsx": xlsx_path, "pdf": pdf_path}

def generate_xlsx_report(summary, filepath):
    wb = Workbook()
    ws = wb.active
    ws.title = "Migration Summary"

    ws.append(["Total Rows", "Success", "Skipped", "Duration (s)"])
    ws.append([summary["total"], summary["success"], summary["skipped"], summary["duration"]])

    if summary["errors"]:
        ws.append([])
        ws.append(["Skipped Reasons"])
        for reason in summary["errors"]:
            ws.append([reason])

    wb.save(filepath)

def generate_pdf_summary(summary, filepath, adapter_name, entity, migration_type, timestamp):
    c = canvas.Canvas(filepath, pagesize=A4)
    width, height = A4

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

    if summary["errors"]:
        c.drawString(50, height - 280, "Skipped Reasons:")
        y = height - 300
        for reason in summary["errors"]:
            c.drawString(70, y, f"- {reason}")
            y -= 20
            if y < 50:
                c.showPage()
                y = height - 50

    c.save()