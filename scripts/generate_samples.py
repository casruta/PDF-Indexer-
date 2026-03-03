"""Generate sample PDF documents with tables for demo purposes."""

from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate,
    Table,
    TableStyle,
    Paragraph,
    Spacer,
)
from reportlab.lib.styles import getSampleStyleSheet
import os

SAMPLES_DIR = os.path.join(os.path.dirname(__file__), "samples")


def create_financial_report(path: str) -> None:
    """Create a sample financial report PDF with multiple tables."""
    doc = SimpleDocTemplate(path, pagesize=letter)
    styles = getSampleStyleSheet()
    elements = []

    elements.append(Paragraph("Acme Corp — Q4 2025 Financial Summary", styles["Title"]))
    elements.append(Spacer(1, 0.3 * inch))

    # Revenue table
    elements.append(Paragraph("Revenue by Segment", styles["Heading2"]))
    elements.append(Spacer(1, 0.15 * inch))
    revenue_data = [
        ["Segment", "Q3 2025", "Q4 2025", "Change", "YoY Growth"],
        ["Cloud Services", "$12,450,000", "$14,200,000", "$1,750,000", "18.5%"],
        ["Enterprise", "$8,300,000", "$9,100,000", "$800,000", "11.2%"],
        ["Consumer", "$3,750,000", "$3,920,000", "$170,000", "4.8%"],
        ["Consulting", "$2,100,000", "$1,850,000", "($250,000)", "(10.6%)"],
        ["Total", "$26,600,000", "$29,070,000", "$2,470,000", "12.4%"],
    ]
    t = Table(revenue_data, colWidths=[1.4 * inch, 1.2 * inch, 1.2 * inch, 1.2 * inch, 1.0 * inch])
    t.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2d3148")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#e8e8e8")),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
    ]))
    elements.append(t)
    elements.append(Spacer(1, 0.4 * inch))

    # Expense table
    elements.append(Paragraph("Operating Expenses", styles["Heading2"]))
    elements.append(Spacer(1, 0.15 * inch))
    expense_data = [
        ["Category", "Budget", "Actual", "Variance"],
        ["Salaries & Benefits", "$9,500,000", "$9,720,000", "($220,000)"],
        ["Infrastructure", "$3,200,000", "$2,980,000", "$220,000"],
        ["Marketing", "$2,800,000", "$3,150,000", "($350,000)"],
        ["R&D", "$4,100,000", "$4,050,000", "$50,000"],
        ["General & Admin", "$1,400,000", "$1,380,000", "$20,000"],
        ["Total", "$21,000,000", "$21,280,000", "($280,000)"],
    ]
    t2 = Table(expense_data, colWidths=[1.6 * inch, 1.3 * inch, 1.3 * inch, 1.3 * inch])
    t2.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2d3148")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#e8e8e8")),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
    ]))
    elements.append(t2)

    doc.build(elements)


def create_sales_metrics(path: str) -> None:
    """Create a sample sales metrics PDF with KPI tables."""
    doc = SimpleDocTemplate(path, pagesize=letter)
    styles = getSampleStyleSheet()
    elements = []

    elements.append(Paragraph("Regional Sales Metrics — FY 2024-25", styles["Title"]))
    elements.append(Spacer(1, 0.3 * inch))

    elements.append(Paragraph("Sales by Region", styles["Heading2"]))
    elements.append(Spacer(1, 0.15 * inch))
    sales_data = [
        ["Region", "Units Sold", "Revenue", "Avg Price", "Market Share"],
        ["North America", "45,200", "$6,780,000", "$150.00", "34.2%"],
        ["Europe", "38,100", "$5,334,000", "$140.00", "28.5%"],
        ["Asia Pacific", "52,800", "$6,336,000", "$120.00", "22.1%"],
        ["Latin America", "12,500", "$1,500,000", "$120.00", "8.7%"],
        ["Middle East & Africa", "8,900", "$1,157,000", "$130.00", "6.5%"],
        ["Global Total", "157,500", "$21,107,000", "$134.01", "100.0%"],
    ]
    t = Table(sales_data, colWidths=[1.4 * inch, 0.9 * inch, 1.1 * inch, 0.9 * inch, 1.0 * inch])
    t.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a5276")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#d5e8d4")),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
    ]))
    elements.append(t)
    elements.append(Spacer(1, 0.4 * inch))

    elements.append(Paragraph("Quarterly Trend", styles["Heading2"]))
    elements.append(Spacer(1, 0.15 * inch))
    trend_data = [
        ["Quarter", "Revenue", "COGS", "Gross Margin", "Net Income"],
        ["Q1 FY24-25", "$4,800,000", "$2,880,000", "40.0%", "$720,000"],
        ["Q2 FY24-25", "$5,100,000", "$2,958,000", "42.0%", "$856,000"],
        ["Q3 FY24-25", "$5,400,000", "$3,024,000", "44.0%", "$1,012,000"],
        ["Q4 FY24-25", "$5,807,000", "$3,136,000", "46.0%", "$1,250,000"],
    ]
    t2 = Table(trend_data, colWidths=[1.1 * inch, 1.1 * inch, 1.1 * inch, 1.0 * inch, 1.0 * inch])
    t2.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a5276")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
    ]))
    elements.append(t2)

    doc.build(elements)


def create_inventory_report(path: str) -> None:
    """Create a sample inventory/product report PDF."""
    doc = SimpleDocTemplate(path, pagesize=letter)
    styles = getSampleStyleSheet()
    elements = []

    elements.append(Paragraph("Warehouse Inventory Report — March 2026", styles["Title"]))
    elements.append(Spacer(1, 0.3 * inch))

    elements.append(Paragraph("Stock Levels by Category", styles["Heading2"]))
    elements.append(Spacer(1, 0.15 * inch))
    stock_data = [
        ["SKU", "Product", "Category", "Qty On Hand", "Unit Cost", "Total Value"],
        ["WH-1001", "Widget Alpha", "Components", "12,500", "$2.45", "$30,625.00"],
        ["WH-1002", "Widget Beta", "Components", "8,200", "$3.10", "$25,420.00"],
        ["WH-2001", "Sensor Module", "Electronics", "3,400", "$18.75", "$63,750.00"],
        ["WH-2002", "Control Board", "Electronics", "1,850", "$42.50", "$78,625.00"],
        ["WH-3001", "Steel Frame", "Structural", "620", "$125.00", "$77,500.00"],
        ["WH-3002", "Aluminum Panel", "Structural", "1,100", "$85.00", "$93,500.00"],
        ["WH-4001", "Power Supply", "Electrical", "2,200", "$32.00", "$70,400.00"],
        ["WH-4002", "Cable Assembly", "Electrical", "5,600", "$8.50", "$47,600.00"],
    ]
    t = Table(stock_data, colWidths=[0.8 * inch, 1.1 * inch, 0.9 * inch, 0.85 * inch, 0.8 * inch, 1.0 * inch])
    t.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#4a235a")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (3, 1), (-1, -1), "RIGHT"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
    ]))
    elements.append(t)

    doc.build(elements)


def generate_all_samples() -> list[str]:
    """Generate all sample PDFs and return their paths."""
    os.makedirs(SAMPLES_DIR, exist_ok=True)

    files = []
    creators = [
        ("financial_report_q4_2025.pdf", create_financial_report),
        ("regional_sales_fy2024-25.pdf", create_sales_metrics),
        ("warehouse_inventory_mar2026.pdf", create_inventory_report),
    ]

    for filename, creator_fn in creators:
        path = os.path.join(SAMPLES_DIR, filename)
        creator_fn(path)
        files.append(path)
        print(f"  Created: {path}")

    return files


if __name__ == "__main__":
    print("Generating sample PDFs...")
    generate_all_samples()
    print("Done.")
