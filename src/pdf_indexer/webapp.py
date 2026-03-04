"""Flask web application for PDF table extraction and markdown/CSV export."""

from __future__ import annotations

import csv
import io
import os
import re
import tempfile
import uuid
from pathlib import Path

from flask import (
    Flask,
    render_template,
    request,
    send_file,
    jsonify,
)

import pdfplumber

from pdf_indexer.extractors.table_extractor import TableExtractor
from pdf_indexer.extractors.data_typer import DataTyper
from pdf_indexer.extractors.metadata_extractor import MetadataExtractor
from pdf_indexer.export import (
    generate_combined_csv,
    generate_json_export,
    generate_excel,
    generate_zip_bundle,
)
from pdf_indexer.models import CellData, DocumentRecord, PageRecord, TableData
from pdf_indexer.database import PDFDatabase
from pdf_indexer.scanner import compute_file_hash

UPLOAD_FOLDER = os.path.join(tempfile.gettempdir(), "pdf_indexer_uploads")
OUTPUT_FOLDER = os.path.join(tempfile.gettempdir(), "pdf_indexer_outputs")
DB_FOLDER = os.path.join(tempfile.gettempdir(), "pdf_indexer_db")

app = Flask(
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), "templates"),
)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB limit


def _ensure_dirs() -> None:
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    os.makedirs(DB_FOLDER, exist_ok=True)


def _get_index_db_path() -> str:
    """Return path to the shared SQLite index database."""
    return os.path.join(DB_FOLDER, "web_index.db")


def _persist_to_index(
    pdf_path: str,
    filename: str,
    extraction: dict,
) -> None:
    """Persist extraction results to the SQLite index database.

    This bridges the web app and the MCP server — once a PDF is uploaded
    and processed, its data becomes immediately queryable via MCP tools.
    Set PDF_INDEX_DB to the web_index.db path in the MCP server config.
    """
    db_path = _get_index_db_path()
    db = PDFDatabase(db_path)
    typer = DataTyper()

    try:
        content_hash = compute_file_hash(pdf_path)
        file_size = os.path.getsize(pdf_path) if os.path.exists(pdf_path) else 0
        meta = extraction["metadata"]

        doc_record = DocumentRecord(
            file_path=filename,
            content_hash=content_hash,
            title=meta.get("title", ""),
            author=meta.get("author", ""),
            page_count=int(meta.get("page_count", 0)),
            file_size_bytes=file_size,
        )
        doc_id = db.upsert_document(doc_record)

        for page_info in extraction["pages"]:
            page_num = page_info["page_number"]
            page_record = PageRecord(
                document_id=doc_id,
                page_number=page_num,
                raw_text="",
            )
            page_id = db.upsert_page(page_record)
            db.clear_tables_for_page(page_id)

            for table_info in page_info["tables"]:
                table_data = TableData(
                    page_id=page_id,
                    table_index=table_info["table_number"],
                    headers=table_info["headers"],
                    rows=[],
                    table_type=table_info["table_type"],
                )
                table_id = db.insert_table(table_data)

                # Insert cells with typed data
                cells: list[list[CellData]] = []
                for row in table_info["rows"]:
                    cell_row: list[CellData] = []
                    for cell in row:
                        cell_row.append(CellData(
                            value=cell["value"],
                            data_type=cell["data_type"],
                            numeric_value=cell["numeric_value"],
                        ))
                    cells.append(cell_row)
                db.insert_cells_batch(table_id, cells)
    finally:
        db.close()


def _extract_table_context(page, table_bbox: tuple) -> tuple[str, str]:
    """Extract text immediately before and after a table on a page.

    Looks at text above the table's top edge (context_before) and below
    its bottom edge (context_after), limited to ~200 chars each.
    """
    page_text = page.extract_text() or ""
    if not page_text:
        return "", ""

    # Use the table's bounding box to split text spatially.
    # pdfplumber can crop by bbox: (x0, top, x1, bottom)
    context_before = ""
    context_after = ""

    try:
        # Text above the table
        if table_bbox[1] > 5:
            above = page.crop((0, 0, page.width, table_bbox[1]))
            above_text = (above.extract_text() or "").strip()
            if above_text:
                # Take last ~200 chars (closest to table)
                context_before = above_text[-200:].strip()

        # Text below the table
        if table_bbox[3] < page.height - 5:
            below = page.crop((0, table_bbox[3], page.width, page.height))
            below_text = (below.extract_text() or "").strip()
            if below_text:
                # Take first ~200 chars (closest to table)
                context_after = below_text[:200].strip()
    except Exception:
        pass

    return context_before, context_after


def extract_all_tables(pdf_path: str) -> dict:
    """Extract all tables and numeric data from a PDF file.

    Returns a dict with metadata, per-page tables (with surrounding text
    context), and a numeric data summary.
    """
    extractor = TableExtractor()
    typer = DataTyper()
    meta_extractor = MetadataExtractor()

    metadata = meta_extractor.extract(pdf_path)
    page_count = int(metadata.get("page_count", 0))

    pages_data: list[dict] = []
    all_numeric: list[dict] = []
    table_counter = 0

    with pdfplumber.open(pdf_path) as pdf:
        for page_num in range(1, page_count + 1):
            page = pdf.pages[page_num - 1]
            tables = extractor._extract_from_page(page)

            page_tables: list[dict] = []
            for table in tables:
                table_counter += 1
                typed_rows: list[list[dict]] = []

                for row in table.rows:
                    typed_cells = []
                    for col_idx, cell_value in enumerate(row):
                        cell_data = typer.classify(cell_value)
                        typed_cells.append({
                            "value": cell_data.value,
                            "data_type": cell_data.data_type,
                            "numeric_value": cell_data.numeric_value,
                        })
                        if cell_data.numeric_value is not None:
                            header = (
                                table.headers[col_idx]
                                if col_idx < len(table.headers)
                                else f"Column {col_idx + 1}"
                            )
                            all_numeric.append({
                                "page": page_num,
                                "table": table_counter,
                                "header": header,
                                "value": cell_data.value,
                                "numeric_value": cell_data.numeric_value,
                                "data_type": cell_data.data_type,
                            })
                    typed_rows.append(typed_cells)

                # Extract surrounding text context
                context_before, context_after = _extract_table_context(
                    page, table.bbox,
                )

                page_tables.append({
                    "table_number": table_counter,
                    "headers": table.headers,
                    "rows": typed_rows,
                    "row_count": table.row_count,
                    "col_count": table.col_count,
                    "table_type": table.table_type,
                    "context_before": context_before,
                    "context_after": context_after,
                })

            if page_tables:
                pages_data.append({
                    "page_number": page_num,
                    "tables": page_tables,
                })

    return {
        "metadata": metadata,
        "pages": pages_data,
        "numeric_data": all_numeric,
        "total_tables": table_counter,
        "total_numeric_values": len(all_numeric),
    }


def generate_markdown(filename: str, extraction: dict) -> str:
    """Generate a markdown report from the extraction results."""
    lines: list[str] = []
    meta = extraction["metadata"]

    lines.append(f"# PDF Table Extraction Report")
    lines.append("")
    lines.append(f"**Source file:** {filename}")
    if meta.get("title"):
        lines.append(f"**Title:** {meta['title']}")
    if meta.get("author"):
        lines.append(f"**Author:** {meta['author']}")
    lines.append(f"**Pages:** {meta.get('page_count', 'N/A')}")
    lines.append(f"**Tables found:** {extraction['total_tables']}")
    lines.append(f"**Numeric values extracted:** {extraction['total_numeric_values']}")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Tables section
    if extraction["pages"]:
        lines.append("## Extracted Tables")
        lines.append("")

        for page_info in extraction["pages"]:
            page_num = page_info["page_number"]

            for table_info in page_info["tables"]:
                tnum = table_info["table_number"]
                ttype = table_info["table_type"]
                lines.append(
                    f"### Table {tnum} (Page {page_num}, Type: {ttype})"
                )
                lines.append("")

                headers = table_info["headers"]
                if headers:
                    lines.append("| " + " | ".join(_escape_md(h) for h in headers) + " |")
                    lines.append("| " + " | ".join("---" for _ in headers) + " |")

                    for typed_row in table_info["rows"]:
                        values = [_escape_md(cell["value"]) for cell in typed_row]
                        # Pad or trim to match header count
                        while len(values) < len(headers):
                            values.append("")
                        values = values[: len(headers)]
                        lines.append("| " + " | ".join(values) + " |")

                    lines.append("")
    else:
        lines.append("## Extracted Tables")
        lines.append("")
        lines.append("*No tables were detected in this PDF.*")
        lines.append("")

    # Numeric data summary
    lines.append("---")
    lines.append("")
    lines.append("## Numeric Data Summary")
    lines.append("")

    numeric = extraction["numeric_data"]
    if numeric:
        lines.append("| Page | Table | Column | Raw Value | Numeric Value | Type |")
        lines.append("| --- | --- | --- | --- | --- | --- |")

        for entry in numeric:
            lines.append(
                f"| {entry['page']} "
                f"| {entry['table']} "
                f"| {_escape_md(entry['header'])} "
                f"| {_escape_md(entry['value'])} "
                f"| {entry['numeric_value']} "
                f"| {entry['data_type']} |"
            )
        lines.append("")

        # Summary statistics by type
        type_counts: dict[str, int] = {}
        for entry in numeric:
            dt = entry["data_type"]
            type_counts[dt] = type_counts.get(dt, 0) + 1

        lines.append("### Summary by Data Type")
        lines.append("")
        lines.append("| Data Type | Count |")
        lines.append("| --- | --- |")
        for dt, count in sorted(type_counts.items()):
            lines.append(f"| {dt} | {count} |")
        lines.append("")
    else:
        lines.append("*No numeric data was found in the extracted tables.*")
        lines.append("")

    return "\n".join(lines)


def generate_csv(extraction: dict) -> str:
    """Generate a CSV string containing all extracted table data."""
    output = io.StringIO()
    writer = csv.writer(output)

    # Write all tables
    for page_info in extraction["pages"]:
        page_num = page_info["page_number"]
        for table_info in page_info["tables"]:
            tnum = table_info["table_number"]
            ttype = table_info["table_type"]
            # Section header row
            writer.writerow([f"--- Table {tnum} | Page {page_num} | Type: {ttype} ---"])
            # Column headers
            writer.writerow(table_info["headers"])
            # Data rows
            for typed_row in table_info["rows"]:
                writer.writerow([cell["value"] for cell in typed_row])
            # Blank separator
            writer.writerow([])

    # Numeric data summary section
    if extraction["numeric_data"]:
        writer.writerow(["--- Numeric Data Summary ---"])
        writer.writerow(["Page", "Table", "Column", "Raw Value", "Numeric Value", "Data Type"])
        for entry in extraction["numeric_data"]:
            writer.writerow([
                entry["page"],
                entry["table"],
                entry["header"],
                entry["value"],
                entry["numeric_value"],
                entry["data_type"],
            ])

    return output.getvalue()


def _escape_md(text: str) -> str:
    """Escape pipe characters for markdown tables."""
    if not text:
        return ""
    return text.replace("|", "\\|").replace("\n", " ")


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload():
    _ensure_dirs()

    if "pdf_file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files["pdf_file"]
    if not file.filename:
        return jsonify({"error": "No file selected"}), 400

    if not file.filename.lower().endswith(".pdf"):
        return jsonify({"error": "Only PDF files are accepted"}), 400

    # Save uploaded file with a unique name to prevent collisions
    file_id = uuid.uuid4().hex[:12]
    safe_name = re.sub(r"[^\w.\-]", "_", file.filename)
    upload_path = os.path.join(UPLOAD_FOLDER, f"{file_id}_{safe_name}")
    file.save(upload_path)

    try:
        extraction = extract_all_tables(upload_path)
        markdown = generate_markdown(file.filename, extraction)
        legacy_csv_content = generate_csv(extraction)

        stem = Path(file.filename).stem

        # Save markdown output
        md_filename = stem + "_tables.md"
        md_path = os.path.join(OUTPUT_FOLDER, f"{file_id}_{md_filename}")
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(markdown)

        # Save tidy CSV output (replaces legacy CSV as primary download)
        csv_filename = stem + "_tidy.csv"
        csv_path = os.path.join(OUTPUT_FOLDER, f"{file_id}_{csv_filename}")
        tidy_csv_content = generate_combined_csv(extraction, file.filename)
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            f.write(tidy_csv_content)

        # Save structured JSON
        json_filename = stem + "_data.json"
        json_path = os.path.join(OUTPUT_FOLDER, f"{file_id}_{json_filename}")
        json_content = generate_json_export(extraction, file.filename)
        with open(json_path, "w", encoding="utf-8") as f:
            f.write(json_content)

        # Save Excel workbook
        xlsx_filename = stem + "_tables.xlsx"
        xlsx_path = os.path.join(OUTPUT_FOLDER, f"{file_id}_{xlsx_filename}")
        xlsx_content = generate_excel(extraction, file.filename)
        with open(xlsx_path, "wb") as f:
            f.write(xlsx_content)

        # Save ZIP bundle with all formats
        zip_filename = stem + "_export.zip"
        zip_path = os.path.join(OUTPUT_FOLDER, f"{file_id}_{zip_filename}")
        zip_content = generate_zip_bundle(
            extraction, file.filename, markdown, legacy_csv_content,
        )
        with open(zip_path, "wb") as f:
            f.write(zip_content)

        # Persist to SQLite index for MCP server access
        try:
            _persist_to_index(upload_path, file.filename, extraction)
        except Exception:
            pass  # Non-critical — exports still work without the index

        return jsonify({
            "success": True,
            "file_id": file_id,
            "filename": file.filename,
            "md_filename": md_filename,
            "csv_filename": csv_filename,
            "json_filename": json_filename,
            "xlsx_filename": xlsx_filename,
            "zip_filename": zip_filename,
            "total_tables": extraction["total_tables"],
            "total_numeric_values": extraction["total_numeric_values"],
            "page_count": extraction["metadata"].get("page_count", "0"),
            "markdown_preview": markdown,
            "tables": extraction["pages"],
            "numeric_data": extraction["numeric_data"],
            "index_db_path": _get_index_db_path(),
        })
    except Exception as e:
        return jsonify({"error": f"Failed to process PDF: {e}"}), 500
    finally:
        # Clean up uploaded PDF
        if os.path.exists(upload_path):
            os.remove(upload_path)


@app.route("/download/<file_id>/<filename>")
def download(file_id: str, filename: str):
    # Validate file_id format (hex chars only)
    if not re.match(r"^[a-f0-9]+$", file_id):
        return jsonify({"error": "Invalid file ID"}), 400

    safe_name = re.sub(r"[^\w.\-]", "_", filename)
    file_path = os.path.join(OUTPUT_FOLDER, f"{file_id}_{safe_name}")

    if not os.path.exists(file_path):
        return jsonify({"error": "File not found"}), 404

    mimetype_map = {
        ".csv": "text/csv",
        ".md": "text/markdown",
        ".json": "application/json",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".zip": "application/zip",
    }
    ext = os.path.splitext(filename)[1].lower()
    mimetype = mimetype_map.get(ext, "application/octet-stream")

    return send_file(
        file_path,
        as_attachment=True,
        download_name=filename,
        mimetype=mimetype,
    )


def main():
    """Run the web application."""
    import argparse

    parser = argparse.ArgumentParser(description="PDF Table Extractor Web App")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--port", type=int, default=5000, help="Port to bind to")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    args = parser.parse_args()

    _ensure_dirs()
    print(f"Starting PDF Table Extractor at http://{args.host}:{args.port}")
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
