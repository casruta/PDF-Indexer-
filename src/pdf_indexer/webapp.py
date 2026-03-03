"""Flask web application for PDF table extraction and markdown export."""

from __future__ import annotations

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

UPLOAD_FOLDER = os.path.join(tempfile.gettempdir(), "pdf_indexer_uploads")
OUTPUT_FOLDER = os.path.join(tempfile.gettempdir(), "pdf_indexer_outputs")

app = Flask(
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), "templates"),
)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB limit


def _ensure_dirs() -> None:
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)


def extract_all_tables(pdf_path: str) -> dict:
    """Extract all tables and numeric data from a PDF file.

    Returns a dict with metadata, per-page tables, and a numeric data summary.
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

                page_tables.append({
                    "table_number": table_counter,
                    "headers": table.headers,
                    "rows": typed_rows,
                    "row_count": table.row_count,
                    "col_count": table.col_count,
                    "table_type": table.table_type,
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

        # Save markdown output
        md_filename = Path(file.filename).stem + "_tables.md"
        md_path = os.path.join(OUTPUT_FOLDER, f"{file_id}_{md_filename}")
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(markdown)

        return jsonify({
            "success": True,
            "file_id": file_id,
            "filename": file.filename,
            "md_filename": md_filename,
            "total_tables": extraction["total_tables"],
            "total_numeric_values": extraction["total_numeric_values"],
            "page_count": extraction["metadata"].get("page_count", "0"),
            "markdown_preview": markdown,
        })
    except Exception as e:
        return jsonify({"error": f"Failed to process PDF: {e}"}), 500
    finally:
        # Clean up uploaded PDF
        if os.path.exists(upload_path):
            os.remove(upload_path)


@app.route("/download/<file_id>/<md_filename>")
def download(file_id: str, md_filename: str):
    # Validate file_id format (hex chars only)
    if not re.match(r"^[a-f0-9]+$", file_id):
        return jsonify({"error": "Invalid file ID"}), 400

    safe_md = re.sub(r"[^\w.\-]", "_", md_filename)
    md_path = os.path.join(OUTPUT_FOLDER, f"{file_id}_{safe_md}")

    if not os.path.exists(md_path):
        return jsonify({"error": "File not found"}), 404

    return send_file(
        md_path,
        as_attachment=True,
        download_name=md_filename,
        mimetype="text/markdown",
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
