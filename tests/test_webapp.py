"""Tests for the PDF Table Extractor web application."""

from __future__ import annotations

import io
import json
import os
import tempfile
import time
from unittest.mock import patch, MagicMock

import pytest

from pdf_indexer.webapp import (
    app,
    extract_all_tables,
    generate_markdown,
    generate_csv,
    _escape_md,
    _cleanup_old_files,
    _start_cleanup_thread,
    OUTPUT_FOLDER,
    FILE_TTL_SECONDS,
)


@pytest.fixture
def client():
    """Create a Flask test client."""
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


@pytest.fixture
def sample_pdf(tmp_path):
    """Create a minimal PDF with a table using pdfplumber-compatible structure.

    Uses reportlab if available, otherwise creates a minimal valid PDF by hand.
    """
    pdf_path = tmp_path / "test_sample.pdf"

    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
        from reportlab.lib import colors

        doc = SimpleDocTemplate(str(pdf_path), pagesize=letter)
        data = [
            ["Item", "Q1 Sales", "Q2 Sales", "Growth"],
            ["Product A", "$1,200.00", "$1,500.00", "25%"],
            ["Product B", "$800.50", "$950.75", "18.8%"],
            ["Product C", "$3,400.00", "$3,100.00", "(8.8%)"],
        ]
        table = Table(data)
        table.setStyle(TableStyle([
            ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
            ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
        ]))
        doc.build([table])
    except ImportError:
        # Fallback: create a minimal valid PDF without reportlab
        _write_minimal_pdf(pdf_path)

    return str(pdf_path)


def _write_minimal_pdf(path):
    """Write a minimal valid PDF that pdfplumber can open (no tables)."""
    content = (
        b"%PDF-1.4\n"
        b"1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
        b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
        b"3 0 obj<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R/Resources<<>>>>endobj\n"
        b"xref\n0 4\n"
        b"0000000000 65535 f \n"
        b"0000000009 00000 n \n"
        b"0000000058 00000 n \n"
        b"0000000115 00000 n \n"
        b"trailer<</Size 4/Root 1 0 R>>\nstartxref\n210\n%%EOF"
    )
    with open(path, "wb") as f:
        f.write(content)


# ---------------------------------------------------------------------------
# Unit tests for helper functions
# ---------------------------------------------------------------------------


class TestEscapeMd:
    def test_empty_string(self):
        assert _escape_md("") == ""

    def test_none_like(self):
        assert _escape_md("") == ""

    def test_pipe_escape(self):
        assert _escape_md("a|b") == "a\\|b"

    def test_newline_escape(self):
        assert _escape_md("line1\nline2") == "line1 line2"

    def test_combined(self):
        assert _escape_md("a|b\nc") == "a\\|b c"

    def test_plain_text(self):
        assert _escape_md("hello world") == "hello world"


# ---------------------------------------------------------------------------
# Unit tests for markdown generation
# ---------------------------------------------------------------------------


class TestGenerateMarkdown:
    def test_basic_structure(self):
        extraction = {
            "metadata": {"title": "Test", "author": "Author", "page_count": "2"},
            "pages": [],
            "numeric_data": [],
            "total_tables": 0,
            "total_numeric_values": 0,
        }
        md = generate_markdown("test.pdf", extraction)
        assert "# PDF Table Extraction Report" in md
        assert "**Source file:** test.pdf" in md
        assert "**Title:** Test" in md
        assert "**Author:** Author" in md
        assert "**Tables found:** 0" in md
        assert "No tables were detected" in md

    def test_with_tables(self):
        extraction = {
            "metadata": {"title": "", "author": "", "page_count": "1"},
            "pages": [{
                "page_number": 1,
                "tables": [{
                    "table_number": 1,
                    "headers": ["Name", "Value"],
                    "rows": [
                        [{"value": "Item", "data_type": "text", "numeric_value": None},
                         {"value": "$100", "data_type": "currency", "numeric_value": 100.0}],
                    ],
                    "row_count": 1,
                    "col_count": 2,
                    "table_type": "financial",
                }],
            }],
            "numeric_data": [{
                "page": 1, "table": 1, "header": "Value",
                "value": "$100", "numeric_value": 100.0, "data_type": "currency",
            }],
            "total_tables": 1,
            "total_numeric_values": 1,
        }
        md = generate_markdown("money.pdf", extraction)
        assert "### Table 1 (Page 1, Type: financial)" in md
        assert "| Name | Value |" in md
        assert "| Item | $100 |" in md
        assert "## Numeric Data Summary" in md
        assert "| currency | 1 |" in md

    def test_no_numeric_data(self):
        extraction = {
            "metadata": {"page_count": "1"},
            "pages": [{
                "page_number": 1,
                "tables": [{
                    "table_number": 1,
                    "headers": ["A", "B"],
                    "rows": [
                        [{"value": "x", "data_type": "text", "numeric_value": None},
                         {"value": "y", "data_type": "text", "numeric_value": None}],
                    ],
                    "row_count": 1,
                    "col_count": 2,
                    "table_type": "general",
                }],
            }],
            "numeric_data": [],
            "total_tables": 1,
            "total_numeric_values": 0,
        }
        md = generate_markdown("text.pdf", extraction)
        assert "No numeric data was found" in md

    def test_pipe_in_cell_value_escaped(self):
        extraction = {
            "metadata": {"page_count": "1"},
            "pages": [{
                "page_number": 1,
                "tables": [{
                    "table_number": 1,
                    "headers": ["Col|A", "ColB"],
                    "rows": [
                        [{"value": "val|1", "data_type": "text", "numeric_value": None},
                         {"value": "val2", "data_type": "text", "numeric_value": None}],
                    ],
                    "row_count": 1,
                    "col_count": 2,
                    "table_type": "general",
                }],
            }],
            "numeric_data": [],
            "total_tables": 1,
            "total_numeric_values": 0,
        }
        md = generate_markdown("pipe.pdf", extraction)
        assert "Col\\|A" in md
        assert "val\\|1" in md


# ---------------------------------------------------------------------------
# Unit tests for extraction with mocked pdfplumber
# ---------------------------------------------------------------------------


class TestExtractAllTables:
    def test_extraction_with_mock(self, tmp_path):
        """Test extraction using a mock to avoid needing a real PDF with tables."""
        from pdf_indexer.models import TableData
        from pdf_indexer.extractors.table_extractor import TableExtractor

        mock_table = TableData(
            page_id=0,
            table_index=0,
            headers=["Category", "Amount", "Percent"],
            rows=[
                ["Sales", "$1,000", "50%"],
                ["Costs", "$500", "25%"],
            ],
            bbox=(0, 0, 612, 792),
            table_type="financial",
        )

        with (
            patch("pdf_indexer.webapp.MetadataExtractor") as MockMeta,
            patch("pdf_indexer.webapp.pdfplumber") as mock_plumber,
            patch.object(TableExtractor, "_extract_from_page", return_value=[mock_table]),
        ):
            MockMeta.return_value.extract.return_value = {
                "title": "Test Doc",
                "author": "",
                "page_count": "1",
            }

            mock_pdf = MagicMock()
            mock_pdf.pages = [MagicMock()]
            mock_plumber.open.return_value.__enter__ = MagicMock(return_value=mock_pdf)
            mock_plumber.open.return_value.__exit__ = MagicMock(return_value=False)

            result = extract_all_tables("/fake/path.pdf")

        assert result["total_tables"] == 1
        assert result["total_numeric_values"] > 0
        assert result["metadata"]["title"] == "Test Doc"

        # Check numeric data classified correctly
        types = {e["data_type"] for e in result["numeric_data"]}
        assert "currency" in types or "number" in types


# ---------------------------------------------------------------------------
# Flask route tests
# ---------------------------------------------------------------------------


class TestIndexRoute:
    def test_get_index(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert b"PDF Table Extractor" in resp.data
        assert b"upload" in resp.data.lower()


class TestUploadRoute:
    def test_no_file(self, client):
        resp = client.post("/upload")
        assert resp.status_code == 400
        data = json.loads(resp.data)
        assert "error" in data

    def test_empty_filename(self, client):
        resp = client.post("/upload", data={
            "pdf_file": (io.BytesIO(b""), ""),
        }, content_type="multipart/form-data")
        assert resp.status_code == 400

    def test_non_pdf_file(self, client):
        resp = client.post("/upload", data={
            "pdf_file": (io.BytesIO(b"hello"), "test.txt"),
        }, content_type="multipart/form-data")
        assert resp.status_code == 400
        data = json.loads(resp.data)
        assert "PDF" in data["error"]

    def test_successful_upload(self, client, sample_pdf):
        with open(sample_pdf, "rb") as f:
            resp = client.post("/upload", data={
                "pdf_file": (f, "test_sample.pdf"),
            }, content_type="multipart/form-data")

        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert data["success"] is True
        assert "file_id" in data
        assert "markdown_preview" in data
        assert data["md_filename"] == "test_sample_tables.md"
        assert data["csv_filename"] == "test_sample_tidy.csv"
        assert data["json_filename"] == "test_sample_data.json"
        assert data["xlsx_filename"] == "test_sample_tables.xlsx"
        assert data["zip_filename"] == "test_sample_export.zip"
        assert "tables" in data
        assert "numeric_data" in data
        assert "# PDF Table Extraction Report" in data["markdown_preview"]

    def test_upload_cleans_up_pdf(self, client, sample_pdf):
        """Uploaded PDF should be deleted after processing."""
        with open(sample_pdf, "rb") as f:
            resp = client.post("/upload", data={
                "pdf_file": (f, "cleanup_test.pdf"),
            }, content_type="multipart/form-data")

        assert resp.status_code == 200
        # The uploaded file should have been cleaned up from UPLOAD_FOLDER
        from pdf_indexer.webapp import UPLOAD_FOLDER
        remaining = [f for f in os.listdir(UPLOAD_FOLDER) if "cleanup_test" in f]
        assert len(remaining) == 0


class TestDownloadRoute:
    def test_invalid_file_id(self, client):
        resp = client.get("/download/ZZZZ/test.md")
        assert resp.status_code == 400

    def test_file_not_found(self, client):
        resp = client.get("/download/abcdef123456/missing.md")
        assert resp.status_code == 404

    def test_download_md_after_upload(self, client, sample_pdf):
        with open(sample_pdf, "rb") as f:
            resp = client.post("/upload", data={
                "pdf_file": (f, "download_test.pdf"),
            }, content_type="multipart/form-data")

        data = json.loads(resp.data)
        file_id = data["file_id"]
        md_filename = data["md_filename"]

        resp = client.get(f"/download/{file_id}/{md_filename}")
        assert resp.status_code == 200
        assert b"# PDF Table Extraction Report" in resp.data

    def test_download_csv_after_upload(self, client, sample_pdf):
        with open(sample_pdf, "rb") as f:
            resp = client.post("/upload", data={
                "pdf_file": (f, "csv_test.pdf"),
            }, content_type="multipart/form-data")

        data = json.loads(resp.data)
        file_id = data["file_id"]
        csv_filename = data["csv_filename"]

        resp = client.get(f"/download/{file_id}/{csv_filename}")
        assert resp.status_code == 200
        # CSV should be non-empty
        assert len(resp.data) > 0

    def test_download_json_after_upload(self, client, sample_pdf):
        with open(sample_pdf, "rb") as f:
            resp = client.post("/upload", data={
                "pdf_file": (f, "json_test.pdf"),
            }, content_type="multipart/form-data")

        data = json.loads(resp.data)
        file_id = data["file_id"]
        json_filename = data["json_filename"]

        resp = client.get(f"/download/{file_id}/{json_filename}")
        assert resp.status_code == 200
        parsed = json.loads(resp.data)
        assert "tables" in parsed
        assert "source_file" in parsed

    def test_download_xlsx_after_upload(self, client, sample_pdf):
        with open(sample_pdf, "rb") as f:
            resp = client.post("/upload", data={
                "pdf_file": (f, "xlsx_test.pdf"),
            }, content_type="multipart/form-data")

        data = json.loads(resp.data)
        file_id = data["file_id"]
        xlsx_filename = data["xlsx_filename"]

        resp = client.get(f"/download/{file_id}/{xlsx_filename}")
        assert resp.status_code == 200
        assert len(resp.data) > 0

    def test_download_zip_after_upload(self, client, sample_pdf):
        import zipfile as zf_mod

        with open(sample_pdf, "rb") as f:
            resp = client.post("/upload", data={
                "pdf_file": (f, "zip_test.pdf"),
            }, content_type="multipart/form-data")

        data = json.loads(resp.data)
        file_id = data["file_id"]
        zip_filename = data["zip_filename"]

        resp = client.get(f"/download/{file_id}/{zip_filename}")
        assert resp.status_code == 200
        archive = zf_mod.ZipFile(io.BytesIO(resp.data))
        names = archive.namelist()
        assert any("combined_tidy.csv" in n for n in names)
        assert any("data.json" in n for n in names)


# ---------------------------------------------------------------------------
# Unit tests for CSV generation
# ---------------------------------------------------------------------------


class TestGenerateCsv:
    def test_csv_with_tables(self):
        extraction = {
            "pages": [{
                "page_number": 1,
                "tables": [{
                    "table_number": 1,
                    "headers": ["Name", "Amount"],
                    "rows": [
                        [{"value": "Item A", "data_type": "text", "numeric_value": None},
                         {"value": "$500", "data_type": "currency", "numeric_value": 500.0}],
                    ],
                    "row_count": 1,
                    "col_count": 2,
                    "table_type": "financial",
                }],
            }],
            "numeric_data": [{
                "page": 1, "table": 1, "header": "Amount",
                "value": "$500", "numeric_value": 500.0, "data_type": "currency",
            }],
        }
        csv_out = generate_csv(extraction)
        assert "Name" in csv_out
        assert "Amount" in csv_out
        assert "Item A" in csv_out
        assert "$500" in csv_out
        assert "Numeric Data Summary" in csv_out

    def test_csv_empty(self):
        extraction = {"pages": [], "numeric_data": []}
        csv_out = generate_csv(extraction)
        assert csv_out == "" or csv_out.strip() == ""

    def test_csv_no_numeric(self):
        extraction = {
            "pages": [{
                "page_number": 1,
                "tables": [{
                    "table_number": 1,
                    "headers": ["A"],
                    "rows": [[{"value": "text", "data_type": "text", "numeric_value": None}]],
                    "row_count": 1,
                    "col_count": 1,
                    "table_type": "general",
                }],
            }],
            "numeric_data": [],
        }
        csv_out = generate_csv(extraction)
        assert "Table 1" in csv_out
        assert "Numeric Data Summary" not in csv_out


# ---------------------------------------------------------------------------
# Integration test: full round-trip with a real PDF (if reportlab available)
# ---------------------------------------------------------------------------


class TestIntegrationRoundTrip:
    def test_full_extraction_pipeline(self, sample_pdf):
        """End-to-end: extract tables from a real PDF and generate markdown."""
        result = extract_all_tables(sample_pdf)

        assert "metadata" in result
        assert "pages" in result
        assert "numeric_data" in result
        assert isinstance(result["total_tables"], int)
        assert isinstance(result["total_numeric_values"], int)

        md = generate_markdown("integration_test.pdf", result)
        assert "# PDF Table Extraction Report" in md
        assert "integration_test.pdf" in md
        assert "## Numeric Data Summary" in md

    def test_markdown_is_valid_structure(self, sample_pdf):
        """The generated markdown should have proper table formatting."""
        result = extract_all_tables(sample_pdf)
        md = generate_markdown("test.pdf", result)

        lines = md.split("\n")
        # Check header exists
        assert any(line.startswith("# ") for line in lines)
        # Check divider
        assert any(line.strip() == "---" for line in lines)

        # If tables were found, verify markdown table syntax
        if result["total_tables"] > 0:
            table_header_lines = [
                l for l in lines if l.startswith("|") and "---" in l
            ]
            assert len(table_header_lines) > 0, "Expected markdown table separator rows"


# ---------------------------------------------------------------------------
# Tests for automatic file cleanup
# ---------------------------------------------------------------------------


class TestFileCleanup:
    @pytest.fixture(autouse=True)
    def _setup_output_dir(self, tmp_path):
        """Redirect OUTPUT_FOLDER to a temp dir for isolation."""
        self._orig = OUTPUT_FOLDER
        import pdf_indexer.webapp as _mod

        _mod.OUTPUT_FOLDER = str(tmp_path)
        self.output_dir = str(tmp_path)
        yield
        _mod.OUTPUT_FOLDER = self._orig

    def test_cleanup_deletes_old_files(self):
        """Files older than TTL should be deleted."""
        old_file = os.path.join(self.output_dir, "abc123_report.csv")
        with open(old_file, "w") as f:
            f.write("data")

        # Set mtime to 10 minutes ago
        old_time = time.time() - 600
        os.utime(old_file, (old_time, old_time))

        _cleanup_old_files()

        assert not os.path.exists(old_file)

    def test_cleanup_preserves_recent_files(self):
        """Files younger than TTL should be kept."""
        new_file = os.path.join(self.output_dir, "def456_report.csv")
        with open(new_file, "w") as f:
            f.write("data")

        _cleanup_old_files()

        assert os.path.exists(new_file)

    def test_cleanup_handles_missing_directory(self):
        """Should not raise if OUTPUT_FOLDER does not exist."""
        import pdf_indexer.webapp as _mod

        _mod.OUTPUT_FOLDER = "/tmp/nonexistent_pdf_indexer_test_dir"
        _cleanup_old_files()  # Should not raise

    def test_cleanup_thread_starts_and_stops(self):
        """Thread should start and stop cleanly via stop event."""
        thread, stop_event = _start_cleanup_thread()
        assert thread.is_alive()

        stop_event.set()
        thread.join(timeout=5)
        assert not thread.is_alive()
