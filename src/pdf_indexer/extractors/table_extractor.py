"""Extract tables from PDF pages using pdfplumber."""

from __future__ import annotations

from pdf_indexer.config import PDFIndexConfig
from pdf_indexer.models import TableData


class TableExtractor:
    """Extract tables from PDF pages using pdfplumber's table detection.

    Falls back to word-level coordinate-based extraction for pages
    where automatic table detection fails.
    """

    def __init__(self, config: PDFIndexConfig | None = None) -> None:
        self._min_rows = config.min_table_rows if config else 2
        self._min_cols = config.min_table_cols if config else 2

    def extract_tables(self, pdf_path: str, page_num: int) -> list[TableData]:
        """Extract all tables from a specific page.

        Args:
            pdf_path: Absolute path to the PDF file.
            page_num: 1-indexed page number.

        Returns:
            List of TableData objects for each detected table.
        """
        import pdfplumber

        with pdfplumber.open(pdf_path) as pdf:
            if page_num < 1 or page_num > len(pdf.pages):
                return []
            page = pdf.pages[page_num - 1]
            return self._extract_from_page(page)

    def _extract_from_page(self, page) -> list[TableData]:
        """Extract tables from a pdfplumber page object."""
        results: list[TableData] = []

        # Try pdfplumber's built-in table detection first
        tables = page.extract_tables() or []

        for idx, raw_table in enumerate(tables):
            if not raw_table or len(raw_table) < self._min_rows:
                continue

            # Clean the table: replace None with empty strings
            cleaned = [
                [cell.strip() if isinstance(cell, str) else "" for cell in row]
                for row in raw_table
            ]

            # Skip tables that are too narrow
            max_cols = max(len(row) for row in cleaned) if cleaned else 0
            if max_cols < self._min_cols:
                continue

            # Pad rows to uniform width
            cleaned = [row + [""] * (max_cols - len(row)) for row in cleaned]

            # First row is headers, rest is data
            headers = cleaned[0]
            rows = cleaned[1:]

            # Skip if no data rows
            if not rows:
                continue

            # Get bounding box from table finder if available
            bbox = self._get_table_bbox(page, idx)

            # Classify table type
            table_type = self._classify_table(headers, rows)

            results.append(TableData(
                page_id=0,  # Set by caller
                table_index=idx,
                headers=headers,
                rows=rows,
                bbox=bbox,
                table_type=table_type,
            ))

        # If no tables found via automatic detection, try word-based extraction
        if not results:
            word_table = self._extract_via_words(page)
            if word_table is not None:
                results.append(word_table)

        return results

    def _get_table_bbox(self, page, table_idx: int) -> tuple[float, float, float, float]:
        """Get bounding box for a detected table."""
        try:
            table_finder = page.find_tables()
            if table_idx < len(table_finder):
                bbox = table_finder[table_idx].bbox
                return (bbox[0], bbox[1], bbox[2], bbox[3])
        except Exception:
            pass
        return (0.0, 0.0, float(page.width), float(page.height))

    def _extract_via_words(self, page) -> TableData | None:
        """Fallback: extract table structure from word-level coordinates.

        Uses coordinate-based column detection — groups words by y-coordinate
        (rows) and separates columns by x-coordinate gaps.
        """
        words = page.extract_words(x_tolerance=1, y_tolerance=3)
        if not words:
            return None

        # Group words by y-coordinate (rows)
        y_tolerance = 5
        rows_by_y: dict[float, list[dict]] = {}
        for w in words:
            y_key = round(w["top"] / y_tolerance) * y_tolerance
            rows_by_y.setdefault(y_key, []).append(w)

        if len(rows_by_y) < self._min_rows:
            return None

        # Sort rows by y position
        sorted_y_keys = sorted(rows_by_y.keys())

        # Detect column boundaries from x-coordinates
        all_x_positions = sorted(set(
            round(w["x0"]) for ws in rows_by_y.values() for w in ws
        ))
        col_boundaries = self._detect_column_boundaries(all_x_positions)

        if len(col_boundaries) < self._min_cols:
            return None

        # Build table grid
        grid: list[list[str]] = []
        for y_key in sorted_y_keys:
            row_words = sorted(rows_by_y[y_key], key=lambda w: w["x0"])
            row = self._assign_words_to_columns(row_words, col_boundaries)
            grid.append(row)

        if len(grid) < self._min_rows:
            return None

        headers = grid[0]
        data_rows = grid[1:]

        # Only keep if it looks tabular (at least some cells filled in each row)
        filled_ratios = [
            sum(1 for c in row if c.strip()) / len(row) for row in data_rows
        ]
        avg_fill = sum(filled_ratios) / len(filled_ratios) if filled_ratios else 0
        if avg_fill < 0.3:
            return None

        return TableData(
            page_id=0,
            table_index=0,
            headers=headers,
            rows=data_rows,
            bbox=(0.0, 0.0, float(page.width), float(page.height)),
            table_type=self._classify_table(headers, data_rows),
        )

    def _detect_column_boundaries(self, x_positions: list[int]) -> list[float]:
        """Detect column start positions from x-coordinate clustering."""
        if not x_positions:
            return []

        # Find gaps between x-positions to determine column breaks
        min_gap = 30  # Minimum pixel gap between columns
        boundaries = [float(x_positions[0])]

        for i in range(1, len(x_positions)):
            if x_positions[i] - x_positions[i - 1] > min_gap:
                boundaries.append(float(x_positions[i]))

        return boundaries

    def _assign_words_to_columns(
        self, words: list[dict], col_boundaries: list[float],
    ) -> list[str]:
        """Assign words to the nearest column based on x-position."""
        n_cols = len(col_boundaries)
        cells = [""] * n_cols

        for word in words:
            x = word["x0"]
            # Find the closest column boundary
            best_col = 0
            best_dist = abs(x - col_boundaries[0])
            for i, boundary in enumerate(col_boundaries):
                dist = abs(x - boundary)
                if dist < best_dist:
                    best_dist = dist
                    best_col = i
            text = word.get("text", "")
            if cells[best_col]:
                cells[best_col] += " " + text
            else:
                cells[best_col] = text

        return cells

    def _classify_table(self, headers: list[str], rows: list[list[str]]) -> str:
        """Classify table type based on content patterns."""
        all_text = " ".join(headers) + " " + " ".join(
            cell for row in rows[:5] for cell in row
        )
        all_lower = all_text.lower()

        if "$" in all_text or "million" in all_lower or "budget" in all_lower:
            return "financial"
        if "%" in all_text:
            return "statistical"
        if any(
            kw in all_lower
            for kw in ("population", "rate", "growth", "index", "forecast")
        ):
            return "statistical"
        return "general"
