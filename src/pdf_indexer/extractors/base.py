"""Protocol interface for PDF content extractors."""

from __future__ import annotations

from typing import Any, Protocol


class PDFExtractor(Protocol):
    """Interface that all PDF extractors must implement."""

    def extract(self, pdf_path: str, page_num: int) -> Any:
        """Extract content from a specific page of a PDF.

        Args:
            pdf_path: Absolute path to the PDF file.
            page_num: 1-indexed page number.

        Returns:
            Extracted content (type depends on extractor).
        """
        ...
