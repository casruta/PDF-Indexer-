"""Extract raw text content from PDF pages."""

from __future__ import annotations


class TextExtractor:
    """Extract plain text from PDF pages using pdfplumber."""

    def extract(self, pdf_path: str, page_num: int) -> str:
        """Extract text from a specific page.

        Args:
            pdf_path: Absolute path to the PDF file.
            page_num: 1-indexed page number.

        Returns:
            Extracted text as a string.
        """
        import pdfplumber

        with pdfplumber.open(pdf_path) as pdf:
            if page_num < 1 or page_num > len(pdf.pages):
                return ""
            page = pdf.pages[page_num - 1]
            return page.extract_text() or ""
