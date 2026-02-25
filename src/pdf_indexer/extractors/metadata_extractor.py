"""Extract PDF-level metadata (title, author, dates)."""

from __future__ import annotations


class MetadataExtractor:
    """Extract metadata from a PDF file."""

    def extract(self, pdf_path: str) -> dict[str, str]:
        """Extract metadata from a PDF.

        Returns:
            Dict with keys: title, author, subject, creator.
        """
        import pdfplumber

        with pdfplumber.open(pdf_path) as pdf:
            metadata = pdf.metadata or {}
            page_count = len(pdf.pages)

        return {
            "title": metadata.get("Title", "") or "",
            "author": metadata.get("Author", "") or "",
            "subject": metadata.get("Subject", "") or "",
            "creator": metadata.get("Creator", "") or "",
            "page_count": str(page_count),
        }
