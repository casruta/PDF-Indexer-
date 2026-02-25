"""Detect and classify financial data types in table cells."""

from __future__ import annotations

import re

from pdf_indexer.models import CellData

# Patterns for financial data detection
_CURRENCY_RE = re.compile(
    r"^\s*\(?\$?\s*[\d,]+\.?\d*\)?\s*$"
)
_PERCENT_RE = re.compile(
    r"^\s*\(?\s*[\d,]+\.?\d*\s*%\s*\)?\s*$"
)
_FISCAL_YEAR_RE = re.compile(
    r"^\s*(?:FY\s*)?(\d{4})\s*[-/]\s*(\d{2,4})\s*$", re.IGNORECASE
)
_NUMBER_RE = re.compile(
    r"^\s*\(?\s*[\d,]+\.?\d*\)?\s*$"
)


class DataTyper:
    """Classify cell values into financial data types."""

    def classify(self, value: str) -> CellData:
        """Classify a cell value and extract its numeric representation.

        Args:
            value: Raw cell text.

        Returns:
            CellData with data_type and optional numeric_value.
        """
        if value is None:
            return CellData(value="", data_type="text")

        stripped = value.strip()
        if not stripped:
            return CellData(value="", data_type="text")

        # Check currency first ($ sign is definitive)
        if "$" in stripped or (
            _CURRENCY_RE.match(stripped) and self._looks_like_currency(stripped)
        ):
            numeric = self._parse_numeric(stripped)
            if numeric is not None:
                return CellData(value=stripped, data_type="currency", numeric_value=numeric)

        # Check percentage
        if _PERCENT_RE.match(stripped):
            numeric = self._parse_percent(stripped)
            if numeric is not None:
                return CellData(value=stripped, data_type="percent", numeric_value=numeric)

        # Check fiscal year
        m = _FISCAL_YEAR_RE.match(stripped)
        if m:
            year = int(m.group(1))
            return CellData(value=stripped, data_type="fiscal_year", numeric_value=float(year))

        # Check plain number
        if _NUMBER_RE.match(stripped):
            numeric = self._parse_numeric(stripped)
            if numeric is not None:
                return CellData(value=stripped, data_type="number", numeric_value=numeric)

        # Default to text
        return CellData(value=stripped, data_type="text")

    def _looks_like_currency(self, value: str) -> bool:
        """Heuristic: contains digits and possibly commas/periods."""
        return any(c.isdigit() for c in value)

    def _parse_numeric(self, value: str) -> float | None:
        """Parse a numeric value, handling parentheses as negative and commas."""
        try:
            cleaned = value.strip()
            is_negative = cleaned.startswith("(") and cleaned.endswith(")")
            cleaned = cleaned.strip("()")
            cleaned = cleaned.replace("$", "").replace(",", "").replace(" ", "")
            if not cleaned or not any(c.isdigit() for c in cleaned):
                return None
            result = float(cleaned)
            return -result if is_negative else result
        except (ValueError, TypeError):
            return None

    def _parse_percent(self, value: str) -> float | None:
        """Parse a percentage value."""
        try:
            cleaned = value.strip()
            is_negative = cleaned.startswith("(") and cleaned.endswith(")")
            cleaned = cleaned.strip("()")
            cleaned = cleaned.replace("%", "").replace(",", "").replace(" ", "")
            if not cleaned:
                return None
            result = float(cleaned)
            return -result if is_negative else result
        except (ValueError, TypeError):
            return None
