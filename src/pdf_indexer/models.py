"""Core data models for the PDF indexer."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class DocumentRecord:
    """Represents an indexed PDF document."""

    file_path: str
    content_hash: str
    title: str = ""
    author: str = ""
    page_count: int = 0
    file_size_bytes: int = 0
    id: int = 0


@dataclass
class PageRecord:
    """Represents a single page within a PDF."""

    document_id: int
    page_number: int
    raw_text: str = ""
    word_count: int = 0
    id: int = 0


@dataclass
class TableData:
    """Represents an extracted table from a PDF page."""

    page_id: int
    table_index: int
    headers: list[str] = field(default_factory=list)
    rows: list[list[str]] = field(default_factory=list)
    bbox: tuple[float, float, float, float] = (0.0, 0.0, 0.0, 0.0)
    table_type: str = ""
    id: int = 0

    @property
    def row_count(self) -> int:
        return len(self.rows)

    @property
    def col_count(self) -> int:
        return len(self.headers) if self.headers else 0


@dataclass
class CellData:
    """Represents a single cell with typed data."""

    value: str
    data_type: str = "text"  # text, number, currency, percent, fiscal_year
    numeric_value: float | None = None


@dataclass
class TableSearchResult:
    """A table result from a search query."""

    table_id: int
    document_name: str
    document_path: str
    page_number: int
    headers: list[str]
    rows: list[list[str]]
    row_count: int
    col_count: int
    table_type: str = ""
    match_context: str = ""


@dataclass
class PageContent:
    """All content extracted from a single page."""

    document_name: str
    page_number: int
    raw_text: str
    tables: list[TableData] = field(default_factory=list)


@dataclass
class DocumentSummary:
    """Overview of an indexed PDF document."""

    id: int
    file_path: str
    title: str
    author: str
    page_count: int
    table_count: int
    cell_count: int
    file_size_bytes: int
    last_indexed: str = ""
