"""Markdown formatters for MCP server responses."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pdf_indexer.models import (
        DocumentSummary,
        PageContent,
        TableSearchResult,
    )


class TableFormatter:
    """Format PDF index query results as markdown."""

    def format_search_results(self, results: list[TableSearchResult]) -> str:
        """Render search results as markdown."""
        if not results:
            return "No tables found matching the search criteria."

        lines = [f"## Found {len(results)} table(s)\n"]
        for r in results:
            lines.append(
                f"### Table {r.table_id} "
                f"(Page {r.page_number} of `{r.document_name}`)"
            )
            if r.table_type:
                lines.append(f"**Type:** {r.table_type}")
            lines.append(
                f"**Size:** {r.row_count} rows x {r.col_count} cols\n"
            )
            lines.append(self._render_markdown_table(r.headers, r.rows))
            lines.append("")

        return "\n".join(lines)

    def format_table(self, table: TableSearchResult) -> str:
        """Render a single table with full detail."""
        lines = [
            f"## Table {table.table_id}",
            f"**Document:** `{table.document_name}`",
            f"**Page:** {table.page_number}",
            f"**Size:** {table.row_count} rows x {table.col_count} cols",
        ]
        if table.table_type:
            lines.append(f"**Type:** {table.table_type}")
        lines.append("")
        lines.append(self._render_markdown_table(table.headers, table.rows))
        return "\n".join(lines)

    def format_document_summary(self, doc: DocumentSummary) -> str:
        """Render a document overview."""
        name = os.path.basename(doc.file_path)
        size_mb = doc.file_size_bytes / (1024 * 1024)
        lines = [
            f"## {name}",
            f"**Title:** {doc.title or '(none)'}",
            f"**Author:** {doc.author or '(none)'}",
            f"**Pages:** {doc.page_count}",
            f"**Tables:** {doc.table_count}",
            f"**Cells:** {doc.cell_count}",
            f"**Size:** {size_mb:.1f} MB",
            f"**Indexed:** {doc.last_indexed or 'unknown'}",
        ]
        return "\n".join(lines)

    def format_document_list(self, docs: list[DocumentSummary]) -> str:
        """Render a list of all documents."""
        if not docs:
            return "No documents indexed."

        lines = ["## Indexed Documents\n"]
        lines.append("| Document | Pages | Tables | Cells |")
        lines.append("| --- | ---: | ---: | ---: |")
        for doc in docs:
            name = os.path.basename(doc.file_path)
            lines.append(
                f"| {name} | {doc.page_count} | {doc.table_count} | {doc.cell_count} |"
            )
        total_pages = sum(d.page_count for d in docs)
        total_tables = sum(d.table_count for d in docs)
        total_cells = sum(d.cell_count for d in docs)
        lines.append(
            f"| **Total ({len(docs)} docs)** "
            f"| **{total_pages}** | **{total_tables}** | **{total_cells}** |"
        )
        return "\n".join(lines)

    def format_page(self, page: PageContent) -> str:
        """Render all content from a single page."""
        lines = [
            f"## Page {page.page_number} of `{page.document_name}`\n",
        ]

        if page.raw_text:
            lines.append("### Text Content")
            # Truncate to avoid token overflow
            text = page.raw_text
            if len(text) > 2000:
                text = text[:2000] + "\n\n_(truncated, {0} chars total)_".format(
                    len(page.raw_text)
                )
            lines.append(text)
            lines.append("")

        if page.tables:
            lines.append(f"### Tables ({len(page.tables)} found)\n")
            for t in page.tables:
                lines.append(f"**Table {t.table_index + 1}** ({t.table_type or 'general'})")
                lines.append(self._render_markdown_table(t.headers, t.rows))
                lines.append("")
        else:
            lines.append("_No tables on this page._")

        return "\n".join(lines)

    def format_query_results(self, results: list[dict]) -> str:
        """Render SQL query results."""
        if not results:
            return "Query returned no results."

        columns = list(results[0].keys())

        lines = ["## Query Results\n"]
        lines.append("| " + " | ".join(columns) + " |")
        lines.append("| " + " | ".join(["---"] * len(columns)) + " |")

        max_rows = 50
        for row in results[:max_rows]:
            vals = [str(row.get(c, "")) for c in columns]
            lines.append("| " + " | ".join(vals) + " |")

        if len(results) > max_rows:
            lines.append(f"\n_({len(results) - max_rows} more rows omitted)_")

        lines.append(f"\n**{len(results)} row(s) returned**")
        return "\n".join(lines)

    def _render_markdown_table(
        self, headers: list[str], rows: list[list[str]], max_rows: int = 20,
    ) -> str:
        """Convert headers + rows into a markdown table."""
        if not headers:
            return "_Empty table_"

        # Escape pipe characters in cell values
        def escape(val: str) -> str:
            return val.replace("|", "\\|") if val else ""

        md = "| " + " | ".join(escape(h) for h in headers) + " |\n"
        md += "| " + " | ".join(["---"] * len(headers)) + " |\n"

        for row in rows[:max_rows]:
            # Pad or trim to match header count
            padded = row + [""] * (len(headers) - len(row)) if len(row) < len(headers) else row[:len(headers)]
            md += "| " + " | ".join(escape(c) for c in padded) + " |\n"

        if len(rows) > max_rows:
            md += f"\n_({len(rows) - max_rows} more rows omitted)_\n"

        return md
