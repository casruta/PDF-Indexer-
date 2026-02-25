"""FastMCP server exposing PDF index query tools."""

from __future__ import annotations

import os

from mcp.server.fastmcp import FastMCP

from pdf_indexer.mcp_server.formatters import TableFormatter

mcp = FastMCP(
    "pdf-indexer",
    instructions=(
        "Query indexed PDF tables and financial data. "
        "Use search_tables to find data, get_table for full detail, "
        "list_documents for an overview, and query_data for custom SQL."
    ),
)

_formatter = TableFormatter()


def _get_db():
    """Lazy database connection from environment variable."""
    from pdf_indexer.database import PDFDatabase

    db_path = os.environ.get("PDF_INDEX_DB", "")
    if not db_path or not os.path.isfile(db_path):
        raise RuntimeError(
            f"PDF index database not found: {db_path!r}. "
            "Set PDF_INDEX_DB environment variable or run 'pdf-index <dir>' first."
        )
    return PDFDatabase(db_path)


# ── Tools ──────────────────────────────────────────────────────────────


@mcp.tool()
def search_tables(
    query: str,
    document_name: str | None = None,
    data_type: str | None = None,
    min_value: float | None = None,
    max_value: float | None = None,
    max_results: int = 10,
) -> str:
    """Search for tables matching criteria across all indexed PDFs.

    Args:
        query: Text to search for in table cells (e.g. "Education", "revenue").
        document_name: Filter by document filename (partial match, optional).
        data_type: Filter by cell data type: 'currency', 'percent', 'number' (optional).
        min_value: Minimum numeric value filter (optional).
        max_value: Maximum numeric value filter (optional).
        max_results: Maximum number of tables to return (default 10).
    """
    db = _get_db()
    try:
        results = db.search_tables(
            query=query,
            document_name=document_name,
            data_type=data_type,
            min_value=min_value,
            max_value=max_value,
            limit=max_results,
        )
        return _formatter.format_search_results(results)
    finally:
        db.close()


@mcp.tool()
def get_table(table_id: int) -> str:
    """Get complete table data by its ID.

    Args:
        table_id: The ID of the table to retrieve (from search results).
    """
    db = _get_db()
    try:
        table = db.get_table_by_id(table_id)
        if table is None:
            return f"Table {table_id} not found."
        return _formatter.format_table(table)
    finally:
        db.close()


@mcp.tool()
def get_document_summary(document_name: str) -> str:
    """Get an overview of an indexed PDF document.

    Args:
        document_name: Filename or partial match (e.g. "2024-2025 Budget").
    """
    db = _get_db()
    try:
        doc = db.find_document(document_name)
        if doc is None:
            return f"Document '{document_name}' not found."
        return _formatter.format_document_summary(doc)
    finally:
        db.close()


@mcp.tool()
def get_page_content(document_name: str, page_number: int) -> str:
    """Get all extracted content (text + tables) from a specific page.

    Args:
        document_name: Document filename or partial match.
        page_number: Page number (1-indexed).
    """
    db = _get_db()
    try:
        page = db.get_page_content(document_name, page_number)
        if page is None:
            return f"Page {page_number} not found in '{document_name}'."
        return _formatter.format_page(page)
    finally:
        db.close()


@mcp.tool()
def list_documents() -> str:
    """List all indexed PDF documents with their page, table, and cell counts."""
    db = _get_db()
    try:
        docs = db.list_documents()
        return _formatter.format_document_list(docs)
    finally:
        db.close()


@mcp.tool()
def query_data(sql_query: str) -> str:
    """Run a read-only SQL query against the indexed PDF data.

    Only SELECT queries are allowed. Available tables:
    - documents (id, file_path, title, author, page_count)
    - pages (id, document_id, page_number, raw_text, word_count)
    - tables (id, page_id, table_index, row_count, col_count, headers_json, table_type)
    - table_cells (id, table_id, row_idx, col_idx, value, data_type, numeric_value)

    Args:
        sql_query: A SQL SELECT query.
    """
    stripped = sql_query.strip()
    if not stripped.upper().startswith("SELECT"):
        return "Error: Only SELECT queries are allowed for safety."

    # Block dangerous keywords
    upper = stripped.upper()
    for keyword in ("DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE"):
        if keyword in upper:
            return f"Error: {keyword} operations are not allowed."

    db = _get_db()
    try:
        results = db.execute_readonly_sql(stripped)
        return _formatter.format_query_results(results)
    except Exception as e:
        return f"Query error: {e}"
    finally:
        db.close()


# ── Session / Observation tools ────────────────────────────────────────


def _get_session_manager():
    """Get a SessionManager instance."""
    from pdf_indexer.session.manager import SessionManager

    db = _get_db()
    db_dir = os.path.dirname(os.environ.get("PDF_INDEX_DB", ""))
    return SessionManager(db, db_dir), db


@mcp.tool()
def add_observation(content: str, table_id: int | None = None) -> str:
    """Record a research observation in the active session.

    Use this to note findings, insights, or questions about the data.
    Observations persist across conversations and are linked to the session.

    Args:
        content: The observation text (e.g. "Education spending grew 5% from 2020-2024").
        table_id: Optional table ID to link this observation to.
    """
    mgr, db = _get_session_manager()
    try:
        obs_id = mgr.observe(content, table_id=table_id)
        if obs_id is None:
            # Auto-start a session if none exists
            session_id = mgr.start()
            obs_id = mgr.observe(content, table_id=table_id)
            return f"Started session {session_id} and recorded observation {obs_id}."
        return f"Observation {obs_id} recorded."
    finally:
        db.close()


@mcp.tool()
def get_session_notes(session_id: int | None = None) -> str:
    """Retrieve observations from the current or a specific session.

    Args:
        session_id: Session ID to query. If omitted, uses the latest session.
    """
    mgr, db = _get_session_manager()
    try:
        observations = mgr.get_observations(session_id=session_id)
        if not observations:
            return "No observations found."

        lines = ["## Session Observations\n"]
        for obs in observations:
            stale = " **[STALE]**" if obs["is_stale"] else ""
            doc = ""
            if obs.get("file_path"):
                doc = f" (`{os.path.basename(obs['file_path'])}`)"
            lines.append(f"- {obs['content']}{doc}{stale}")
        return "\n".join(lines)
    finally:
        db.close()


# ── Server entry point ─────────────────────────────────────────────────


def run_server() -> None:
    """Start the MCP server over stdio."""
    mcp.run(transport="stdio")
