"""Command-line interface for the PDF indexer."""

from __future__ import annotations

import argparse
import os
import sys
import time

from pdf_indexer.config import PDFIndexConfig
from pdf_indexer.models import CellData, DocumentRecord, PageRecord, TableData


def main(argv: list[str] | None = None) -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="pdf-index",
        description="Index PDF tables for data science workflows.",
    )
    parser.add_argument(
        "path", nargs="?", default=".",
        help="Directory containing PDFs to index (default: current dir)",
    )
    parser.add_argument(
        "--db-path", type=str, default=None,
        help="Database path (default: <path>/.pdfindex/index.db)",
    )
    parser.add_argument(
        "--exclude", type=str, default=None,
        help="Comma-separated filename patterns to exclude",
    )
    parser.add_argument(
        "--min-rows", type=int, default=None,
        help="Minimum rows for table detection (default: 2)",
    )
    parser.add_argument(
        "--min-cols", type=int, default=None,
        help="Minimum columns for table detection (default: 2)",
    )
    parser.add_argument(
        "--list", action="store_true",
        help="List all indexed documents",
    )
    parser.add_argument(
        "--search", type=str, default=None,
        help="Search tables by keyword",
    )
    parser.add_argument(
        "--serve-mcp", action="store_true",
        help="Start the MCP server",
    )
    parser.add_argument(
        "--generate-mcp-config", action="store_true",
        help="Print .mcp.json configuration snippet",
    )
    parser.add_argument(
        "--session-start", action="store_true",
        help="Start a new research session",
    )
    parser.add_argument(
        "--session-end", action="store_true",
        help="End the active research session",
    )
    parser.add_argument(
        "--observe", type=str, default=None,
        help="Add an observation to the active session",
    )
    parser.add_argument(
        "--observations", action="store_true",
        help="Show observations from the current/latest session",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Enable verbose output",
    )

    args = parser.parse_args(argv)
    root_path = os.path.abspath(args.path)

    config = PDFIndexConfig.resolve(
        root_path=root_path,
        db_path=args.db_path,
        exclude=args.exclude,
        min_table_rows=args.min_rows,
        min_table_cols=args.min_cols,
        verbose=args.verbose,
    )

    if args.serve_mcp:
        _run_mcp_server(config)
    elif args.generate_mcp_config:
        _print_mcp_config(config)
    elif args.list:
        _run_list(config)
    elif args.search is not None:
        _run_search(config, args.search)
    elif args.session_start:
        _run_session_start(config)
    elif args.session_end:
        _run_session_end(config)
    elif args.observe is not None:
        _run_observe(config, args.observe)
    elif args.observations:
        _run_show_observations(config)
    else:
        _run_index(config)


# ── Index pipeline ─────────────────────────────────────────────────────


def _run_index(config: PDFIndexConfig) -> None:
    """Main indexing pipeline: scan, hash, extract, store."""
    from pdf_indexer.database import PDFDatabase
    from pdf_indexer.extractors.data_typer import DataTyper
    from pdf_indexer.extractors.metadata_extractor import MetadataExtractor
    from pdf_indexer.extractors.table_extractor import TableExtractor
    from pdf_indexer.extractors.text_extractor import TextExtractor
    from pdf_indexer.scanner import compute_file_hash, find_pdf_files

    print(f"Scanning: {config.root_path}")
    pdf_files = find_pdf_files(config.root_path, config.exclude_patterns)

    if not pdf_files:
        print("No PDF files found.")
        return

    print(f"Found {len(pdf_files)} PDF file(s)")

    db = PDFDatabase(config.db_path)
    table_ext = TableExtractor(config)
    text_ext = TextExtractor()
    meta_ext = MetadataExtractor()
    typer = DataTyper()

    stats = {"indexed": 0, "skipped": 0, "tables": 0, "pages": 0}
    start_time = time.time()

    # Remove documents that no longer exist on disk
    existing_paths = db.get_all_document_paths()
    current_paths = set(pdf_files)
    for old_path in existing_paths - current_paths:
        if config.verbose:
            print(f"  Removed (deleted): {os.path.basename(old_path)}")
        db.delete_document(old_path)

    for pdf_path in pdf_files:
        fname = os.path.basename(pdf_path)

        # Hash-based change detection
        content_hash = compute_file_hash(pdf_path)
        stored_hash = db.get_file_hash(pdf_path)

        if stored_hash == content_hash:
            if config.verbose:
                print(f"  Skipped (unchanged): {fname}")
            stats["skipped"] += 1
            continue

        print(f"  Indexing: {fname}")

        # Extract metadata
        try:
            metadata = meta_ext.extract(pdf_path)
        except Exception as e:
            print(f"    [!] Failed to read metadata: {e}")
            metadata = {"title": "", "author": "", "page_count": "0"}

        page_count = int(metadata.get("page_count", "0"))
        file_size = os.path.getsize(pdf_path)

        # Upsert document
        doc_id = db.upsert_document(DocumentRecord(
            file_path=pdf_path,
            content_hash=content_hash,
            title=metadata.get("title", ""),
            author=metadata.get("author", ""),
            page_count=page_count,
            file_size_bytes=file_size,
        ))

        # Process each page
        doc_tables = 0
        for page_num in range(1, page_count + 1):
            # Extract text
            try:
                raw_text = text_ext.extract(pdf_path, page_num)
            except Exception:
                raw_text = ""

            page_id = db.upsert_page(PageRecord(
                document_id=doc_id,
                page_number=page_num,
                raw_text=raw_text,
            ))

            # Clear old tables for this page (re-index)
            db.clear_tables_for_page(page_id)

            # Extract tables
            try:
                tables = table_ext.extract_tables(pdf_path, page_num)
            except Exception as e:
                if config.verbose:
                    print(f"    [!] Page {page_num}: table extraction failed: {e}")
                tables = []

            for table in tables:
                table.page_id = page_id
                table_id = db.insert_table(table)

                # Type and insert cells
                typed_rows: list[list[CellData]] = []
                for row in table.rows:
                    typed_rows.append([typer.classify(cell) for cell in row])
                db.insert_cells_batch(table_id, typed_rows)

                doc_tables += 1

            stats["pages"] += 1

        stats["indexed"] += 1
        stats["tables"] += doc_tables

        if config.verbose:
            print(f"    {page_count} pages, {doc_tables} tables")

    elapsed = time.time() - start_time
    db_stats = db.stats()
    db.close()

    print(f"\nDone in {elapsed:.1f}s")
    print(f"  Indexed: {stats['indexed']} | Skipped: {stats['skipped']}")
    print(f"  Database: {db_stats['documents']} docs, "
          f"{db_stats['pages']} pages, "
          f"{db_stats['tables']} tables, "
          f"{db_stats['cells']} cells")


# ── List command ───────────────────────────────────────────────────────


def _run_list(config: PDFIndexConfig) -> None:
    """List all indexed documents."""
    from pdf_indexer.database import PDFDatabase

    if not os.path.isfile(config.db_path):
        print("No index found. Run 'pdf-index <path>' first.")
        return

    db = PDFDatabase(config.db_path)
    docs = db.list_documents()
    db.close()

    if not docs:
        print("No documents indexed.")
        return

    print(f"{'Document':<45} {'Pages':>6} {'Tables':>7} {'Cells':>7}")
    print("-" * 70)
    for doc in docs:
        name = os.path.basename(doc.file_path)
        if len(name) > 44:
            name = name[:41] + "..."
        print(f"{name:<45} {doc.page_count:>6} {doc.table_count:>7} {doc.cell_count:>7}")


# ── Search command ─────────────────────────────────────────────────────


def _run_search(config: PDFIndexConfig, query: str) -> None:
    """Search tables by keyword."""
    from pdf_indexer.database import PDFDatabase

    if not os.path.isfile(config.db_path):
        print("No index found. Run 'pdf-index <path>' first.")
        return

    db = PDFDatabase(config.db_path)
    results = db.search_tables(query, limit=10)
    db.close()

    if not results:
        print(f"No tables found matching '{query}'.")
        return

    print(f"Found {len(results)} table(s) matching '{query}':\n")
    for r in results:
        print(f"--- Table {r.table_id} (Page {r.page_number} of {r.document_name}) ---")
        if r.headers:
            print("| " + " | ".join(r.headers) + " |")
            print("| " + " | ".join(["---"] * len(r.headers)) + " |")
        for row in r.rows[:10]:
            print("| " + " | ".join(row) + " |")
        if len(r.rows) > 10:
            print(f"  ... ({len(r.rows) - 10} more rows)")
        print()


# ── MCP server ─────────────────────────────────────────────────────────


def _run_mcp_server(config: PDFIndexConfig) -> None:
    """Start the MCP server."""
    os.environ["PDF_INDEX_DB"] = config.db_path
    try:
        from pdf_indexer.mcp_server.server import run_server
        run_server()
    except ImportError:
        print("MCP dependencies not installed. Run: pip install pdf-indexer[mcp]")
        sys.exit(1)


def _print_mcp_config(config: PDFIndexConfig) -> None:
    """Print the .mcp.json configuration snippet."""
    python_path = sys.executable.replace("\\", "\\\\")
    db_path = os.path.abspath(config.db_path).replace("\\", "\\\\")

    print("""{
  "mcpServers": {
    "pdf-indexer": {
      "command": "%s",
      "args": ["-X", "utf8", "-m", "pdf_indexer.mcp_server"],
      "env": {
        "PDF_INDEX_DB": "%s"
      }
    }
  }
}""" % (python_path, db_path))


# ── Session commands ───────────────────────────────────────────────────


def _run_session_start(config: PDFIndexConfig) -> None:
    """Start a new research session."""
    from pdf_indexer.database import PDFDatabase
    from pdf_indexer.session.manager import SessionManager

    if not os.path.isfile(config.db_path):
        print("No index found. Run 'pdf-index <path>' first.")
        return

    db = PDFDatabase(config.db_path)
    mgr = SessionManager(db, os.path.dirname(config.db_path))
    session_id = mgr.start()
    db.close()
    print(f"Session {session_id} started.")


def _run_session_end(config: PDFIndexConfig) -> None:
    """End the active research session."""
    from pdf_indexer.database import PDFDatabase
    from pdf_indexer.session.manager import SessionManager

    if not os.path.isfile(config.db_path):
        print("No index found.")
        return

    db = PDFDatabase(config.db_path)
    mgr = SessionManager(db, os.path.dirname(config.db_path))
    ended = mgr.end()
    db.close()
    if ended:
        print(f"Session {ended} ended.")
    else:
        print("No active session.")


def _run_observe(config: PDFIndexConfig, content: str) -> None:
    """Add an observation to the active session."""
    from pdf_indexer.database import PDFDatabase
    from pdf_indexer.session.manager import SessionManager

    if not os.path.isfile(config.db_path):
        print("No index found.")
        return

    db = PDFDatabase(config.db_path)
    mgr = SessionManager(db, os.path.dirname(config.db_path))
    obs_id = mgr.observe(content)
    db.close()
    if obs_id:
        print(f"Observation {obs_id} added.")
    else:
        print("No active session. Start one with --session-start.")


def _run_show_observations(config: PDFIndexConfig) -> None:
    """Show observations from the current or latest session."""
    from pdf_indexer.database import PDFDatabase
    from pdf_indexer.session.manager import SessionManager

    if not os.path.isfile(config.db_path):
        print("No index found.")
        return

    db = PDFDatabase(config.db_path)
    mgr = SessionManager(db, os.path.dirname(config.db_path))
    observations = mgr.get_observations()
    db.close()

    if not observations:
        print("No observations found.")
        return

    for obs in observations:
        stale = " [STALE]" if obs["is_stale"] else ""
        doc = f" ({os.path.basename(obs['file_path'])})" if obs.get("file_path") else ""
        print(f"  [{obs['created_at']}]{stale}{doc} {obs['content']}")


if __name__ == "__main__":
    main()
