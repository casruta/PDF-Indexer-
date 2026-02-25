"""SQLite-backed storage for indexed PDF data."""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

from pdf_indexer.models import (
    CellData,
    DocumentRecord,
    DocumentSummary,
    PageContent,
    PageRecord,
    TableData,
    TableSearchResult,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_path TEXT UNIQUE NOT NULL,
    content_hash TEXT NOT NULL,
    title TEXT DEFAULT '',
    author TEXT DEFAULT '',
    page_count INTEGER DEFAULT 0,
    file_size_bytes INTEGER DEFAULT 0,
    last_indexed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_stale INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS pages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    page_number INTEGER NOT NULL,
    raw_text TEXT DEFAULT '',
    word_count INTEGER DEFAULT 0,
    is_processed INTEGER DEFAULT 1,
    UNIQUE(document_id, page_number)
);

CREATE TABLE IF NOT EXISTS tables (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    page_id INTEGER NOT NULL REFERENCES pages(id) ON DELETE CASCADE,
    table_index INTEGER NOT NULL,
    row_count INTEGER DEFAULT 0,
    col_count INTEGER DEFAULT 0,
    headers_json TEXT DEFAULT '[]',
    bbox_x0 REAL DEFAULT 0,
    bbox_y0 REAL DEFAULT 0,
    bbox_x1 REAL DEFAULT 0,
    bbox_y1 REAL DEFAULT 0,
    table_type TEXT DEFAULT '',
    UNIQUE(page_id, table_index)
);

CREATE TABLE IF NOT EXISTS table_cells (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_id INTEGER NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
    row_idx INTEGER NOT NULL,
    col_idx INTEGER NOT NULL,
    value TEXT DEFAULT '',
    data_type TEXT DEFAULT 'text',
    numeric_value REAL,
    UNIQUE(table_id, row_idx, col_idx)
);

CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ended_at TIMESTAMP,
    document_count_at_start INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER REFERENCES sessions(id),
    document_id INTEGER REFERENCES documents(id) ON DELETE SET NULL,
    page_id INTEGER REFERENCES pages(id) ON DELETE SET NULL,
    table_id INTEGER REFERENCES tables(id) ON DELETE SET NULL,
    observation_type TEXT NOT NULL DEFAULT 'note',
    content TEXT NOT NULL,
    is_stale INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pages_doc ON pages(document_id);
CREATE INDEX IF NOT EXISTS idx_tables_page ON tables(page_id);
CREATE INDEX IF NOT EXISTS idx_cells_table ON table_cells(table_id);
CREATE INDEX IF NOT EXISTS idx_cells_value ON table_cells(value);
CREATE INDEX IF NOT EXISTS idx_cells_numeric ON table_cells(numeric_value);
CREATE INDEX IF NOT EXISTS idx_cells_type ON table_cells(data_type);
CREATE INDEX IF NOT EXISTS idx_observations_session ON observations(session_id);
CREATE INDEX IF NOT EXISTS idx_observations_document ON observations(document_id);
"""


class PDFDatabase:
    """SQLite-backed storage for PDF documents, pages, tables, and cells."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._create_schema()

    def _create_schema(self) -> None:
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    # ── Document operations ────────────────────────────────────────────

    def get_file_hash(self, file_path: str) -> str | None:
        """Return the stored content hash for a document, or None."""
        row = self._conn.execute(
            "SELECT content_hash FROM documents WHERE file_path = ?", (file_path,)
        ).fetchone()
        return row[0] if row else None

    def upsert_document(self, record: DocumentRecord) -> int:
        """Insert or update a document record. Returns the document id."""
        self._conn.execute(
            """INSERT INTO documents
               (file_path, content_hash, title, author, page_count, file_size_bytes, is_stale)
               VALUES (?, ?, ?, ?, ?, ?, 0)
               ON CONFLICT(file_path) DO UPDATE SET
                   content_hash = excluded.content_hash,
                   title = excluded.title,
                   author = excluded.author,
                   page_count = excluded.page_count,
                   file_size_bytes = excluded.file_size_bytes,
                   last_indexed = CURRENT_TIMESTAMP,
                   is_stale = 0""",
            (
                record.file_path,
                record.content_hash,
                record.title,
                record.author,
                record.page_count,
                record.file_size_bytes,
            ),
        )
        self._conn.commit()
        row = self._conn.execute(
            "SELECT id FROM documents WHERE file_path = ?", (record.file_path,)
        ).fetchone()
        return row[0]

    def get_document_id(self, file_path: str) -> int | None:
        row = self._conn.execute(
            "SELECT id FROM documents WHERE file_path = ?", (file_path,)
        ).fetchone()
        return row[0] if row else None

    def get_all_document_paths(self) -> set[str]:
        rows = self._conn.execute("SELECT file_path FROM documents").fetchall()
        return {r[0] for r in rows}

    def delete_document(self, file_path: str) -> None:
        """Remove a document and all its pages/tables/cells (cascading)."""
        self._conn.execute("DELETE FROM documents WHERE file_path = ?", (file_path,))
        self._conn.commit()

    def list_documents(self) -> list[DocumentSummary]:
        """Return all documents with summary stats."""
        rows = self._conn.execute("""
            SELECT
                d.id, d.file_path, d.title, d.author, d.page_count,
                d.file_size_bytes, d.last_indexed,
                COALESCE(tc.table_count, 0),
                COALESCE(cc.cell_count, 0)
            FROM documents d
            LEFT JOIN (
                SELECT p.document_id, COUNT(t.id) AS table_count
                FROM pages p JOIN tables t ON t.page_id = p.id
                GROUP BY p.document_id
            ) tc ON tc.document_id = d.id
            LEFT JOIN (
                SELECT p.document_id, COUNT(c.id) AS cell_count
                FROM pages p
                JOIN tables t ON t.page_id = p.id
                JOIN table_cells c ON c.table_id = t.id
                GROUP BY p.document_id
            ) cc ON cc.document_id = d.id
            ORDER BY d.file_path
        """).fetchall()
        return [
            DocumentSummary(
                id=r[0], file_path=r[1], title=r[2], author=r[3],
                page_count=r[4], file_size_bytes=r[5], last_indexed=r[6],
                table_count=r[7], cell_count=r[8],
            )
            for r in rows
        ]

    def find_document(self, name: str) -> DocumentSummary | None:
        """Find a document by exact path or partial filename match."""
        # Try exact match first
        row = self._conn.execute(
            "SELECT id, file_path FROM documents WHERE file_path = ?", (name,)
        ).fetchone()
        if not row:
            # Try partial match on filename
            row = self._conn.execute(
                "SELECT id, file_path FROM documents WHERE file_path LIKE ?",
                (f"%{name}%",),
            ).fetchone()
        if not row:
            return None

        docs = self.list_documents()
        return next((d for d in docs if d.id == row[0]), None)

    # ── Page operations ────────────────────────────────────────────────

    def upsert_page(self, record: PageRecord) -> int:
        """Insert or update a page. Returns the page id."""
        word_count = len(record.raw_text.split()) if record.raw_text else 0
        self._conn.execute(
            """INSERT INTO pages (document_id, page_number, raw_text, word_count)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(document_id, page_number) DO UPDATE SET
                   raw_text = excluded.raw_text,
                   word_count = excluded.word_count,
                   is_processed = 1""",
            (record.document_id, record.page_number, record.raw_text, word_count),
        )
        self._conn.commit()
        row = self._conn.execute(
            "SELECT id FROM pages WHERE document_id = ? AND page_number = ?",
            (record.document_id, record.page_number),
        ).fetchone()
        return row[0]

    def clear_tables_for_page(self, page_id: int) -> None:
        """Remove all tables (and their cells) for a page before re-indexing."""
        self._conn.execute("DELETE FROM tables WHERE page_id = ?", (page_id,))
        self._conn.commit()

    def get_page_content(self, document_name: str, page_number: int) -> PageContent | None:
        """Get all content for a specific page."""
        row = self._conn.execute(
            """SELECT p.id, p.raw_text, d.file_path
               FROM pages p
               JOIN documents d ON p.document_id = d.id
               WHERE d.file_path LIKE ? AND p.page_number = ?""",
            (f"%{document_name}%", page_number),
        ).fetchone()
        if not row:
            return None

        page_id, raw_text, doc_path = row
        tables = self._get_tables_for_page(page_id)

        return PageContent(
            document_name=os.path.basename(doc_path),
            page_number=page_number,
            raw_text=raw_text,
            tables=tables,
        )

    # ── Table operations ───────────────────────────────────────────────

    def insert_table(self, table: TableData) -> int:
        """Insert a table record. Returns the table id."""
        headers_json = json.dumps(table.headers, ensure_ascii=False)
        self._conn.execute(
            """INSERT OR REPLACE INTO tables
               (page_id, table_index, row_count, col_count, headers_json,
                bbox_x0, bbox_y0, bbox_x1, bbox_y1, table_type)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                table.page_id,
                table.table_index,
                table.row_count,
                table.col_count,
                headers_json,
                table.bbox[0], table.bbox[1], table.bbox[2], table.bbox[3],
                table.table_type,
            ),
        )
        self._conn.commit()
        row = self._conn.execute(
            "SELECT id FROM tables WHERE page_id = ? AND table_index = ?",
            (table.page_id, table.table_index),
        ).fetchone()
        return row[0]

    def insert_cell(
        self, table_id: int, row_idx: int, col_idx: int, cell: CellData,
    ) -> None:
        """Insert a single table cell."""
        self._conn.execute(
            """INSERT OR REPLACE INTO table_cells
               (table_id, row_idx, col_idx, value, data_type, numeric_value)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (table_id, row_idx, col_idx, cell.value, cell.data_type, cell.numeric_value),
        )

    def insert_cells_batch(
        self, table_id: int, rows: list[list[CellData]],
    ) -> None:
        """Insert all cells for a table in a single transaction."""
        params = []
        for row_idx, row in enumerate(rows):
            for col_idx, cell in enumerate(row):
                params.append((
                    table_id, row_idx, col_idx,
                    cell.value, cell.data_type, cell.numeric_value,
                ))
        self._conn.executemany(
            """INSERT OR REPLACE INTO table_cells
               (table_id, row_idx, col_idx, value, data_type, numeric_value)
               VALUES (?, ?, ?, ?, ?, ?)""",
            params,
        )
        self._conn.commit()

    def get_table_by_id(self, table_id: int) -> TableSearchResult | None:
        """Retrieve a complete table with all its cells reconstructed."""
        row = self._conn.execute(
            """SELECT t.id, t.headers_json, t.row_count, t.col_count,
                      t.table_type, p.page_number, d.file_path
               FROM tables t
               JOIN pages p ON t.page_id = p.id
               JOIN documents d ON p.document_id = d.id
               WHERE t.id = ?""",
            (table_id,),
        ).fetchone()
        if not row:
            return None

        headers = json.loads(row[1])
        rows = self._reconstruct_rows(table_id, row[2], row[3])

        return TableSearchResult(
            table_id=row[0],
            document_name=os.path.basename(row[6]),
            document_path=row[6],
            page_number=row[5],
            headers=headers,
            rows=rows,
            row_count=row[2],
            col_count=row[3],
            table_type=row[4],
        )

    def search_tables(
        self,
        query: str,
        document_name: str | None = None,
        data_type: str | None = None,
        min_value: float | None = None,
        max_value: float | None = None,
        limit: int = 10,
    ) -> list[TableSearchResult]:
        """Search for tables containing cells that match the given criteria."""
        conditions = []
        params: list = []

        if query:
            conditions.append("c.value LIKE ?")
            params.append(f"%{query}%")
        if document_name:
            conditions.append("d.file_path LIKE ?")
            params.append(f"%{document_name}%")
        if data_type:
            conditions.append("c.data_type = ?")
            params.append(data_type)
        if min_value is not None:
            conditions.append("c.numeric_value >= ?")
            params.append(min_value)
        if max_value is not None:
            conditions.append("c.numeric_value <= ?")
            params.append(max_value)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Find distinct tables matching criteria
        sql = f"""
            SELECT DISTINCT t.id, t.headers_json, t.row_count, t.col_count,
                   t.table_type, p.page_number, d.file_path
            FROM table_cells c
            JOIN tables t ON c.table_id = t.id
            JOIN pages p ON t.page_id = p.id
            JOIN documents d ON p.document_id = d.id
            WHERE {where_clause}
            ORDER BY d.file_path, p.page_number, t.table_index
            LIMIT ?
        """
        params.append(limit)

        rows = self._conn.execute(sql, params).fetchall()
        results = []
        for r in rows:
            table_rows = self._reconstruct_rows(r[0], r[2], r[3])
            results.append(TableSearchResult(
                table_id=r[0],
                document_name=os.path.basename(r[6]),
                document_path=r[6],
                page_number=r[5],
                headers=json.loads(r[1]),
                rows=table_rows,
                row_count=r[2],
                col_count=r[3],
                table_type=r[4],
            ))
        return results

    def execute_readonly_sql(self, sql: str) -> list[dict]:
        """Execute a read-only SQL query and return results as dicts."""
        cursor = self._conn.execute(sql)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    # ── Session operations ─────────────────────────────────────────────

    def start_session(self) -> int:
        doc_count = self._conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        cursor = self._conn.execute(
            "INSERT INTO sessions (document_count_at_start) VALUES (?)",
            (doc_count,),
        )
        self._conn.commit()
        return cursor.lastrowid

    def end_session(self, session_id: int) -> None:
        self._conn.execute(
            "UPDATE sessions SET ended_at = CURRENT_TIMESTAMP WHERE id = ?",
            (session_id,),
        )
        self._conn.commit()

    def get_active_session(self) -> int | None:
        row = self._conn.execute(
            "SELECT id FROM sessions WHERE ended_at IS NULL ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return row[0] if row else None

    def get_latest_session_id(self) -> int | None:
        row = self._conn.execute(
            "SELECT id FROM sessions ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return row[0] if row else None

    # ── Observation operations ─────────────────────────────────────────

    def add_observation(
        self,
        session_id: int,
        content: str,
        observation_type: str = "note",
        document_id: int | None = None,
        page_id: int | None = None,
        table_id: int | None = None,
    ) -> int:
        cursor = self._conn.execute(
            """INSERT INTO observations
               (session_id, document_id, page_id, table_id, observation_type, content)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (session_id, document_id, page_id, table_id, observation_type, content),
        )
        self._conn.commit()
        return cursor.lastrowid

    def get_observations(
        self, session_id: int | None = None, include_stale: bool = True,
    ) -> list[dict]:
        query = """
            SELECT o.id, o.session_id, o.document_id, o.page_id, o.table_id,
                   o.observation_type, o.content, o.is_stale, o.created_at,
                   d.file_path
            FROM observations o
            LEFT JOIN documents d ON o.document_id = d.id
        """
        params: list = []
        conditions: list[str] = []

        if session_id is not None:
            conditions.append("o.session_id = ?")
            params.append(session_id)
        if not include_stale:
            conditions.append("o.is_stale = 0")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY o.created_at"

        rows = self._conn.execute(query, params).fetchall()
        return [
            {
                "id": r[0], "session_id": r[1], "document_id": r[2],
                "page_id": r[3], "table_id": r[4], "observation_type": r[5],
                "content": r[6], "is_stale": bool(r[7]), "created_at": r[8],
                "file_path": r[9],
            }
            for r in rows
        ]

    def mark_observations_stale_for_document(self, document_id: int) -> int:
        cursor = self._conn.execute(
            "UPDATE observations SET is_stale = 1 WHERE document_id = ? AND is_stale = 0",
            (document_id,),
        )
        self._conn.commit()
        return cursor.rowcount

    # ── Stats ──────────────────────────────────────────────────────────

    def stats(self) -> dict[str, int]:
        doc_count = self._conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        page_count = self._conn.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
        table_count = self._conn.execute("SELECT COUNT(*) FROM tables").fetchone()[0]
        cell_count = self._conn.execute("SELECT COUNT(*) FROM table_cells").fetchone()[0]
        obs_count = self._conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
        session_count = self._conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        return {
            "documents": doc_count, "pages": page_count,
            "tables": table_count, "cells": cell_count,
            "observations": obs_count, "sessions": session_count,
        }

    # ── Internal helpers ───────────────────────────────────────────────

    def _reconstruct_rows(
        self, table_id: int, row_count: int, col_count: int,
    ) -> list[list[str]]:
        """Reconstruct a 2D table from cell records."""
        cells = self._conn.execute(
            "SELECT row_idx, col_idx, value FROM table_cells "
            "WHERE table_id = ? ORDER BY row_idx, col_idx",
            (table_id,),
        ).fetchall()

        grid: list[list[str]] = [[""] * col_count for _ in range(row_count)]
        for row_idx, col_idx, value in cells:
            if 0 <= row_idx < row_count and 0 <= col_idx < col_count:
                grid[row_idx][col_idx] = value or ""
        return grid

    def _get_tables_for_page(self, page_id: int) -> list[TableData]:
        """Get all tables for a given page."""
        rows = self._conn.execute(
            """SELECT id, table_index, row_count, col_count, headers_json,
                      bbox_x0, bbox_y0, bbox_x1, bbox_y1, table_type
               FROM tables WHERE page_id = ? ORDER BY table_index""",
            (page_id,),
        ).fetchall()

        tables = []
        for r in rows:
            headers = json.loads(r[4])
            reconstructed_rows = self._reconstruct_rows(r[0], r[2], r[3])
            tables.append(TableData(
                page_id=page_id,
                table_index=r[1],
                headers=headers,
                rows=reconstructed_rows,
                bbox=(r[5], r[6], r[7], r[8]),
                table_type=r[9],
                id=r[0],
            ))
        return tables

