"""Session lifecycle management for research workflows."""

from __future__ import annotations

import os
from pathlib import Path

from pdf_indexer.database import PDFDatabase

SESSION_FILE = "current_session.txt"


class SessionManager:
    """Manage research sessions with observation tracking.

    Sessions let users group their work into periods and attach
    observations (notes) to specific documents, pages, or tables.
    When a PDF is re-indexed, any observations linked to it are
    marked as stale.
    """

    def __init__(self, db: PDFDatabase, index_dir: str) -> None:
        self._db = db
        self._index_dir = index_dir
        self._session_file = os.path.join(index_dir, SESSION_FILE)

    def start(self) -> int:
        """Start a new session, ending any active one first.

        Returns:
            The new session ID.
        """
        active = self._db.get_active_session()
        if active is not None:
            self._db.end_session(active)

        session_id = self._db.start_session()
        self._write_session_file(session_id)
        return session_id

    def end(self) -> int | None:
        """End the active session.

        Returns:
            The ended session ID, or None if no session was active.
        """
        active = self._get_active_id()
        if active is None:
            return None
        self._db.end_session(active)
        self._clear_session_file()
        return active

    def get_active_id(self) -> int | None:
        """Return the active session ID, or None."""
        return self._get_active_id()

    def observe(
        self,
        content: str,
        document_id: int | None = None,
        page_id: int | None = None,
        table_id: int | None = None,
    ) -> int | None:
        """Add an observation to the active session.

        Args:
            content: The observation text.
            document_id: Optional document to link to.
            page_id: Optional page to link to.
            table_id: Optional table to link to.

        Returns:
            The observation ID, or None if no active session.
        """
        session_id = self._get_active_id()
        if session_id is None:
            return None

        return self._db.add_observation(
            session_id=session_id,
            content=content,
            document_id=document_id,
            page_id=page_id,
            table_id=table_id,
        )

    def get_observations(self, session_id: int | None = None) -> list[dict]:
        """Get observations, optionally filtered by session."""
        sid = session_id or self._get_active_id() or self._db.get_latest_session_id()
        return self._db.get_observations(session_id=sid)

    def get_stale_observations(self) -> list[dict]:
        """Get all observations that have been invalidated by re-indexing."""
        return self._db.get_observations(include_stale=True)

    # ── Internal helpers ───────────────────────────────────────────────

    def _get_active_id(self) -> int | None:
        """Read the active session ID from file or database."""
        if os.path.isfile(self._session_file):
            try:
                text = Path(self._session_file).read_text().strip()
                if text.isdigit():
                    sid = int(text)
                    # Verify it's still active in the database
                    if self._db.get_active_session() == sid:
                        return sid
            except OSError:
                pass
        return self._db.get_active_session()

    def _write_session_file(self, session_id: int) -> None:
        Path(self._index_dir).mkdir(parents=True, exist_ok=True)
        Path(self._session_file).write_text(str(session_id))

    def _clear_session_file(self) -> None:
        try:
            os.remove(self._session_file)
        except OSError:
            pass
