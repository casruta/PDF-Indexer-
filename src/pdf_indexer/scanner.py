"""PDF file discovery and content hashing."""

from __future__ import annotations

import fnmatch
import hashlib
import os


def compute_file_hash(filepath: str) -> str:
    """Return a SHA-256 hex digest (first 32 chars) for a file's contents."""
    h = hashlib.sha256()
    try:
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
    except OSError:
        return ""
    return h.hexdigest()[:32]


def find_pdf_files(
    root: str,
    exclude_patterns: list[str] | None = None,
) -> list[str]:
    """Walk a directory tree and return absolute paths to all PDF files.

    Args:
        root: Root directory to scan.
        exclude_patterns: Optional list of fnmatch patterns to exclude.

    Returns:
        Sorted list of absolute paths to PDF files.
    """
    exclude = exclude_patterns or []
    pdf_files: list[str] = []

    for dirpath, dirnames, filenames in os.walk(root):
        # Skip hidden directories and common non-content dirs
        dirnames[:] = sorted(
            d for d in dirnames
            if not d.startswith(".") and d not in {"__pycache__", "node_modules", ".git"}
        )

        for fname in sorted(filenames):
            if not fname.lower().endswith(".pdf"):
                continue

            abs_path = os.path.join(dirpath, fname)
            rel_path = os.path.relpath(abs_path, root)

            # Check exclude patterns
            if any(fnmatch.fnmatch(rel_path, p) or fnmatch.fnmatch(fname, p) for p in exclude):
                continue

            pdf_files.append(abs_path)

    return pdf_files
