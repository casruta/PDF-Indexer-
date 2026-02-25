"""Configuration management for the PDF indexer."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_DB_DIR = ".pdfindex"
DEFAULT_DB_NAME = "index.db"
CONFIG_FILENAME = ".pdfindexconfig"


@dataclass
class PDFIndexConfig:
    """Configuration for the PDF indexer."""

    root_path: str = "."
    db_path: str = ""
    exclude_patterns: list[str] = field(default_factory=list)
    min_table_rows: int = 2
    min_table_cols: int = 2
    verbose: bool = False

    def __post_init__(self) -> None:
        if not self.db_path:
            self.db_path = os.path.join(
                self.root_path, DEFAULT_DB_DIR, DEFAULT_DB_NAME
            )

    @classmethod
    def from_file(cls, path: str) -> PDFIndexConfig:
        """Load configuration from a JSON file."""
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    @classmethod
    def resolve(
        cls,
        root_path: str = ".",
        db_path: str | None = None,
        exclude: str | None = None,
        min_table_rows: int | None = None,
        min_table_cols: int | None = None,
        verbose: bool = False,
    ) -> PDFIndexConfig:
        """Build config with priority: CLI args > config file > defaults."""
        config_file = os.path.join(root_path, CONFIG_FILENAME)
        if os.path.isfile(config_file):
            base = cls.from_file(config_file)
            base.root_path = root_path
        else:
            base = cls(root_path=root_path)

        if db_path is not None:
            base.db_path = db_path
        if exclude is not None:
            base.exclude_patterns = [p.strip() for p in exclude.split(",") if p.strip()]
        if min_table_rows is not None:
            base.min_table_rows = min_table_rows
        if min_table_cols is not None:
            base.min_table_cols = min_table_cols
        if verbose:
            base.verbose = True

        return base
