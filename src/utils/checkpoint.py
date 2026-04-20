"""
Checkpoint manager — enables resume after interruption.

Saves progress (last crawled page offset, collected record IDs) to a JSON file
so the crawler can pick up where it left off.
"""

import json
import os
from typing import Any

from loguru import logger


class Checkpoint:
    """Simple JSON-file based checkpoint for resume support."""

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.data: dict[str, Any] = {
            "last_start": 0,
            "total_collected": 0,
            "collected_ids": [],
            "failed_pages": [],
            "pdf_downloaded": [],
            "pdf_failed": [],
        }
        self._load()

    def _load(self):
        """Load checkpoint from file if it exists."""
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, "r") as f:
                    saved = json.load(f)
                self.data.update(saved)
                logger.info(
                    f"Resumed from checkpoint: {self.data['total_collected']} records, "
                    f"last_start={self.data['last_start']}"
                )
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Could not load checkpoint: {e}, starting fresh")

    def save(self):
        """Persist checkpoint to disk."""
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
        with open(self.filepath, "w") as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)

    @property
    def last_start(self) -> int:
        return self.data["last_start"]

    @last_start.setter
    def last_start(self, value: int):
        self.data["last_start"] = value

    @property
    def total_collected(self) -> int:
        return self.data["total_collected"]

    @total_collected.setter
    def total_collected(self, value: int):
        self.data["total_collected"] = value

    @property
    def collected_ids(self) -> list[str]:
        return self.data["collected_ids"]

    def add_collected_ids(self, ids: list[str]):
        """Add new record IDs (deduplicated)."""
        existing = set(self.collected_ids)
        new_ids = [rid for rid in ids if rid and rid not in existing]
        self.data["collected_ids"].extend(new_ids)
        self.data["total_collected"] = len(self.data["collected_ids"])

    def add_failed_page(self, start: int):
        if start not in self.data["failed_pages"]:
            self.data["failed_pages"].append(start)

    def mark_pdf_done(self, record_id: str):
        if record_id not in self.data["pdf_downloaded"]:
            self.data["pdf_downloaded"].append(record_id)

    def mark_pdf_failed(self, record_id: str):
        if record_id not in self.data["pdf_failed"]:
            self.data["pdf_failed"].append(record_id)

    def is_pdf_done(self, record_id: str) -> bool:
        return record_id in self.data["pdf_downloaded"]
