"""
Storage writer — appends thesis metadata to JSONL files.
"""

import csv
import json
import os
from typing import Any

from loguru import logger


class MetadataWriter:
    """Writes thesis records to JSONL (one JSON object per line)."""

    def __init__(self, output_dir: str, filename: str = "theses.jsonl"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.filepath = os.path.join(output_dir, filename)
        # Count existing records
        self._count = 0
        if os.path.exists(self.filepath):
            with open(self.filepath, "r", encoding="utf-8") as f:
                self._count = sum(1 for _ in f)
            logger.info(f"Existing metadata file has {self._count} records")

    @property
    def count(self) -> int:
        return self._count

    def write_batch(self, records: list[dict[str, Any]]):
        """Append a batch of records to the JSONL file."""
        if not records:
            return
        with open(self.filepath, "a", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        self._count += len(records)
        logger.debug(f"Wrote {len(records)} records (total: {self._count})")

    def export_csv(self, csv_path: str | None = None):
        """Export JSONL data to CSV for convenience."""
        if not os.path.exists(self.filepath):
            logger.warning("No metadata file to export")
            return

        csv_path = csv_path or os.path.join(self.output_dir, "theses.csv")
        records = []
        with open(self.filepath, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    records.append(json.loads(line))

        if not records:
            return

        fieldnames = list(records[0].keys())
        seen = set(fieldnames)
        for record in records[1:]:
            for key in record.keys():
                if key not in seen:
                    fieldnames.append(key)
                    seen.add(key)

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)

        logger.info(f"Exported {len(records)} records to {csv_path}")
